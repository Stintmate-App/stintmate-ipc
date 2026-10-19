"""Client checks: serve a test endpoint and check what a client does (README, SPEC.md 17).

The test endpoint declares::

    methods:  check.echo (v1: returns its params), check.fail (v1: error check_failure)
    channels: check.events (event, ~10/s), check.state (state, changes ~5/s),
              check.stream (stream, 100 Hz, layout [t_ms u32, x f32])
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Any

from .. import _win32, naming, wire
from ..decl import STREAM, Declarations
from ..errors import PipeError, ProtocolError
from ..layout import Layout
from .raw import Closed, RawConn
from .report import FAIL, INFO, PASS, SKIP, WARN, Report

STREAM_LAYOUT = Layout([("t_ms", "u32"), ("x", "f32")], name="check_sample")
DECL = {
    "methods": {"check.echo": {"v": [1], "doc": "returns its params"},
                "check.fail": {"v": [1], "doc": "always fails with check_failure"}},
    "channels": {"check.events": {"kind": "event", "v": [1]},
                 "check.state": {"kind": "state", "v": [1]},
                 "check.stream": {"kind": "stream", "v": [1], "rate_hz": 100,
                                  "layout": STREAM_LAYOUT.to_wire()}},
}
PEER = {"app": "ipc-check", "name": "stintmate-ipc-check (test endpoint)", "version": "0.1.0"}


class ClientChecker:
    def __init__(self, pipe: str, key: bytes, *, timeout: float = 30.0, serve_s: float = 5.0,
                 reconnect_s: float = 10.0, listen_s: float = 2.0) -> None:
        self.path, short = naming.split_pipe(pipe)
        self.pipe = pipe
        self.key = key
        self.timeout = timeout
        self.serve_s = serve_s
        self.reconnect_s = reconnect_s
        self.listen_s = listen_s
        self.report = Report("client", self.path)
        self._accepted: queue.Queue[_win32.PipeHandle] = queue.Queue()
        self.short = naming.ascii_lower(short)

    def _ok(self, cid: str, name: str, spec: str, detail: str = "") -> None:
        self.report.add(cid, name, PASS, detail, spec)

    def _fail(self, cid: str, name: str, spec: str, detail: str) -> None:
        self.report.add(cid, name, FAIL, detail, spec)

    def run(self) -> Report:
        listener = _win32.Listener(self.path, self._accepted.put).start()
        self.report.facts["endpoint"] = self.path
        try:
            try:
                h = self._accepted.get(timeout=self.timeout)
            except queue.Empty:
                self._fail("C01", "the client connects", "3.3",
                           f"no connection within {self.timeout:g} s")
                return self.report
            self._ok("C01", "the client connects", "3.3", f"pid {h.peer_pid}")
            self.report.facts["client pid (from the OS)"] = h.peer_pid
            conn = RawConn(h, self.short)
            try:
                if self._session(conn):
                    self._reconnect()
            finally:
                conn.close()
        finally:
            listener.stop()
        return self.report

    # ---------------------------------------------------------------- session 1

    def _session(self, c: RawConn) -> bool:
        try:
            auth = c.server_handshake(self.key, PEER, DECL, heartbeat_ms=5000)
        except PermissionError:
            self._fail("C02", "auth: version, nonce, MAC, identity", "8.3, 8.5",
                       "wrong MAC (does the client read the same key file?)")
            return False
        except (TimeoutError, Closed, ProtocolError, PipeError) as exc:
            self._fail("C02", "auth: version, nonce, MAC, identity", "8.3", str(exc))
            return False
        problems = []
        if auth.get("v") != 2:
            problems.append(f"v {auth.get('v')!r}")
        peer = auth.get("peer")
        if not isinstance(peer, dict) or not isinstance(peer.get("app"), str) \
                or not naming.APP_RE.match(peer["app"]):
            problems.append(f"peer.app {peer!r} invalid")
        mf = auth.get("max_frame")
        if not isinstance(mf, int) or mf < wire.MIN_MAX_FRAME:
            problems.append(f"max_frame {mf!r}")
        try:
            self.client_decl = Declarations.from_wire(auth.get("decl"))
        except ValueError as exc:
            problems.append(f"decl: {exc}")
            self.client_decl = Declarations()
        if problems:
            self._fail("C02", "auth: version, nonce, MAC, identity", "8.3", "; ".join(problems))
            return False
        self._ok("C02", "auth: version, nonce, MAC, identity", "8.3, 8.5",
                 f"app {peer['app']!r}, {len(self.client_decl.methods)} methods, "
                 f"{len(self.client_decl.channels)} channels")
        self.report.facts["client"] = peer
        self.state = _Served(c, self.report)
        self._check_ping(c)
        self._check_unknown(c)
        self._check_unknown_method(c)
        self._check_chunked(c)
        for name, ch in sorted(self.client_decl.channels.items()):
            self._check_client_channel(c, name, ch)
        self._serve(c)
        c.send({"t": "bye", "code": "shutting_down", "msg": "check: reconnect test"})
        time.sleep(0.2)
        return True

    def _pump(self, c: RawConn, until: float, want: Any = None) -> Any:
        """Serve the client until ``until``; return the first message ``want(m)`` accepts."""
        while True:
            left = until - time.monotonic()
            if left <= 0:
                return None
            self.state.tick()
            try:
                kind, m = c.recv(min(left, 0.01))
            except TimeoutError:
                continue
            if kind == "stream":
                self.state.on_stream(m)
                continue
            if want is not None and want(m):
                return m
            self.state.on_message(m)

    def _check_ping(self, c: RawConn) -> None:
        ts = time.perf_counter_ns()
        c.send({"t": "ping", "ts": ts})
        m = self._pump(c, time.monotonic() + 3, lambda m: m.get("t") == "pong")
        if m is None:
            self._fail("C03", "ping is answered", "10.10", "no pong within 3 s")
        elif m.get("ts") != ts:
            self._fail("C03", "ping is answered", "10.10", "pong.ts differs")
        else:
            self._ok("C03", "ping is answered", "10.10")

    def _check_unknown(self, c: RawConn) -> None:
        c.send({"t": "x_check_unknown", "zz": 1})
        c.send({"t": "ping", "ts": 99, "zz_unknown": [1]})
        m = self._pump(c, time.monotonic() + 3, lambda m: m.get("t") in ("pong", "bye"))
        if m is None or m.get("t") != "pong":
            self._fail("C04", "unknown message types and keys are ignored", "6.3, 10.1",
                       f"got {m!r}")
        else:
            self._ok("C04", "unknown message types and keys are ignored", "6.3, 10.1")

    def _check_unknown_method(self, c: RawConn) -> None:
        rid = 7_000_001
        c.send({"t": "req", "id": rid, "m": "ipc_check.no_such_method", "v": 1, "p": {}})
        m = self._pump(c, time.monotonic() + 3,
                       lambda m: m.get("t") == "res" and m.get("id") == rid)
        code = ((m or {}).get("e") or {}).get("code")
        if m is None:
            self._fail("C05", "unknown method -> unknown_method", "10.2", "no res in 3 s")
        elif m.get("ok") is not False or code != "unknown_method":
            self._fail("C05", "unknown method -> unknown_method", "10.2",
                       f"ok={m.get('ok')!r} code={code!r}")
        else:
            self._ok("C05", "unknown method -> unknown_method", "10.2")

    def _check_chunked(self, c: RawConn) -> None:
        c.send_chunked({"t": "ping", "ts": 4242, "pad": "x" * 100}, 16)
        m = self._pump(c, time.monotonic() + 3, lambda m: m.get("t") == "pong")
        if m is None or m.get("ts") != 4242:
            self._fail("C06", "a chunked message is reassembled", "5.4", "no pong")
        else:
            self._ok("C06", "a chunked message is reassembled", "5.4")

    def _check_client_channel(self, c: RawConn, name: str, ch: Any) -> None:
        cid = f"C07.{name}"
        title = f"the client's {ch.kind} channel {name}"
        rid = 7_100_000 + len(self.report.results)
        c.send({"t": "sub", "id": rid, "ch": name, "v": max(ch.versions)})
        res = self._pump(c, time.monotonic() + 3,
                         lambda m: m.get("t") == "res" and m.get("id") == rid)
        if res is None:
            self._fail(cid, title, "10.6", "no res within 3 s")
            return
        if res.get("ok") is not True:
            self.report.add(cid, title, WARN, f"refused: {res.get('e')}", "10.6")
            return
        r = res.get("r") or {}
        sid = r.get("sid")
        if not isinstance(sid, int) or r.get("kind") != ch.kind:
            self._fail(cid, title, "10.6", f"bad res.r {r!r}")
            return
        if ch.kind == STREAM:
            try:
                layout = Layout.from_wire(r.get("layout"))
            except (ValueError, TypeError) as exc:
                self._fail(cid, title, "9.3", f"layout: {exc}")
                return
            self.state.watch_stream(sid, layout)
        else:
            self.state.watch_events(sid)
        self._pump(c, time.monotonic() + self.listen_s)
        c.send({"t": "unsub", "sid": sid})
        err = self.state.stream_errors.get(sid) or self.state.event_errors.get(sid)
        n = self.state.counts.get(sid, 0)
        if err:
            self._fail(cid, title, "10.8, 10.9", err)
        else:
            self._ok(cid, title, "10.6-10.9", f"sid {sid}, {n} items in {self.listen_s:g} s")

    def _serve(self, c: RawConn) -> None:
        self._pump(c, time.monotonic() + self.serve_s)
        s = self.state
        self.report.facts["client requests"] = dict(s.calls)
        self.report.facts["client subscriptions"] = sorted(s.subs_seen)
        if s.errors:
            self._fail("C08", "the client's messages are well formed", "10", "; ".join(
                s.errors[:5]))
        else:
            self._ok("C08", "the client's messages are well formed", "10",
                     f"{sum(s.calls.values())} requests, {len(s.subs_seen)} subscriptions")
        if not s.calls and not s.subs_seen:
            self.report.add("C09", "the client uses the test endpoint", INFO,
                            "it called nothing and subscribed to nothing", "")
        else:
            self._ok("C09", "the client uses the test endpoint", "",
                     f"calls {dict(s.calls)}, subscriptions {sorted(s.subs_seen)}")

    # ---------------------------------------------------------------- session 2

    def _reconnect(self) -> None:
        try:
            h = self._accepted.get(timeout=self.reconnect_s)
        except queue.Empty:
            self.report.add("C10", "the client reconnects after bye shutting_down", WARN,
                            f"no reconnection within {self.reconnect_s:g} s", "15.3")
            self.report.add("C11", "a wrong server MAC makes the client close", SKIP,
                            "needs a reconnection", "8.6")
            return
        self._ok("C10", "the client reconnects after bye shutting_down", "15.3")
        c = RawConn(h, self.short)
        try:
            try:
                c.server_handshake(self.key, PEER, DECL, bad_mac=True)
            except (PermissionError, TimeoutError, Closed, ProtocolError, PipeError) as exc:
                self._fail("C11", "a wrong server MAC makes the client close", "8.6",
                           f"handshake: {exc}")
                return
            t0 = time.monotonic()
            seen = c.wait_closed(3.0)
            dt = time.monotonic() - t0
            other = [m.get("t") for m in seen if m.get("t") != "bye"]
            if dt >= 2.9:
                self._fail("C11", "a wrong server MAC makes the client close", "8.6",
                           "still connected 3 s after a wrong welcome MAC")
            elif other:
                self._fail("C11", "a wrong server MAC makes the client close", "8.6",
                           f"sent {other} after a wrong MAC")
            else:
                self._ok("C11", "a wrong server MAC makes the client close", "8.6",
                         f"closed after {1000 * dt:.0f} ms")
        finally:
            c.close()


class _Served:
    """The test endpoint's behaviour during a session: answers, publishes, validates."""

    def __init__(self, c: RawConn, report: Report) -> None:
        self.c = c
        self.report = report
        self.calls: dict[str, int] = {}
        self.subs_seen: set[str] = set()
        self.errors: list[str] = []
        self.my_subs: dict[int, str] = {}  # sid -> channel (the client subscribed)
        self.next_sid = 1
        self.seq: dict[int, int] = {}
        self.state_n = 0
        self.t0 = time.monotonic()
        self.next_evt = self.next_state = self.next_sample = self.t0
        self.stream_watch: dict[int, Layout] = {}
        self.event_watch: set[int] = set()
        self.stream_errors: dict[int, str] = {}
        self.event_errors: dict[int, str] = {}
        self.counts: dict[int, int] = {}
        self.last: dict[int, tuple[int, int]] = {}
        self.lock = threading.Lock()

    def watch_stream(self, sid: int, layout: Layout) -> None:
        self.stream_watch[sid] = layout

    def watch_events(self, sid: int) -> None:
        self.event_watch.add(sid)

    def on_stream(self, m: tuple[int, int, int, int, bytes]) -> None:
        sid, seq, t_ns, _flags, body = m
        lay = self.stream_watch.get(sid)
        if lay is None:
            return
        self.counts[sid] = self.counts.get(sid, 0) + 1
        if len(body) != lay.size:
            self.stream_errors[sid] = f"sample of {len(body)} bytes, layout {lay.size}"
        prev = self.last.get(sid)
        if prev is not None:
            if ((seq - prev[0]) & 0xFFFFFFFF) < 1:
                self.stream_errors[sid] = f"seq not increasing {prev[0]} -> {seq}"
            if t_ns < prev[1]:
                self.stream_errors[sid] = "t_ns went backwards"
        self.last[sid] = (seq, t_ns)

    def _send(self, m: dict[str, Any]) -> None:
        try:
            self.c.send(m)
        except PipeError:
            pass

    def on_message(self, m: dict[str, Any]) -> None:
        t = m.get("t")
        if t == "ping":
            self._send({"t": "pong", **({"ts": m["ts"]} if "ts" in m else {})})
        elif t == "evt":
            sid = m.get("sid")
            if sid in self.event_watch:
                self.counts[sid] = self.counts.get(sid, 0) + 1
                prev = self.last.get(sid)
                if prev is not None and m.get("seq", 0) <= prev[0]:
                    self.event_errors[sid] = "seq not increasing"
                self.last[sid] = (m.get("seq", 0), 0)
        elif t == "req":
            rid, meth, v = m.get("id"), m.get("m"), m.get("v", 1)
            if not isinstance(rid, int) or rid < 0 or not isinstance(meth, str) \
                    or not isinstance(v, int):
                self.errors.append(f"malformed req {m!r}"[:200])
                return
            self.calls[meth] = self.calls.get(meth, 0) + 1
            if meth == "check.echo" and v == 1:
                self._send({"t": "res", "id": rid, "ok": True, "r": m.get("p")})
            elif meth == "check.fail" and v == 1:
                self._send({"t": "res", "id": rid, "ok": False,
                            "e": {"code": "check_failure", "msg": "as requested"}})
            elif meth in ("check.echo", "check.fail"):
                self._send({"t": "res", "id": rid, "ok": False,
                            "e": {"code": "version_unsupported", "msg": "v1 only",
                                  "details": {"versions": [1]}}})
            else:
                self._send({"t": "res", "id": rid, "ok": False,
                            "e": {"code": "unknown_method", "msg": f"unknown method {meth}"}})
        elif t == "sub":
            rid, ch, v = m.get("id"), m.get("ch"), m.get("v", 1)
            if not isinstance(rid, int) or not isinstance(ch, str) or not isinstance(v, int):
                self.errors.append(f"malformed sub {m!r}"[:200])
                return
            self.subs_seen.add(ch)
            decl = DECL["channels"].get(ch)
            if decl is None:
                self._send({"t": "res", "id": rid, "ok": False,
                            "e": {"code": "unknown_channel", "msg": f"unknown channel {ch}"}})
                return
            if v != 1:
                self._send({"t": "res", "id": rid, "ok": False,
                            "e": {"code": "version_unsupported", "msg": "v1 only"}})
                return
            for sid, name in self.my_subs.items():
                if name == ch:
                    break
            else:
                sid = self.next_sid
                self.next_sid += 1
                self.my_subs[sid] = ch
                self.seq[sid] = 0
            r: dict[str, Any] = {"sid": sid, "kind": decl["kind"], "seq": self.seq[sid]}
            if decl["kind"] == "state":
                r |= {"snap": {"n": self.state_n}, "ts": time.perf_counter_ns()}
            if decl["kind"] == "stream":
                r["layout"] = STREAM_LAYOUT.to_wire()
            self._send({"t": "res", "id": rid, "ok": True, "r": r})
        elif t == "unsub":
            sid = m.get("sid")
            if not isinstance(sid, int):
                self.errors.append(f"malformed unsub {m!r}"[:200])
            self.my_subs.pop(sid, None)
        elif t in ("res", "pong", "decl", "bye", "end", "prog", "cancel"):
            pass

    def tick(self) -> None:
        now = time.monotonic()
        if now >= self.next_evt:
            self.next_evt = now + 0.1
            for sid, ch in list(self.my_subs.items()):
                if ch == "check.events":
                    self.seq[sid] += 1
                    self._send({"t": "evt", "sid": sid, "seq": self.seq[sid],
                                "ts": time.perf_counter_ns(), "d": {"n": self.seq[sid]}})
        if now >= self.next_state:
            self.next_state = now + 0.2
            self.state_n += 1
            for sid, ch in list(self.my_subs.items()):
                if ch == "check.state":
                    self.seq[sid] += 1
                    self._send({"t": "evt", "sid": sid, "seq": self.seq[sid],
                                "ts": time.perf_counter_ns(), "d": {"n": self.state_n}})
        if now >= self.next_sample:
            self.next_sample = now + 0.01
            for sid, ch in list(self.my_subs.items()):
                if ch == "check.stream":
                    self.seq[sid] += 1
                    body = STREAM_LAYOUT.pack(int((now - self.t0) * 1000), 1.5)
                    try:
                        self.c.send_frame(wire.stream_frame(sid, self.seq[sid],
                                                            time.perf_counter_ns(), 0, body))
                    except PipeError:
                        pass
