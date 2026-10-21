"""Server checks: connect to an endpoint and test what it does (README, SPEC.md 17)."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from .. import wire
from ..decl import STATE, STREAM, Declarations
from ..errors import PipeError, ProtocolError
from ..layout import Layout
from .raw import Closed, RawConn
from .report import FAIL, INFO, PASS, SKIP, WARN, Report

UNKNOWN_METHOD = "ipc_check.no_such_method"
UNKNOWN_CHANNEL = "ipc_check.no_such_channel"


class _Fail(Exception):
    pass


def _expect(cond: bool, message: str) -> None:
    if not cond:
        raise _Fail(message)


class ServerChecker:
    def __init__(self, pipe: str, key: bytes, *, listen_s: float = 2.0, slow: bool = True,
                 channels: list[str] | None = None,
                 calls: list[tuple[str, Any]] | None = None) -> None:
        self.pipe = pipe
        self.key = key
        self.listen_s = listen_s
        self.slow = slow
        self.only_channels = channels
        self.calls = calls or []
        self.report = Report("server", pipe)
        self.decl: Declarations | None = None
        self._next_id = 1

    # ---------------------------------------------------------------- helpers

    def _id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _open(self) -> RawConn:
        return RawConn.open(self.pipe)

    def _session(self) -> RawConn:
        c = self._open()
        reply = c.client_handshake(self.key)
        _expect(reply.get("t") == "welcome", f"no welcome: {reply}")
        return c

    def _run(self, cid: str, name: str, spec: str, fn: Callable[[], str | None]) -> None:
        try:
            detail = fn() or ""
            if detail.startswith("SKIP:"):
                self.report.add(cid, name, SKIP, detail[5:].strip(), spec)
            elif detail.startswith("WARN:"):
                self.report.add(cid, name, WARN, detail[5:].strip(), spec)
            elif detail.startswith("INFO:"):
                self.report.add(cid, name, INFO, detail[5:].strip(), spec)
            else:
                self.report.add(cid, name, PASS, detail, spec)
        except _Fail as exc:
            self.report.add(cid, name, FAIL, str(exc), spec)
        except (TimeoutError, Closed, PipeError, ProtocolError, OSError) as exc:
            self.report.add(cid, name, FAIL, f"{type(exc).__name__}: {exc}", spec)

    def _req(self, c: RawConn, method: str, params: Any = None, v: int = 1,
             chunked: bool = False, rid: int | None = None) -> dict[str, Any]:
        rid = self._id() if rid is None else rid
        msg = {"t": "req", "id": rid, "m": method, "v": v, "p": params or {}, "dl": 5000}
        if chunked:
            c.send_chunked(msg, 16)
        else:
            c.send(msg)
        while True:
            m = c.recv_msg(skip=("ping", "pong", "decl", "evt", "end"))
            if m.get("t") == "res" and m.get("id") == rid:
                return m

    def _closes_with(self, c: RawConn, code: str | None, timeout: float = 5.0) -> str:
        seen = c.wait_closed(timeout)
        byes = [m for m in seen if m.get("t") == "bye"]
        if code is not None and byes and byes[-1].get("code") != code:
            raise _Fail(f"closed with bye {byes[-1].get('code')!r}, expected {code!r}")
        return f"closed with bye {byes[-1].get('code')}" if byes else "closed (no bye)"

    # ---------------------------------------------------------------- checks

    def run(self) -> Report:
        r = self._run
        r("S01", "hello: first frame, fields", "8.2", self.c_hello)
        r("S02", "handshake: welcome, server MAC, declarations", "8.4, 8.5, 9", self.c_welcome)
        if self.decl is None:
            self.report.add("S00", "remaining checks", SKIP, "no successful handshake")
            return self.report
        r("S03", "wrong client MAC is refused (bye unauthorized)", "8.6", self.c_bad_mac)
        r("S04", "unsupported protocol version is refused", "8.7", self.c_bad_version)
        r("S05", "malformed auth is refused", "8.6", self.c_bad_auth)
        if self.slow:
            r("S06", "handshake deadline: silent client is closed", "15.1", self.c_silent)
        r("S07", "ping is answered with the same ts", "10.10", self.c_ping)
        r("S08", "unknown message type and unknown keys are ignored", "6.3, 10.1",
          self.c_unknown)
        r("S09", "unknown method -> unknown_method", "10.2, 11", self.c_unknown_method)
        r("S10", "declared method, unsupported version -> version_unsupported", "7.2",
          self.c_method_version)
        r("S11", "unknown channel -> unknown_channel", "10.6, 11", self.c_unknown_channel)
        r("S12", "chunked request is reassembled", "5.4", self.c_chunked)
        r("S13", "large request id is echoed exactly", "10.2", self.c_big_id)
        r("S14", "reserved frame kind 0x80 is skipped", "5.2", self.c_reserved_kind)
        r("S15", "unknown frame kind -> protocol_error", "5.5", self.c_bad_kind)
        r("S16", "frame over max_frame -> closed", "5.5", self.c_oversize)
        r("S17", "message that is not a map -> protocol_error", "6.2", self.c_not_map)
        r("S18", "message frame inside a chunked transfer -> protocol_error", "5.4",
          self.c_chunk_interleave)
        for name, ch in self._channels():
            r(f"S19.{name}", f"subscription to {ch.kind} channel {name}", "10.6-10.9, 12",
              lambda n=name, d=ch: self.c_channel(n, d))
        r("S20", "the same subscription twice -> the same sid", "10.6", self.c_resub)
        for i, (m, p) in enumerate(self.calls):
            r(f"S21.{i + 1}", f"call {m}", "10.2", lambda m=m, p=p: self.c_call(m, p))
        if self.slow:
            r("S22", "idle connection gets a ping within heartbeat_ms", "15.2", self.c_heartbeat)
        r("S24", "cancel for an unknown id is ignored", "10.3", self.c_cancel_unknown)
        r("S23", "bye closes the connection", "10.12", self.c_bye)
        return self.report

    def _channels(self) -> list[tuple[str, Any]]:
        assert self.decl is not None
        names = sorted(self.decl.channels)
        if self.only_channels is not None:
            names = [n for n in names if n in self.only_channels]
        return [(n, self.decl.channels[n]) for n in names]

    def c_hello(self) -> str:
        c = self._open()
        try:
            t0 = time.monotonic()
            kind, m = c.recv(5.0)
            _expect(kind == "msg", "the first frame is not a message (kind 1)")
            _expect(m.get("t") == "hello", f"first message is {m.get('t')!r}, not hello")
            _expect(m.get("proto") == "stintmate-ipc", f"proto is {m.get('proto')!r}")
            vs = m.get("versions")
            _expect(isinstance(vs, list) and 2 in vs, f"versions {vs!r} lacks 2")
            n = m.get("nonce")
            _expect(isinstance(n, bytes) and len(n) == 32, "nonce is not a 32-byte bin")
            mf = m.get("max_frame")
            _expect(isinstance(mf, int) and mf >= wire.MIN_MAX_FRAME,
                    f"max_frame {mf!r} < {wire.MIN_MAX_FRAME}")
            self.report.facts["server max_frame"] = mf
            self.report.facts["server max_message"] = m.get("max_message")
            return f"hello after {1000 * (time.monotonic() - t0):.0f} ms, versions {vs}"
        finally:
            c.close()

    def c_welcome(self) -> str:
        c = self._open()
        try:
            reply = c.client_handshake(self.key)
            _expect(reply.get("t") == "welcome",
                    f"got {reply.get('t')} {reply.get('code', '')}: wrong key file?")
            _expect(c.server_mac_ok, "the server's MAC is wrong")
            _expect(reply.get("v") == 2, f"welcome.v is {reply.get('v')!r}")
            _expect(isinstance(reply.get("run"), str) and reply["run"], "no run id")
            peer = reply.get("peer")
            _expect(isinstance(peer, dict) and isinstance(peer.get("app"), str),
                    "welcome.peer.app missing")
            hb = reply.get("heartbeat_ms")
            _expect(isinstance(hb, int) and hb >= 100, f"heartbeat_ms {hb!r}")
            try:
                self.decl = Declarations.from_wire(reply.get("decl"))
            except ValueError as exc:
                raise _Fail(f"declarations invalid: {exc}") from None
            self.heartbeat_ms = hb
            self.report.facts["server"] = peer
            self.report.facts["server pid (from the OS)"] = c.pipe.peer_pid
            self.report.facts["methods"] = {n: m.versions for n, m in self.decl.methods.items()}
            self.report.facts["channels"] = {n: (ch.kind, ch.versions)
                                             for n, ch in self.decl.channels.items()}
            return (f"{len(self.decl.methods)} methods, {len(self.decl.channels)} channels, "
                    f"heartbeat {hb} ms")
        finally:
            c.close()

    def c_bad_mac(self) -> str:
        c = self._open()
        try:
            reply = c.client_handshake(self.key, bad_mac=True)
            _expect(reply.get("t") == "bye", f"got {reply.get('t')!r} instead of bye")
            _expect(reply.get("code") == "unauthorized", f"bye code {reply.get('code')!r}")
            c.wait_closed(3.0)
            return "bye unauthorized, closed"
        except Closed:
            return "WARN: closed without bye unauthorized"
        finally:
            c.close()

    def c_bad_version(self) -> str:
        c = self._open()
        try:
            reply = c.client_handshake(self.key, version=99)
            _expect(reply.get("t") != "welcome", "welcomed protocol version 99")
            _expect(reply.get("code") == "version_unsupported",
                    f"bye code {reply.get('code')!r}, expected version_unsupported")
            return "bye version_unsupported"
        except Closed:
            return "WARN: closed without bye version_unsupported"
        finally:
            c.close()

    def c_bad_auth(self) -> str:
        c = self._open()
        try:
            c.recv_msg(skip=())
            c.send({"t": "auth", "v": 2, "nonce": b"short", "mac": b"x"})
            seen = c.wait_closed(5.0)
            _expect(not any(m.get("t") == "welcome" for m in seen), "welcomed a bad auth")
            byes = [m.get("code") for m in seen if m.get("t") == "bye"]
            return f"refused ({byes[0] if byes else 'closed'})"
        finally:
            c.close()

    def c_silent(self) -> str:
        c = self._open()
        try:
            c.recv_msg(skip=())
            t0 = time.monotonic()
            c.wait_closed(9.0)
            dt = time.monotonic() - t0
            _expect(dt < 8.5, "still open 8.5 s after hello without auth")
            return f"closed after {dt:.1f} s"
        finally:
            c.close()

    def c_ping(self) -> str:
        c = self._session()
        try:
            ts = time.perf_counter_ns()
            c.send({"t": "ping", "ts": ts})
            m = c.recv_msg(skip=("ping", "decl"))
            _expect(m.get("t") == "pong", f"got {m.get('t')!r}")
            _expect(m.get("ts") == ts, "pong.ts differs from ping.ts")
            return f"rtt {(time.perf_counter_ns() - ts) / 1000:.0f} us"
        finally:
            c.close()

    def c_unknown(self) -> str:
        c = self._session()
        try:
            c.send({"t": "x_check_unknown", "zz": 1})
            ts = 12345
            c.send({"t": "ping", "ts": ts, "zz_unknown": {"a": [1, 2]}})
            m = c.recv_msg(skip=("ping", "decl"))
            _expect(m.get("t") == "pong" and m.get("ts") == ts,
                    f"expected pong, got {m.get('t')!r} {m.get('code', '')}")
            return ""
        finally:
            c.close()

    def c_cancel_unknown(self) -> str:
        c = self._session()
        try:
            c.send({"t": "cancel", "id": 424242})
            ts = 54321
            c.send({"t": "ping", "ts": ts})
            m = c.recv_msg(skip=("ping", "decl"))
            _expect(m.get("t") == "pong" and m.get("ts") == ts,
                    f"expected pong, got {m.get('t')!r} {m.get('code', '')}")
            return ""
        finally:
            c.close()

    def c_unknown_method(self) -> str:
        c = self._session()
        try:
            m = self._req(c, UNKNOWN_METHOD)
            _expect(m.get("ok") is False, "ok is not false")
            e = m.get("e") or {}
            _expect(e.get("code") == "unknown_method", f"code {e.get('code')!r}")
            _expect(isinstance(e.get("msg"), str), "e.msg is not a string")
            return ""
        finally:
            c.close()

    def c_method_version(self) -> str:
        assert self.decl is not None
        if not self.decl.methods:
            return "SKIP: no methods declared"
        name = sorted(self.decl.methods)[0]
        c = self._session()
        try:
            m = self._req(c, name, v=65535)
            e = m.get("e") or {}
            _expect(m.get("ok") is False and e.get("code") == "version_unsupported",
                    f"{name} v65535: ok={m.get('ok')!r} code={e.get('code')!r}")
            return f"{name} v65535 refused"
        finally:
            c.close()

    def c_unknown_channel(self) -> str:
        c = self._session()
        try:
            rid = self._id()
            c.send({"t": "sub", "id": rid, "ch": UNKNOWN_CHANNEL, "v": 1})
            m = c.recv_msg(skip=("ping", "pong", "decl", "evt"))
            _expect(m.get("t") == "res" and m.get("id") == rid, f"got {m}")
            e = m.get("e") or {}
            _expect(m.get("ok") is False and e.get("code") == "unknown_channel",
                    f"code {e.get('code')!r}")
            return ""
        finally:
            c.close()

    def c_chunked(self) -> str:
        c = self._session()
        try:
            m = self._req(c, UNKNOWN_METHOD, {"pad": "x" * 200}, chunked=True)
            e = m.get("e") or {}
            _expect(e.get("code") == "unknown_method", f"code {e.get('code')!r}")
            return "16-byte chunks"
        finally:
            c.close()

    def c_big_id(self) -> str:
        c = self._session()
        try:
            rid = 2**53 + 1
            m = self._req(c, UNKNOWN_METHOD, rid=rid)
            _expect(m.get("id") == rid, "id changed")
            return ""
        finally:
            c.close()

    def c_reserved_kind(self) -> str:
        c = self._session()
        try:
            c.send_frame(wire.frame(0x80, b"reserved for extensions"))
            c.send({"t": "ping", "ts": 7})
            m = c.recv_msg(skip=("ping", "decl"))
            _expect(m.get("t") == "pong", f"got {m.get('t')!r} {m.get('code', '')}")
            return ""
        finally:
            c.close()

    def c_bad_kind(self) -> str:
        c = self._session()
        try:
            c.send_frame(wire.frame(0x10, b"\x00"))
            return self._closes_with(c, "protocol_error")
        finally:
            c.close()

    def c_oversize(self) -> str:
        c = self._session()
        try:
            mf = c.peer_max_frame
            c.send_frame(wire.HEADER.pack(mf + 1, wire.KIND_MESSAGE))
            return self._closes_with(c, "protocol_error")
        finally:
            c.close()

    def c_not_map(self) -> str:
        c = self._session()
        try:
            c.send_frame(wire.frame(wire.KIND_MESSAGE, wire.pack([1, 2, 3])))
            return self._closes_with(c, "protocol_error")
        finally:
            c.close()

    def c_chunk_interleave(self) -> str:
        c = self._session()
        try:
            frames = list(wire.chunk_frames(wire.pack({"t": "ping", "ts": 1, "pad": "x" * 64}),
                                            16))
            c.send_frame(bytes(frames[0]))
            c.send({"t": "ping", "ts": 2})
            return self._closes_with(c, "protocol_error")
        finally:
            c.close()

    def c_channel(self, name: str, decl: Any) -> str:
        c = self._session()
        try:
            v = max(decl.versions)
            rid = self._id()
            c.send({"t": "sub", "id": rid, "ch": name, "v": v})
            res = None
            early: list[tuple[str, Any]] = []
            end = time.monotonic() + 5
            while res is None:
                kind, m = c.recv(max(0.001, end - time.monotonic()))
                if kind == "msg" and m.get("t") == "res" and m.get("id") == rid:
                    res = m
                elif kind == "msg" and m.get("t") == "ping":
                    c.send({"t": "pong", "ts": m.get("ts")})
                else:
                    early.append((kind, m))
            if res.get("ok") is not True:
                e = res.get("e") or {}
                return f"WARN: refused: {e.get('code')} {e.get('msg', '')}"
            r = res.get("r") or {}
            sid = r.get("sid")
            _expect(isinstance(sid, int), "res.r.sid is not an integer")
            _expect(r.get("kind") == decl.kind, f"res.r.kind {r.get('kind')!r} != declared "
                                                f"{decl.kind!r}")
            _expect(not any(k == "stream" and p[0] == sid for k, p in early) and not any(
                k == "msg" and p.get("t") == "evt" and p.get("sid") == sid for k, p in early),
                "data for the subscription arrived before its res")
            layout = None
            if decl.kind == STATE:
                _expect("snap" in r, "state res has no snap")
            if decl.kind == STREAM:
                try:
                    layout = Layout.from_wire(r.get("layout"))
                except (ValueError, TypeError) as exc:
                    raise _Fail(f"res.r.layout invalid: {exc}") from None
                _expect(layout == decl.layout, "res layout differs from the declared layout")
            seqs: list[int] = []
            times: list[int] = []
            dropped = 0
            end = time.monotonic() + self.listen_s
            while time.monotonic() < end:
                try:
                    kind, m = c.recv(max(0.001, end - time.monotonic()))
                except TimeoutError:
                    break
                if kind == "stream":
                    s_sid, seq, t_ns, flags, body = m
                    if s_sid != sid:
                        continue
                    _expect(decl.kind == STREAM, "stream frame on a non-stream subscription")
                    assert layout is not None
                    _expect(len(body) == layout.size,
                            f"sample of {len(body)} bytes, layout {layout.size}")
                    seqs.append(seq)
                    times.append(t_ns)
                    dropped += flags & 1
                elif m.get("t") == "evt" and m.get("sid") == sid:
                    _expect(decl.kind != STREAM, "evt on a stream subscription")
                    seqs.append(m.get("seq"))
                    _expect(isinstance(m.get("ts"), int), "evt.ts is not an integer")
                elif m.get("t") == "ping":
                    c.send({"t": "pong", "ts": m.get("ts")})
                elif m.get("t") == "end" and m.get("sid") == sid:
                    return f"WARN: ended by the owner: {m.get('code')}"
            for a, b in zip(seqs, seqs[1:]):
                _expect(((b - a) & 0xFFFFFFFF) >= 1 if decl.kind == STREAM else b > a,
                        f"seq not increasing: {a} -> {b}")
            for a, b in zip(times, times[1:]):
                _expect(b >= a, "t_ns went backwards")
            c.send({"t": "unsub", "sid": sid})
            detail = f"sid {sid}, {len(seqs)} items in {self.listen_s:g} s"
            if decl.kind == STREAM and len(times) > 2:
                rate = (len(times) - 1) / ((times[-1] - times[0]) / 1e9)
                detail += f", {rate:.0f} Hz"
                if decl.rate_hz and rate > 1.5 * decl.rate_hz:
                    return f"WARN: {detail} (declared {decl.rate_hz:g} Hz)"
                if dropped:
                    detail += f", {dropped} dropped flags"
            return detail
        finally:
            c.close()

    def c_resub(self) -> str:
        chans = [(n, d) for n, d in self._channels() if d.kind != STREAM]
        if not chans:
            return "SKIP: no event or state channel declared"
        name, d = chans[0]
        c = self._session()
        try:
            sids = []
            for _ in range(2):
                rid = self._id()
                c.send({"t": "sub", "id": rid, "ch": name, "v": max(d.versions)})
                while True:
                    m = c.recv_msg(skip=("ping", "pong", "decl", "evt"))
                    if m.get("t") == "res" and m.get("id") == rid:
                        break
                if m.get("ok") is not True:
                    return f"SKIP: {name} refused the subscription"
                sids.append((m.get("r") or {}).get("sid"))
            _expect(sids[0] == sids[1], f"sids {sids[0]} and {sids[1]}")
            return f"{name}: sid {sids[0]}"
        finally:
            c.close()

    def c_call(self, method: str, params: Any) -> str:
        c = self._session()
        try:
            m = self._req(c, method, params)
            if m.get("ok") is True:
                return f"INFO: result {str(m.get('r'))[:300]}"
            e = m.get("e") or {}
            _expect(isinstance(e.get("code"), str), "error response without e.code")
            return f"INFO: error {e.get('code')}: {e.get('msg', '')}"
        finally:
            c.close()

    def c_heartbeat(self) -> str:
        c = self._session()
        try:
            hb = getattr(self, "heartbeat_ms", 5000)
            end = time.monotonic() + hb / 1000 * 1.6 + 0.5
            while time.monotonic() < end:
                try:
                    kind, m = c.recv(max(0.001, end - time.monotonic()))
                except TimeoutError:
                    break
                if kind == "msg" and m.get("t") == "ping":
                    c.send({"t": "pong", "ts": m.get("ts")})
                    return f"ping after idle (heartbeat {hb} ms)"
            return f"WARN: no ping within 1.6 x heartbeat_ms ({hb} ms)"
        finally:
            c.close()

    def c_bye(self) -> str:
        c = self._session()
        try:
            c.send({"t": "bye", "code": "normal", "msg": "check done"})
            t0 = time.monotonic()
            c.wait_closed(3.0)
            dt = time.monotonic() - t0
            _expect(dt < 2.9, "still open 3 s after bye")
            return f"closed after {1000 * dt:.0f} ms"
        finally:
            c.close()
