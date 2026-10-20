"""What a process offers (methods and channels), the server (:class:`Endpoint`) and the
client (:class:`Client`). Both are nodes: after the handshake either end calls the other's
methods and subscribes to the other's channels (SPEC.md 10)."""

from __future__ import annotations

import hmac
import logging
import os
import random
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import _win32
from ._threads import Gate, Workers
from . import errors as E
from . import handshake as H
from . import naming, wire
from .decl import EVENT, STATE, STREAM, ChannelDecl, Declarations, MethodDecl
from .errors import (HandshakeError, NotConnected, PipeBusy, PipeClosed, PipeError,
                     PipeNotFound, ProtocolError, RemoteError)
from .handshake import PeerInfo
from .layout import Layout
from .peer import Event, PendingCall, Peer, Sample, Subscription, _OutSub

log = logging.getLogger(__name__)
now_ns = time.perf_counter_ns

KeySource = bytes | Callable[[], bytes]
DEFAULT_HEARTBEAT_MS = 5000
DEFAULT_TIMEOUT_S = 5.0
MAX_ID = 2**63 - 1


def clock_pair() -> tuple[int, int]:
    """``(QPC ns, Unix ns)`` read together (SPEC.md 14.2)."""
    return now_ns(), time.time_ns()


# ------------------------------------------------------------------------- handlers


class Context:
    """What a method handler gets with its params.

    ``peer`` (who calls: ``peer.info``, ``peer.pid``), ``method``, ``version``, ``id``,
    ``idempotency_key`` (``ik``), ``deadline`` (``time.perf_counter()`` value or ``None``).
    """

    def __init__(self, peer: Peer, rid: Any, method: str, version: int, params: Any,
                 deadline: float | None, idempotency_key: str | None) -> None:
        self.peer = peer
        self.id = rid
        self.method = method
        self.version = version
        self.params = params
        self.deadline = deadline
        self.idempotency_key = idempotency_key
        self._cancel_flag = False
        self._cancel_event: threading.Event | None = None

    @property
    def cancelled(self) -> bool:
        """The caller sent ``cancel`` or went away: stop and return anything."""
        return self._cancel_flag

    def wait_cancelled(self, timeout: float | None = None) -> bool:
        """Block until the request is cancelled (True) or ``timeout`` passes (False)."""
        if self._cancel_event is None:
            self._cancel_event = threading.Event()
            if self._cancel_flag:
                self._cancel_event.set()
        return self._cancel_event.wait(timeout)

    def _cancel(self) -> None:
        self._cancel_flag = True
        ev = self._cancel_event
        if ev is not None:
            ev.set()

    @property
    def remaining(self) -> float | None:
        return None if self.deadline is None else self.deadline - time.perf_counter()

    def expired(self) -> bool:
        return self.deadline is not None and time.perf_counter() >= self.deadline

    def progress(self, data: Any = None, extend_s: float | None = None) -> None:
        """Send ``prog`` (a long request is alive); ``extend_s`` gives the caller a new
        deadline that many seconds from now."""
        msg: dict[str, Any] = {"t": "prog", "id": self.id, "d": data}
        if extend_s is not None:
            msg["dl"] = max(1, int(extend_s * 1000))
            self.deadline = time.perf_counter() + extend_s
        try:
            self.peer.send_message(msg)
        except NotConnected:
            self._cancel()


Handler = Callable[[Context, Any], Any]


class _Method:
    def __init__(self, decl: MethodDecl, handler: Handler, inline: bool,
                 cache: bool = True, ttl: float | None = None) -> None:
        self.decl = decl
        self.handler = handler
        self.inline = inline
        self.cache: IdempotencyCache | None = (
            IdempotencyCache(ttl=ttl) if decl.idempotent and cache else None)


# Error codes that mean "not done, try later": they never consume an idempotency key
# (SPEC.md 10.4), so a repeat with the same key runs the handler again. ``writer_busy`` is
# not a protocol code; it is the application code Pitwall's write method answers.
NOT_CACHED_CODES = frozenset({E.BUSY, E.UNAVAILABLE, E.TIMEOUT, E.CANCELLED, E.OVERLOADED,
                              E.SHUTTING_DOWN, "writer_busy"})


class IdempotencyCache:
    """Results of idempotent requests by ``(peer app, method, ik)`` for this process's
    lifetime (SPEC.md 10.4): a retry after a reconnect gets the first result instead of a
    second execution. Durable deduplication across restarts is the application's job."""

    def __init__(self, max_entries: int = 4096, ttl: float | None = None) -> None:
        self.max_entries = max_entries
        self.ttl = ttl  # seconds an answer is remembered; None: for the process lifetime
        self._lock = threading.Lock()
        self._done: OrderedDict[tuple[str, str, str], tuple[bool, Any, float]] = OrderedDict()
        self._running: dict[tuple[str, str, str], threading.Event] = {}

    def run(self, key: tuple[str, str, str], fn: Callable[[], Any]) -> Any:
        while True:
            with self._lock:
                entry = self._done.get(key)
                if entry is not None and self.ttl is not None                         and time.monotonic() - entry[2] >= self.ttl:
                    del self._done[key]
                    entry = None
                if entry is not None:
                    self._done.move_to_end(key)
                    if entry[0]:
                        return entry[1]
                    raise entry[1]
                ev = self._running.get(key)
                if ev is None:
                    ev = self._running[key] = threading.Event()
                    break
            ev.wait()
        try:
            value = fn()
            result = (True, value)
        except RemoteError as exc:
            result = (False, exc)
        except BaseException:
            with self._lock:
                self._running.pop(key, None)
            ev.set()
            raise
        with self._lock:
            self._running.pop(key, None)
            if result[0] or result[1].code not in NOT_CACHED_CODES:
                self._done[key] = (*result, time.monotonic())
                while len(self._done) > self.max_entries:
                    self._done.popitem(last=False)
        ev.set()
        if result[0]:
            return result[1]
        raise result[1]


# ------------------------------------------------------------------------- channels


class SubscribeRequest:
    """What an ``on_subscribe`` hook gets: ``peer``, ``version``, ``options``. Raise
    :class:`RemoteError` to refuse."""

    def __init__(self, peer: Peer, version: int, options: dict[str, Any] | None) -> None:
        self.peer = peer
        self.version = version
        self.options = options


class Channel:
    """A channel this process owns. Subclasses: :class:`EventChannel`,
    :class:`StateChannel`, :class:`StreamChannel`.

    ``on_subscribe(req)``: runs on the reader thread before a subscription is accepted
    (must be quick; raise :class:`RemoteError` to refuse). ``on_active(bool)``: the first
    subscriber came (True) or the last one left (False): start or stop producing (the VR
    rule: a stream costs nothing while nobody listens)."""

    kind = ""

    def __init__(self, node: _Node, decl: ChannelDecl) -> None:
        self.node = node
        self.decl = decl
        self.name = decl.name
        self._lock = threading.Lock()
        self._subs: tuple[_OutSub, ...] = ()
        self.on_subscribe: Callable[[SubscribeRequest], None] | None = None
        self.on_active: Callable[[bool], None] | None = None

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)

    @property
    def active(self) -> bool:
        return bool(self._subs)

    def _remove(self, sub: _OutSub) -> None:
        with self._lock:
            if sub not in self._subs:
                return
            self._subs = tuple(s for s in self._subs if s is not sub)
            last = not self._subs
        if last:
            self._notify_active(False)

    def _notify_active(self, value: bool) -> None:
        cb = self.on_active
        if cb is not None:
            try:
                cb(value)
            except Exception:  # noqa: BLE001
                log.exception("%s: on_active failed", self.name)

    def _snapshot(self, sub: _OutSub) -> dict[str, Any]:  # with self._lock
        return {}

    def close(self, code: str = E.CHANNEL_CLOSED, message: str = "") -> None:
        """End every subscription (``end``) and withdraw the channel."""
        for sub in self._subs:
            sub.peer._drop_outsub(sub, code, message)
        self.node._remove_channel(self.name)


class EventChannel(Channel):
    """Discrete events; a subscriber too slow for its bounded queue gets ``end``
    ``overloaded`` (SPEC.md 12.2)."""

    kind = EVENT

    def publish(self, data: Any, *, ts: int | None = None, version: int | None = None) -> int:
        """Send ``data`` to every subscriber (only those of ``version`` when given).
        Never blocks. Returns the number of subscriptions it was queued for."""
        subs = self._subs
        if not subs:
            return 0
        packed = wire.pack(data)
        t = now_ns() if ts is None else ts
        n = 0
        for sub in subs:
            if version is None or sub.version == version:
                sub.peer._offer_event(sub, t, packed)
                n += 1
        return n


class StateChannel(Channel):
    """A current value: a new subscriber gets it in the ``sub`` response (the snapshot),
    then every change; a slow subscriber gets only the latest (coalesced, SPEC.md 12.3)."""

    kind = STATE

    def __init__(self, node: _Node, decl: ChannelDecl, initial: Any) -> None:
        super().__init__(node, decl)
        self._value = initial
        self._packed = wire.pack(initial)
        self._ts = now_ns()

    @property
    def value(self) -> Any:
        return self._value

    def set(self, value: Any, *, ts: int | None = None) -> None:
        """Replace the value and push it (always the whole value, never a delta)."""
        packed = wire.pack(value)
        t = now_ns() if ts is None else ts
        with self._lock:
            self._value, self._packed, self._ts = value, packed, t
            subs = self._subs
            for sub in subs:
                sub.peer._offer_state(sub, t, packed)

    def _snapshot(self, sub: _OutSub) -> dict[str, Any]:
        return {"snap": self._value, "ts": self._ts}


class StreamChannel(Channel):
    """High-rate fixed-layout samples; a subscriber's ring holds ``queue`` samples (default
    one second at ``rate_hz``, at least 64): the oldest is dropped when it is full and the
    next sample carries the ``dropped`` flag (SPEC.md 12.1)."""

    kind = STREAM

    def __init__(self, node: _Node, decl: ChannelDecl, queue: int | None) -> None:
        super().__init__(node, decl)
        assert decl.layout is not None
        self.layout: Layout = decl.layout
        rate = decl.rate_hz or 0
        self.queue = queue or max(64, int(rate))

    def send(self, body: bytes, *, t_ns: int | None = None) -> int:
        """Send one sample (``body``: exactly ``layout.size`` bytes, e.g. from
        :meth:`Layout.pack`). ``t_ns``: when it was sampled (QPC ns), default now."""
        subs = self._subs
        if not subs:
            return 0
        if len(body) != self.layout.size:
            raise ValueError(f"{self.name}: sample of {len(body)} bytes, layout is "
                             f"{self.layout.size}")
        t = now_ns() if t_ns is None else t_ns
        for sub in subs:
            sub.peer._offer_sample(sub, t, body)
        return len(subs)

    def send_values(self, *values: Any, t_ns: int | None = None) -> int:
        if not self._subs:
            return 0
        return self.send(self.layout.pack(*values), t_ns=t_ns)

    def _snapshot(self, sub: _OutSub) -> dict[str, Any]:
        return {"layout": self.layout.to_wire()}


# ----------------------------------------------------------------------------- node


class _Node:
    """Common part of :class:`Endpoint` and :class:`Client`: identity, key, offered methods
    and channels, the handler pool and the heartbeat."""

    def __init__(self, info: PeerInfo, key: KeySource, *, handler_threads: int = 8,
                 heartbeat_ms: int = DEFAULT_HEARTBEAT_MS,
                 max_frame: int = wire.DEFAULT_MAX_FRAME,
                 max_message: int = wire.DEFAULT_MAX_MESSAGE, event_queue: int = 1024,
                 event_queue_bytes: int = 8 * 1024 * 1024, max_inflight: int = 1024,
                 features: Iterable[str] = ()) -> None:
        if max_frame < wire.MIN_MAX_FRAME:
            raise ValueError(f"max_frame must be >= {wire.MIN_MAX_FRAME}")
        if not max_frame <= max_message <= wire.MAX_MESSAGE_LIMIT:
            raise ValueError("max_message must be >= max_frame and < 4 GiB")
        self.info = info
        self._key = key
        self.heartbeat_ms = heartbeat_ms
        self.max_frame = max_frame
        self.max_message = max_message
        self.event_queue = event_queue
        self.event_queue_bytes = event_queue_bytes
        self.max_inflight = max_inflight
        self.features = list(features)
        self._methods: dict[str, _Method] = {}
        self._channels: dict[str, Channel] = {}
        self._decl_lock = threading.Lock()
        self._peers: set[Peer] = set()
        self._peers_lock = threading.Lock()
        self._workers = Workers()
        self._gate = Gate(handler_threads)
        self._hb_stop = threading.Event()
        self._hb_thread: threading.Thread | None = None
        self.on_connect: Callable[[Peer], None] | None = None
        self.on_disconnect: Callable[[Peer], None] | None = None
        self.on_redeclare: Callable[[Peer], None] | None = None

    # -------------------------------------------------------------- declarations

    def key(self) -> bytes:
        k = self._key() if callable(self._key) else self._key
        if len(k) != 32:
            raise E.KeyFileError("the key must be 32 bytes")
        return k

    def method(self, name: str, handler: Handler | None = None, *,
               versions: Iterable[int] = (1,), ordered: bool = False,
               idempotent: bool = False, idempotency_cache: bool = True,
               idempotency_ttl: float | None = None, doc: str | None = None,
               params: dict[str, Any] | None = None, result: dict[str, Any] | None = None,
               inline: bool = False) -> Any:
        """Offer a method; ``handler(ctx, params) -> result`` runs on a pool thread
        (``ordered``: one at a time per connection, in arrival order; ``inline``: on the
        connection's reader thread, for handlers that answer from memory in microseconds
        and never block or call the peer). Usable as a decorator. Raise
        :class:`RemoteError` from the handler to answer an error.

        ``idempotent`` declares the method idempotent (SPEC.md 10.4). By default the
        endpoint remembers the answers by ``ik`` for the process lifetime;
        ``idempotency_cache=False`` declares it but keeps no cache (the handler
        deduplicates itself, e.g. in its database transaction), and ``idempotency_ttl``
        (seconds) forgets answers after that long."""
        if not naming.valid_name(name) or name.startswith("ipc."):
            raise ValueError(f"invalid method name {name!r}")
        if idempotency_ttl is not None and idempotency_ttl <= 0:
            raise ValueError("idempotency_ttl must be positive")
        decl = MethodDecl(name, sorted(set(versions)), ordered, idempotent, doc, params,
                          result)

        def register(fn: Handler) -> Handler:
            with self._decl_lock:
                self._methods[name] = _Method(decl, fn, inline, idempotency_cache,
                                              idempotency_ttl)
            self._redeclare()
            return fn

        return register if handler is None else register(handler)

    def remove_method(self, name: str) -> None:
        with self._decl_lock:
            self._methods.pop(name, None)
        self._redeclare()

    def _add_channel(self, ch: Channel) -> None:
        if not naming.valid_name(ch.name) or ch.name.startswith("ipc."):
            raise ValueError(f"invalid channel name {ch.name!r}")
        with self._decl_lock:
            if ch.name in self._channels:
                raise ValueError(f"channel {ch.name} already declared")
            self._channels[ch.name] = ch
        self._redeclare()

    def _remove_channel(self, name: str) -> None:
        with self._decl_lock:
            self._channels.pop(name, None)
        self._redeclare()

    def event_channel(self, name: str, *, versions: Iterable[int] = (1,),
                      doc: str | None = None, data: dict[str, Any] | None = None
                      ) -> EventChannel:
        ch = EventChannel(self, ChannelDecl(name, EVENT, sorted(set(versions)), doc,
                                            data=data))
        self._add_channel(ch)
        return ch

    def state_channel(self, name: str, initial: Any = None, *,
                      versions: Iterable[int] = (1,), doc: str | None = None,
                      data: dict[str, Any] | None = None) -> StateChannel:
        ch = StateChannel(self, ChannelDecl(name, STATE, sorted(set(versions)), doc,
                                            data=data), initial)
        self._add_channel(ch)
        return ch

    def stream_channel(self, name: str, layout: Layout, *, versions: Iterable[int] = (1,),
                       rate_hz: float | None = None, doc: str | None = None,
                       queue: int | None = None) -> StreamChannel:
        ch = StreamChannel(self, ChannelDecl(name, STREAM, sorted(set(versions)), doc,
                                             layout=layout, rate_hz=rate_hz), queue)
        self._add_channel(ch)
        return ch

    def channel(self, name: str) -> Channel | None:
        return self._channels.get(name)

    def declarations(self) -> Declarations:
        with self._decl_lock:
            return Declarations({n: m.decl for n, m in self._methods.items()},
                                {n: c.decl for n, c in self._channels.items()})

    def _redeclare(self) -> None:
        peers = self.peers
        if not peers:
            return
        msg = {"t": "decl", "decl": self.declarations().to_wire()}
        for p in peers:
            try:
                p.send_message(msg)
            except NotConnected:
                pass

    # -------------------------------------------------------------- peers

    @property
    def peers(self) -> list[Peer]:
        with self._peers_lock:
            return [p for p in self._peers if p.is_open]

    def _peer_opened(self, peer: Peer) -> None:
        with self._peers_lock:
            self._peers.add(peer)
        self._ensure_heartbeat()
        peer._start()
        if self.on_connect is not None:
            try:
                self.on_connect(peer)
            except Exception:  # noqa: BLE001
                log.exception("on_connect failed")

    def _peer_closed(self, peer: Peer) -> None:
        with self._peers_lock:
            self._peers.discard(peer)
        if self.on_disconnect is not None:
            try:
                self.on_disconnect(peer)
            except Exception:  # noqa: BLE001
                log.exception("on_disconnect failed")

    def _peer_redeclared(self, peer: Peer) -> None:
        if self.on_redeclare is not None:
            try:
                self.on_redeclare(peer)
            except Exception:  # noqa: BLE001
                log.exception("on_redeclare failed")

    def _ensure_heartbeat(self) -> None:
        if self._hb_thread is None:
            self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True,
                                               name="ipc-heartbeat")
            self._hb_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._hb_stop.wait(0.25):
            t = time.monotonic()
            with self._peers_lock:
                peers = list(self._peers)
            for p in peers:
                hb = p.heartbeat_ms / 1000
                if t - p.last_recv > 3 * hb:
                    log.warning("%s: nothing received for %.1f s; closing", p.info.app,
                                t - p.last_recv)
                    p.close_code = p.close_code or E.TIMEOUT
                    p.abort()
                elif t - p.last_send > hb:
                    p.last_send = t
                    try:
                        p.send_message({"t": "ping", "ts": now_ns()})
                    except NotConnected:
                        pass

    def _shutdown_node(self) -> None:
        self._hb_stop.set()
        self._workers.close()

    # -------------------------------------------------------------- inbound

    def _on_request(self, peer: Peer, msg: dict[str, Any]) -> None:
        rid = msg["id"]
        if not isinstance(rid, int) or isinstance(rid, bool) or not 0 <= rid <= MAX_ID:
            raise ProtocolError("req.id must be an integer 0..2^63-1")
        method = msg["m"]
        version = msg.get("v", 1)
        params = msg.get("p", {})
        if not isinstance(method, str) or not isinstance(version, int):
            self._reply_error(peer, rid, RemoteError(E.BAD_REQUEST, "m must be a string and "
                                                                    "v an integer"))
            return
        m = self._methods.get(method)
        if m is None:
            self._reply_error(peer, rid, RemoteError(E.UNKNOWN_METHOD,
                                                     f"unknown method {method}"))
            return
        if version not in m.decl.versions:
            self._reply_error(peer, rid, RemoteError(
                E.VERSION_UNSUPPORTED, f"{method} v{version} is not offered",
                {"versions": m.decl.versions}))
            return
        dl = msg.get("dl")
        deadline = time.perf_counter() + dl / 1000 if isinstance(dl, int) and dl > 0 else None
        ik = msg.get("ik")
        ctx = Context(peer, rid, method, version, params, deadline,
                      ik if isinstance(ik, str) else None)
        with peer._lock:
            if rid in peer._inbound:
                raise ProtocolError(f"request id {rid} is already in flight")
            busy = len(peer._inbound) >= self.max_inflight
            if not busy:
                peer._inbound[rid] = ctx
        if busy:
            self._reply_error(peer, rid, RemoteError(
                E.BUSY, f"more than {self.max_inflight} requests in flight", retry_ms=100))
            return
        job = self._job(m, ctx)
        if m.inline:
            job()
        elif m.decl.ordered:
            peer._schedule(peer._serial_lane.offer(job))
        else:
            peer._schedule(self._gate.offer(job))

    def _job(self, m: _Method, ctx: Context) -> Callable[[], None]:
        def run() -> None:
            peer = ctx.peer
            try:
                if ctx.expired():
                    raise RemoteError(E.TIMEOUT, "the deadline passed before the request ran")
                if m.cache is not None and ctx.idempotency_key is not None:
                    key = (peer.info.app, ctx.method, ctx.idempotency_key)
                    result = m.cache.run(key, lambda: m.handler(ctx, ctx.params))
                else:
                    result = m.handler(ctx, ctx.params)
                reply: dict[str, Any] = {"t": "res", "id": ctx.id, "ok": True, "r": result}
                try:
                    peer.send_message(reply)
                except ValueError as exc:
                    raise RemoteError(E.INTERNAL, f"result not encodable: {exc}") from None
            except RemoteError as exc:
                self._reply_error(peer, ctx.id, exc)
            except NotConnected:
                pass
            except Exception as exc:  # noqa: BLE001
                log.exception("handler of %s failed", ctx.method)
                self._reply_error(peer, ctx.id, RemoteError(
                    E.INTERNAL, f"{type(exc).__name__}: {exc}"[:500]))
            finally:
                with peer._lock:
                    peer._inbound.pop(ctx.id, None)
        return run

    @staticmethod
    def _reply_error(peer: Peer, rid: Any, err: RemoteError) -> None:
        try:
            peer.send_message({"t": "res", "id": rid, "ok": False, "e": err.to_wire()})
        except NotConnected:
            pass

    def _on_subscribe(self, peer: Peer, msg: dict[str, Any]) -> None:
        rid = msg["id"]
        if not isinstance(rid, int) or isinstance(rid, bool) or not 0 <= rid <= MAX_ID:
            raise ProtocolError("sub.id must be an integer 0..2^63-1")
        name = msg["ch"]
        version = msg.get("v", 1)
        options = msg.get("o") if isinstance(msg.get("o"), dict) else None
        ch = self._channels.get(name) if isinstance(name, str) else None
        if ch is None:
            self._reply_error(peer, rid, RemoteError(E.UNKNOWN_CHANNEL,
                                                     f"unknown channel {name}"))
            return
        if version not in ch.decl.versions:
            self._reply_error(peer, rid, RemoteError(
                E.VERSION_UNSUPPORTED, f"{name} v{version} is not offered",
                {"versions": ch.decl.versions}))
            return
        with peer._lock:
            for o in peer._outsubs.values():
                if o.channel is ch and o.version == version and o.active:
                    existing = o
                    break
            else:
                existing = None
        if existing is not None:  # SPEC.md 10.6: the same subscription again
            with ch._lock:
                r = {"sid": existing.sid, "kind": ch.kind, "seq": existing.seq,
                     **ch._snapshot(existing)}
            peer.send_message({"t": "res", "id": rid, "ok": True, "r": r})
            return
        if ch.on_subscribe is not None:
            try:
                ch.on_subscribe(SubscribeRequest(peer, version, options))
            except RemoteError as exc:
                self._reply_error(peer, rid, exc)
                return
        sid = next(peer._sids)
        sub = _OutSub(peer, ch, sid, version, options)
        if isinstance(ch, StreamChannel):
            sub.ring_max = ch.queue
        with ch._lock:
            r = {"sid": sid, "kind": ch.kind, "seq": 0, **ch._snapshot(sub)}
            res = wire.message_frame({"t": "res", "id": rid, "ok": True, "r": r})
            with peer._lock:
                peer._outsubs[sid] = sub
            # Queue the response before the channel can offer this subscriber anything, so
            # the snapshot always precedes the first change.
            peer._enqueue(("r", res, sub))
            first = not ch._subs
            ch._subs = ch._subs + (sub,)
        if first:
            ch._notify_active(True)


# ------------------------------------------------------------------------- handshake


def _read_handshake(pipe: _win32.PipeHandle, parser: wire.FrameParser, deadline: float,
                    early: list[tuple[int, bytes]]) -> dict[str, Any]:
    while not early:
        left = deadline - time.monotonic()
        if left <= 0:
            raise HandshakeError(E.TIMEOUT, "no handshake message in time")
        try:
            data = pipe.read(max(1, int(left * 1000)))
        except TimeoutError:
            raise HandshakeError(E.TIMEOUT, "no handshake message in time") from None
        except PipeClosed:
            raise HandshakeError(E.DISCONNECTED, "the peer closed during the handshake") \
                from None
        try:
            early.extend(parser.feed(data))
        except ProtocolError as exc:
            raise HandshakeError(E.PROTOCOL_ERROR, str(exc)) from None
    kind, payload = early.pop(0)
    if kind != wire.KIND_MESSAGE:
        raise HandshakeError(E.PROTOCOL_ERROR, "a stream frame during the handshake")
    try:
        return wire.unpack_message(payload)
    except ProtocolError as exc:
        raise HandshakeError(E.PROTOCOL_ERROR, str(exc)) from None


def _bye(pipe: _win32.PipeHandle, code: str, message: str) -> None:
    try:
        pipe.write(wire.message_frame({"t": "bye", "code": code, "msg": message}), 1000)
    except PipeError:
        pass


def _int_field(msg: dict[str, Any], name: str, default: int) -> int:
    v = msg.get(name, default)
    if not isinstance(v, int) or isinstance(v, bool):
        raise ValueError(f"{name} must be an integer")
    return v


def server_handshake(node: _Node, pipe: _win32.PipeHandle, short: str, run: str,
                     timeout: float = H.HANDSHAKE_TIMEOUT_S) -> Peer:
    deadline = time.monotonic() + timeout
    s_nonce = H.new_nonce()
    pipe.write(wire.message_frame(H.hello_message(s_nonce, node.max_frame, node.features,
                                                  clock_pair()) | {
        "max_message": node.max_message}), 5000)
    parser = wire.FrameParser(wire.HANDSHAKE_MAX_FRAME, wire.HANDSHAKE_MAX_FRAME)
    early: list[tuple[int, bytes]] = []
    try:
        try:
            auth = _read_handshake(pipe, parser, deadline, early)
        except HandshakeError as exc:
            if exc.code == E.PROTOCOL_ERROR:
                _bye(pipe, E.PROTOCOL_ERROR, exc.message)
            raise
        if auth["t"] == "bye":
            raise HandshakeError(str(auth.get("code", E.NORMAL)), str(auth.get("msg", "")))
        if auth["t"] != "auth":
            _bye(pipe, E.PROTOCOL_ERROR, f"expected auth, got {auth['t']}")
            raise HandshakeError(E.PROTOCOL_ERROR, f"expected auth, got {auth['t']}")
        version = auth.get("v")
        if version not in wire.SUPPORTED_VERSIONS:
            _bye(pipe, E.VERSION_UNSUPPORTED, f"protocol version {version!r} not supported")
            raise HandshakeError(E.VERSION_UNSUPPORTED, f"client asked for {version!r}")
        c_nonce, mac = auth.get("nonce"), auth.get("mac")
        if not isinstance(c_nonce, bytes) or len(c_nonce) != H.NONCE_SIZE \
                or not isinstance(mac, bytes) or len(mac) != H.MAC_SIZE:
            _bye(pipe, E.PROTOCOL_ERROR, "auth.nonce and auth.mac must be 32-byte bin")
            raise HandshakeError(E.PROTOCOL_ERROR, "bad auth nonce or mac")
        try:
            key = node.key()
        except E.KeyFileError as exc:
            _bye(pipe, E.UNAVAILABLE, "the server has no key")
            raise HandshakeError(E.UNAVAILABLE, str(exc)) from None
        if not hmac.compare_digest(mac, H.client_mac(key, version, s_nonce, c_nonce, short)):
            _bye(pipe, E.UNAUTHORIZED, "authentication failed")
            raise HandshakeError(E.UNAUTHORIZED, "the client's MAC is wrong (another key)")
        try:
            info = PeerInfo.from_wire(auth.get("peer"))
            decl = Declarations.from_wire(auth.get("decl"))
            peer_max_frame = _int_field(auth, "max_frame", wire.MIN_MAX_FRAME)
            peer_max_message = _int_field(auth, "max_message", peer_max_frame)
            if peer_max_frame < wire.MIN_MAX_FRAME or peer_max_message < peer_max_frame:
                raise ValueError("max_frame must be >= 65536 and max_message >= max_frame")
        except ValueError as exc:
            _bye(pipe, E.PROTOCOL_ERROR, str(exc))
            raise HandshakeError(E.PROTOCOL_ERROR, str(exc)) from None
        feats = auth.get("features")
        features = [f for f in feats if isinstance(f, str) and f in node.features] \
            if isinstance(feats, list) else []
        welcome = {"t": "welcome", "v": version,
                   "mac": H.server_mac(key, version, s_nonce, c_nonce, short),
                   "run": run, "peer": node.info.to_wire(),
                   "decl": node.declarations().to_wire(), "heartbeat_ms": node.heartbeat_ms,
                   "features": features}
        pipe.write(wire.message_frame(welcome), 5000)
    except (PipeError, OSError) as exc:
        raise HandshakeError(E.DISCONNECTED, str(exc)) from None
    parser.max_frame = node.max_frame
    parser.max_message = node.max_message
    return Peer(node, pipe, info=info, decl=decl, version=version, run=run,
                peer_max_frame=peer_max_frame, peer_max_message=peer_max_message,
                heartbeat_ms=node.heartbeat_ms, parser=parser, early=early, server_side=True,
                pipe_name=short, features=features)


def client_handshake(node: _Node, pipe: _win32.PipeHandle, short: str,
                     timeout: float = H.HANDSHAKE_TIMEOUT_S) -> Peer:
    deadline = time.monotonic() + timeout
    parser = wire.FrameParser(wire.HANDSHAKE_MAX_FRAME, wire.HANDSHAKE_MAX_FRAME)
    early: list[tuple[int, bytes]] = []
    try:
        hello = _read_handshake(pipe, parser, deadline, early)
        if hello["t"] == "bye":
            raise HandshakeError(str(hello.get("code", E.NORMAL)), str(hello.get("msg", "")))
        if hello["t"] != "hello" or hello.get("proto") != wire.PROTOCOL_NAME:
            raise HandshakeError(E.PROTOCOL_ERROR, "the server did not send a stintmate-ipc "
                                                   "hello")
        version = H.choose_version(hello.get("versions"), wire.SUPPORTED_VERSIONS)
        if version is None:
            _bye(pipe, E.VERSION_UNSUPPORTED, f"client speaks {list(wire.SUPPORTED_VERSIONS)}")
            raise HandshakeError(E.VERSION_UNSUPPORTED,
                                 f"server speaks {hello.get('versions')!r}")
        s_nonce = hello.get("nonce")
        if not isinstance(s_nonce, bytes) or len(s_nonce) != H.NONCE_SIZE:
            raise HandshakeError(E.PROTOCOL_ERROR, "hello.nonce must be a 32-byte bin")
        try:
            peer_max_frame = _int_field(hello, "max_frame", wire.MIN_MAX_FRAME)
            peer_max_message = _int_field(hello, "max_message", peer_max_frame)
        except ValueError as exc:
            raise HandshakeError(E.PROTOCOL_ERROR, str(exc)) from None
        if peer_max_frame < wire.MIN_MAX_FRAME or peer_max_message < peer_max_frame:
            raise HandshakeError(E.PROTOCOL_ERROR, "bad max_frame / max_message")
        key = node.key()
        c_nonce = H.new_nonce()
        feats = hello.get("features")
        offered = [f for f in node.features if isinstance(feats, list) and f in feats]
        auth = {"t": "auth", "v": version, "nonce": c_nonce,
                "mac": H.client_mac(key, version, s_nonce, c_nonce, short),
                "peer": node.info.to_wire(), "decl": node.declarations().to_wire(),
                "max_frame": node.max_frame, "max_message": node.max_message,
                "features": offered}
        pipe.write(wire.message_frame(auth), 5000)
        welcome = _read_handshake(pipe, parser, deadline, early)
        if welcome["t"] == "bye":
            code = str(welcome.get("code", E.NORMAL))
            raise HandshakeError(code, str(welcome.get("msg", "")) + (
                " (check that both ends read the same key file)"
                if code == E.UNAUTHORIZED else ""))
        if welcome["t"] != "welcome":
            raise HandshakeError(E.PROTOCOL_ERROR, f"expected welcome, got {welcome['t']}")
        mac = welcome.get("mac")
        if not isinstance(mac, bytes) or not hmac.compare_digest(
                mac, H.server_mac(key, version, s_nonce, c_nonce, short)):
            _bye(pipe, E.UNAUTHORIZED, "server authentication failed")
            raise HandshakeError(E.UNAUTHORIZED, "the server's MAC is wrong: another key, or "
                                                 "not the real endpoint")
        try:
            info = PeerInfo.from_wire(welcome.get("peer"))
            decl = Declarations.from_wire(welcome.get("decl"))
            hb = _int_field(welcome, "heartbeat_ms", DEFAULT_HEARTBEAT_MS)
            if hb < 100:
                raise ValueError("heartbeat_ms must be >= 100")
        except ValueError as exc:
            _bye(pipe, E.PROTOCOL_ERROR, str(exc))
            raise HandshakeError(E.PROTOCOL_ERROR, str(exc)) from None
        run = welcome.get("run") if isinstance(welcome.get("run"), str) else ""
        wf = welcome.get("features")
        features = [f for f in wf if isinstance(f, str)] if isinstance(wf, list) else []
    except (PipeError, OSError) as exc:
        if isinstance(exc, HandshakeError):
            raise
        raise HandshakeError(E.DISCONNECTED, str(exc)) from None
    parser.max_frame = node.max_frame
    parser.max_message = node.max_message
    return Peer(node, pipe, info=info, decl=decl, version=version, run=run,
                peer_max_frame=peer_max_frame, peer_max_message=peer_max_message,
                heartbeat_ms=hb, parser=parser, early=early, server_side=False,
                pipe_name=short, features=features)


# ----------------------------------------------------------------------------- server


class Endpoint(_Node):
    """The server of one endpoint (SPEC.md 4): creates the pipe, authenticates every client
    and serves the methods and channels declared on it.

    ``pipe``: full path or short name; default ``stintmate-<info.app>[-<info.process>]-<user>``.
    ``registry``: a folder for the endpoint registry entry (``<workspace>\\ipc\\endpoints``),
    or ``None``. ``key``: 32 bytes or a callable returning them (e.g. :class:`KeyFile`).
    """

    def __init__(self, info: PeerInfo, key: KeySource, *, pipe: str | None = None,
                 registry: str | os.PathLike[str] | None = None,
                 handshake_timeout: float = H.HANDSHAKE_TIMEOUT_S, **kw: Any) -> None:
        super().__init__(info, key, **kw)
        if pipe is None:
            pipe = naming.pipe_path(info.app, info.process)
        self.path, self.short_name = naming.split_pipe(pipe)
        self.short_name = naming.ascii_lower(self.short_name)
        eid = self.short_name.removeprefix("stintmate-")
        suffix = "-" + naming.pipe_user()
        self.endpoint_id = eid[:-len(suffix)] if eid.endswith(suffix) else eid
        self.registry = Path(registry) if registry is not None else None
        self.handshake_timeout = handshake_timeout
        self.run = naming.new_run_id()
        self._listener: _win32.Listener | None = None
        self.on_handshake_failed: Callable[[int | None, HandshakeError], None] | None = None

    def start(self) -> Endpoint:
        """Create the pipe and serve. Raises :class:`EndpointInUse` when another process
        serves this name."""
        self._listener = _win32.Listener(self.path, self._on_pipe).start()
        if self.registry is not None:
            try:
                naming.register(self.registry, naming.RegistryEntry(
                    endpoint=self.endpoint_id,
                    pipe=self.path, app=self.info.app, process=self.info.process,
                    pid=os.getpid(), run=self.run, versions=list(wire.SUPPORTED_VERSIONS),
                    started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    name=self.info.name, version=self.info.version))
            except OSError as exc:
                log.warning("cannot write the registry entry: %s", exc)
        return self

    def _on_pipe(self, pipe: _win32.PipeHandle) -> None:
        threading.Thread(target=self._accept, args=(pipe,), daemon=True,
                         name="ipc-handshake").start()

    def _accept(self, pipe: _win32.PipeHandle) -> None:
        try:
            peer = server_handshake(self, pipe, self.short_name, self.run,
                                    self.handshake_timeout)
        except HandshakeError as exc:
            log.info("handshake with pid %s failed: %s", pipe.peer_pid, exc)
            pipe.close()
            if self.on_handshake_failed is not None:
                try:
                    self.on_handshake_failed(pipe.peer_pid, exc)
                except Exception:  # noqa: BLE001
                    log.exception("on_handshake_failed failed")
            return
        except Exception:  # noqa: BLE001
            log.exception("handshake failed")
            pipe.close()
            return
        self._peer_opened(peer)

    def stop(self, code: str = E.SHUTTING_DOWN, message: str = "the endpoint stops") -> None:
        """Stop listening, say ``bye`` to every peer and wait briefly for them to close."""
        if self._listener is not None:
            self._listener.stop()
            self._listener = None
        peers = self.peers
        for p in peers:
            p.close(code, message)
        for p in peers:
            p.wait_closed(2.0)
        if self.registry is not None:
            naming.unregister(self.registry, self.endpoint_id, self.run)
        self._shutdown_node()

    def __enter__(self) -> Endpoint:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()


# ----------------------------------------------------------------------------- client


def connect(pipe: str, info: PeerInfo, key: KeySource, *, timeout: float = 5.0,
            node: _Node | None = None, **kw: Any) -> Peer:
    """Open one connection (no reconnect): handshake and return the :class:`Peer`.
    ``node``: a :class:`Client` (or any node) whose methods and channels to offer."""
    path, short = naming.split_pipe(pipe)
    short = naming.ascii_lower(short)
    n = node if node is not None else _Node(info, key, **kw)
    deadline = time.monotonic() + timeout
    while True:
        try:
            handle = _win32.open_pipe(path, busy_wait_ms=max(1, int(min(1.0, timeout) * 1000)))
            break
        except PipeBusy:
            if time.monotonic() > deadline:
                raise
    try:
        peer = client_handshake(n, handle, short, max(0.1, deadline - time.monotonic()))
    except BaseException:
        handle.close()
        raise
    n._peer_opened(peer)
    return peer


class ClientSubscription:
    """A subscription that a :class:`Client` keeps across reconnects (and after an
    ``overloaded`` end): ``callback`` gets the state snapshot again after each one."""

    def __init__(self, client: Client, channel: str, callback: Callable[[Any], None],
                 version: int, options: dict[str, Any] | None,
                 on_end: Callable[[ClientSubscription, str, str], None] | None,
                 deliver: str) -> None:
        self.client = client
        self.channel = channel
        self.callback = callback
        self.version = version
        self.options = options
        self.on_end = on_end
        self.deliver = deliver
        self.current: Subscription | None = None
        self.closed = False
        self.error: BaseException | None = None
        self.subscribed = threading.Event()
        self._lock = threading.Lock()

    def close(self) -> None:
        self.closed = True
        with self.client._lock:
            if self in self.client._subs:
                self.client._subs.remove(self)
        cur = self.current
        if cur is not None:
            cur.close()

    def _ended(self, sub: Subscription, code: str, message: str) -> None:
        self.subscribed.clear()
        if self.on_end is not None:
            try:
                self.on_end(self, code, message)
            except Exception:  # noqa: BLE001
                log.exception("on_end failed")
        if code == E.OVERLOADED and not self.closed:
            threading.Timer(1.0, self.client._resubscribe_one, args=(self,)).start()


class Client(_Node):
    """A client that stays connected: connects in the background, retries with backoff
    (0.25, 0.5, 1, 2 s cap, with jitter), re-subscribes after every reconnect.

    ``pipe``: full path or short name of the endpoint (:func:`pipe_path`)."""

    BACKOFF = (0.25, 0.5, 1.0, 2.0)

    def __init__(self, pipe: str, info: PeerInfo, key: KeySource, *,
                 reconnect: bool = True, **kw: Any) -> None:
        super().__init__(info, key, **kw)
        self.path, short = naming.split_pipe(pipe)
        self.short_name = naming.ascii_lower(short)
        self.reconnect = reconnect
        self._lock = threading.Lock()
        self._peer: Peer | None = None
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._subs: list[ClientSubscription] = []
        self.last_error: BaseException | None = None
        self.on_connect_error: Callable[[BaseException], None] | None = None
        self._user_on_connect: Callable[[Peer], None] | None = None

    @property
    def peer(self) -> Peer | None:
        p = self._peer
        return p if p is not None and p.is_open else None

    @property
    def connected(self) -> bool:
        return self.peer is not None

    def start(self) -> Client:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, daemon=True,
                                            name=f"ipc-client-{self.short_name}")
            self._thread.start()
        return self

    def connect(self, timeout: float = 5.0) -> Peer:
        """Start and wait for the first connection."""
        self.start()
        return self.wait_connected(timeout)

    def wait_connected(self, timeout: float | None = None) -> Peer:
        end = None if timeout is None else time.monotonic() + timeout
        while True:
            p = self.peer
            if p is not None:
                return p
            left = None if end is None else end - time.monotonic()
            if left is not None and left <= 0:
                raise NotConnected(f"not connected to {self.short_name}"
                                   + (f": {self.last_error}" if self.last_error else ""))
            self._connected.wait(0.05 if left is None else min(0.05, left))

    def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                peer = connect(self.path, self.info, self._key, node=self,
                               timeout=H.HANDSHAKE_TIMEOUT_S)
            except (PipeError, HandshakeError, E.KeyFileError, OSError) as exc:
                self.last_error = exc
                if self.on_connect_error is not None:
                    try:
                        self.on_connect_error(exc)
                    except Exception:  # noqa: BLE001
                        log.exception("on_connect_error failed")
                if not self.reconnect:
                    return
                if isinstance(exc, HandshakeError) and exc.code == E.VERSION_UNSUPPORTED:
                    log.error("%s speaks no protocol version this client speaks; giving up",
                              self.short_name)
                    return  # SPEC.md 15.3: stop and report (last_error)
                delay = self.BACKOFF[min(attempt, len(self.BACKOFF) - 1)]
                if isinstance(exc, HandshakeError) and exc.code == E.UNAUTHORIZED:
                    delay = 10.0  # a wrong key does not fix itself in 2 s
                attempt += 1
                self._stop.wait(delay * random.uniform(0.8, 1.2))
                continue
            attempt = 0
            self.last_error = None
            self._peer = peer
            self._connected.set()
            for cs in list(self._subs):
                self._resubscribe_one(cs)
            peer.closed.wait()
            self._connected.clear()
            if not self.reconnect:
                return
            self._stop.wait(0.1)

    def _peer_opened(self, peer: Peer) -> None:
        # Visible as connected before on_connect runs, so the hook may call the endpoint.
        self._peer = peer
        self._connected.set()
        super()._peer_opened(peer)

    def _resubscribe_one(self, cs: ClientSubscription) -> None:
        peer = self.peer
        if peer is None or cs.closed:
            return
        with cs._lock:  # once per connection, whoever gets here first
            cur = cs.current
            if cur is not None and cur.peer is peer and not cur.ended.is_set():
                return
            try:
                cs.current = peer.subscribe(cs.channel, cs.callback, version=cs.version,
                                            options=cs.options, on_end=cs._ended,
                                            deliver=cs.deliver)
                cs.error = None
                cs.subscribed.set()
            except (RemoteError, NotConnected, E.ConnectionLost, E.CallTimeout) as exc:
                cs.error = exc
                log.info("subscribe %s failed: %s", cs.channel, exc)

    def call(self, method: str, params: Any = None, *, version: int = 1,
             timeout: float | None = DEFAULT_TIMEOUT_S, idempotency_key: str | None = None,
             on_progress: Callable[[Any], None] | None = None) -> Any:
        """Call a method of the endpoint. Raises :class:`NotConnected` when not connected
        now (use :meth:`wait_connected` first to wait)."""
        peer = self.peer
        if peer is None:
            raise NotConnected(f"not connected to {self.short_name}")
        return peer.call(method, params, version=version, timeout=timeout,
                         idempotency_key=idempotency_key, on_progress=on_progress)

    def call_async(self, method: str, params: Any = None, **kw: Any) -> PendingCall:
        peer = self.peer
        if peer is None:
            raise NotConnected(f"not connected to {self.short_name}")
        return peer.call_async(method, params, **kw)

    def subscribe(self, channel: str, callback: Callable[[Any], None], *, version: int = 1,
                  options: dict[str, Any] | None = None,
                  on_end: Callable[[ClientSubscription, str, str], None] | None = None,
                  deliver: str = "auto") -> ClientSubscription:
        """Subscribe now if connected, and again after every reconnect. Errors of a
        subscription attempt are kept in ``.error`` (e.g. ``unknown_channel``)."""
        cs = ClientSubscription(self, channel, callback, version, options, on_end, deliver)
        with self._lock:
            self._subs.append(cs)
        self._resubscribe_one(cs)
        return cs

    def close(self) -> None:
        self._stop.set()
        p = self._peer
        if p is not None:
            p.close()
            p.wait_closed(2.0)
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(3.0)
        self._shutdown_node()

    def __enter__(self) -> Client:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.close()


__all__ = ["Endpoint", "Client", "ClientSubscription", "Context", "Channel", "EventChannel",
           "StateChannel", "StreamChannel", "SubscribeRequest", "IdempotencyCache", "connect",
           "Event", "Sample", "PeerInfo"]
