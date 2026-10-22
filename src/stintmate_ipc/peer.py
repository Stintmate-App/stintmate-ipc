"""One authenticated connection, the protocol engine of both ends (SPEC.md sections 10-13).

Connections are symmetric after the handshake: either side calls the other's methods and
subscribes to the other's channels. Threads per connection:

- **reader**: reads frames, answers ``ping``, resolves responses, routes requests to the
  node's handler pool and events to the dispatch thread. Never runs application code
  except ``deliver="inline"`` subscription callbacks and channel ``on_subscribe`` hooks.
- **sender**: the only writer. Producers (``publish``, ``call``, handlers' results) only
  enqueue; the bounded per-subscription queues (SPEC.md 12) live here.
- **dispatch** (started on first use): runs subscription callbacks and progress callbacks
  in arrival order, so a callback may itself call the peer.
"""

from __future__ import annotations

import itertools
from functools import partial
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from . import errors as E
from . import wire
from .decl import EVENT, STATE, STREAM, Declarations
from .errors import (CallCancelled, CallTimeout, ConnectionLost, NotConnected, PipeClosed,
                     ProtocolError,
                     RemoteError)
from .handshake import PeerInfo
from ._threads import Lane
from .layout import Layout
from .wire import STREAM_FLAG_DROPPED, STREAM_HEADER

if TYPE_CHECKING:
    from ._win32 import PipeHandle
    from .node import Channel, Context, _Node

log = logging.getLogger(__name__)
now_ns = time.perf_counter_ns
BATCH_BYTES = 256 * 1024
BYE_LINGER_S = 1.0


class Event:
    """One event or state value delivered to a subscriber."""

    __slots__ = ("channel", "seq", "ts", "data", "snapshot")

    def __init__(self, channel: str, seq: int, ts: int | None, data: Any,
                 snapshot: bool = False) -> None:
        self.channel = channel
        self.seq = seq
        self.ts = ts
        self.data = data
        self.snapshot = snapshot

    def __repr__(self) -> str:
        return (f"Event({self.channel!r}, seq={self.seq}, snapshot={self.snapshot}, "
                f"data={self.data!r})")


class Sample:
    """One stream sample: ``seq``, ``t_ns`` (QPC ns at the source), ``dropped`` (samples
    were dropped before this one), ``body`` (the layout's bytes)."""

    __slots__ = ("channel", "seq", "t_ns", "dropped", "body", "layout")

    def __init__(self, channel: str, seq: int, t_ns: int, dropped: bool, body: bytes,
                 layout: Layout) -> None:
        self.channel = channel
        self.seq = seq
        self.t_ns = t_ns
        self.dropped = dropped
        self.body = body
        self.layout = layout

    def values(self) -> tuple[Any, ...]:
        return self.layout.unpack(self.body)

    def as_dict(self) -> dict[str, Any]:
        return self.layout.unpack_dict(self.body)


class PendingCall:
    """A request in flight (future-like). :meth:`result` waits and returns or raises."""

    def __init__(self, peer: Peer, rid: int, method: str, timeout: float | None,
                 on_progress: Callable[[Any], None] | None) -> None:
        self.peer = peer
        self.id = rid
        self.method = method
        self.deadline = None if timeout is None else time.perf_counter() + timeout
        self.on_progress = on_progress
        self._done = False
        self._gate = threading.Lock()  # held until done: waiting = acquiring it
        self._gate.acquire()
        self._result: Any = None
        self._error: BaseException | None = None
        self._callbacks: list[Callable[[PendingCall], None]] = []
        self._lock = threading.Lock()

    def _finish(self, result: Any = None, error: BaseException | None = None) -> None:
        with self._lock:
            if self._done:
                return
            self._result, self._error = result, error
            self._done = True
            self._gate.release()
            cbs, self._callbacks = self._callbacks, []
        for cb in cbs:
            try:
                cb(self)
            except Exception:  # noqa: BLE001
                log.exception("call callback failed")

    def done(self) -> bool:
        return self._done

    def _wait(self, timeout: float | None) -> bool:
        if self._done:
            return True
        if self._gate.acquire(timeout=-1 if timeout is None else timeout):
            self._gate.release()
            return True
        return self._done

    def add_done_callback(self, cb: Callable[[PendingCall], None]) -> None:
        """``cb(call)`` once done, on the thread that completes it (or now if done)."""
        with self._lock:
            if not self._done:
                self._callbacks.append(cb)
                return
        cb(self)

    def exception(self) -> BaseException | None:
        return self._error

    def cancel(self, reason: str = "") -> bool:
        """Give up on the call: send ``cancel`` to the owner (when connected), fail the call
        with :class:`CallCancelled` and run its done callbacks on this thread; the late
        ``res`` is ignored. Returns False (and does nothing) when the call was already done
        or its response won the race. Non-blocking and callable from any thread, including a
        done or progress callback and an inline handler."""
        if self._done:
            return False
        text = f"{self.method}: cancelled" + (f": {reason}" if reason else "")
        return self.peer._give_up(self, CallCancelled(text))

    def result(self, timeout: float | None = None) -> Any:
        """Wait for the response. When the call's own deadline passes a ``cancel`` is sent
        and :class:`CallTimeout` raised; ``timeout`` only limits this wait (the call stays
        pending)."""
        end = None if timeout is None else time.perf_counter() + timeout
        while not self._done:
            limits = [x for x in (self.deadline, end) if x is not None]
            wait = None if not limits else max(0.0, min(limits) - time.perf_counter())
            if self._wait(wait):
                break
            t = time.perf_counter()
            if self.deadline is not None and t >= self.deadline:
                if not self.peer._give_up(self, CallTimeout(f"{self.method}: no response in "
                                                            "time")):
                    # The response or the heartbeat's sweep took the call first and is about
                    # to finish it: wait for that outcome instead of returning a None result.
                    self._wait(None)
                break
            if end is not None and t >= end:
                raise CallTimeout(f"{self.method}: no response yet")
        if self._error is not None:
            raise self._error
        return self._result


class Subscription:
    """A subscription on one connection (ends with it; :class:`Client` re-subscribes)."""

    def __init__(self, peer: Peer, channel: str, version: int, callback: Callable[[Any], None],
                 on_end: Callable[[Subscription, str, str], None] | None, deliver: str,
                 options: dict[str, Any] | None) -> None:
        self.peer = peer
        self.channel = channel
        self.version = version
        self.callback = callback
        self.on_end = on_end
        self.deliver = deliver
        self.options = options
        self.sid = 0
        self.kind = ""
        self.layout: Layout | None = None
        self.snapshot: Any = None
        self.last_seq = 0
        self.ended = threading.Event()
        self.end_code: str | None = None
        self.end_message = ""
        self.pending: deque[Any] = deque()  # delivered, waiting for the callback
        self.limit = 0
        self.local_drops = 0

    def close(self) -> None:
        """Unsubscribe (``unsub``); no callback runs after this returns on the dispatch
        thread's next turn."""
        self.peer._unsubscribe(self)

    def __repr__(self) -> str:
        return f"Subscription({self.channel!r}, sid={self.sid}, kind={self.kind!r})"


class _OutSub:
    """Owner side of one subscription (the subscriber is the remote peer)."""

    __slots__ = ("peer", "channel", "sid", "version", "kind", "seq", "ready", "active",
                 "queued", "queued_bytes", "state_pending", "ring", "ring_max", "dropped",
                 "options")

    def __init__(self, peer: Peer, channel: Channel, sid: int, version: int,
                 options: dict[str, Any] | None) -> None:
        self.peer = peer
        self.channel = channel
        self.sid = sid
        self.version = version
        self.kind = channel.kind
        self.seq = 0
        self.ready = False
        self.active = True
        self.queued = 0
        self.queued_bytes = 0
        self.state_pending: tuple[int, int, bytes] | None = None
        self.ring: deque[tuple[int, int, bytes]] = deque()
        self.ring_max = 0
        self.dropped = False
        self.options = options


# Pre-packed MessagePack pieces of the hot messages (a 5-entry map).
_EVT_HEAD = b"\x85" + wire.pack("t") + wire.pack("evt") + wire.pack("sid")
_K_SEQ, _K_TS, _K_D = wire.pack("seq"), wire.pack("ts"), wire.pack("d")


def evt_frame(sid: int, seq: int, ts: int, packed_data: bytes) -> bytes:
    body = b"".join((_EVT_HEAD, wire.pack(sid), _K_SEQ, wire.pack(seq), _K_TS, wire.pack(ts),
                     _K_D, packed_data))
    return wire.HEADER.pack(len(body) + 1, wire.KIND_MESSAGE) + body


class Peer:
    """The remote end of one authenticated connection.

    ``info``: who it says it is; ``pid``: its process id from the OS
    (``GetNamedPipeClientProcessId`` on a server, ``...ServerProcessId`` on a client);
    ``decl``: what it offers; ``run``: the server's run id; ``version``: the protocol
    version spoken.
    """

    def __init__(self, node: _Node, pipe: PipeHandle, *, info: PeerInfo, decl: Declarations,
                 version: int, run: str, peer_max_frame: int, peer_max_message: int,
                 heartbeat_ms: int, parser: wire.FrameParser, early: list[tuple[int, bytes]],
                 server_side: bool, pipe_name: str, features: list[str]) -> None:
        self.node = node
        self.pipe = pipe
        self.info = info
        self.decl = decl
        self.version = version
        self.run = run
        self.pid = pipe.peer_pid
        self.server_side = server_side
        self.pipe_name = pipe_name
        self.features = features
        self.heartbeat_ms = heartbeat_ms
        self.peer_max_frame = peer_max_frame
        self.peer_max_message = peer_max_message
        self.connected_at = time.time()
        self.closed = threading.Event()
        self.close_code: str | None = None
        self.close_message = ""
        self._parser = parser
        self._early = early
        self._cv = threading.Condition(threading.Lock())
        self._fifo: deque[tuple[Any, ...]] = deque()
        self._ready_rings: list[_OutSub] = []
        self._closing = False
        self._writing = False
        self._inflight = False
        self._discard = False
        self._torn_down = False
        self._reader_done = threading.Event()
        self._ids = itertools.count(1)
        self._sids = itertools.count(1)
        self._calls: dict[int, PendingCall] = {}
        self._sub_waits: dict[int, tuple[Subscription, PendingCall]] = {}
        self._abandoned: set[int] = set()  # sub ids that timed out before their res
        self._insubs: dict[int, Subscription] = {}
        self._outsubs: dict[int, _OutSub] = {}
        self._inbound: dict[int, Context] = {}
        self._lock = threading.Lock()
        self._lane = Lane()  # subscription callbacks, in order
        self._serial_lane = Lane()  # ordered methods
        self._work: list[Callable[[], None]] = []
        self._reader_ident: int | None = None
        self._pings: dict[int, threading.Event] = {}
        self.last_recv = time.monotonic()
        self.last_send = time.monotonic()
        self.rtt_ns: int | None = None
        self._chunk_data = wire.chunk_data_for(peer_max_frame)

    # ------------------------------------------------------------------ lifecycle

    def _start(self) -> None:
        self._sender = threading.Thread(target=self._send_loop, daemon=True,
                                        name=f"ipc-send-{self.info.app}")
        self._sender.start()
        early, self._early = deque(self._early), []
        self.node._workers.run(partial(self._read_loop, early))

    @property
    def is_open(self) -> bool:
        return not self._closing and not self.closed.is_set()

    def close(self, code: str = E.NORMAL, message: str = "") -> None:
        """Send ``bye`` and close (after the peer closes or 1 s)."""
        with self._cv:
            if self._closing:
                return
            self._closing = True
            self.close_code = self.close_code or code
            self.close_message = self.close_message or message
            self._fifo.append(("x", wire.message_frame({"t": "bye", "code": code,
                                                        "msg": message})))
            self._cv.notify()

    def abort(self) -> None:
        """Close at once, without ``bye``."""
        with self._cv:
            self._closing = True
            self._cv.notify()
        self.pipe.close()

    def wait_closed(self, timeout: float | None = None) -> bool:
        return self.closed.wait(timeout)

    def _teardown(self) -> None:
        with self._cv:
            if self._torn_down:
                return
            self._torn_down = True
            self._closing = True
            self._cv.notify()
        self.pipe.close()
        lost = ConnectionLost(f"connection to {self.info.app} closed"
                              + (f" ({self.close_code})" if self.close_code else ""))
        with self._lock:
            calls = list(self._calls.values())
            self._calls.clear()
            waits = list(self._sub_waits.values())
            self._sub_waits.clear()
            insubs = list(self._insubs.values())
            self._insubs.clear()
            outsubs = list(self._outsubs.values())
            self._outsubs.clear()
            inbound = list(self._inbound.values())
            pings = list(self._pings.values())
            self._pings.clear()
        for c in calls:
            c._finish(error=lost)
        for _s, c in waits:
            c._finish(error=lost)
        for ctx in inbound:
            ctx._cancel()
        for ev in pings:
            ev.set()
        for o in outsubs:
            o.active = False
            o.channel._remove(o)
        for s in insubs:
            self._end_sub(s, E.DISCONNECTED, "connection closed")
        self.closed.set()
        try:
            self.node._peer_closed(self)
        except Exception:  # noqa: BLE001
            log.exception("peer-closed hook failed")

    # ------------------------------------------------------------------ sending

    # The write side has one owner at a time (``_writing``). When it is free and nothing is
    # queued, the producing thread writes its frame itself (no thread hop); a write the pipe
    # cannot take at once is handed to the sender thread to finish (``_inflight``), so a
    # producer never waits for a slow reader. Otherwise frames are queued for the sender.

    def _idle(self) -> bool:  # with self._cv
        return not self._writing and not self._fifo and not self._ready_rings

    def _inline_write(self, frame: bytes) -> None:
        """Write ``frame`` from this thread; the caller set ``_writing`` under the lock."""
        try:
            done = self.pipe.write_begin(frame)
        except PipeClosed:
            done = True  # the reader sees the broken pipe and tears down
        self.last_send = time.monotonic()
        with self._cv:
            if done:
                self._writing = False
                if self._fifo or self._ready_rings:
                    self._cv.notify()
            else:
                self._inflight = True
                self._cv.notify()

    def _enqueue(self, item: tuple[Any, ...]) -> None:
        with self._cv:
            if self._closing:
                raise NotConnected(f"connection to {self.info.app} is closing")
            if item[0] == "f" and self._idle():
                self._writing = True
            else:
                self._fifo.append(item)
                self._cv.notify()
                return
        self._inline_write(item[1])

    def send_message(self, msg: dict[str, Any]) -> None:
        """Send one kind-1 message (chunked when larger than one frame may be)."""
        encoded = wire.pack(msg)
        if len(encoded) > self.peer_max_message:
            raise ValueError(f"message of {len(encoded)} bytes exceeds the peer's max_message")
        if len(encoded) + 1 > min(self.peer_max_frame, wire.CHUNK_DATA):
            self._enqueue(("b", encoded))
        else:
            self._enqueue(("f", wire.frame(wire.KIND_MESSAGE, encoded)))

    def _send_raw_frame(self, frame: bytes) -> None:
        """Send a pre-built frame as is (the conformance tool uses it for error cases)."""
        self._enqueue(("f", frame))

    @staticmethod
    def _sample_frame(sub: _OutSub, seq: int, t: int, body: bytes) -> bytes:
        flags = STREAM_FLAG_DROPPED if sub.dropped else 0
        sub.dropped = False
        return wire.HEADER.pack(STREAM_HEADER.size + len(body) + 1, wire.KIND_STREAM) \
            + STREAM_HEADER.pack(sub.sid, seq & 0xFFFFFFFF, t, flags) + body

    def _collect_streams(self, out: list[bytes]) -> int:  # with self._cv
        size = 0
        rings, self._ready_rings = self._ready_rings, []
        for sub in rings:
            if not sub.active or not sub.ready:
                continue
            ring = sub.ring
            while ring:
                seq, t, body = ring.popleft()
                fr = self._sample_frame(sub, seq, t, body)
                out.append(fr)
                size += len(fr)
        return size

    def _take_batch(self, out: list[bytes], after: list[_OutSub]) -> tuple[bytes | None, bool]:
        """Move queued frames into ``out`` (with self._cv); returns (big message, bye)."""
        size = self._collect_streams(out) if self._ready_rings else 0
        fifo = self._fifo
        while fifo and size < BATCH_BYTES:
            item = fifo.popleft()
            tag = item[0]
            if tag == "f":
                out.append(item[1])
                size += len(item[1])
            elif tag == "e":
                sub = item[2]
                sub.queued -= 1
                sub.queued_bytes -= len(item[1])
                if sub.active:
                    out.append(item[1])
                    size += len(item[1])
            elif tag == "s":
                sub = item[1]
                p = sub.state_pending
                sub.state_pending = None
                if sub.active and p is not None:
                    fr = evt_frame(sub.sid, p[0], p[1], p[2])
                    out.append(fr)
                    size += len(fr)
            elif tag == "r":
                out.append(item[1])
                size += len(item[1])
                after.append(item[2])
            elif tag == "b":
                return item[1], False
            elif tag == "x":
                out.append(item[1])
                return None, True
        return None, False

    def _send_loop(self) -> None:
        pipe = self.pipe
        bye = False
        try:
            while True:
                out: list[bytes] = []
                after: list[_OutSub] = []
                big: bytes | None = None
                with self._cv:
                    while True:
                        if self._torn_down:
                            return
                        if self._inflight or (not self._writing
                                              and (self._fifo or self._ready_rings)):
                            break
                        self._cv.wait()
                    finish = self._inflight
                    if finish:
                        self._inflight = False
                    else:
                        self._writing = True
                        big, bye = self._take_batch(out, after)
                if finish:
                    pipe.write_finish()
                else:
                    if out:
                        pipe.write(out[0] if len(out) == 1 else b"".join(out))
                    if after:
                        with self._cv:
                            for sub in after:
                                sub.ready = True
                                if sub.ring:
                                    self._ready_rings.append(sub)
                    if big is not None:
                        for fr in wire.chunk_frames(big, self._chunk_data):
                            pipe.write(bytes(fr))
                            if self._ready_rings:
                                streams: list[bytes] = []
                                with self._cv:
                                    self._collect_streams(streams)
                                if streams:
                                    pipe.write(b"".join(streams))
                self.last_send = time.monotonic()
                with self._cv:
                    self._writing = False
                if bye:
                    break
        except PipeClosed:
            pass
        except Exception:  # noqa: BLE001
            log.exception("sender failed")
        finally:
            if bye:
                self._reader_done.wait(BYE_LINGER_S)
            self.pipe.close()

    # ------------------------------------------------------------------ owner side

    def _offer_event(self, sub: _OutSub, ts: int, packed: bytes) -> None:
        overloaded = False
        inline = False
        with self._cv:
            if not sub.active or self._closing:
                return
            sub.seq += 1
            fr = evt_frame(sub.sid, sub.seq, ts, packed)
            if self._idle():
                self._writing = True
                inline = True
            else:
                node = self.node
                if sub.queued + 1 > node.event_queue or \
                        sub.queued_bytes + len(fr) > node.event_queue_bytes:
                    sub.active = False
                    overloaded = True
                else:
                    sub.queued += 1
                    sub.queued_bytes += len(fr)
                    self._fifo.append(("e", fr, sub))
                    self._cv.notify()
        if inline:
            self._inline_write(fr)
        elif overloaded:
            self._drop_outsub(sub, E.OVERLOADED, "subscriber too slow: queue full")

    def _offer_state(self, sub: _OutSub, ts: int, packed: bytes) -> None:
        with self._cv:
            if not sub.active or self._closing:
                return
            sub.seq += 1
            if sub.state_pending is None and self._idle():
                self._writing = True
                fr = evt_frame(sub.sid, sub.seq, ts, packed)
            else:
                pending = sub.state_pending is not None
                sub.state_pending = (sub.seq, ts, packed)
                if not pending:
                    self._fifo.append(("s", sub))
                    self._cv.notify()
                return
        self._inline_write(fr)

    def _offer_sample(self, sub: _OutSub, t_ns: int, body: bytes) -> None:
        with self._cv:
            if not sub.active or self._closing:
                return
            sub.seq += 1
            if sub.ready and self._idle():
                self._writing = True
                fr = self._sample_frame(sub, sub.seq, t_ns, body)
            else:
                ring = sub.ring
                if len(ring) >= sub.ring_max:
                    ring.popleft()
                    sub.dropped = True
                was_empty = not ring
                ring.append((sub.seq, t_ns, body))
                if was_empty and sub.ready:
                    self._ready_rings.append(sub)
                    self._cv.notify()
                return
        self._inline_write(fr)

    def _drop_outsub(self, sub: _OutSub, code: str, message: str) -> None:
        """The owner ends a subscription (``end`` with a code, SPEC.md 10.7)."""
        sub.active = False
        with self._lock:
            self._outsubs.pop(sub.sid, None)
        sub.channel._remove(sub)
        try:
            self._enqueue(("f", wire.message_frame({"t": "end", "sid": sub.sid,
                                                    "code": code, "msg": message})))
        except NotConnected:
            pass

    # ------------------------------------------------------------------ calls

    def call_async(self, method: str, params: Any = None, *, version: int = 1,
                   timeout: float | None = 5.0, idempotency_key: str | None = None,
                   on_progress: Callable[[Any], None] | None = None) -> PendingCall:
        rid = next(self._ids)
        pc = PendingCall(self, rid, method, timeout, on_progress)
        msg: dict[str, Any] = {"t": "req", "id": rid, "m": method, "v": version,
                               "p": {} if params is None else params}
        if timeout is not None:
            msg["dl"] = max(1, int(timeout * 1000))
        if idempotency_key is not None:
            msg["ik"] = idempotency_key
        with self._lock:
            self._calls[rid] = pc
        try:
            self.send_message(msg)
        except BaseException:
            with self._lock:
                self._calls.pop(rid, None)
            raise
        return pc

    def call(self, method: str, params: Any = None, *, version: int = 1,
             timeout: float | None = 5.0, idempotency_key: str | None = None,
             on_progress: Callable[[Any], None] | None = None) -> Any:
        """Call a method of the peer; returns ``r`` or raises :class:`RemoteError`,
        :class:`CallTimeout`, :class:`ConnectionLost` or :class:`NotConnected`."""
        self._not_on_reader("call")
        return self.call_async(method, params, version=version, timeout=timeout,
                               idempotency_key=idempotency_key,
                               on_progress=on_progress).result()

    def _not_on_reader(self, what: str) -> None:
        if self._reader_ident == threading.get_ident():
            raise RuntimeError(f"{what}() would wait for a response on this connection's "
                               "reader thread (an inline callback or method): use "
                               "call_async(), or deliver='thread'")

    def _give_up(self, pc: PendingCall, error: BaseException, send_cancel: bool = True) -> bool:
        """Stop waiting for ``pc``: forget it, tell the owner (``cancel``, not for a ``sub``
        wait: a late ``sid`` gets ``unsub``) and fail it with ``error``. The pop from the
        in-flight tables decides a race with the response: True when this call won it."""
        with self._lock:
            if self._calls.pop(pc.id, None) is None:
                if self._sub_waits.pop(pc.id, None) is None:
                    return False
                self._abandoned.add(pc.id)
                send_cancel = False
        if send_cancel:
            try:
                self._enqueue(("f", wire.message_frame({"t": "cancel", "id": pc.id})))
            except NotConnected:
                pass
        pc._finish(error=error)
        return True

    def _expire_calls(self, now: float) -> None:
        """Fail every call whose deadline passed (the heartbeat tick), so a call nobody
        waits on still ends on time."""
        with self._lock:
            if not self._calls and not self._sub_waits:
                return
            late = [pc for pc in self._calls.values()
                    if pc.deadline is not None and now >= pc.deadline]
            late += [pc for _, pc in self._sub_waits.values()
                     if pc.deadline is not None and now >= pc.deadline]
        for pc in late:
            self._give_up(pc, CallTimeout(f"{pc.method}: no response in time"))

    def ping(self, timeout: float = 2.0) -> float:
        """Round trip in seconds (``ping``/``pong``)."""
        ts = now_ns()
        ev = threading.Event()
        with self._lock:
            self._pings[ts] = ev
        self._enqueue(("f", wire.message_frame({"t": "ping", "ts": ts})))
        if not ev.wait(timeout) or self.closed.is_set():
            with self._lock:
                self._pings.pop(ts, None)
            raise CallTimeout("no pong")
        return (now_ns() - ts) / 1e9

    # ------------------------------------------------------------------ subscriptions

    def subscribe(self, channel: str, callback: Callable[[Any], None], *, version: int = 1,
                  options: dict[str, Any] | None = None,
                  on_end: Callable[[Subscription, str, str], None] | None = None,
                  deliver: str = "auto", timeout: float = 5.0) -> Subscription:
        """Subscribe to a channel of the peer. ``callback(Event)`` for event and state
        channels (a state's snapshot first, ``snapshot=True``), ``callback(Sample)`` for
        streams; ``on_end(sub, code, message)`` when it ends.

        ``deliver``: ``"thread"``: the connection's callback lane on a worker thread, one
        callback at a time in arrival order; it may block and call the peer. ``"inline"``:
        the reader thread: lowest latency and CPU, but the callback must be quick and
        cannot wait on the peer. ``"auto"`` (default): inline for streams (high-rate,
        quick callbacks: 0.5 % less CPU per 200 Hz stream), thread for events and state."""
        if deliver not in ("thread", "inline", "auto"):
            raise ValueError("deliver must be 'auto', 'thread' or 'inline'")
        self._not_on_reader("subscribe")
        sub = Subscription(self, channel, version, callback, on_end, deliver, options)
        rid = next(self._ids)
        pc = PendingCall(self, rid, f"sub {channel}", timeout, None)
        msg: dict[str, Any] = {"t": "sub", "id": rid, "ch": channel, "v": version}
        if options:
            msg["o"] = options
        with self._lock:
            self._sub_waits[rid] = (sub, pc)
        try:
            self.send_message(msg)
        except BaseException:
            with self._lock:
                self._sub_waits.pop(rid, None)
            raise
        pc.result()
        return sub

    def _unsubscribe(self, sub: Subscription) -> None:
        with self._lock:
            known = self._insubs.pop(sub.sid, None) is not None
        if known:
            try:
                self._enqueue(("f", wire.message_frame({"t": "unsub", "sid": sub.sid})))
            except NotConnected:
                pass
            self._end_sub(sub, E.NORMAL, "unsubscribed")

    def _end_sub(self, sub: Subscription, code: str, message: str) -> None:
        if sub.ended.is_set():
            return
        sub.end_code, sub.end_message = code, message
        sub.ended.set()
        if sub.on_end is not None:
            self._dispatch(sub.on_end, sub, code, message)

    # ------------------------------------------------------------------ dispatch

    def _schedule(self, work: Callable[[], None] | None) -> None:
        """Run ``work`` off the reader: on the reader's own thread after it hands the
        reading over (leader/follower), or on a worker when called from another thread."""
        if work is None:
            return
        if self._reader_ident == threading.get_ident():
            self._work.append(work)
        else:
            self.node._workers.run(work)

    def _dispatch(self, fn: Callable[..., Any], *args: Any) -> None:
        """Run ``fn(*args)`` in this connection's callback lane (in order, off the
        reader)."""
        self._schedule(self._lane.offer(partial(self._run_cb, fn, args)))

    @staticmethod
    def _run_cb(fn: Callable[..., Any], args: tuple[Any, ...]) -> None:
        try:
            fn(*args)
        except Exception:  # noqa: BLE001
            log.exception("subscription callback failed")

    def _deliver(self, sub: Subscription, item: Any) -> None:
        """Hand ``item`` to the subscription's callback. ``inline``: now, on the reader.
        ``thread``: through the connection's callback lane, with the same bounded-queue
        rules as the owner (SPEC.md 12) so a slow callback never makes the reader stop or
        memory grow: streams drop the oldest (the next sample says ``dropped``), state keeps
        the latest, events end the subscription ``overloaded``."""
        if sub.deliver == "inline":
            self._run_cb(sub.callback, (item,))
            return
        pending = sub.pending
        if sub.kind == STATE and pending:
            pending[-1] = item  # coalesce: the queued token delivers the latest value
            return
        if len(pending) >= sub.limit:
            if sub.kind == STREAM:
                pending.popleft()
                item.dropped = True
                sub.local_drops += 1
                pending.append(item)
                return
            pending.clear()
            self._local_overload(sub)
            return
        pending.append(item)
        self._schedule(self._lane.offer(partial(self._deliver_one, sub)))

    @staticmethod
    def _deliver_one(sub: Subscription) -> None:
        try:
            item = sub.pending.popleft()
        except IndexError:
            return  # dropped or coalesced
        if not sub.ended.is_set():
            sub.callback(item)

    def _local_overload(self, sub: Subscription) -> None:
        with self._lock:
            known = self._insubs.pop(sub.sid, None) is not None
        if known:
            try:
                self._enqueue(("f", wire.message_frame({"t": "unsub", "sid": sub.sid})))
            except NotConnected:
                pass
            self._end_sub(sub, E.OVERLOADED, "callback too slow: local queue full")

    # ------------------------------------------------------------------ receiving

    def _read_loop(self, frames: deque[tuple[int, bytes]]) -> None:
        """Holds the "reader baton": reads and handles frames; when a frame produced work
        (a handler, a callback) it starts a new reader with the frames left and runs the
        work itself."""
        self._reader_ident = threading.get_ident()
        read = self.pipe.read
        feed = self._parser.feed
        try:
            while True:
                while frames:
                    kind, payload = frames.popleft()
                    self._on_frame(kind, payload)
                    if self._discard:
                        frames.clear()
                    elif self._work:
                        work, self._work = self._work, []
                        self._reader_ident = None
                        self.node._workers.run(partial(self._read_loop, frames))
                        for w in work:
                            w()
                        return
                data = read()
                self.last_recv = time.monotonic()
                if self._discard:
                    continue
                try:
                    frames = deque(feed(data))
                except ProtocolError as exc:
                    self._protocol_error(str(exc))
        except PipeClosed:
            pass
        except Exception:  # noqa: BLE001
            log.exception("reader failed")
        self._reader_ident = None
        work, self._work = self._work, []
        self._reader_done.set()
        self._teardown()
        for w in work:
            self.node._workers.run(w)

    def _protocol_error(self, message: str) -> None:
        log.warning("%s: protocol error: %s", self.info.app, message)
        self._discard = True
        self.close_code = E.PROTOCOL_ERROR
        self.close_message = message
        self.close(E.PROTOCOL_ERROR, message)

    def _on_frame(self, kind: int, payload: bytes) -> None:
        if kind == wire.KIND_STREAM:
            self._on_sample(payload)
            return
        try:
            msg = wire.unpack_message(payload)
        except ProtocolError as exc:
            self._protocol_error(str(exc))
            return
        handler = _HANDLERS.get(msg["t"])
        if handler is None:
            return  # unknown message types are ignored (SPEC.md 10.1)
        try:
            handler(self, msg)
        except ProtocolError as exc:
            self._protocol_error(str(exc))
        except (KeyError, TypeError, ValueError) as exc:
            self._protocol_error(f"malformed {msg['t']} message: {exc!r}")

    def _on_sample(self, payload: bytes) -> None:
        sid, seq, t_ns, flags = STREAM_HEADER.unpack_from(payload)
        sub = self._insubs.get(sid)
        if sub is None:
            return  # after an unsub, or never subscribed: ignore
        if sub.kind != STREAM:
            self._protocol_error(f"stream frame for the non-stream subscription {sid}")
            return
        body = payload[STREAM_HEADER.size:]
        assert sub.layout is not None
        if len(body) != sub.layout.size:
            self._protocol_error(f"stream {sub.channel}: body of {len(body)} bytes, layout "
                                 f"says {sub.layout.size}")
            return
        sub.last_seq = seq
        self._deliver(sub, Sample(sub.channel, seq, t_ns, bool(flags & STREAM_FLAG_DROPPED),
                                  body, sub.layout))

    # message handlers (reader thread) ------------------------------------------

    def _h_req(self, msg: dict[str, Any]) -> None:
        self.node._on_request(self, msg)

    def _h_res(self, msg: dict[str, Any]) -> None:
        rid = msg["id"]
        with self._lock:
            pc = self._calls.pop(rid, None)
            wait = self._sub_waits.pop(rid, None) if pc is None else None
        ok = msg.get("ok")
        if pc is not None:
            if ok is True:
                pc._finish(result=msg.get("r"))
            else:
                pc._finish(error=RemoteError.from_wire(msg.get("e")))
            return
        if wait is None:
            with self._lock:
                late = rid in self._abandoned
                self._abandoned.discard(rid)
            r = msg.get("r")
            if late and ok is True and isinstance(r, dict) and isinstance(r.get("sid"), int):
                try:  # a subscription nobody waits for any more
                    self._enqueue(("f", wire.message_frame({"t": "unsub", "sid": r["sid"]})))
                except NotConnected:
                    pass
            return
        sub, spc = wait
        if ok is not True:
            spc._finish(error=RemoteError.from_wire(msg.get("e")))
            return
        r = msg.get("r")
        if not isinstance(r, dict) or not isinstance(r.get("sid"), int):
            spc._finish(error=ProtocolError("sub response without a sid"))
            self._protocol_error("sub response without a sid")
            return
        sub.sid = r["sid"]
        sub.kind = r.get("kind", "")
        if sub.deliver == "auto":
            sub.deliver = "inline" if sub.kind == STREAM else "thread"
        if sub.kind == STREAM:
            try:
                sub.layout = Layout.from_wire(r.get("layout"))
            except (ValueError, TypeError) as exc:
                spc._finish(error=ProtocolError(f"bad stream layout: {exc}"))
                self._protocol_error(f"bad stream layout: {exc}")
                return
        if sub.kind == STREAM:
            cd = self.decl.channels.get(sub.channel)
            sub.limit = max(64, int(cd.rate_hz)) if cd is not None and cd.rate_hz else 256
        elif sub.kind == STATE:
            sub.limit = 1
        else:
            sub.limit = self.node.event_queue
        with self._lock:
            self._insubs[sub.sid] = sub
        if sub.kind == STATE:
            sub.snapshot = r.get("snap")
            sub.last_seq = r.get("seq", 0)
            self._deliver(sub, Event(sub.channel, sub.last_seq, r.get("ts"), sub.snapshot,
                                     snapshot=True))
        spc._finish(result=sub)

    def _h_prog(self, msg: dict[str, Any]) -> None:
        with self._lock:
            pc = self._calls.get(msg["id"])
        if pc is None:
            return
        dl = msg.get("dl")
        if isinstance(dl, int) and dl > 0:
            pc.deadline = time.perf_counter() + dl / 1000
        if pc.on_progress is not None:
            self._dispatch(pc.on_progress, msg.get("d"))

    def _h_cancel(self, msg: dict[str, Any]) -> None:
        with self._lock:
            ctx = self._inbound.get(msg["id"])
        if ctx is not None:
            ctx._cancel()

    def _h_sub(self, msg: dict[str, Any]) -> None:
        self.node._on_subscribe(self, msg)

    def _h_unsub(self, msg: dict[str, Any]) -> None:
        """The subscriber ends one of our subscriptions (``sid`` is ours)."""
        with self._lock:
            out = self._outsubs.pop(msg["sid"], None)
        if out is not None:
            out.active = False
            out.channel._remove(out)

    def _h_end(self, msg: dict[str, Any]) -> None:
        """The owner ended our subscription (``sid`` is the owner's, SPEC.md 10.7)."""
        with self._lock:
            ins = self._insubs.pop(msg["sid"], None)
        if ins is not None:
            code = msg.get("code")
            self._end_sub(ins, code if isinstance(code, str) else E.CHANNEL_CLOSED,
                          msg.get("msg") if isinstance(msg.get("msg"), str) else "")

    def _h_evt(self, msg: dict[str, Any]) -> None:
        sub = self._insubs.get(msg["sid"])
        if sub is None:
            return
        if sub.kind == STREAM:
            self._protocol_error(f"evt for the stream subscription {sub.sid}")
            return
        seq = msg.get("seq", 0)
        sub.last_seq = seq
        if sub.kind == STATE:
            sub.snapshot = msg.get("d")
        self._deliver(sub, Event(sub.channel, seq, msg.get("ts"), msg.get("d")))

    def _h_decl(self, msg: dict[str, Any]) -> None:
        try:
            self.decl = Declarations.from_wire(msg.get("decl"))
        except ValueError as exc:
            self._protocol_error(f"bad decl: {exc}")
            return
        self.node._peer_redeclared(self)

    def _h_ping(self, msg: dict[str, Any]) -> None:
        reply: dict[str, Any] = {"t": "pong"}
        if "ts" in msg:
            reply["ts"] = msg["ts"]
        try:
            self._enqueue(("f", wire.message_frame(reply)))
        except NotConnected:
            pass

    def _h_pong(self, msg: dict[str, Any]) -> None:
        ts = msg.get("ts")
        if isinstance(ts, int):
            self.rtt_ns = now_ns() - ts
            with self._lock:
                ev = self._pings.pop(ts, None)
            if ev is not None:
                ev.set()

    def _h_bye(self, msg: dict[str, Any]) -> None:
        code = msg.get("code")
        self.close_code = code if isinstance(code, str) else E.NORMAL
        self.close_message = msg.get("msg") if isinstance(msg.get("msg"), str) else ""
        self._discard = True
        with self._cv:
            self._closing = True
        self.pipe.close()

    def _h_handshake(self, msg: dict[str, Any]) -> None:
        self._protocol_error(f"{msg['t']} after the handshake")

    # ------------------------------------------------------------------ misc

    def __repr__(self) -> str:
        return (f"Peer(app={self.info.app!r}, process={self.info.process!r}, pid={self.pid}, "
                f"open={self.is_open})")


_HANDLERS: dict[str, Callable[[Peer, dict[str, Any]], None]] = {
    "req": Peer._h_req, "res": Peer._h_res, "prog": Peer._h_prog, "cancel": Peer._h_cancel,
    "sub": Peer._h_sub, "unsub": Peer._h_unsub, "end": Peer._h_end, "evt": Peer._h_evt, "decl": Peer._h_decl,
    "ping": Peer._h_ping, "pong": Peer._h_pong, "bye": Peer._h_bye,
    "hello": Peer._h_handshake, "auth": Peer._h_handshake, "welcome": Peer._h_handshake,
}

__all__ = ["Event", "Sample", "PendingCall", "Subscription", "Peer", "EVENT", "STATE",
           "STREAM"]
