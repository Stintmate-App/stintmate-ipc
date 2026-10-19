"""A raw protocol connection for the conformance checker and the tests: frames by hand,
handshakes with chosen nonces or wrong MACs, no threads of its own."""

from __future__ import annotations

import time
from collections import deque
from typing import Any

from .. import _win32, wire
from .. import handshake as H
from ..errors import PipeClosed, ProtocolError
from ..naming import ascii_lower, split_pipe


class Closed(Exception):
    """The peer closed the connection."""


class RawConn:
    """One pipe connection driven by hand. :meth:`recv` returns ``("msg", dict)`` or
    ``("stream", (sid, seq, t_ns, flags, body))``."""

    def __init__(self, pipe: _win32.PipeHandle, short: str) -> None:
        self.pipe = pipe
        self.short = short
        self.parser = wire.FrameParser(wire.DEFAULT_MAX_FRAME, wire.DEFAULT_MAX_MESSAGE)
        self.frames: deque[tuple[int, bytes]] = deque()
        self.peer_max_frame = wire.MIN_MAX_FRAME
        self.version = 2
        self.hello: dict[str, Any] | None = None
        self.welcome: dict[str, Any] | None = None
        self.auth: dict[str, Any] | None = None

    @classmethod
    def open(cls, pipe: str, timeout: float = 5.0) -> RawConn:
        path, short = split_pipe(pipe)
        end = time.monotonic() + timeout
        while True:
            try:
                return cls(_win32.open_pipe(path), ascii_lower(short))
            except _win32.PipeBusy:
                if time.monotonic() > end:
                    raise

    # ------------------------------------------------------------------ I/O

    def send_frame(self, data: bytes) -> None:
        self.pipe.write(data, 5000)

    def send(self, msg: dict[str, Any]) -> None:
        self.send_frame(wire.message_frame(msg))

    def send_chunked(self, msg: dict[str, Any], chunk: int = 16) -> None:
        for fr in wire.chunk_frames(wire.pack(msg), chunk):
            self.send_frame(bytes(fr))

    def _fill(self, deadline: float) -> None:
        while not self.frames:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("nothing received in time")
            try:
                data = self.pipe.read(max(1, int(left * 1000)))
            except PipeClosed:
                raise Closed("the peer closed the connection") from None
            self.frames.extend(self.parser.feed(data))

    def recv(self, timeout: float = 5.0) -> tuple[str, Any]:
        self._fill(time.monotonic() + timeout)
        kind, payload = self.frames.popleft()
        if kind == wire.KIND_STREAM:
            sid, seq, t_ns, flags = wire.STREAM_HEADER.unpack_from(payload)
            return "stream", (sid, seq, t_ns, flags, payload[wire.STREAM_HEADER.size:])
        return "msg", wire.unpack_message(payload)

    def recv_msg(self, timeout: float = 5.0, skip: tuple[str, ...] = ("ping", "pong", "decl")
                 ) -> dict[str, Any]:
        """The next kind-1 message whose ``t`` is not in ``skip`` (pings are answered)."""
        end = time.monotonic() + timeout
        while True:
            kind, m = self.recv(max(0.001, end - time.monotonic()))
            if kind != "msg":
                continue
            if m["t"] == "ping":
                self.send({"t": "pong", **({"ts": m["ts"]} if "ts" in m else {})})
            if m["t"] in skip:
                continue
            return m

    def wait_closed(self, timeout: float = 5.0) -> list[dict[str, Any]]:
        """Read until the peer closes; returns the messages seen (a ``bye`` among them)."""
        seen: list[dict[str, Any]] = []
        end = time.monotonic() + timeout
        while True:
            try:
                kind, m = self.recv(max(0.001, end - time.monotonic()))
            except Closed:
                return seen
            except ProtocolError:
                return seen
            if kind == "msg":
                seen.append(m)

    def close(self) -> None:
        self.pipe.close()

    # ------------------------------------------------------------------ handshakes

    def client_handshake(self, key: bytes, peer: dict[str, Any] | None = None,
                         decl: dict[str, Any] | None = None, *, nonce: bytes | None = None,
                         bad_mac: bool = False, version: int = 2,
                         extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """Act as a client: read ``hello``, send ``auth``; returns the ``welcome`` (or the
        ``bye``) message. Checks the server's MAC into ``self.server_mac_ok``."""
        hello = self.recv_msg(skip=())
        self.hello = hello
        s_nonce = hello.get("nonce", b"")
        c_nonce = nonce or H.new_nonce()
        mac = H.client_mac(key, version, s_nonce if isinstance(s_nonce, bytes) else b"",
                           c_nonce, self.short)
        if bad_mac:
            mac = bytes([mac[0] ^ 0xFF]) + mac[1:]
        auth = {"t": "auth", "v": version, "nonce": c_nonce, "mac": mac,
                "peer": peer or {"app": "ipc-check", "name": "stintmate-ipc-check",
                                 "version": "0.1.0"},
                "decl": decl or {"methods": {}, "channels": {}},
                "max_frame": wire.DEFAULT_MAX_FRAME, "max_message": wire.DEFAULT_MAX_MESSAGE}
        if extra:
            auth.update(extra)
        self.auth = auth
        self.send(auth)
        reply = self.recv_msg(skip=("decl",))
        if reply.get("t") == "welcome":
            self.welcome = reply
            self.server_mac_ok = reply.get("mac") == H.server_mac(
                key, version, s_nonce, c_nonce, self.short)
            pmf = hello.get("max_frame")
            self.peer_max_frame = pmf if isinstance(pmf, int) else wire.MIN_MAX_FRAME
        return reply

    def server_handshake(self, key: bytes, peer: dict[str, Any] | None = None,
                         decl: dict[str, Any] | None = None, *, nonce: bytes | None = None,
                         bad_mac: bool = False, heartbeat_ms: int = 5000,
                         run: str = "check-run") -> dict[str, Any]:
        """Act as a server on an accepted pipe: send ``hello``, read ``auth``, verify it and
        send ``welcome`` (with a wrong MAC when ``bad_mac``). Returns the ``auth``; raises
        ``PermissionError`` on a wrong client MAC (after sending ``bye unauthorized``)."""
        s_nonce = nonce or H.new_nonce()
        self.send(H.hello_message(s_nonce, wire.DEFAULT_MAX_FRAME)
                  | {"max_message": wire.DEFAULT_MAX_MESSAGE})
        auth = self.recv_msg(skip=())
        self.auth = auth
        if auth.get("t") != "auth":
            raise ProtocolError(f"expected auth, got {auth.get('t')}")
        c_nonce = auth.get("nonce")
        if not isinstance(c_nonce, bytes) or auth.get("mac") != H.client_mac(
                key, auth.get("v", 2), s_nonce, c_nonce, self.short):
            self.send({"t": "bye", "code": "unauthorized", "msg": "authentication failed"})
            raise PermissionError("wrong client MAC")
        mac = H.server_mac(key, 2, s_nonce, c_nonce, self.short)
        if bad_mac:
            mac = bytes([mac[0] ^ 0xFF]) + mac[1:]
        self.send({"t": "welcome", "v": 2, "mac": mac, "run": run,
                   "peer": peer or {"app": "ipc-check", "name": "stintmate-ipc-check",
                                    "version": "0.1.0"},
                   "decl": decl or {"methods": {}, "channels": {}},
                   "heartbeat_ms": heartbeat_ms, "features": []})
        pmf = auth.get("max_frame")
        self.peer_max_frame = pmf if isinstance(pmf, int) else wire.MIN_MAX_FRAME
        return auth
