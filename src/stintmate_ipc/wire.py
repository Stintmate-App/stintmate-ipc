"""Framing and encoding (SPEC.md sections 5 and 6).

::

    frame   = u32le length | u8 kind | payload        ; length = 1 + len(payload)
    kind 1  = message: one MessagePack map with a string "t"
    kind 2  = stream sample: u32le sid | u32le seq | i64le t_ns | u32le flags | body
    kind 3  = chunk of a message: u32le total | u32le offset | bytes
    kinds 0x80-0xFF: reserved, skipped by receivers; 0x00 and 0x04-0x7F: protocol error
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import Any

import msgpack

from .errors import ProtocolError

PROTOCOL_NAME = "stintmate-ipc"
PROTOCOL_VERSION = 2
SUPPORTED_VERSIONS = (2,)

KIND_MESSAGE = 0x01
KIND_STREAM = 0x02
KIND_CHUNK = 0x03

HEADER = struct.Struct("<IB")
STREAM_HEADER = struct.Struct("<IIqI")  # sid, seq, t_ns, flags
CHUNK_HEADER = struct.Struct("<II")  # total, offset
STREAM_FLAG_DROPPED = 0x1

HANDSHAKE_MAX_FRAME = 65536  # every frame before the handshake completes
MIN_MAX_FRAME = 65536
DEFAULT_MAX_FRAME = 1024 * 1024
DEFAULT_MAX_MESSAGE = 256 * 1024 * 1024
MAX_MESSAGE_LIMIT = 0xFFFFFFFF
CHUNK_DATA = 256 * 1024  # data per chunk this implementation sends


# ----------------------------------------------------------------------------- codec


def _no_ext(code: int, data: bytes) -> Any:
    raise ProtocolError(f"MessagePack extension type {code} is not allowed")


def pack(obj: Any) -> bytes:
    """MessagePack with the subset of SPEC.md 6.1 (``bin`` for bytes, float64 for floats)."""
    try:
        return msgpack.packb(obj, use_bin_type=True, datetime=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"cannot encode as MessagePack: {exc}") from None


def unpack(data: bytes | memoryview) -> Any:
    try:
        obj = msgpack.unpackb(data, raw=False, strict_map_key=True, ext_hook=_no_ext,
                              timestamp=0, use_list=True)
    except ProtocolError:
        raise
    except Exception as exc:  # noqa: BLE001 - every decode failure is a protocol error
        raise ProtocolError(f"invalid MessagePack: {exc}") from None
    if isinstance(obj, msgpack.Timestamp):
        raise ProtocolError("MessagePack extension type -1 is not allowed")
    return obj


def unpack_message(data: bytes | memoryview) -> dict[str, Any]:
    obj = unpack(data)
    if not isinstance(obj, dict) or not isinstance(obj.get("t"), str):
        raise ProtocolError("a message must be a map with a string 't'")
    return obj


# ----------------------------------------------------------------------------- frames


def frame(kind: int, payload: bytes) -> bytes:
    return HEADER.pack(len(payload) + 1, kind) + payload


def message_frame(obj: dict[str, Any]) -> bytes:
    return frame(KIND_MESSAGE, pack(obj))


def stream_frame(sid: int, seq: int, t_ns: int, flags: int, body: bytes) -> bytes:
    return HEADER.pack(STREAM_HEADER.size + len(body) + 1, KIND_STREAM) \
        + STREAM_HEADER.pack(sid, seq & 0xFFFFFFFF, t_ns, flags) + body


def chunk_frames(encoded: bytes, chunk_data: int = CHUNK_DATA) -> Iterator[bytes]:
    """The kind-3 frames carrying one encoded message (SPEC.md 5.4)."""
    total = len(encoded)
    if total > MAX_MESSAGE_LIMIT:
        raise ValueError("message larger than 4 GiB - 1")
    if chunk_data < 1:
        raise ValueError("chunk size must be positive")
    view = memoryview(encoded)
    for off in range(0, total, chunk_data):
        part = view[off:off + chunk_data]
        yield HEADER.pack(CHUNK_HEADER.size + len(part) + 1, KIND_CHUNK) \
            + CHUNK_HEADER.pack(total, off) + part


def chunk_data_for(max_frame: int) -> int:
    """Data bytes per chunk so a chunk frame fits a peer's ``max_frame``."""
    return max(1, min(CHUNK_DATA, max_frame - 1 - CHUNK_HEADER.size))


class FrameParser:
    """Incremental receiver: :meth:`feed` bytes, get ``(kind, payload)`` for every complete
    message (kind 1, chunked or not) and stream sample (kind 2). Unknown kinds 0x80-0xFF are
    skipped. Raises :class:`ProtocolError` on any violation (SPEC.md 5.5)."""

    def __init__(self, max_frame: int = HANDSHAKE_MAX_FRAME,
                 max_message: int = DEFAULT_MAX_MESSAGE) -> None:
        self.max_frame = max_frame
        self.max_message = max_message
        self._buf = bytearray()
        self._chunk: bytearray | None = None
        self._chunk_total = 0

    @property
    def in_transfer(self) -> bool:
        return self._chunk is not None

    def feed(self, data: bytes) -> list[tuple[int, bytes]]:
        buf = self._buf
        if buf:
            buf += data
            src: bytes | bytearray = buf
        else:
            src = data
        out: list[tuple[int, bytes]] = []
        pos = 0
        end = len(src)
        while end - pos >= 4:
            length = int.from_bytes(src[pos:pos + 4], "little")
            if length < 1:
                raise ProtocolError("frame length 0")
            if length > self.max_frame:
                raise ProtocolError(f"frame of {length} bytes exceeds max_frame {self.max_frame}")
            if end - pos - 4 < length:
                break
            kind = src[pos + 4]
            start = pos + 5
            stop = pos + 4 + length
            pos = stop
            if kind == KIND_MESSAGE:
                if self._chunk is not None:
                    raise ProtocolError("a message frame inside a chunked transfer")
                out.append((KIND_MESSAGE, bytes(src[start:stop])))
            elif kind == KIND_STREAM:
                if stop - start < STREAM_HEADER.size:
                    raise ProtocolError("stream frame shorter than its header")
                out.append((KIND_STREAM, bytes(src[start:stop])))
            elif kind == KIND_CHUNK:
                msg = self._on_chunk(src, start, stop)
                if msg is not None:
                    out.append((KIND_MESSAGE, msg))
            elif kind >= 0x80:
                continue
            else:
                raise ProtocolError(f"unknown frame kind 0x{kind:02x}")
        if src is buf:
            del buf[:pos]
        elif pos < end:
            buf += src[pos:]
        return out

    def _on_chunk(self, src: bytes | bytearray, start: int, stop: int) -> bytes | None:
        if stop - start < CHUNK_HEADER.size:
            raise ProtocolError("chunk frame shorter than its header")
        total, offset = CHUNK_HEADER.unpack_from(src, start)
        data = src[start + CHUNK_HEADER.size:stop]
        if self._chunk is None:
            if offset != 0:
                raise ProtocolError("first chunk with offset != 0")
            if total > self.max_message:
                raise ProtocolError(f"message of {total} bytes exceeds max_message "
                                    f"{self.max_message}")
            if total == 0:
                raise ProtocolError("chunked message of 0 bytes")
            self._chunk = bytearray()
            self._chunk_total = total
        elif total != self._chunk_total or offset != len(self._chunk):
            raise ProtocolError("chunk out of sequence")
        if not data:
            raise ProtocolError("empty chunk")
        self._chunk += data
        if len(self._chunk) > total:
            raise ProtocolError("chunks longer than their total")
        if len(self._chunk) == total:
            msg = bytes(self._chunk)
            self._chunk = None
            return msg
        return None
