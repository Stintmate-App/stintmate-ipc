"""Framing and the MessagePack subset (SPEC.md 5, 6)."""

from __future__ import annotations

import msgpack
import pytest

from stintmate_ipc import wire
from stintmate_ipc.errors import ProtocolError


def test_message_frame_layout():
    fr = wire.message_frame({"t": "ping", "ts": 1})
    body = wire.pack({"t": "ping", "ts": 1})
    assert fr[:4] == (len(body) + 1).to_bytes(4, "little")
    assert fr[4] == wire.KIND_MESSAGE
    assert fr[5:] == body


def test_stream_frame_layout():
    fr = wire.stream_frame(3, 7, -5, 1, b"\x01\x02")
    assert fr[:4] == (1 + 20 + 2).to_bytes(4, "little")
    assert fr[4] == wire.KIND_STREAM
    assert wire.STREAM_HEADER.unpack_from(fr, 5) == (3, 7, -5, 1)
    assert fr[25:] == b"\x01\x02"


def test_parser_partial_feeds_byte_by_byte():
    frames = wire.message_frame({"t": "a"}) + wire.stream_frame(1, 2, 3, 0, b"xyz") \
        + wire.message_frame({"t": "b"})
    p = wire.FrameParser(1 << 20)
    out = []
    for i in range(len(frames)):
        out += p.feed(frames[i:i + 1])
    assert [k for k, _ in out] == [1, 2, 1]
    assert wire.unpack_message(out[2][1]) == {"t": "b"}


def test_parser_many_frames_in_one_feed():
    data = b"".join(wire.message_frame({"t": "x", "i": i}) for i in range(100))
    out = wire.FrameParser(1 << 20).feed(data)
    assert [wire.unpack_message(p)["i"] for _, p in out] == list(range(100))


def test_chunks_reassemble_and_interleave_with_streams():
    msg = {"t": "req", "id": 1, "m": "a.b", "p": {"blob": b"z" * 1000}}
    enc = wire.pack(msg)
    chunks = list(wire.chunk_frames(enc, 100))
    assert len(chunks) == 11
    p = wire.FrameParser(1 << 20)
    out = []
    for i, ch in enumerate(chunks):
        out += p.feed(bytes(ch))
        if i == 3:
            out += p.feed(wire.stream_frame(9, 1, 1, 0, b"s"))
        assert p.in_transfer == (i < len(chunks) - 1)
    kinds = [k for k, _ in out]
    assert kinds == [2, 1]
    assert wire.unpack_message(out[1][1]) == msg


@pytest.mark.parametrize("bad", [
    wire.frame(0x00, b"x"),
    wire.frame(0x10, b"x"),
    wire.frame(0x7F, b"x"),
    b"\x00\x00\x00\x00\x01",
])
def test_bad_kinds_and_lengths(bad):
    with pytest.raises(ProtocolError):
        wire.FrameParser(1 << 20).feed(bad)


def test_reserved_kinds_are_skipped():
    out = wire.FrameParser(1 << 20).feed(wire.frame(0x80, b"ext") + wire.frame(0xFF, b"")
                                         + wire.message_frame({"t": "x"}))
    assert [k for k, _ in out] == [1]


def test_frame_over_max_frame_fails_on_the_header():
    p = wire.FrameParser(65536)
    with pytest.raises(ProtocolError, match="max_frame"):
        p.feed((65537).to_bytes(4, "little") + b"\x01")


def test_message_inside_chunked_transfer_is_an_error():
    chunks = list(wire.chunk_frames(wire.pack({"t": "x", "pad": "y" * 50}), 10))
    p = wire.FrameParser(1 << 20)
    p.feed(bytes(chunks[0]))
    with pytest.raises(ProtocolError, match="inside a chunked"):
        p.feed(wire.message_frame({"t": "y"}))


def test_chunk_out_of_sequence_and_too_long():
    enc = wire.pack({"t": "x", "pad": "y" * 50})
    chunks = list(wire.chunk_frames(enc, 10))
    p = wire.FrameParser(1 << 20)
    p.feed(bytes(chunks[0]))
    with pytest.raises(ProtocolError, match="out of sequence"):
        p.feed(bytes(chunks[2]))
    p = wire.FrameParser(1 << 20)
    with pytest.raises(ProtocolError, match="offset"):
        p.feed(bytes(chunks[1]))
    p = wire.FrameParser(1 << 20, max_message=20)
    with pytest.raises(ProtocolError, match="max_message"):
        p.feed(bytes(chunks[0]))


def test_stream_frame_shorter_than_header():
    with pytest.raises(ProtocolError):
        wire.FrameParser(1 << 20).feed(wire.frame(wire.KIND_STREAM, b"\x00" * 19))


def test_codec_subset():
    obj = {"s": "é", "b": b"\x00\xff", "i": -(2**63), "u": 2**64 - 1, "f": 1.5, "n": None,
           "a": [True, False, {"k": []}]}
    assert wire.unpack(wire.pack(obj)) == obj
    assert wire.pack(1.5)[0] == 0xCB  # floats are float64


def test_ext_types_are_rejected():
    with pytest.raises(ProtocolError, match="extension"):
        wire.unpack(msgpack.packb(msgpack.ExtType(5, b"x")))
    with pytest.raises(ProtocolError):
        wire.unpack(msgpack.packb({"t": "x", "e": msgpack.ExtType(1, b"")}))
    with pytest.raises(ProtocolError):  # the timestamp type, at the top level
        wire.unpack(msgpack.packb(msgpack.Timestamp(1, 0), datetime=False))


def test_messages_must_be_maps_with_string_t():
    for bad in ([1], {"x": 1}, {"t": 5}, "t"):
        with pytest.raises(ProtocolError):
            wire.unpack_message(wire.pack(bad))
    with pytest.raises(ProtocolError):
        wire.unpack_message(b"\xc1")  # never-used byte


def test_float32_is_accepted_on_decode():
    assert wire.unpack(msgpack.packb(1.5, use_single_float=True)) == 1.5


def test_chunk_data_for_peer_limit():
    assert wire.chunk_data_for(65536) == 65536 - 9
    assert wire.chunk_data_for(16 << 20) == wire.CHUNK_DATA
