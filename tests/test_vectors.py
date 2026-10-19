"""The reference implementation against vectors/ (what every implementation must match)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from stintmate_ipc import Layout, wire
from stintmate_ipc.check.raw import RawConn
from stintmate_ipc.errors import KeyFileError
from stintmate_ipc.handshake import client_mac, server_mac
from stintmate_ipc.keyfile import parse_key

ROOT = Path(__file__).resolve().parent.parent
V = ROOT / "vectors"


def load(name: str) -> dict[str, Any]:
    return json.loads((V / name).read_text(encoding="utf-8"))


def from_json(v: Any) -> Any:
    if isinstance(v, dict):
        if set(v) == {"$bin"}:
            return bytes.fromhex(v["$bin"])
        if set(v) == {"$int"}:
            return int(v["$int"])
        return {k: from_json(x) for k, x in v.items()}
    if isinstance(v, list):
        return [from_json(x) for x in v]
    return v


def decode_all(data: bytes) -> list[dict[str, Any]]:
    out = []
    for kind, payload in wire.FrameParser(1 << 20).feed(data):
        if kind == 1:
            out.append({"kind": 1, "message": wire.unpack_message(payload)})
        else:
            sid, seq, t, flags = wire.STREAM_HEADER.unpack_from(payload)
            out.append({"kind": 2, "sid": sid, "seq": seq, "t_ns": t, "flags": flags,
                        "body": payload[20:].hex()})
    return out


def test_vectors_are_up_to_date():
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "make_vectors.py"), "--check"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


@pytest.mark.parametrize("case", load("frames.json")["cases"], ids=lambda c: c["name"])
def test_frame_vectors_decode(case):
    got = decode_all(bytes.fromhex(case["hex"]))
    want = case["frames"]
    assert len(got) == len(want)
    for g, w in zip(got, want):
        assert g["kind"] == w["kind"]
        if w["kind"] == 1:
            assert g["message"] == from_json(w["message"])
        else:
            assert {k: g[k] for k in ("sid", "seq", "t_ns", "flags", "body")} == \
                {k: w[k] for k in ("sid", "seq", "t_ns", "flags", "body")}
            if "values" in w:
                lay = Layout.from_wire(case["layout"])
                vals = lay.unpack_dict(bytes.fromhex(w["body"]))
                assert {k: list(v) if isinstance(v, tuple) else v for k, v in vals.items()} \
                    == w["values"]


def _one_message(c: dict[str, Any]) -> bool:
    data = bytes.fromhex(c["hex"])
    return c["frames"][0]["kind"] == 1 and int.from_bytes(data[:4], "little") + 4 == len(data)


@pytest.mark.parametrize("case", [c for c in load("frames.json")["cases"] if _one_message(c)],
                         ids=lambda c: c["name"])
def test_frame_vectors_encode_byte_exact(case):
    """MessagePack encoders may legally differ; the reference one writes the shortest
    forms in the listed key order, which these bytes are."""
    msg = from_json(case["frames"][0]["message"])
    assert wire.message_frame(msg).hex() == case["hex"]


def test_handshake_vectors():
    h = load("handshake.json")
    key = parse_key(bytes.fromhex(h["key_file_contents"]))
    assert key.hex() == h["key"]
    s, c, pipe = bytes.fromhex(h["server_nonce"]), bytes.fromhex(h["client_nonce"]), \
        h["pipe_short_name"]
    assert (b"stintmate-ipc/2 client\x00" + s + c + pipe.encode()).hex() == \
        h["client_mac_input"]
    assert client_mac(key, 2, s, c, pipe).hex() == h["client_mac"]
    assert server_mac(key, 2, s, c, pipe).hex() == h["server_mac"]
    for step in h["transcript"]:
        (only,) = decode_all(bytes.fromhex(step["hex"]))
        assert only["message"] == from_json(step["message"])
    assert from_json(h["transcript"][1]["message"])["mac"].hex() == h["client_mac"]
    assert from_json(h["transcript"][2]["message"])["mac"].hex() == h["server_mac"]
    w = h["wrong_key"]
    assert client_mac(bytes.fromhex(w["key"]), 2, s, c, pipe).hex() == w["client_mac"]
    o = h["other_pipe"]
    assert client_mac(key, 2, s, c, o["pipe_short_name"]).hex() == o["client_mac"] \
        != h["client_mac"]


def test_keyfile_vectors():
    k = load("keyfile.json")
    for v in k["valid"]:
        assert parse_key(bytes.fromhex(v)).hex() == k["key"]
    for v in k["invalid"]:
        with pytest.raises(KeyFileError):
            parse_key(bytes.fromhex(v))


def test_layout_vectors():
    for case in load("layouts.json")["cases"]:
        lay = Layout.from_wire(case["layout"])
        want = from_json(case["values"])
        assert lay.pack_dict(want).hex() == case["body"]
        got = lay.unpack_dict(bytes.fromhex(case["body"]))
        assert {k: list(v) if isinstance(v, tuple) else v for k, v in got.items()} == want


@pytest.mark.parametrize("case", load("errors.json")["cases"], ids=lambda c: c["name"])
def test_error_vectors_against_a_live_endpoint(case, endpoint, key, pipe_name):
    endpoint().start()
    rc = RawConn.open(pipe_name)
    try:
        assert rc.client_handshake(key)["t"] == "welcome"
        rc.send_frame(bytes.fromhex(case["hex"]))
        seen = rc.wait_closed(5)
        byes = [m for m in seen if m.get("t") == "bye"]
        assert byes and byes[-1]["code"] == case["expect"], seen
    finally:
        rc.close()
