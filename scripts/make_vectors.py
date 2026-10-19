"""Generate the language-neutral test vectors in ``vectors/`` from the reference code.

    uv run python scripts/make_vectors.py          # rewrite vectors/*.json
    uv run python scripts/make_vectors.py --check  # fail if they would change

JSON conventions (vectors/README.md): ``{"$bin": "<hex>"}`` is a MessagePack ``bin``;
``{"$int": "<decimal>"}`` an integer outside +-2^53 (exact in every language); every other
value is the JSON value itself. Map keys appear in the order the encoder wrote them.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from stintmate_ipc import Layout, wire
from stintmate_ipc.handshake import client_mac, server_mac
from stintmate_ipc.keyfile import parse_key
from stintmate_ipc.peer import evt_frame

OUT = Path(__file__).resolve().parent.parent / "vectors"
KEY = bytes.fromhex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
S_NONCE = bytes(range(0xA0, 0xC0))
C_NONCE = bytes(range(0xC0, 0xE0))
PIPE = "stintmate-pitwall-leona"
TEL = Layout([("speed_kmh", "f32"), ("throttle", "f32"), ("brake", "f32"), ("gear", "i8"),
              ("flags", "u8"), ("rpm", "u16"), ("tyre_c", "f32", 4)], name="telemetry")


def j(v: Any) -> Any:
    """To the JSON convention."""
    if isinstance(v, (bytes, bytearray)):
        return {"$bin": bytes(v).hex()}
    if isinstance(v, bool) or v is None or isinstance(v, (str, float)):
        return v
    if isinstance(v, int):
        return v if -(2**53) <= v <= 2**53 else {"$int": str(v)}
    if isinstance(v, dict):
        return {k: j(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [j(x) for x in v]
    raise TypeError(type(v))


def msg_case(name: str, desc: str, msg: dict[str, Any]) -> dict[str, Any]:
    fr = wire.message_frame(msg)
    return {"name": name, "description": desc, "hex": fr.hex(),
            "frames": [{"kind": 1, "message": j(msg)}]}


def frames() -> dict[str, Any]:
    cases = [
        msg_case("ping", "the smallest useful message", {"t": "ping", "ts": 123456789}),
        msg_case("pong", "the answer echoes ts", {"t": "pong", "ts": 123456789}),
        msg_case("req", "a request with params, a deadline and an idempotency key",
                 {"t": "req", "id": 7, "m": "pitwall.write", "v": 1,
                  "p": {"type": "lap", "lap": 4, "channels": {"speed": b"\x00\x00\xc0\x3f"}},
                  "dl": 30000, "ik": "examplesim/2b9f1c/lap/4"}),
        msg_case("res_ok", "a successful response",
                 {"t": "res", "id": 7, "ok": True, "r": {"session_id": 28, "stored": True}}),
        msg_case("res_error", "an error response with details and a retry hint",
                 {"t": "res", "id": 8, "ok": False,
                  "e": {"code": "game_not_running", "msg": "ExampleSim is not running.",
                        "details": {"sim": "examplesim"}, "retry_ms": 2000}}),
        msg_case("res_big_id", "ids are unsigned 64-bit: echo them exactly",
                 {"t": "res", "id": 2**53 + 1, "ok": False,
                  "e": {"code": "unknown_method", "msg": "unknown method a.b"}}),
        msg_case("prog", "progress of a long request, with a new deadline",
                 {"t": "prog", "id": 9, "d": {"shot": 2, "of": 6}, "dl": 15000}),
        msg_case("cancel", "the caller gave up", {"t": "cancel", "id": 9}),
        msg_case("sub", "subscribe to a state channel",
                 {"t": "sub", "id": 10, "ch": "pitwall.connectors", "v": 1}),
        msg_case("sub_res_state", "a state subscription answered with its snapshot",
                 {"t": "res", "id": 10, "ok": True,
                  "r": {"sid": 1, "kind": "state", "seq": 0,
                        "snap": {"connected": ["connector-examplesim"]},
                        "ts": 5000000000}}),
        msg_case("sub_res_stream", "a stream subscription answered with its layout",
                 {"t": "res", "id": 11, "ok": True,
                  "r": {"sid": 2, "kind": "stream", "seq": 0, "layout": TEL.to_wire()}}),
        {"name": "evt", "description": "an event (the hot path builds it from pre-packed parts)",
         "hex": evt_frame(1, 3, 5000000123, wire.pack({"lap": 4, "time_s": 91.234})).hex(),
         "frames": [{"kind": 1, "message": j({"t": "evt", "sid": 1, "seq": 3,
                                              "ts": 5000000123,
                                              "d": {"lap": 4, "time_s": 91.234}})}]},
        msg_case("unsub", "the subscriber ends its subscription", {"t": "unsub", "sid": 1}),
        msg_case("end", "the owner ends a subscription",
                 {"t": "end", "sid": 1, "code": "overloaded",
                  "msg": "subscriber too slow: queue full"}),
        msg_case("decl", "new declarations replace the old ones",
                 {"t": "decl", "decl": {"methods": {"connector.live_frame": {"v": [1]}},
                                        "channels": {}}}),
        msg_case("bye", "closing", {"t": "bye", "code": "shutting_down",
                                    "msg": "the endpoint stops"}),
    ]
    body = TEL.pack(212.5, 1.0, 0.0, 5, 0, 7400, 85.0, 85.5, 86.0, 86.5)
    st = wire.stream_frame(2, 41, 5000000999, 1, body)
    cases.append({"name": "stream_sample",
                  "description": "a stream sample (kind 2) for the telemetry layout, with the "
                                 "dropped flag",
                  "hex": st.hex(), "layout": TEL.to_wire(),
                  "frames": [{"kind": 2, "sid": 2, "seq": 41, "t_ns": 5000000999, "flags": 1,
                              "body": body.hex(),
                              "values": j(TEL.unpack_dict(body))}]})
    req = {"t": "req", "id": 12, "m": "pitwall.setup_capture", "v": 1,
           "p": {"png": bytes(range(64)), "source": "button"}}
    enc = wire.pack(req)
    chunks = [bytes(c) for c in wire.chunk_frames(enc, 40)]
    mixed = chunks[0] + st + b"".join(chunks[1:])
    cases.append({"name": "chunked_with_stream",
                  "description": f"a {len(enc)}-byte message in 40-byte chunks (kind 3), a "
                                 "stream sample between the first and second chunk",
                  "hex": mixed.hex(),
                  "frames": [{"kind": 2, "sid": 2, "seq": 41, "t_ns": 5000000999, "flags": 1,
                              "body": body.hex()},
                             {"kind": 1, "message": j(req)}],
                  "chunks": [{"total": len(enc), "offset": o, "length": len(c) - 13}
                             for o, c in zip(range(0, len(enc), 40), chunks)]})
    ext = wire.frame(0x80, b"future") + wire.message_frame({"t": "ping", "ts": 1})
    cases.append({"name": "reserved_kind", "description": "kinds 0x80-0xFF are skipped",
                  "hex": ext.hex(), "frames": [{"kind": 1, "message": {"t": "ping", "ts": 1}}]})
    unknown = wire.message_frame({"t": "x_future", "a": 1}) + wire.message_frame(
        {"t": "ping", "ts": 2, "zz_new_field": [1, 2]})
    cases.append({"name": "unknown_type_and_keys",
                  "description": "an unknown message type is ignored; unknown keys too",
                  "hex": unknown.hex(),
                  "frames": [{"kind": 1, "message": {"t": "x_future", "a": 1},
                              "action": "ignore"},
                             {"kind": 1, "message": {"t": "ping", "ts": 2,
                                                     "zz_new_field": [1, 2]},
                              "action": "answer pong ts 2"}]})
    return {"description": "Frames after the handshake: hex bytes as received, and what they "
                           "decode to (SPEC.md 5, 6, 10).", "cases": cases}


def handshake() -> dict[str, Any]:
    hello = {"t": "hello", "proto": "stintmate-ipc", "versions": [2], "nonce": S_NONCE,
             "max_frame": 1048576, "features": [], "clock": [5000000000, 1792500000000000000],
             "max_message": 268435456}
    cmac = client_mac(KEY, 2, S_NONCE, C_NONCE, PIPE)
    smac = server_mac(KEY, 2, S_NONCE, C_NONCE, PIPE)
    auth = {"t": "auth", "v": 2, "nonce": C_NONCE, "mac": cmac,
            "peer": {"app": "connector-examplesim", "name": "ExampleSim connector",
                     "version": "1.2.0", "process": "recorder",
                     "meta": {"sim": "examplesim"}},
            "decl": {"methods": {"connector.live_frame": {"v": [1]}},
                     "channels": {"connector.telemetry": {"kind": "stream", "v": [1],
                                                          "rate_hz": 200,
                                                          "layout": TEL.to_wire()}}},
            "max_frame": 1048576, "max_message": 268435456, "features": []}
    welcome = {"t": "welcome", "v": 2, "mac": smac, "run": "5f1c2a9be03d4471",
               "peer": {"app": "pitwall", "name": "Pitwall", "version": "0.9.0",
                        "process": "core"},
               "decl": {"methods": {"pitwall.write": {"v": [1], "ordered": True,
                                                      "idempotent": True}},
                        "channels": {"pitwall.lap_completed": {"kind": "event", "v": [1]}}},
               "heartbeat_ms": 5000, "features": []}
    c_in = b"stintmate-ipc/2 client\x00" + S_NONCE + C_NONCE + PIPE.encode()
    s_in = b"stintmate-ipc/2 server\x00" + C_NONCE + S_NONCE + PIPE.encode()
    wrong = bytes(32)
    return {
        "description": "The handshake with a fixed key and nonces (SPEC.md 8).",
        "key_file_contents": (KEY.hex() + "\r\n").encode().hex(),
        "key": KEY.hex(), "pipe_short_name": PIPE, "version": 2,
        "server_nonce": S_NONCE.hex(), "client_nonce": C_NONCE.hex(),
        "client_mac_input": c_in.hex(), "client_mac": cmac.hex(),
        "server_mac_input": s_in.hex(), "server_mac": smac.hex(),
        "transcript": [
            {"from": "server", "hex": wire.message_frame(hello).hex(), "message": j(hello)},
            {"from": "client", "hex": wire.message_frame(auth).hex(), "message": j(auth)},
            {"from": "server", "hex": wire.message_frame(welcome).hex(),
             "message": j(welcome)},
        ],
        "wrong_key": {
            "key": wrong.hex(),
            "client_mac": client_mac(wrong, 2, S_NONCE, C_NONCE, PIPE).hex(),
            "server_reply_hex": wire.message_frame(
                {"t": "bye", "code": "unauthorized", "msg": "authentication failed"}).hex(),
            "note": "the server compares in constant time, answers bye unauthorized and "
                    "closes; it sends no welcome",
        },
        "other_pipe": {
            "pipe_short_name": "stintmate-voice-leona",
            "client_mac": client_mac(KEY, 2, S_NONCE, C_NONCE, "stintmate-voice-leona").hex(),
            "note": "the MAC binds the pipe name: an auth for one endpoint fails on another",
        },
    }


def errors() -> dict[str, Any]:
    big = wire.HEADER.pack(1048577, 1)
    enc = wire.pack({"t": "ping", "ts": 1, "pad": "x" * 30})
    ch = [bytes(c) for c in wire.chunk_frames(enc, 16)]
    cases = [
        ("kind_zero", wire.frame(0x00, b"\x80"), "protocol_error", "kind 0x00 is not defined"),
        ("kind_unknown", wire.frame(0x10, b"\x80"), "protocol_error",
         "kinds 0x04-0x7F are not defined"),
        ("length_zero", b"\x00\x00\x00\x00", "protocol_error", "a frame has at least a kind"),
        ("over_max_frame", big, "protocol_error",
         "length 1048577 > the receiver's max_frame 1048576: refuse on the header, do not "
         "wait for the body"),
        ("not_a_map", wire.frame(1, wire.pack([1, 2])), "protocol_error",
         "a message is a map"),
        ("no_t", wire.frame(1, wire.pack({"id": 1})), "protocol_error", "t is required"),
        ("t_not_string", wire.frame(1, wire.pack({"t": 1})), "protocol_error",
         "t is a string"),
        ("invalid_msgpack", wire.frame(1, b"\xc1"), "protocol_error",
         "0xc1 is never used in MessagePack"),
        ("truncated_msgpack", wire.frame(1, wire.pack({"t": "ping"})[:-2]), "protocol_error",
         "the map is cut short"),
        ("ext_type", wire.frame(1, bytes.fromhex("82a174a470696e67a165d40501")),
         "protocol_error", "extension types are not allowed ({t: ping, e: ext 5})"),
        ("message_inside_chunks", ch[0] + wire.message_frame({"t": "ping", "ts": 2}),
         "protocol_error", "no kind-1 frame between the chunks of one message"),
        ("chunk_first_offset", ch[1], "protocol_error", "the first chunk has offset 0"),
        ("chunk_gap", ch[0] + ch[2], "protocol_error", "offsets must follow"),
        ("chunk_total_changes", ch[0] + wire.HEADER.pack(1 + 8 + 4, 3)
         + wire.CHUNK_HEADER.pack(len(enc) + 1, 16) + b"abcd", "protocol_error",
         "total is the same in every chunk"),
        ("stream_short", wire.frame(2, b"\x00" * 19), "protocol_error",
         "a stream frame has a 20-byte header"),
        ("req_id_not_int", wire.message_frame({"t": "req", "id": "a", "m": "x.y", "v": 1}),
         "protocol_error", "req.id is an unsigned integer"),
        ("auth_after_handshake", wire.message_frame({"t": "auth", "v": 2}), "protocol_error",
         "handshake messages after the handshake"),
    ]
    return {"description": "Bytes received after a completed handshake that the receiver "
                           "must treat as protocol errors: send bye protocol_error, then "
                           "close (SPEC.md 5.5, 6.2, 11.2). The receiver's max_frame is "
                           "1048576.",
            "cases": [{"name": n, "hex": h.hex(), "expect": e, "why": w}
                      for n, h, e, w in cases]}


def keyfiles() -> dict[str, Any]:
    k = KEY.hex()
    valid = [k, k + "\r\n", k + "\n", k.upper(), "﻿" + k + "\r\n", k + "  \t\r\n"]
    invalid = ["", k[:-2], k + "00", k[:-1] + "g", " " + k[:-1], k[:32] + " " + k[32:]]
    for v in valid:
        assert parse_key(v.encode("utf-8")) == KEY
    return {"description": "Key file contents (SPEC.md 8.1): UTF-8 bytes as hex; valid ones "
                           "give the key below.",
            "key": k,
            "valid": [v.encode("utf-8").hex() for v in valid],
            "invalid": [v.encode("utf-8").hex() for v in invalid]}


def layouts() -> dict[str, Any]:
    cases = []
    lays = [
        (TEL, (212.5, 1.0, 0.0, 5, 0, 7400, 85.0, 85.5, 86.0, 86.5)),
        (Layout([("a", "u8"), ("b", "i8"), ("c", "u16"), ("d", "i16"), ("e", "u32"),
                 ("f", "i32"), ("g", "u64"), ("h", "i64"), ("i", "f32"), ("k", "f64")],
                name="every_type"),
         (255, -128, 65535, -32768, 4294967295, -2147483648, 2**64 - 1, -(2**63), -1.5,
          3.141592653589793)),
        (Layout([("bench", "f32", 12)], name="bench48"), tuple([1.5] * 12)),
    ]
    for lay, vals in lays:
        body = lay.pack(*vals)
        cases.append({"layout": lay.to_wire(), "values": j(lay.unpack_dict(body)),
                      "body": body.hex()})
    return {"description": "Stream layouts: values packed little-endian in field order, no "
                           "padding (SPEC.md 9.3).", "cases": cases}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    files = {"frames.json": frames(), "handshake.json": handshake(), "errors.json": errors(),
             "keyfile.json": keyfiles(), "layouts.json": layouts()}
    changed = []
    OUT.mkdir(exist_ok=True)
    for name, doc in files.items():
        text = json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
        p = OUT / name
        if not p.exists() or p.read_text(encoding="utf-8") != text:
            changed.append(name)
            if not a.check:
                p.write_text(text, encoding="utf-8", newline="\n")
    if a.check and changed:
        print("vectors out of date:", ", ".join(changed))
        return 1
    print("changed:" if changed else "up to date", ", ".join(changed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
