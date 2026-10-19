"""Stream layouts and declarations (SPEC.md 9)."""

from __future__ import annotations

import pytest

from stintmate_ipc import Declarations, Layout
from stintmate_ipc.decl import ChannelDecl, MethodDecl


def test_layout_pack_unpack():
    lay = Layout([("speed", "f32"), ("gear", "i8"), ("flags", "u8"), ("rpm", "u16"),
                  ("temps", "f32", 4), ("t", "i64")], name="telemetry")
    assert lay.size == 4 + 1 + 1 + 2 + 16 + 8
    body = lay.pack_dict({"speed": 1.5, "gear": -1, "flags": 3, "rpm": 7000,
                          "temps": (1.0, 2.0, 3.0, 4.0), "t": -9})
    assert len(body) == lay.size
    assert body[:4] == b"\x00\x00\xc0\x3f"  # 1.5f little-endian
    d = lay.unpack_dict(body)
    assert d["gear"] == -1 and d["temps"] == (1.0, 2.0, 3.0, 4.0) and d["rpm"] == 7000
    assert lay.unpack(body)[0] == 1.5
    w = lay.to_wire()
    assert w == {"size": 32, "name": "telemetry", "fields": [
        ["speed", "f32"], ["gear", "i8"], ["flags", "u8"], ["rpm", "u16"],
        ["temps", "f32", 4], ["t", "i64"]]}
    assert Layout.from_wire(w) == lay


@pytest.mark.parametrize("fields", [
    [], [("a", "f16")], [("A", "u8")], [("a", "u8"), ("a", "u8")], [("a", "u8", 0)],
    [("a",)],
])
def test_layout_rejects(fields):
    with pytest.raises(ValueError):
        Layout(fields)


def test_layout_from_wire_checks_size():
    with pytest.raises(ValueError, match="size"):
        Layout.from_wire({"size": 5, "fields": [["a", "u32"]]})


def test_declarations_roundtrip():
    d = Declarations(
        {"a.write": MethodDecl("a.write", [1, 2], ordered=True, idempotent=True, doc="w")},
        {"a.ev": ChannelDecl("a.ev", "event", [1]),
         "a.st": ChannelDecl("a.st", "state", [3]),
         "a.s": ChannelDecl("a.s", "stream", [1], layout=Layout([("x", "f32")]),
                            rate_hz=200)})
    w = d.to_wire()
    assert w["methods"]["a.write"] == {"v": [1, 2], "ordered": True, "idempotent": True,
                                       "doc": "w"}
    assert w["channels"]["a.s"]["layout"] == {"size": 4, "fields": [["x", "f32"]]}
    back = Declarations.from_wire(w)
    assert back.methods["a.write"].versions == [1, 2]
    assert back.channels["a.s"].layout == Layout([("x", "f32")])
    assert back.channels["a.s"].rate_hz == 200


@pytest.mark.parametrize("bad", [
    [1], {"methods": []}, {"methods": {"Bad": {"v": [1]}}}, {"methods": {"a.b": {"v": []}}},
    {"methods": {"a.b": {"v": [0]}}}, {"channels": {"a.b": {"kind": "x", "v": [1]}}},
    {"channels": {"a.b": {"kind": "stream", "v": [1]}}},
])
def test_declarations_reject(bad):
    with pytest.raises(ValueError):
        Declarations.from_wire(bad)


def test_declarations_ignore_unknown_keys():
    d = Declarations.from_wire({"methods": {"a.b": {"v": [1], "future": True}},
                                "channels": {}, "other": 1})
    assert list(d.methods) == ["a.b"]
