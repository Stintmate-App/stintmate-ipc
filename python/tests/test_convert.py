"""Python <-> MessagePack conversion (SPEC 6.1)."""

import math

import pytest

from stintmate_ipc import _native


def rt(v):
    return _native._decode(_native._encode(v))


def test_scalars_roundtrip():
    for v in (None, True, False, 0, -1, 127, 128, -33, 2**32, -(2**63), 2**64 - 1, 1.5, "",
              "ñé€", b"", b"\x00\xff"):
        out = rt(v)
        assert out == v and type(out) is type(v)


def test_tuples_are_arrays_and_bytes_are_bin():
    assert rt((1, 2, (3,))) == [1, 2, [3]]
    assert isinstance(rt(b"ab"), bytes)
    assert rt(bytearray(b"ab")) == b"ab"
    assert _native._encode(b"ab") == b"\xc4\x02ab"  # bin 8, not a string
    assert _native._encode((1, 2)) == b"\x92\x01\x02"  # array, shortest form


def test_nested_map_keeps_order():
    v = {"b": 1, "a": {"x": [1, {"y": None}]}}
    assert list(rt(v)) == ["b", "a"] and rt(v) == v


def test_bool_is_not_int():
    assert _native._encode(True) == b"\xc3"


@pytest.mark.parametrize("bad", [{1: "a"}, {b"k": 1}, {None: 1}, {("a",): 1}])
def test_non_string_keys_raise_type_error(bad):
    with pytest.raises(TypeError, match="keys"):
        _native._encode(bad)


@pytest.mark.parametrize("big", [2**64, -(2**63) - 1, 10**30])
def test_integers_beyond_64_bits_raise_type_error(big):
    with pytest.raises(TypeError, match="64-bit"):
        _native._encode([big])


@pytest.mark.parametrize("bad", [object(), {1, 2}, 1 + 2j, lambda: 0])
def test_unencodable_types_raise_type_error(bad):
    with pytest.raises(TypeError):
        _native._encode({"x": bad})


def test_too_deep():
    v = []
    for _ in range(200):
        v = [v]
    with pytest.raises(ValueError, match="deeply"):
        _native._encode(v)


def test_float_special_values():
    assert math.isinf(rt(float("inf")))
    assert math.isnan(rt(float("nan")))


def test_send_rejects_before_the_wire(client):
    with pytest.raises(TypeError):
        client.call("t.echo", {1: 2})
    with pytest.raises(TypeError):
        client.call_async("t.echo", [2**70])
    assert client.call("t.echo", {"ok": (1, 2)}) == {"ok": [1, 2]}
