"""The endpoint side of the binding: decorator, hooks, decl, channels, check."""

from __future__ import annotations

import secrets

import pytest

import stintmate_ipc
from stintmate_ipc import Client, Endpoint, Layout, PeerInfo, RemoteError, decl
from stintmate_ipc.decl import Declarations


def _pair():
    key = secrets.token_bytes(32)
    pipe = f"stintmate-t7-{secrets.token_hex(5)}"
    return Endpoint(PeerInfo("t7srv"), key, pipe=pipe), pipe, key


def test_decorator_decl_and_hooks():
    ep, pipe, key = _pair()
    seen = []
    ep.on_connect = seen.append

    @ep.method("t.add", idempotent=True, doc="adds")
    def add(ctx, p):
        if p.get("boom"):
            raise RemoteError("custom", "no", data={"x": 1}, retry_ms=7)
        return p["a"] + p["b"]

    ep.start()
    c = Client(pipe, PeerInfo("t7cli"), key)
    peer = c.connect(5)
    try:
        assert c.call("t.add", {"a": 1, "b": 2}) == 3
        with pytest.raises(RemoteError) as ei:
            c.call("t.add", {"boom": True})
        assert (ei.value.code, ei.value.data, ei.value.retry_ms) == ("custom", {"x": 1}, 7)
        d = peer.decl
        assert isinstance(d, Declarations) and d.methods["t.add"].idempotent
        assert d.methods["t.add"].doc == "adds"
        assert seen and seen[0].info.app == "t7cli" and ep.peers == [seen[0]]
    finally:
        c.close()
        ep.stop()


def test_channels_and_active_hook():
    ep, pipe, key = _pair()
    ev = ep.event_channel("t.ev")
    st = ep.state_channel("t.st", {"n": 0})
    sm = ep.stream_channel("t.sm", Layout([("a", "u32")]), rate_hz=100)
    flips = []
    ev.on_active = flips.append
    assert (ev.kind, st.kind, sm.kind) == (decl.EVENT, decl.STATE, decl.STREAM)
    ep.start()
    c = Client(pipe, PeerInfo("t7cli"), key)
    c.connect(5)
    got = []
    sub = c.subscribe("t.ev", got.append)
    import time
    end = time.monotonic() + 3
    while not (ev.active and flips) and time.monotonic() < end:
        time.sleep(0.01)
    assert ev.subscriber_count == 1 and flips == [True]
    assert ev.publish({"k": 1}) == 1
    assert st.value == {"n": 0}
    sub.close()
    c.close()
    ep.stop()


def test_check_tool(capfd):
    assert stintmate_ipc.check.main(["--help"]) == 0
    assert stintmate_ipc.check.main([]) == 2
    ep, pipe, key = _pair()

    @ep.method("t.echo")
    def echo(ctx, p):
        return p

    ep.start()
    try:
        capfd.readouterr()
        code = stintmate_ipc.check.main(
            ["server", pipe, "--key-hex", key.hex(), "--fast", "--listen", "0.2", "--json"])
        out = capfd.readouterr().out
    finally:
        ep.stop()
    assert code == 0, out
    assert '"ok": true' in out and "S07" in out
