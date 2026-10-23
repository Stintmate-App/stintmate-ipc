"""Reconnects, heartbeat, closing, other processes, asyncio (SPEC.md 15)."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest

from stintmate_ipc import Layout, NotConnected, PeerInfo, connect

from .conftest import wait_for


def test_client_reconnects_and_resubscribes(endpoint, client):
    ep1 = endpoint()
    st1 = ep1.state_channel("t.state", {"gen": 1})
    ep1.start()
    c = client()
    got = []
    ups, downs = [], []
    c.on_connect = ups.append
    c.on_disconnect = downs.append
    c.subscribe("t.state", got.append)  # before connecting: subscribes on connect
    first = c.connect(5)
    wait_for(lambda: got and got[-1].data == {"gen": 1})
    run1 = first.run
    ep1.stop()
    wait_for(lambda: downs)
    with pytest.raises(NotConnected):
        c.call("t.anything")
    ep2 = endpoint()
    ep2.state_channel("t.state", {"gen": 2})
    ep2.start()
    second = c.wait_connected(5)
    assert second.run != run1
    wait_for(lambda: len(ups) == 2)
    wait_for(lambda: got[-1].data == {"gen": 2} and got[-1].snapshot)
    assert st1.subscriber_count == 0


def test_client_keeps_trying_until_the_server_starts(endpoint, client):
    c = client()
    c.start()
    time.sleep(0.6)
    assert not c.connected
    endpoint().start()
    c.wait_connected(5)




def test_client_close_is_seen_by_the_server(endpoint, client):
    ep = endpoint()
    gone = []
    ep.on_disconnect = gone.append
    ep.start()
    c = client()
    c.connect(5)
    wait_for(lambda: ep.peers)
    c.close()
    wait_for(lambda: gone)
    assert gone[0].close_code == "normal" and ep.peers == []








SERVER = textwrap.dedent("""
    import sys, time
    from stintmate_ipc import Endpoint, PeerInfo, Layout
    ep = Endpoint(PeerInfo("pyproc"), bytes.fromhex(sys.argv[2]), pipe=sys.argv[1])
    @ep.method("p.pid")
    def pid(ctx, p):
        import os
        return {"server": os.getpid(), "client_seen": ctx.peer.pid}
    s = ep.stream_channel("p.s", Layout([("i", "u32")]), rate_hz=200)
    ep.start()
    print("ready", flush=True)
    i = 0
    end = time.monotonic() + 8
    while i < 400 and time.monotonic() < end:
        if s.active:
            i += 1
            s.send_values(i)
        time.sleep(0.005)
    sys.stdin.read()
""")








def test_on_connect_may_call_the_endpoint(endpoint, client):
    ep = endpoint()
    ep.method("t.hello", lambda ctx, p: "hi")
    ep.start()
    c = client()
    got = []
    c.on_connect = lambda peer: got.append(c.call("t.hello"))
    c.connect(5)
    wait_for(lambda: got == ["hi"])


def test_reconnect_delay_follows_the_bye_code(client):
    c = client(reconnect=False)
    assert c.reconnect_delay("shutting_down") == 0.25
    assert c.reconnect_delay("shutting_down", 5) == 0.5
    assert c.reconnect_delay("unauthorized") == 10.0
    assert c.reconnect_delay("version_unsupported") is None
    assert [c.reconnect_delay(None, n) for n in range(6)] == [0.25, 0.5, 1.0, 2.0, 2.0, 2.0]
    assert c.reconnect_delay("protocol_error", 2) == 1.0




def _aio_cancel_rig(endpoint, client):
    ep = endpoint()
    saw = threading.Event()

    def slow(ctx, params):
        if ctx.wait_cancelled(5):
            saw.set()
        return "late"

    ep.method("t.slow", slow)
    ep.start()
    return saw, client().start()






