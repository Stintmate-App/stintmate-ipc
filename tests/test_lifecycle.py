"""Reconnects, heartbeat, closing, other processes, asyncio (SPEC.md 15)."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap
import time

import pytest

from stintmate_ipc import Layout, NotConnected, PeerInfo, connect
from stintmate_ipc.aio import AsyncClient
from stintmate_ipc.check.raw import Closed, RawConn

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


def test_endpoint_stop_says_bye(endpoint, client):
    ep = endpoint().start()
    peer = client(reconnect=False).connect(5)
    ep.stop()
    assert peer.wait_closed(3)
    assert peer.close_code == "shutting_down"


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


def test_heartbeat_pings_idle_peers(endpoint, key, pipe_name):
    endpoint(heartbeat_ms=300).start()
    rc = RawConn.open(pipe_name)
    try:
        welcome = rc.client_handshake(key)
        assert welcome["heartbeat_ms"] == 300
        kind, m = rc.recv(2)
        assert m["t"] == "ping" and isinstance(m["ts"], int)
    finally:
        rc.close()


def test_silent_peer_is_dropped_after_three_heartbeats(endpoint, key, pipe_name):
    ep = endpoint(heartbeat_ms=200)
    gone = []
    ep.on_disconnect = gone.append
    ep.start()
    rc = RawConn.open(pipe_name)
    try:
        rc.client_handshake(key)
        t0 = time.monotonic()
        with pytest.raises(Closed):
            while True:
                rc.recv(3)  # never answers the pings
        assert 0.5 < time.monotonic() - t0 < 2.0
        wait_for(lambda: gone)
        assert gone[0].close_code == "timeout"
    finally:
        rc.close()


def test_protocol_error_closes_with_bye(endpoint, key, pipe_name):
    ep = endpoint()
    gone = []
    ep.on_disconnect = gone.append
    ep.start()
    rc = RawConn.open(pipe_name)
    try:
        rc.client_handshake(key)
        rc.send({"t": "req", "id": "seven", "m": "a.b"})
        seen = rc.wait_closed(3)
        assert seen[-1]["t"] == "bye" and seen[-1]["code"] == "protocol_error"
        wait_for(lambda: gone)
        assert gone[0].close_code == "protocol_error"
    finally:
        rc.close()


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


def test_other_process(key, pipe_name):
    proc = subprocess.Popen([sys.executable, "-c", SERVER, pipe_name, key.hex()],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == "ready"
        peer = connect(pipe_name, PeerInfo("tester"), key)
        r = peer.call("p.pid")  # (the venv's python.exe is a launcher: proc.pid differs)
        assert r == {"server": peer.pid, "client_seen": os.getpid()} and peer.pid != os.getpid()
        got = []
        lat = []
        peer.subscribe("p.s", lambda x: (got.append(x), lat.append(time.perf_counter_ns()
                                                                   - x.t_ns)))
        wait_for(lambda: len(got) >= 50)
        assert [x.values()[0] for x in got[:50]] == list(range(got[0].values()[0],
                                                              got[0].values()[0] + 50))
        assert all(0 < v < 50_000_000 for v in lat)  # one QPC clock for both processes
        peer.close()
    finally:
        proc.stdin.close()
        proc.wait(30)


def test_asyncio_bridge(endpoint, client):
    ep = endpoint()
    ep.method("t.add", lambda ctx, p: p["a"] + p["b"])
    ch = ep.event_channel("t.ev")
    ep.start()
    c = client().start()

    async def main():
        ac = AsyncClient(c)
        await ac.wait_connected(5)
        assert await ac.call("t.add", {"a": 2, "b": 3}) == 5
        got = []
        async with ac.subscribe("t.ev") as events:
            while ch.subscriber_count == 0:
                await asyncio.sleep(0.01)
            for i in range(3):
                ch.publish(i)
            async for ev in events:
                got.append(ev.data)
                if len(got) == 3:
                    break
        return got

    assert asyncio.run(main()) == [0, 1, 2]


def test_stream_layouts_differ_per_endpoint(endpoint, client):
    ep = endpoint()
    ep.stream_channel("t.a", Layout([("x", "f64")]))
    ep.start()
    peer = client().connect(5)
    sub = peer.subscribe("t.a", lambda s: None)
    assert sub.layout == Layout([("x", "f64")]) and sub.kind == "stream"


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


def test_client_stops_after_version_unsupported(client, monkeypatch):
    c = client()
    monkeypatch.setattr(c, "reconnect_delay", lambda code, attempt=0: None)
    c.start()
    assert c._thread is not None
    c._thread.join(5)  # no endpoint: the first failed attempt ends the loop
    assert not c._thread.is_alive()
