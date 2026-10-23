"""Event, state and stream channels (SPEC.md 10.6-10.9, 12, 13)."""

from __future__ import annotations

import threading
import time

import pytest

from stintmate_ipc import Layout, RemoteError

from .conftest import wait_for

LAY = Layout([("speed", "f32"), ("gear", "i8"), ("pad", "u8"), ("rpm", "u16")])


def test_event_channel_order_and_fields(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.laps")
    ep.start()
    c = client()
    c.connect(5)
    got = []
    sub = c.subscribe("t.laps", got.append)
    wait_for(lambda: ch.subscriber_count == 1)
    for i in range(200):
        ch.publish({"lap": i})
    wait_for(lambda: len(got) == 200)
    assert [e.data["lap"] for e in got] == list(range(200))
    assert [e.seq for e in got] == list(range(1, 201))
    assert all(e.channel == "t.laps" and isinstance(e.ts, int) and not e.snapshot for e in got)
    sub.close()
    wait_for(lambda: ch.subscriber_count == 0)
    assert ch.publish({"lap": -1}) == 0


def test_state_snapshot_then_changes(endpoint, client):
    ep = endpoint()
    st = ep.state_channel("t.status", {"game_running": False})
    ep.start()
    st.set({"game_running": True, "lap": 1})
    c = client()
    c.connect(5)
    got = []
    c.subscribe("t.status", got.append)
    wait_for(lambda: got)
    assert got[0].snapshot and got[0].seq == 0
    assert got[0].data == {"game_running": True, "lap": 1}
    st.set({"game_running": True, "lap": 2})
    wait_for(lambda: len(got) == 2)
    assert got[1].data["lap"] == 2 and got[1].seq == 1 and not got[1].snapshot










def test_stream_drops_oldest_for_a_slow_subscriber(endpoint, client):
    big = Layout([("v", "f64", 4096)])  # 32 KiB samples fill the pipe fast
    ep = endpoint()
    s = ep.stream_channel("t.big", big, rate_hz=100, queue=8)
    ep.start()
    c = client()
    c.connect(5)
    got = []
    gate = threading.Event()

    def slow(x):
        got.append((x.seq, x.dropped))
        gate.wait(5)

    c.subscribe("t.big", slow)
    wait_for(lambda: s.subscriber_count == 1)
    time.sleep(0.1)
    body = big.pack(*([0.5] * 4096))
    for _ in range(300):
        s.send(body)
    gate.set()
    wait_for(lambda: got and got[-1][0] == 300, timeout=10)
    seqs = [q for q, _ in got]
    assert len(seqs) < 300 and seqs == sorted(seqs)
    assert any(d for _, d in got)  # the first sample after a gap says so
    gap_at = next(i for i in range(1, len(seqs)) if seqs[i] != seqs[i - 1] + 1)
    assert got[gap_at][1] is True




def test_on_subscribe_can_refuse_and_see_options(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.f")
    seen = []

    def check(req):
        seen.append((req.version, req.options, req.peer.info.app))
        if req.options and req.options.get("deny"):
            raise RemoteError("forbidden", "not for you")

    ch.on_subscribe = check
    ep.start()
    peer = client().connect(5)
    with pytest.raises(RemoteError, match="forbidden"):
        peer.subscribe("t.f", print, options={"deny": True})
    peer.subscribe("t.f", print, options={"rate": 10})
    assert seen == [(1, {"deny": True}, "testcli"), (1, {"rate": 10}, "testcli")]




def test_channel_close_ends_subscriptions(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.gone")
    ep.start()
    c = client()
    peer = c.connect(5)
    ends = []
    peer.subscribe("t.gone", print, on_end=lambda s, code, msg: ends.append(code))
    ch.close()
    wait_for(lambda: ends == ["channel_closed"])
    wait_for(lambda: "t.gone" not in peer.decl.channels)


def test_many_subscribers(endpoint, client, key, pipe_name):
    ep = endpoint()
    ch = ep.event_channel("t.fan")
    ep.start()
    clients = [client() for _ in range(5)]
    got = [[] for _ in clients]
    for c, g in zip(clients, got):
        c.connect(5)
        c.subscribe("t.fan", g.append)
    wait_for(lambda: ch.subscriber_count == 5)
    for i in range(50):
        ch.publish(i)
    wait_for(lambda: all(len(g) == 50 for g in got))
    assert all([e.data for e in g] == list(range(50)) for g in got)




def test_callbacks_may_call_the_peer(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.ev")
    ep.method("t.echo", lambda ctx, p: p)
    ep.start()
    c = client()
    c.connect(5)
    out = []
    c.subscribe("t.ev", lambda e: out.append(c.call("t.echo", e.data)))
    wait_for(lambda: ch.subscriber_count == 1)
    for i in range(5):
        ch.publish(i)
    wait_for(lambda: out == [0, 1, 2, 3, 4])






# ------------------------------------------------------------ filtered publish (0.1.3)


def _two_clients(endpoint, client, app_b="other"):
    from stintmate_ipc import PeerInfo

    ep = endpoint()
    ev = ep.event_channel("t.rep")
    ep.start()
    a = client()
    b = client(info=PeerInfo(app_b, name="b", version="1")) if app_b != "other" else client()
    a.connect(5)
    b.connect(5)
    got_a, got_b = [], []
    a.subscribe("t.rep", got_a.append, options={"mine": True})
    b.subscribe("t.rep", got_b.append)
    wait_for(lambda: ev.subscriber_count == 2)
    return ev, got_a, got_b


def test_publish_accept_filters_by_option_and_counts(endpoint, client):
    ev, got_a, got_b = _two_clients(endpoint, client)
    seen = []

    def accept(view):
        seen.append(view)
        return bool(view.options.get("mine"))

    assert ev.publish({"x": 1}, accept=accept) == 1
    wait_for(lambda: len(got_a) == 1)
    assert got_a[0].data == {"x": 1} and got_b == []
    v = [s for s in seen if s.options.get("mine")][0]
    assert v.version == 1 and v.pid and v.peer.app
    assert [s.options for s in seen if not s.options.get("mine")] == [{}]
    with pytest.raises(TypeError):
        v.options["mine"] = False
    assert ev.publish({"x": 2}, accept=lambda v: False) == 0
    assert ev.publish({"x": 3}) == 2
    wait_for(lambda: len(got_a) == 2 and len(got_b) == 1)
    assert got_a[1].data == {"x": 3}


def test_publish_accept_filters_by_peer_app(endpoint, client):
    ev, got_a, got_b = _two_clients(endpoint, client, app_b="hub-test")
    assert ev.publish("hi", accept=lambda v: v.peer.app == "hub-test") == 1
    wait_for(lambda: len(got_b) == 1)
    assert got_a == []


def test_publish_accept_sees_the_connection(endpoint, client):
    """``view.connection`` is the owner-side Peer of the subscription, one per client: an
    event can go to the connection that made a request only."""
    from stintmate_ipc import Peer
    ev, got_a, got_b = _two_clients(endpoint, client, app_b="hub-test")
    views = []
    ev.publish("x", accept=lambda v: views.append(v) or False)
    assert len(views) == 2 and all(isinstance(v.connection, Peer) for v in views)
    assert views[0].connection is not views[1].connection
    owner = next(v.connection for v in views if v.peer.app == "hub-test")
    assert ev.publish("only b", accept=lambda v: v.connection == owner) == 1
    wait_for(lambda: len(got_b) == 1)
    assert got_a == []


def test_publish_accept_raising_skips_only_that_subscription(endpoint, client):
    ev, got_a, got_b = _two_clients(endpoint, client)

    def accept(view):
        if view.options.get("mine"):
            raise RuntimeError("boom")
        return True

    assert ev.publish("ok", accept=accept) == 1
    wait_for(lambda: len(got_b) == 1)
    assert got_a == []




