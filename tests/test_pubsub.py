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


def test_state_coalesces_for_a_slow_subscriber(endpoint, client):
    ep = endpoint()
    st = ep.state_channel("t.n", 0)
    ep.start()
    c = client()
    c.connect(5)
    got = []
    gate = threading.Event()

    def slow(e):
        got.append(e.data)
        gate.wait(5)

    c.subscribe("t.n", slow)
    wait_for(lambda: got)  # the snapshot is being handled: the callback blocks
    big = "x" * 60000  # fill the pipe so the owner's queue holds the values
    for i in range(1, 400):
        st.set({"n": i, "pad": big})
    gate.set()
    wait_for(lambda: got and isinstance(got[-1], dict) and got[-1]["n"] == 399, timeout=10)
    assert len(got) < 100  # intermediate values were coalesced


def test_slow_callback_overload_ends_locally_and_client_resubscribes(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.burst")
    ep.start()
    c = client(event_queue=50)
    c.connect(5)
    got = []
    ends = []
    gate = threading.Event()

    def slow(e):
        got.append(e.seq)
        gate.wait(5)

    cs = c.subscribe("t.burst", slow, on_end=lambda s, code, msg: ends.append(code))
    wait_for(lambda: ch.subscriber_count == 1)
    for i in range(500):
        ch.publish({"i": i})
    first = cs.current
    wait_for(lambda: first.end_code == "overloaded")
    wait_for(lambda: ch.subscriber_count == 0)  # the unsub reached the owner
    gate.set()
    wait_for(lambda: ends == ["overloaded"])  # on_end runs after the callback in its lane
    # The Client subscribes again after 1 s.
    wait_for(lambda: ch.subscriber_count == 1, timeout=5)
    assert cs.subscribed.is_set()


def test_owner_ends_a_subscriber_that_does_not_read(endpoint, key, pipe_name):
    from stintmate_ipc.check.raw import RawConn
    ep = endpoint(event_queue=20)
    ch = ep.event_channel("t.burst")
    ep.start()
    rc = RawConn.open(pipe_name)
    try:
        rc.client_handshake(key)
        rc.send({"t": "sub", "id": 1, "ch": "t.burst", "v": 1})
        res = rc.recv_msg()
        assert res["ok"] and res["r"]["kind"] == "event"
        sid = res["r"]["sid"]
        pad = "z" * 30000
        t0 = time.monotonic()
        for i in range(200):  # the pipe fills, then the queue; publish never blocks
            ch.publish({"i": i, "pad": pad})
        assert time.monotonic() - t0 < 1.0
        wait_for(lambda: ch.subscriber_count == 0)
        seen = []
        while True:
            m = rc.recv_msg(timeout=3)
            seen.append(m)
            if m["t"] == "end":
                break
        assert seen[-1] == {"t": "end", "sid": sid, "code": "overloaded",
                            "msg": "subscriber too slow: queue full"}
        evts = [m["seq"] for m in seen if m["t"] == "evt"]
        assert evts == list(range(1, len(evts) + 1)) and len(evts) < 200
        rc.send({"t": "ping", "ts": 5})  # the connection itself is fine
        assert rc.recv_msg(skip=("evt",)) == {"t": "pong", "ts": 5}
    finally:
        rc.close()


def test_stream_samples(endpoint, client):
    ep = endpoint()
    s = ep.stream_channel("t.tel", LAY, rate_hz=200)
    active = []
    s.on_active = active.append
    ep.start()
    assert s.send_values(1.0, 1, 0, 5000) == 0  # nobody listens: nothing done
    c = client()
    peer = c.connect(5)
    assert peer.decl.channels["t.tel"].layout == LAY
    got = []
    sub = c.subscribe("t.tel", got.append, deliver="inline")
    wait_for(lambda: active == [True])
    t0 = time.perf_counter_ns()
    for i in range(100):
        s.send_values(100.0 + i, 3, 0, 7000 + i, t_ns=t0 + i)
    wait_for(lambda: len(got) == 100)
    assert [x.seq for x in got] == list(range(1, 101))
    assert [x.t_ns for x in got] == [t0 + i for i in range(100)]
    assert got[5].as_dict() == {"speed": 105.0, "gear": 3, "pad": 0, "rpm": 7005}
    assert not any(x.dropped for x in got)
    with pytest.raises(ValueError):
        s.send(b"short")
    sub.close()
    wait_for(lambda: active == [True, False])


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

    c.subscribe("t.big", slow, deliver="thread")
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


def test_unknown_channel_and_version(endpoint, client):
    ep = endpoint()
    ep.event_channel("t.ev", versions=(2,))
    ep.start()
    c = client()
    peer = c.connect(5)
    with pytest.raises(RemoteError) as ei:
        peer.subscribe("t.none", print)
    assert ei.value.code == "unknown_channel"
    with pytest.raises(RemoteError) as ei:
        peer.subscribe("t.ev", print, version=1)
    assert ei.value.code == "version_unsupported" and ei.value.details == {"versions": [2]}
    cs = c.subscribe("t.none", print)
    assert isinstance(cs.error, RemoteError) and not cs.subscribed.is_set()


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


def test_same_subscription_twice_is_one(endpoint, client):
    ep = endpoint()
    ch = ep.state_channel("t.s", 1)
    ep.start()
    peer = client().connect(5)
    a = peer.subscribe("t.s", lambda e: None)
    b = peer.subscribe("t.s", lambda e: None)
    assert a.sid == b.sid and ch.subscriber_count == 1


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


def test_client_offers_a_stream_to_the_server(endpoint, client):
    ep = endpoint()
    peers = []
    ep.on_connect = peers.append
    ep.start()
    c = client()
    tel = c.stream_channel("conn.telemetry", LAY, rate_hz=50)
    c.connect(5)
    srv = wait_for(lambda: peers and peers[0])
    got = []
    srv.subscribe("conn.telemetry", got.append, deliver="inline")
    wait_for(lambda: tel.active)
    for i in range(10):
        tel.send_values(float(i), 1, 0, 1)
    wait_for(lambda: len(got) == 10)
    assert got[9].values()[0] == 9.0


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


def test_inline_callbacks_cannot_wait_on_their_own_reader(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.ev")
    ep.method("t.echo", lambda ctx, p: p)
    ep.start()
    c = client()
    peer = c.connect(5)
    errors = []

    def cb(e):
        try:
            peer.call("t.echo", 1)
        except RuntimeError as exc:
            errors.append(str(exc))
        pc = peer.call_async("t.echo", 2)  # allowed: it does not wait
        pc.add_done_callback(lambda x: errors.append(x.result()))

    peer.subscribe("t.ev", cb, deliver="inline")
    wait_for(lambda: ch.subscriber_count == 1)
    ch.publish(0)
    wait_for(lambda: len(errors) == 2)
    assert "reader thread" in errors[0] and errors[1] == 2


def test_send_to_one_subscriber_on_event_and_state_channels(endpoint, client):
    ep = endpoint()
    ev = ep.event_channel("t.laps")
    st = ep.state_channel("t.status", {"n": 0})
    peers = {}

    def seen(req):
        peers[req.options["who"]] = req.peer

    ev.on_subscribe = seen
    ep.start()
    a, b = client(), client()
    a.connect(5)
    b.connect(5)
    got_a, got_b, st_a, st_b = [], [], [], []
    a.subscribe("t.laps", got_a.append, options={"who": "a"})
    b.subscribe("t.laps", got_b.append, options={"who": "b"})
    a.subscribe("t.status", st_a.append)
    b.subscribe("t.status", st_b.append)
    wait_for(lambda: ev.subscriber_count == 2 and st.subscriber_count == 2)
    assert ev.send_to(peers["a"], {"only": "a"}) == 1
    assert st.send_to(peers["a"], {"n": 99}) == 1
    wait_for(lambda: len(got_a) == 1 and st_a and st_a[-1].data == {"n": 99})
    ev.publish({"all": 1})
    wait_for(lambda: len(got_a) == 2 and len(got_b) == 1)
    assert got_b[0].data == {"all": 1}
    assert all(e.data != {"n": 99} for e in st_b)
    assert st.value == {"n": 0}
    assert ev.send_to(peers["a"], 1, version=2) == 0
