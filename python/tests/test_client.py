"""The client against a Rust endpoint: calls, timeouts, cancel, progress, subscriptions, GIL."""

import os
import threading
import time

import pytest

import stintmate_ipc as ipc
from stintmate_ipc import Client, PeerInfo


def wait_for(cond, timeout=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.005)
    return cond()


def test_call_roundtrip(client):
    assert client.call("t.echo", {"a": [1, 2.5, "x", None, True], "b": b"\x00\x01"}) == {
        "a": [1, 2.5, "x", None, True], "b": b"\x00\x01"}
    assert client.call("t.echo") == {}
    assert client.call("t.echo", None) == {}
    assert client.call("t.echo", [1, 2]) == [1, 2]


def test_peer_properties(client, endpoint):
    p = client.peer
    assert p is not None and p.is_open and client.connected
    assert p.info.app == "t6test" and p.pid == os.getpid()  # the server, from the pipe
    assert len(p.run) == 16
    assert "t.echo" in p.decl.methods and "t.events" in p.decl.channels
    assert p.call("t.echo", {"x": 1}) == {"x": 1}
    p.close()
    assert wait_for(lambda: not p.is_open)
    assert p.close_code == "normal"


def test_connect_function_returns_a_peer(endpoint, key):
    p = ipc.connect(endpoint.path, PeerInfo("c"), key)
    assert p.is_open and p.call("t.echo", {"k": 1}) == {"k": 1} and p.info.app == "t6test"
    p.close()


def test_keyfile_as_key(endpoint, key, tmp_path):
    f = tmp_path / "key"
    f.write_bytes(ipc.keyfile.format_key(key))
    c = Client(endpoint.path, PeerInfo("c"), ipc.KeyFile(f))
    try:
        assert c.connect(3).call("t.echo", {"ok": 1}) == {"ok": 1}
    finally:
        c.close()


def test_timeout_is_call_timeout(client):
    t0 = time.monotonic()
    with pytest.raises(ipc.CallTimeout):
        client.call("t.sleep", {"ms": 2000}, timeout=0.2)
    assert time.monotonic() - t0 < 1.0
    assert client.call("t.echo", {"after": 1}) == {"after": 1}


def test_call_async_result_and_done(client):
    pc = client.call_async("t.sleep", {"ms": 100})
    assert isinstance(pc, ipc.PendingCall) and pc.method == "t.sleep" and pc.id > 0
    assert pc.result(5) == {"slept": 100}
    assert pc.done() and pc.exception() is None
    assert pc.result() == {"slept": 100}  # idempotent


def test_result_wait_timeout_keeps_the_call(client):
    pc = client.call_async("t.sleep", {"ms": 300})
    with pytest.raises(ipc.CallTimeout):
        pc.result(0.05)
    assert not pc.done()
    assert pc.result(5) == {"slept": 300}


def test_exception_of_a_failed_call(client):
    pc = client.call_async("t.fail")
    assert wait_for(pc.done)
    e = pc.exception()
    assert isinstance(e, ipc.RemoteError) and e.code == "game_not_running"
    with pytest.raises(ipc.RemoteError):
        pc.result()
    assert pc.exception() is e or pc.exception().code == e.code


def test_cancel(client):
    pc = client.call_async("t.sleep", {"ms": 5000}, timeout=10)
    done = []
    pc.add_done_callback(lambda c: done.append((c, threading.current_thread().name)))
    assert pc.cancel() is True
    with pytest.raises(ipc.CallCancelled):
        pc.result(2)
    assert isinstance(pc.exception(), ipc.CallCancelled)
    assert wait_for(lambda: done) and done[0][0] is pc
    assert pc.cancel() is False
    # the server stops working on it: the connection is still usable and fast
    t0 = time.monotonic()
    assert client.call("t.echo", {"x": 1}) == {"x": 1}
    assert time.monotonic() - t0 < 1.0


def test_done_callback_runs_now_when_already_done(client):
    pc = client.call_async("t.echo", {"a": 1})
    pc.result(3)
    assert wait_for(pc.done)
    seen = []
    pc.add_done_callback(seen.append)
    assert seen == [pc]
    here = threading.current_thread()
    got = []
    pc.add_done_callback(lambda c: got.append(threading.current_thread()))
    assert got == [here]


def test_done_callback_after_completion_on_library_thread(client):
    pc = client.call_async("t.sleep", {"ms": 100})
    got = []
    pc.add_done_callback(lambda c: got.append(threading.current_thread()))
    pc.result(3)
    assert wait_for(lambda: got)
    assert got[0] is not threading.current_thread()


def test_progress_in_order(client):
    seen = []
    r = client.call("t.sleep", {"ms": 200, "progress": True}, on_progress=seen.append)
    assert r == {"slept": 200}
    assert wait_for(lambda: len(seen) >= 10)
    assert seen[:10] == list(range(10))


def test_on_progress_on_call_async(client):
    seen = []
    pc = client.call_async("t.sleep", {"ms": 100, "progress": True}, on_progress=seen.append)
    pc.result(3)
    assert wait_for(lambda: seen) and seen == sorted(seen)


def test_a_failing_callback_does_not_break_the_call(client, caplog):
    def bad(_v):
        raise RuntimeError("boom")

    with caplog.at_level("ERROR", logger="stintmate_ipc"):
        assert client.call("t.sleep", {"ms": 50, "progress": True}, on_progress=bad) == {"slept": 50}
        assert wait_for(lambda: any("progress callback failed" in r.message for r in caplog.records))


def test_idempotency_key(client):
    a = client.call("t.count", idempotency_key="k1")
    b = client.call("t.count", idempotency_key="k1")
    c = client.call("t.count", idempotency_key="k2")
    assert a == b == 1 and c == 2


def test_subscribe_event_in_order(client, endpoint):
    got = []
    sub = client.subscribe("t.events", got.append)
    assert isinstance(sub, ipc.ClientSubscription) and sub.channel == "t.events"
    assert wait_for(lambda: endpoint.subscriber_count() == 1)
    for i in range(50):
        endpoint.publish({"i": i, "raw": b"\x01"})
    assert wait_for(lambda: len(got) == 50)
    assert [e.data["i"] for e in got] == list(range(50))
    assert got[0].channel == "t.events" and got[0].snapshot is False and got[0].data["raw"] == b"\x01"
    assert [e.seq for e in got] == sorted(e.seq for e in got)
    sub.close()
    assert wait_for(lambda: endpoint.subscriber_count() == 0)


def test_subscribe_state_snapshot_first(client, endpoint):
    got = []
    sub = client.subscribe("t.state", got.append)
    assert wait_for(lambda: got)
    assert got[0].snapshot is True and got[0].data == {"n": 0}
    endpoint.set_state({"n": 1})
    assert wait_for(lambda: got[-1].data == {"n": 1}) and got[-1].snapshot is False
    sub.close()


def test_subscribe_stream_samples(client, endpoint):
    got = []
    sub = client.subscribe("t.stream", got.append)
    assert wait_for(lambda: sub.subscribed)
    for i in range(5):
        endpoint.send_sample(i, 1.5, -2.0)
    assert wait_for(lambda: len(got) == 5)
    s = got[2]
    assert s.channel == "t.stream" and len(s.body) == 12 and s.t_ns > 0
    assert s.values() == (2, 1.5, -2.0) and s.as_dict() == {"a": 2, "b": (1.5, -2.0)}
    assert s.layout.size == 12 and s.layout.name == "t"
    assert [x.seq for x in got] == sorted(x.seq for x in got)
    sub.close()


def test_peer_subscribe_and_close(client, endpoint):
    got = []
    sub = client.peer.subscribe("t.events", got.append)
    assert isinstance(sub, ipc.Subscription)
    endpoint.publish({"i": 1})
    assert wait_for(lambda: got)
    sub.close()


def test_subscribe_unknown_channel_sets_error(client):
    sub = client.subscribe("t.nothing", lambda e: None)
    assert wait_for(lambda: sub.error is not None)
    assert not sub.subscribed
    sub.close()


def test_subscribe_callback_error_is_logged_and_delivery_continues(client, endpoint, caplog):
    got = []

    def cb(e):
        got.append(e.data["i"])
        if e.data["i"] == 0:
            raise RuntimeError("boom")

    with caplog.at_level("ERROR", logger="stintmate_ipc"):
        sub = client.subscribe("t.events", cb)
        assert wait_for(lambda: endpoint.subscriber_count() == 1)
        endpoint.publish({"i": 0})
        endpoint.publish({"i": 1})
        assert wait_for(lambda: got == [0, 1])
        assert wait_for(lambda: any("subscription callback failed" in r.message for r in caplog.records))
    sub.close()


def test_callback_may_call_the_peer(client, endpoint):
    out = []

    def cb(e):
        out.append(client.call("t.echo", {"from": e.data["i"]}))

    sub = client.subscribe("t.events", cb)
    assert wait_for(lambda: endpoint.subscriber_count() == 1)
    endpoint.publish({"i": 7})
    assert wait_for(lambda: out) and out == [{"from": 7}]
    sub.close()


def test_resubscribes_after_reconnect(key):
    import os
    import secrets

    from stintmate_ipc import _testing
    pipe = f"\\\\.\\pipe\\stintmate-t6test-{secrets.token_hex(4)}-{os.getpid()}"
    ep = _testing.start(pipe, key)
    c = Client(pipe, PeerInfo("c"), key)
    got = []
    try:
        c.connect(3)
        sub = c.subscribe("t.events", got.append)
        assert wait_for(lambda: ep.subscriber_count() == 1)
        ep.stop()
        assert wait_for(lambda: not c.connected, 5)
        ep = _testing.start(pipe, key)
        assert wait_for(lambda: c.connected and ep.subscriber_count() == 1, 10)
        ep.publish({"again": 1})
        assert wait_for(lambda: got)
        sub.close()
    finally:
        c.close()
        ep.stop()


def test_reconnect_false_does_not_retry(endpoint, key):
    c = Client(endpoint.path, PeerInfo("c"), key, reconnect=False)
    try:
        c.connect(3)
        c.peer.close()
        assert wait_for(lambda: not c.connected)
        time.sleep(1.0)
        assert not c.connected
    finally:
        c.close()


def test_on_connect_error_callback(key):
    errs = []
    c = Client("\\\\.\\pipe\\stintmate-nobody-here-z", PeerInfo("c"), key)
    c.on_connect_error = errs.append
    c.start()
    try:
        assert wait_for(lambda: errs, 5)
        assert isinstance(errs[0], ipc.PipeNotFound)
    finally:
        c.close()


def test_reconnect_delay_policy(key):
    c = Client("x", PeerInfo("c"), key)
    assert c.reconnect_delay("version_unsupported") is None
    assert c.reconnect_delay("unauthorized") == 10.0
    assert c.reconnect_delay("shutting_down", 0) == 0.25 and c.reconnect_delay("shutting_down", 5) == 0.5
    assert [c.reconnect_delay(None, n) for n in range(5)] == [0.25, 0.5, 1.0, 2.0, 2.0]


def test_client_subclass_overrides_reconnect_delay(key):
    class Mine(Client):
        def reconnect_delay(self, code, attempt=0):
            return 99.0 if code == "connector_busy" else super().reconnect_delay(code, attempt)

    c = Mine("x", PeerInfo("c"), key)
    assert c.reconnect_delay("connector_busy") == 99.0 and c.reconnect_delay(None) == 0.25


def test_call_when_not_connected(key):
    c = Client("\\\\.\\pipe\\stintmate-nobody-here-w", PeerInfo("c"), key)
    with pytest.raises(ipc.NotConnected):
        c.call("t.echo", timeout=0.5)


def test_blocking_call_releases_the_gil(client):
    ticks = []
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            ticks.append(time.perf_counter())
            time.sleep(0.005)

    th = threading.Thread(target=ticker)
    th.start()
    try:
        t0 = time.perf_counter()
        client.call("t.sleep", {"ms": 400})
        t1 = time.perf_counter()
    finally:
        stop.set()
        th.join()
    inside = [t for t in ticks if t0 <= t <= t1]
    assert len(inside) >= 20, f"only {len(inside)} ticks during a 400 ms blocking call"


def test_wait_connected_releases_the_gil(key):
    c = Client("\\\\.\\pipe\\stintmate-nobody-here-v", PeerInfo("c"), key)
    c.start()
    n = [0]
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            n[0] += 1
            time.sleep(0.005)

    th = threading.Thread(target=ticker)
    th.start()
    try:
        with pytest.raises(ipc.IpcError):
            c.wait_connected(0.5)
    finally:
        stop.set()
        th.join()
        c.close()
    assert n[0] >= 30


def test_many_threads_call_concurrently(client):
    out = []

    def work(i):
        for j in range(20):
            out.append(client.call("t.echo", {"i": i, "j": j}) == {"i": i, "j": j})

    ts = [threading.Thread(target=work, args=(i,)) for i in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(out) == 160 and all(out)


def test_import_is_clean_in_a_fresh_process():
    import subprocess
    import sys
    r = subprocess.run([sys.executable, "-c", "import stintmate_ipc as i; print(i.__version__, i.PROTOCOL_VERSION)"],
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 0 and r.stdout.split() == [ipc.__version__, "2"], r.stderr


def test_core_logs_reach_python_logging(endpoint, caplog):
    import logging
    logging.getLogger("stintmate_ipc").setLevel(logging.DEBUG)
    from stintmate_ipc import _native
    _native.refresh_log_level()
    try:
        with caplog.at_level(logging.DEBUG):
            c = Client(endpoint.path, PeerInfo("c"), b"\x09" * 32, reconnect=False)
            with pytest.raises(ipc.IpcError):
                c.connect(2)
            c.close()
            assert wait_for(lambda: any(r.name.startswith("stintmate_ipc") and r.name != "stintmate_ipc"
                                        for r in caplog.records), 3)
    finally:
        logging.getLogger("stintmate_ipc").setLevel(logging.NOTSET)
        _native.refresh_log_level()
