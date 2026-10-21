"""Request/response (SPEC.md 10.2-10.5)."""

from __future__ import annotations

import threading
import time

import pytest

from stintmate_ipc import (CallCancelled, CallTimeout, ConnectionLost, NotConnected,
                          RemoteError)

from .conftest import wait_for


def test_call_echo_and_context(endpoint, client):
    ep = endpoint()
    seen = {}

    @ep.method("t.echo", versions=(1, 2))
    def echo(ctx, params):
        seen.update(version=ctx.version, ik=ctx.idempotency_key, app=ctx.peer.info.app,
                    remaining=ctx.remaining, method=ctx.method)
        return {"p": params, "v": ctx.version}

    ep.start()
    c = client()
    c.connect(5)
    assert c.call("t.echo", {"a": [1, 2], "b": b"\x00"}) == {"p": {"a": [1, 2], "b": b"\x00"},
                                                              "v": 1}
    assert c.call("t.echo", None, version=2, idempotency_key="k1", timeout=2)["v"] == 2
    assert seen["ik"] == "k1" and seen["app"] == "testcli" and seen["method"] == "t.echo"
    assert 0 < seen["remaining"] <= 2


def test_errors(endpoint, client):
    ep = endpoint()

    @ep.method("t.fail")
    def fail(ctx, params):
        raise RemoteError("game_not_running", "the game is not running", {"sim": "x"},
                          retry_ms=500)

    @ep.method("t.crash")
    def crash(ctx, params):
        raise ZeroDivisionError("boom")

    @ep.method("t.bad_result")
    def bad_result(ctx, params):
        return object()

    ep.start()
    c = client()
    c.connect(5)
    with pytest.raises(RemoteError) as ei:
        c.call("t.fail")
    assert (ei.value.code, ei.value.message, ei.value.details, ei.value.retry_ms) == \
        ("game_not_running", "the game is not running", {"sim": "x"}, 500)
    with pytest.raises(RemoteError) as ei:
        c.call("t.crash")
    assert ei.value.code == "internal" and "ZeroDivisionError" in ei.value.message
    with pytest.raises(RemoteError) as ei:
        c.call("t.bad_result")
    assert ei.value.code == "internal"
    with pytest.raises(RemoteError) as ei:
        c.call("t.nope")
    assert ei.value.code == "unknown_method"
    with pytest.raises(RemoteError) as ei:
        c.call("t.fail", version=7)
    assert ei.value.code == "version_unsupported" and ei.value.details == {"versions": [1]}


def test_timeout_sends_cancel(endpoint, client):
    ep = endpoint()
    cancelled = threading.Event()

    @ep.method("t.slow")
    def slow(ctx, params):
        if ctx.wait_cancelled(5):
            cancelled.set()
        return "late"

    ep.start()
    c = client()
    c.connect(5)
    t0 = time.monotonic()
    with pytest.raises(CallTimeout):
        c.call("t.slow", timeout=0.3)
    assert 0.25 < time.monotonic() - t0 < 1.5
    assert cancelled.wait(3)


def test_result_wait_without_giving_up(endpoint, client):
    ep = endpoint()
    gate = threading.Event()
    ep.method("t.wait", lambda ctx, p: gate.wait(5) and "done")
    ep.start()
    c = client()
    c.connect(5)
    pc = c.call_async("t.wait", timeout=10)
    with pytest.raises(CallTimeout):
        pc.result(timeout=0.1)  # only the wait timed out
    assert not pc.done()
    gate.set()
    assert pc.result() == "done"
    done = []
    pc.add_done_callback(done.append)
    assert done == [pc]


def test_progress_extends_deadline(endpoint, client):
    ep = endpoint()

    @ep.method("t.capture")
    def capture(ctx, params):
        for i in range(4):
            time.sleep(0.15)
            ctx.progress({"shot": i}, extend_s=0.5)
        return "ok"

    ep.start()
    c = client()
    c.connect(5)
    prog = []
    assert c.call("t.capture", timeout=0.3, on_progress=prog.append) == "ok"
    assert prog == [{"shot": i} for i in range(4)]


def test_expired_before_run(endpoint, client):
    ep = endpoint(handler_threads=1)
    block = threading.Event()
    ran = []
    ep.method("t.block", lambda ctx, p: block.wait(5))
    ep.method("t.quick", lambda ctx, p: ran.append(1))
    ep.start()
    c = client()
    c.connect(5)
    first = c.call_async("t.block", timeout=10)
    late = c.call_async("t.quick", timeout=0.2)
    time.sleep(0.4)
    block.set()
    assert first.result() is True
    with pytest.raises((CallTimeout, RemoteError)):  # whichever deadline fired first
        late.result()
    time.sleep(0.2)
    assert ran == []  # the server answered timeout without running it


def test_ordered_methods_run_in_order_one_at_a_time(endpoint, client):
    ep = endpoint()
    log = []
    active = []

    @ep.method("t.write", ordered=True)
    def write(ctx, params):
        active.append(1)
        assert len(active) == 1
        time.sleep(0.002)
        log.append(params["i"])
        active.pop()
        return params["i"]

    ep.start()
    c = client()
    c.connect(5)
    calls = [c.call_async("t.write", {"i": i}, timeout=10) for i in range(50)]
    assert [pc.result() for pc in calls] == list(range(50))
    assert log == list(range(50))


def test_unordered_methods_run_concurrently(endpoint, client):
    ep = endpoint(handler_threads=4)
    barrier = threading.Barrier(4, timeout=3)
    ep.method("t.meet", lambda ctx, p: barrier.wait() >= 0)
    ep.start()
    c = client()
    c.connect(5)
    calls = [c.call_async("t.meet", timeout=5) for _ in range(4)]
    assert all(pc.result() for pc in calls)


def test_inline_methods(endpoint, client):
    ep = endpoint()
    ep.method("t.now", lambda ctx, p: threading.current_thread().name, inline=True)
    ep.start()
    c = client()
    c.connect(5)
    assert c.call("t.now").startswith("ipc-worker")


def test_idempotency_cache(endpoint, client):
    ep = endpoint()
    runs = []

    @ep.method("t.store", idempotent=True)
    def store(ctx, params):
        runs.append(params)
        return {"stored": len(runs)}

    ep.start()
    c = client()
    c.connect(5)
    assert c.call("t.store", {"lap": 1}, idempotency_key="s1/lap/1") == {"stored": 1}
    assert c.call("t.store", {"lap": 1}, idempotency_key="s1/lap/1") == {"stored": 1}
    assert c.call("t.store", {"lap": 2}, idempotency_key="s1/lap/2") == {"stored": 2}
    assert c.call("t.store", {"lap": 3}) == {"stored": 3}  # no key: no deduplication
    assert ep.declarations().methods["t.store"].idempotent


def test_large_payloads_both_ways(endpoint, client):
    ep = endpoint()
    ep.method("t.echo", lambda ctx, p: p)
    ep.start()
    c = client()
    c.connect(5)
    blob = bytes(range(256)) * (7 * 1024 * 4)  # 7 MiB
    r = c.call("t.echo", {"lap": blob, "n": 1}, timeout=30)
    assert r["lap"] == blob


def test_server_calls_client(endpoint, client):
    ep = endpoint()
    peers = []
    ep.on_connect = peers.append
    ep.start()
    c = client()
    c.method("conn.live_frame", lambda ctx, p: {"speed": 101.5, "caller": ctx.peer.info.app})
    c.connect(5)
    srv_peer = wait_for(lambda: peers and peers[0])
    assert "conn.live_frame" in srv_peer.decl.methods
    assert srv_peer.call("conn.live_frame") == {"speed": 101.5, "caller": "testsrv"}


def test_declaring_later_sends_decl(endpoint, client):
    ep = endpoint().start()
    c = client()
    peer = c.connect(5)
    assert "t.late" not in peer.decl.methods
    ep.method("t.late", lambda ctx, p: 1)
    wait_for(lambda: "t.late" in peer.decl.methods)
    assert c.call("t.late") == 1
    ep.remove_method("t.late")
    wait_for(lambda: "t.late" not in peer.decl.methods)


def test_pending_call_fails_when_connection_closes(endpoint, client):
    ep = endpoint()
    ep.method("t.hang", lambda ctx, p: time.sleep(3))
    ep.start()
    c = client(reconnect=False)
    c.connect(5)
    pc = c.call_async("t.hang", timeout=10)
    time.sleep(0.1)
    ep.stop()
    with pytest.raises(ConnectionLost):
        pc.result()
    wait_for(lambda: not c.connected)
    with pytest.raises(NotConnected):
        c.call("t.hang")


def test_ping_rtt(endpoint, client):
    endpoint().start()
    peer = client().connect(5)
    assert 0 < peer.ping() < 0.5


def test_too_many_requests_in_flight_get_busy(endpoint, client):
    ep = endpoint(max_inflight=3)
    gate = threading.Event()
    ep.method("t.hold", lambda ctx, p: gate.wait(5))
    ep.start()
    c = client()
    c.connect(5)
    calls = [c.call_async("t.hold", timeout=5) for _ in range(5)]
    with pytest.raises(RemoteError) as ei:
        calls[4].result()
    assert ei.value.code == "busy" and ei.value.retry_ms == 100
    gate.set()
    assert all(pc.result() for pc in calls[:3])


@pytest.mark.parametrize("code", ["shutting_down", "writer_busy", "busy", "unavailable"])
def test_retryable_errors_are_not_cached(endpoint, client, code):
    ep = endpoint()
    runs = []

    @ep.method("t.store", idempotent=True)
    def store(ctx, params):
        runs.append(1)
        if len(runs) == 1:
            raise RemoteError(code, "not now")
        return {"runs": len(runs)}

    ep.start()
    c = client()
    c.connect(5)
    with pytest.raises(RemoteError) as ei:
        c.call("t.store", None, idempotency_key="k/1")
    assert ei.value.code == code
    assert c.call("t.store", None, idempotency_key="k/1") == {"runs": 2}
    assert c.call("t.store", None, idempotency_key="k/1") == {"runs": 2}  # now cached


def test_permanent_errors_stay_cached(endpoint, client):
    ep = endpoint()
    runs = []

    @ep.method("t.store", idempotent=True)
    def store(ctx, params):
        runs.append(1)
        raise RemoteError("bad_request", "no")

    ep.start()
    c = client()
    c.connect(5)
    for _ in range(2):
        with pytest.raises(RemoteError):
            c.call("t.store", None, idempotency_key="k/1")
    assert len(runs) == 1


def test_idempotency_cache_can_be_disabled(endpoint, client):
    ep = endpoint()
    runs = []

    @ep.method("t.store", idempotent=True, idempotency_cache=False)
    def store(ctx, params):
        runs.append(1)
        return len(runs)

    ep.start()
    c = client()
    c.connect(5)
    assert c.call("t.store", None, idempotency_key="k/1") == 1
    assert c.call("t.store", None, idempotency_key="k/1") == 2
    assert ep.declarations().methods["t.store"].idempotent


def test_idempotency_ttl_expires_answers(endpoint, client):
    ep = endpoint()
    runs = []

    @ep.method("t.store", idempotent=True, idempotency_ttl=0.3)
    def store(ctx, params):
        runs.append(1)
        return len(runs)

    ep.start()
    c = client()
    c.connect(5)
    assert c.call("t.store", None, idempotency_key="k/1") == 1
    assert c.call("t.store", None, idempotency_key="k/1") == 1
    time.sleep(0.4)
    assert c.call("t.store", None, idempotency_key="k/1") == 2
    with pytest.raises(ValueError):
        ep.method("t.bad", lambda ctx, p: 1, idempotent=True, idempotency_ttl=0)


def test_waiter_on_inflight_idempotent_request_gets_busy():
    from stintmate_ipc.node import IdempotencyCache
    cache = IdempotencyCache(wait_s=0.3)
    release = threading.Event()
    started = threading.Event()
    key = ("app", "t.slow", "k/1")

    def slow():
        started.set()
        release.wait(10)
        return "done"

    first = []
    th = threading.Thread(target=lambda: first.append(cache.run(key, slow)))
    th.start()
    assert started.wait(5)
    t0 = time.perf_counter()
    with pytest.raises(RemoteError) as ei:  # default wait
        cache.run(key, lambda: "second")
    assert ei.value.code == "busy" and ei.value.retry_ms
    with pytest.raises(RemoteError) as ei:  # the request's own deadline
        cache.run(key, lambda: "second", time.perf_counter() + 0.05)
    assert ei.value.code == "busy"
    assert time.perf_counter() - t0 < 3
    release.set()
    th.join(5)
    assert first == ["done"]
    assert cache.run(key, lambda: "second") == "done"


# ---------------------------------------------------------------------- cancel (0.1.2)

def _slow_cancel_aware(ep, name="t.slow"):
    saw = threading.Event()

    def slow(ctx, params):
        if ctx.wait_cancelled(5):
            saw.set()
        return "late"

    ep.method(name, slow)
    return saw


def test_cancel_sends_cancel_and_fails_with_call_cancelled(endpoint, client):
    ep = endpoint()
    saw = _slow_cancel_aware(ep)
    ep.start()
    c = client()
    c.connect(5)
    pc = c.call_async("t.slow", timeout=10)
    t0 = time.monotonic()
    assert pc.cancel("not needed") is True
    with pytest.raises(CallCancelled) as ei:
        pc.result()
    assert time.monotonic() - t0 < 0.2
    assert ei.value.code == "cancelled" and "not needed" in str(ei.value)
    assert not isinstance(ei.value, (CallTimeout, TimeoutError))
    assert saw.wait(3)
    time.sleep(0.3)  # the handler's late response is ignored
    assert pc.done() and isinstance(pc.exception(), CallCancelled)


def test_cancel_after_the_answer_is_a_no_op(endpoint, client):
    ep = endpoint()
    ep.method("t.echo", lambda ctx, p: p)
    ep.start()
    c = client()
    c.connect(5)
    pc = c.call_async("t.echo", {"a": 1})
    assert pc.result() == {"a": 1}
    assert pc.cancel() is False
    assert pc.result() == {"a": 1} and pc.exception() is None


def test_cancel_is_idempotent_and_callbacks_run_once(endpoint, client):
    ep = endpoint()
    _slow_cancel_aware(ep)
    ep.start()
    c = client()
    c.connect(5)
    pc = c.call_async("t.slow", timeout=10)
    seen = []
    pc.add_done_callback(lambda p: seen.append(threading.get_ident()))
    assert pc.cancel() is True
    assert pc.cancel() is False
    assert seen == [threading.get_ident()]  # on the cancelling thread, once


def test_cancel_from_threads_races_with_the_answer(endpoint, client):
    ep = endpoint()
    ep.method("t.echo", lambda ctx, p: p)
    ep.start()
    c = client()
    c.connect(5)
    outcomes = {"ok": 0, "cancelled": 0}
    for i in range(200):
        pc = c.call_async("t.echo", i)
        calls = []
        pc.add_done_callback(lambda p, calls=calls: calls.append(1))
        t = threading.Thread(target=pc.cancel)
        t.start()
        t.join(5)
        assert pc._wait(5), "call hung"
        assert calls == [1]
        err = pc.exception()
        if err is None:
            assert pc.result() == i
            outcomes["ok"] += 1
        else:
            assert isinstance(err, CallCancelled)
            outcomes["cancelled"] += 1
    assert sum(outcomes.values()) == 200
    assert c.call("t.echo", "still alive") == "still alive"


def test_cancel_from_a_done_callback_and_from_an_inline_handler(endpoint, client):
    ep = endpoint()
    _slow_cancel_aware(ep)
    ep.method("t.echo", lambda ctx, p: p)
    ep.start()
    c = client()
    c.connect(5)
    # a progress/done callback cancelling another call, and a call cancelling itself
    other = c.call_async("t.slow", timeout=10)
    trigger = c.call_async("t.echo", 1)
    trigger.add_done_callback(lambda p: other.cancel())
    assert trigger.result() == 1
    with pytest.raises(CallCancelled):
        other.result(timeout=3)
    # inline subscription callback (reader thread) cancelling a call
    ch = ep.event_channel("t.ev")
    victim = c.call_async("t.slow", timeout=10)
    got = []
    c.subscribe("t.ev", lambda ev: got.append(victim.cancel()), deliver="inline")
    ch.publish({"x": 1})
    assert wait_for(lambda: got, 3)
    assert got == [True]
    with pytest.raises(CallCancelled):
        victim.result(timeout=3)


def test_cancel_when_disconnected_finishes_locally(endpoint, client):
    ep = endpoint()
    _slow_cancel_aware(ep)
    ep.start()
    c = client()
    c.connect(5)
    pc = c.call_async("t.slow", timeout=10)

    def refuse(item):
        raise NotConnected("gone")

    pc.peer._enqueue = refuse
    assert pc.cancel() is True
    with pytest.raises(CallCancelled):
        pc.result()


def test_deadline_without_a_waiter_times_out(endpoint, client):
    ep = endpoint()
    saw = _slow_cancel_aware(ep)
    ep.start()
    c = client()
    c.connect(5)
    got = []
    t0 = time.monotonic()
    pc = c.call_async("t.slow", timeout=0.3)
    pc.add_done_callback(lambda p: got.append((time.monotonic() - t0, p.exception())))
    assert wait_for(lambda: got, 3)
    elapsed, err = got[0]
    assert isinstance(err, CallTimeout) and 0.25 < elapsed < 0.9
    assert saw.wait(3)


def test_progress_still_extends_the_deadline_for_the_sweep(endpoint, client):
    ep = endpoint()

    @ep.method("t.capture")
    def capture(ctx, params):
        for i in range(4):
            time.sleep(0.15)
            ctx.progress({"shot": i}, extend_s=0.5)
        return "ok"

    ep.start()
    c = client()
    c.connect(5)
    pc = c.call_async("t.capture", timeout=0.3)
    got = []
    pc.add_done_callback(got.append)  # nobody waits: only the sweep watches the deadline
    assert wait_for(lambda: got, 5)
    assert pc.exception() is None and pc.result() == "ok"


def test_cancel_a_subscribe_wait(endpoint, client):
    from stintmate_ipc.peer import PendingCall
    ep = endpoint()
    ep.start()
    c = client()
    c.connect(5)
    peer = c.peer
    sent = []
    real = peer._enqueue
    peer._enqueue = lambda item: sent.append(item) or real(item)
    rid = 424242
    pc = PendingCall(peer, rid, "sub t.x", 5.0, None)
    with peer._lock:
        peer._sub_waits[rid] = (None, pc)
    assert pc.cancel() is True
    with pytest.raises(CallCancelled):
        pc.result()
    assert rid in peer._abandoned
    assert not any(b"cancel" in repr(x).encode() for x in sent)  # no cancel frame for a sub
    peer._h_res({"id": rid, "ok": True, "r": {"sid": 99, "kind": "event"}})
    assert any(b"unsub" in repr(x).encode() for x in sent)  # the late sid is released


def test_deadline_sweep_cost(endpoint, client):
    ep = endpoint()
    ep.start()
    c = client()
    c.connect(5)
    peer = c.peer
    from stintmate_ipc.peer import PendingCall
    with peer._lock:
        for i in range(1000):
            peer._calls[10_000 + i] = PendingCall(peer, 10_000 + i, "t.x", 600.0, None)
    t0 = time.perf_counter()
    peer._expire_calls(time.perf_counter())
    assert time.perf_counter() - t0 < 0.01  # generous bound; the benchmark has the number
    with peer._lock:
        peer._calls.clear()
