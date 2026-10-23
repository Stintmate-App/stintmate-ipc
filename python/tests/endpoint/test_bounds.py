"""The subscriber's local bounds (SPEC 12.4): a slow or stalled callback never stops the
reader and never grows memory without bound. Ported from 0.1.4's test_pubsub, plus T7b."""

from __future__ import annotations

import ctypes
import os
import threading
import time
from ctypes import wintypes

from stintmate_ipc import Layout, PeerInfo, connect

from .conftest import wait_for


class _Counters(ctypes.Structure):
    _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
        (n, ctypes.c_size_t) for n in (
            "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
            "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
            "PagefileUsage", "PeakPagefileUsage", "PrivateUsage")]


def private_bytes() -> int:
    c = _Counters()
    c.cb = ctypes.sizeof(c)
    k32 = ctypes.WinDLL("kernel32")
    k32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi = ctypes.WinDLL("psapi")
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
    assert psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(c), c.cb)
    return c.PrivateUsage


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
    big = "x" * 60000
    for i in range(1, 400):
        st.set({"n": i, "pad": big})
    time.sleep(0.3)
    gate.set()
    wait_for(lambda: isinstance(got[-1], dict) and got[-1]["n"] == 399, timeout=10)
    assert len(got) < 100  # intermediate values were coalesced
    seen = [g["n"] for g in got[1:]]
    assert seen == sorted(seen)


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

    cs = c.subscribe("t.burst", slow, on_end=lambda s, code, msg: ends.append((s, code)))
    wait_for(lambda: ch.subscriber_count == 1)
    for i in range(500):
        ch.publish({"i": i})
    wait_for(lambda: ch.subscriber_count == 0)  # the local overload sent `unsub`
    assert not cs.subscribed
    gate.set()
    wait_for(lambda: [code for _, code in ends] == ["overloaded"])  # after the callback
    assert ends[0][0] is cs
    assert len(got) <= 51  # the one running and at most event_queue waiting
    # The Client subscribes again after 1 s.
    wait_for(lambda: ch.subscriber_count == 1, timeout=5)
    wait_for(lambda: cs.subscribed)
    ch.publish({"i": "again"})
    wait_for(lambda: got[-1] == 1)  # a new subscription counts from 1


def test_stalled_stream_callback_never_grows_memory(endpoint, client):
    big = Layout([("v", "f64", 4096)])  # 32 KiB samples
    ep = endpoint()
    s = ep.stream_channel("t.big", big, rate_hz=100, queue=8)
    ep.start()
    c = client()
    c.connect(5)
    got = []
    gate = threading.Event()

    def stalled(x):
        got.append((x.seq, x.dropped))
        gate.wait(30)

    c.subscribe("t.big", stalled)
    wait_for(lambda: s.subscriber_count == 1)
    body = big.pack(*([0.5] * 4096))
    s.send(body)
    wait_for(lambda: got)  # the callback is stuck from now on
    before = private_bytes()
    for _ in range(3000):  # ~96 MiB through the pipe while the callback is stuck
        s.send(body)
        time.sleep(0.0002)
    time.sleep(0.3)
    grown = private_bytes() - before
    # The local queue holds at most max(64, rate_hz) samples (~2 MiB) and the owner's ring 8.
    assert grown < 24 * 1024 * 1024, grown
    gate.set()
    wait_for(lambda: got[-1][0] == 3001, timeout=10)
    seqs = [q for q, _ in got]
    assert len(seqs) < 3001 and seqs == sorted(seqs)
    gap = next(i for i in range(1, len(seqs)) if seqs[i] != seqs[i - 1] + 1)
    assert got[gap][1] is True  # the first sample after a gap says `dropped`


def test_stalled_event_callback_never_grows_memory(endpoint, client):
    ep = endpoint()
    ch = ep.event_channel("t.burst")
    ep.start()
    c = client(event_queue=64)
    c.connect(5)
    gate = threading.Event()
    ends = []
    c.subscribe("t.burst", lambda e: gate.wait(30), on_end=lambda s, code, m: ends.append(code))
    wait_for(lambda: ch.subscriber_count == 1)
    pad = "z" * 30000
    before = private_bytes()
    for i in range(3000):  # ~90 MB offered while the callback is stuck
        ch.publish({"i": i, "pad": pad})
        if i % 50 == 0:
            time.sleep(0.001)
    time.sleep(0.3)
    grown = private_bytes() - before
    assert grown < 24 * 1024 * 1024, grown
    gate.set()
    wait_for(lambda: "overloaded" in ends)


def test_connect_with_a_node_offers_its_methods_and_reconnects(endpoint, client, key, pipe_name):
    ep = endpoint()
    ep.start()
    node = client(reconnect=False)
    node.method("n.hello")(lambda ctx, p: "hi from the node")
    info = PeerInfo("testcli")
    peer = connect(pipe_name, info, key, node=node, timeout=5)
    assert node.connected and peer.is_open
    assert peer.pid == os.getpid()  # the server's pid, from the pipe
    srv = ep.peers[0]
    assert srv.call("n.hello") == "hi from the node"
    srv.close("normal", "again")
    wait_for(lambda: not node.connected)
    peer2 = connect(pipe_name, info, key, node=node, timeout=5)
    assert peer2.is_open and node.connected
    wait_for(lambda: len(ep.peers) == 1)
    assert ep.peers[0].call("n.hello") == "hi from the node"


def test_connect_with_a_node_fails_fast(client, key, pipe_name):
    node = client(reconnect=False)
    t = time.monotonic()
    try:
        connect(pipe_name, PeerInfo("testcli"), key, node=node, timeout=5)
    except Exception as e:  # noqa: BLE001
        assert type(e).__name__ == "PipeNotFound", e
    else:
        raise AssertionError("connected to nothing")
    assert time.monotonic() - t < 2


def test_endpoint_stop_sends_its_bye(endpoint, client):
    ep = endpoint()
    ep.start()
    c = client(reconnect=False)
    peer = c.connect(5)
    ep.stop("restarting", "back soon")
    wait_for(lambda: not peer.is_open)
    assert (peer.close_code, peer.close_message) == ("restarting", "back soon")
