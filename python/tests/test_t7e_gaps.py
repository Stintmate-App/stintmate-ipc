"""Binding gaps found by the AMS2 connector's migration (T12): behaviours of stintmate-ipc 0.1.4."""

import secrets
import threading
import time

import stintmate_ipc as ipc
from stintmate_ipc import Client, Endpoint, Layout, PeerInfo

from test_client import wait_for


def _pair(client_cls=Client, **kw):
    key = secrets.token_bytes(32)
    pipe = f"stintmate-t7e-{secrets.token_hex(5)}"
    ep = Endpoint(PeerInfo("t7esrv"), key, pipe=pipe)
    return ep, pipe, key, client_cls


def test_peer_closed_is_a_waitable_event():
    ep, pipe, key, _ = _pair()
    ep.start()
    c = Client(pipe, PeerInfo("t7ecli"), key, reconnect=False)
    p = c.connect(5)
    try:
        assert p.closed is p.closed and not p.closed.is_set() and not p.closed
        t0 = time.monotonic()
        assert p.closed.wait(0.1) is False
        assert time.monotonic() - t0 >= 0.09
        threading.Timer(0.2, p.close).start()
        t0 = time.monotonic()
        assert p.closed.wait(5) is True
        assert time.monotonic() - t0 < 2 and p.closed.is_set() and not p.is_open
        assert p.closed.wait() is True
    finally:
        c.close()
        ep.stop()


def test_subscription_layout():
    ep, pipe, key, _ = _pair()
    lay = Layout([("a", "u32"), ("b", "f32", 2)])
    s = ep.stream_channel("t.s", lay, rate_hz=50)
    ep.start()
    c = Client(pipe, PeerInfo("t7ecli"), key)
    p = c.connect(5)
    try:
        got = []
        sub = p.subscribe("t.s", got.append)
        assert sub.layout == lay
        cs = c.subscribe("t.s", got.append)
        assert cs.subscribed.wait(3)
        assert cs.layout == lay
    finally:
        c.close()
        ep.stop()


def test_ended_and_subscribed_wake_without_polling():
    ep, pipe, key, _ = _pair()
    ep.start()
    c = Client(pipe, PeerInfo("t7ecli"), key)
    p = c.connect(5)
    try:
        cs = c.subscribe("t.later", print)  # unknown channel now
        assert cs.subscribed.wait(0.2) is False
        ep.state_channel("t.later", initial=1)
        # 15.1: re-subscribed after a reconnect or when the owner declares it; at least the
        # wait must return false/true promptly and never hang
        cs.subscribed.wait(0.3)
    finally:
        c.close()
        ep.stop()


class Fast(Client):
    calls = []

    def reconnect_delay(self, code, attempt=0):
        Fast.calls.append((code, attempt))
        return 0.05 if len(Fast.calls) < 50 else None


class Broken(Client):
    def reconnect_delay(self, code, attempt=0):
        raise RuntimeError("boom")


def test_reconnect_delay_override_is_called():
    Fast.calls.clear()
    key = secrets.token_bytes(32)
    pipe = f"stintmate-t7e-{secrets.token_hex(5)}"
    c = Fast(pipe, PeerInfo("t7ecli"), key)
    c.start()
    try:
        # nobody listens: each failed attempt asks the override; the built-in backoff would
        # make fewer than 10 attempts in 1 s after 0.25/0.5/1... s
        assert wait_for(lambda: len(Fast.calls) >= 8, 3)
        assert Fast.calls[0][1] == 0 and Fast.calls[1][1] == 1
    finally:
        c.close()


def test_reconnect_delay_override_that_fails_falls_back():
    key = secrets.token_bytes(32)
    pipe = f"stintmate-t7e-{secrets.token_hex(5)}"
    ep = Endpoint(PeerInfo("t7esrv"), key, pipe=pipe)
    c = Broken(pipe, PeerInfo("t7ecli"), key)
    c.start()  # fails over to the built-in 0.25 s backoff
    time.sleep(0.4)
    ep.start()
    try:
        assert c.wait_connected(5) is not None
    finally:
        c.close()
        ep.stop()


def test_plain_client_keeps_the_builtin_policy():
    c = Client("stintmate-t7e-x", PeerInfo("t7ecli"), secrets.token_bytes(32))
    assert c.reconnect_delay("version_unsupported") is None
    assert c.reconnect_delay(None, 0) == 0.25
