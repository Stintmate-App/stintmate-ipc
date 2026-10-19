"""Handshake over real pipes (SPEC.md 3, 8)."""

from __future__ import annotations

import os
import secrets
import time

import pytest

from stintmate_ipc import (EndpointInUse, HandshakeError, PeerInfo, PipeNotFound, connect,
                           wire)
from stintmate_ipc.check.raw import Closed, RawConn
from stintmate_ipc.handshake import client_mac, server_mac


def test_connect_and_identity(endpoint, client):
    ep = endpoint(info=PeerInfo("pitwall", name="Pitwall", version="9.9", process="core",
                                meta={"x": 1}))
    seen = []
    ep.on_connect = seen.append
    ep.start()
    c = client(info=PeerInfo("connector-examplesim", name="Example", version="0.1",
                             process="recorder", meta={"sim": "examplesim"}))
    peer = c.connect(5)
    assert peer.info.app == "pitwall" and peer.info.process == "core"
    assert peer.info.meta == {"x": 1} and peer.version == 2 and peer.run == ep.run
    assert peer.pid == os.getpid()  # GetNamedPipeServerProcessId
    srv_side = seen[0]
    assert srv_side.info.app == "connector-examplesim"
    assert srv_side.info.meta == {"sim": "examplesim"}
    assert srv_side.pid == os.getpid()  # GetNamedPipeClientProcessId
    assert srv_side.run == ep.run


def test_wrong_key_is_refused_both_ways(endpoint, client, key):
    ep = endpoint()
    failures = []
    ep.on_handshake_failed = lambda pid, exc: failures.append((pid, exc.code))
    ep.start()
    c = client(key=secrets.token_bytes(32), reconnect=False)
    c.start()
    with pytest.raises(Exception):
        c.wait_connected(2)
    assert isinstance(c.last_error, HandshakeError) and c.last_error.code == "unauthorized"
    assert "same key file" in str(c.last_error)
    assert failures and failures[0] == (os.getpid(), "unauthorized")
    assert ep.peers == []


def test_impostor_server_is_detected(key, pipe_name):
    """A server without the key cannot produce the welcome MAC (squatting, SPEC.md 18)."""
    import threading

    from stintmate_ipc import _win32
    accepted = []
    lis = _win32.Listener("\\\\.\\pipe\\" + pipe_name, accepted.append).start()
    try:
        def fake():
            while not accepted:
                time.sleep(0.01)
            rc = RawConn(accepted[0], pipe_name)
            try:
                rc.server_handshake(secrets.token_bytes(32))
            except PermissionError:
                pass
            finally:
                rc.close()
        t = threading.Thread(target=fake)
        t.start()
        with pytest.raises(HandshakeError) as ei:
            connect(pipe_name, PeerInfo("c"), key, timeout=3)
        t.join()
        assert ei.value.code == "unauthorized"
    finally:
        lis.stop()

    accepted.clear()
    lis = _win32.Listener("\\\\.\\pipe\\" + pipe_name, accepted.append).start()
    try:
        def bad_mac():
            while not accepted:
                time.sleep(0.01)
            rc = RawConn(accepted[0], pipe_name)
            rc.server_handshake(key, bad_mac=True)
            seen = rc.wait_closed(3)
            rc.close()
            assert [m["t"] for m in seen] == ["bye"] and seen[0]["code"] == "unauthorized"
        t = threading.Thread(target=bad_mac)
        t.start()
        with pytest.raises(HandshakeError, match="server's MAC"):
            connect(pipe_name, PeerInfo("c"), key, timeout=3)
        t.join()
    finally:
        lis.stop()


def test_endpoint_in_use(endpoint):
    endpoint().start()
    with pytest.raises(EndpointInUse):
        endpoint().start()


def test_no_server(pipe_name, key):
    with pytest.raises(PipeNotFound):
        connect(pipe_name, PeerInfo("c"), key, timeout=1)


def test_hello_fields_and_server_mac(endpoint, key, pipe_name):
    endpoint(max_frame=2 << 20).start()
    rc = RawConn.open(pipe_name)
    try:
        nonce = b"\x11" * 32
        welcome = rc.client_handshake(key, nonce=nonce)
        h = rc.hello
        assert h["proto"] == "stintmate-ipc" and h["versions"] == [2]
        assert len(h["nonce"]) == 32 and h["max_frame"] == 2 << 20
        assert isinstance(h["clock"], list) and len(h["clock"]) == 2
        assert welcome["t"] == "welcome" and rc.server_mac_ok
        assert welcome["mac"] == server_mac(key, 2, h["nonce"], nonce, pipe_name)
        assert rc.auth["mac"] == client_mac(key, 2, h["nonce"], nonce, pipe_name)
    finally:
        rc.close()


def test_version_negotiation_refusal(endpoint, key, pipe_name):
    endpoint().start()
    rc = RawConn.open(pipe_name)
    try:
        reply = rc.client_handshake(key, version=3)
        assert reply == {"t": "bye", "code": "version_unsupported",
                         "msg": "protocol version 3 not supported"}
    finally:
        rc.close()


def test_handshake_deadline(endpoint, pipe_name):
    endpoint(handshake_timeout=0.5).start()
    rc = RawConn.open(pipe_name)
    try:
        rc.recv_msg(skip=())
        t0 = time.monotonic()
        rc.wait_closed(5)
        assert time.monotonic() - t0 < 2
    finally:
        rc.close()


def test_anything_before_auth_is_refused(endpoint, key, pipe_name):
    endpoint().start()
    rc = RawConn.open(pipe_name)
    try:
        rc.recv_msg(skip=())
        rc.send({"t": "req", "id": 1, "m": "a.b"})
        with pytest.raises(Closed):
            rc.recv_msg(timeout=3)
    finally:
        rc.close()


def test_handshake_frames_are_limited(endpoint, key, pipe_name):
    endpoint().start()
    rc = RawConn.open(pipe_name)
    try:
        rc.recv_msg(skip=())
        rc.send_frame(wire.HEADER.pack(wire.HANDSHAKE_MAX_FRAME + 1, 1))
        with pytest.raises(Closed):
            rc.recv_msg(timeout=3)
    finally:
        rc.close()


def test_features_are_intersected(endpoint, client):
    ep = endpoint(features=["x.zstd", "x.both"]).start()
    c = client(features=["x.both", "x.other"])
    peer = c.connect(5)
    assert peer.features == ["x.both"]
    assert ep.peers[0].features == ["x.both"]


def test_key_file_missing_on_server(endpoint, client, tmp_path):
    from stintmate_ipc import KeyFile
    endpoint(key=KeyFile(tmp_path / "missing")).start()
    c = client(reconnect=False)
    c.start()
    with pytest.raises(Exception):
        c.wait_connected(2)
    assert isinstance(c.last_error, HandshakeError) and c.last_error.code == "unavailable"
