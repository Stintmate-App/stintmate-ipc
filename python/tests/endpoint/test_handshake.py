"""Handshake over real pipes (SPEC.md 3, 8)."""

from __future__ import annotations

import os
import secrets
import time

import pytest

from stintmate_ipc import EndpointInUse, HandshakeError, PeerInfo, PipeNotFound, connect


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
    # the server's pid is not known on the client side of the Rust core (server side only)
    from .conftest import wait_for
    srv_side = wait_for(lambda: seen and seen[0])
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
    assert failures and failures[0] == (os.getpid(), "unauthorized")
    assert ep.peers == []




def test_endpoint_in_use(endpoint):
    endpoint().start()
    with pytest.raises(EndpointInUse):
        endpoint().start()


def test_no_server(pipe_name, key):
    with pytest.raises(PipeNotFound):
        connect(pipe_name, PeerInfo("c"), key, timeout=1)














def test_key_file_missing_on_server(endpoint, client, tmp_path):
    from stintmate_ipc import KeyFile
    endpoint(key=KeyFile(tmp_path / "missing")).start()
    c = client(reconnect=False)
    c.start()
    with pytest.raises(Exception):
        c.wait_connected(2)
    assert isinstance(c.last_error, HandshakeError) and c.last_error.code == "unavailable"
