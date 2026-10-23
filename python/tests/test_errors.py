"""The exception hierarchy of 3.1 and the mapping from the core."""

import pytest

import stintmate_ipc as ipc
from stintmate_ipc import Client, PeerInfo


def test_hierarchy():
    assert issubclass(ipc.PipeError, ipc.IpcError) and issubclass(ipc.PipeError, OSError)
    for c in (ipc.PipeNotFound, ipc.PipeBusy, ipc.PipeClosed, ipc.EndpointInUse):
        assert issubclass(c, ipc.PipeError)
    for c in (ipc.KeyFileError, ipc.ProtocolError, ipc.HandshakeError, ipc.NotConnected,
              ipc.ConnectionLost, ipc.CallTimeout, ipc.CallCancelled, ipc.RemoteError):
        assert issubclass(c, ipc.IpcError)
    assert issubclass(ipc.CallTimeout, TimeoutError)
    assert not issubclass(ipc.CallCancelled, ipc.CallTimeout)
    assert ipc.ProtocolError.code == "protocol_error" and ipc.CallCancelled.code == "cancelled"


def test_remote_error_fields():
    e = ipc.RemoteError("busy", "try later", {"a": 1}, 250)
    assert (e.code, e.message, e.data, e.details, e.retry_ms) == (
        "busy", "try later", {"a": 1}, {"a": 1}, 250)
    assert str(e) == "busy: try later" and str(ipc.RemoteError("x")) == "x"
    assert ipc.RemoteError("x", details={"d": 1}).data == {"d": 1}


def test_handshake_error_fields():
    e = ipc.HandshakeError("unauthorized", "bad mac")
    assert e.code == "unauthorized" and e.message == "bad mac"
    assert str(e) == "unauthorized: bad mac"


def test_remote_error_from_peer(client):
    with pytest.raises(ipc.RemoteError) as ei:
        client.call("t.fail")
    e = ei.value
    assert (e.code, e.message, e.data, e.retry_ms) == (
        "game_not_running", "AMS2 is not running", {"sim": "ams2"}, 250)


def test_unknown_method(client):
    with pytest.raises(ipc.RemoteError) as ei:
        client.call("t.nothing")
    assert ei.value.code == "unknown_method"


def test_pipe_not_found(key):
    with pytest.raises(ipc.PipeNotFound) as ei:
        ipc.connect("\\\\.\\pipe\\stintmate-nobody-here-x", PeerInfo("c"), key, timeout=0.3)
    assert isinstance(ei.value, OSError) and ei.value.errno == 2


def test_wrong_key_is_a_handshake_error(endpoint):
    with pytest.raises(ipc.HandshakeError) as ei:
        ipc.connect(endpoint.path, PeerInfo("c"), b"\x07" * 32, timeout=3.0)
    assert ei.value.code == "unauthorized"


def test_wait_connected_times_out(key):
    c = Client("\\\\.\\pipe\\stintmate-nobody-here-y", PeerInfo("c"), key)
    c.start()
    try:
        with pytest.raises(ipc.IpcError):
            c.wait_connected(0.5)
        assert not c.connected and c.peer is None
        assert isinstance(c.last_error, ipc.PipeNotFound)
    finally:
        c.close()


def test_bad_key_argument():
    with pytest.raises(TypeError):
        Client("x", PeerInfo("c"), "not a key")
    with pytest.raises(ValueError):
        Client("x", PeerInfo("c"), b"short")
