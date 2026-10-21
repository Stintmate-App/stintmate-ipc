"""The conformance checker against the reference implementation, and against broken peers."""

from __future__ import annotations

import threading
import time

from stintmate_ipc import Layout, PeerInfo, RemoteError, wire
from stintmate_ipc.check import cli
from stintmate_ipc.check.client import ClientChecker
from stintmate_ipc.check.raw import RawConn
from stintmate_ipc.check.server import ServerChecker


def _producer(ep, stop):
    ev = ep.event_channel("t.ev")
    st = ep.state_channel("t.st", {"n": 0})
    s = ep.stream_channel("t.s", Layout([("a", "f32", 12)]), rate_hz=200)

    def run():
        i = 0
        while not stop.is_set():
            i += 1
            time.sleep(0.005)
            s.send_values(*([1.0] * 12))
            if i % 20 == 0:
                ev.publish({"i": i})
                st.set({"n": i})
    threading.Thread(target=run, daemon=True).start()


def test_server_checker_passes_the_reference(endpoint, key, pipe_name):
    ep = endpoint(heartbeat_ms=500)
    ep.method("t.echo", lambda ctx, p: p)
    stop = threading.Event()
    _producer(ep, stop)
    ep.start()
    try:
        report = ServerChecker(pipe_name, key, listen_s=0.5, calls=[("t.echo", {"a": 1})]).run()
    finally:
        stop.set()
    text = report.to_text()
    assert report.ok, text
    ids = {r.id for r in report.results}
    assert {"S01", "S02", "S03", "S06", "S15", "S19.t.s", "S19.t.st", "S22", "S24"} <= ids
    assert all(r.status in ("PASS", "INFO") for r in report.results), text


def test_client_checker_passes_the_reference(client, key, pipe_name):
    def run_client():
        time.sleep(0.3)
        c = client()
        tel = c.stream_channel("cli.s", Layout([("v", "u32")]), rate_hz=50)
        c.start()
        c.wait_connected(10)
        c.subscribe("check.events", lambda e: None)
        c.subscribe("check.stream", lambda e: None)
        try:
            c.call("check.fail")
        except RemoteError:
            pass
        for i in range(400):
            tel.send_values(i)
            time.sleep(0.02)

    threading.Thread(target=run_client, daemon=True).start()
    report = ClientChecker(pipe_name, key, timeout=10, serve_s=1.0, listen_s=0.5).run()
    assert report.ok, report.to_text()
    assert all(r.status == "PASS" for r in report.results), report.to_text()
    assert report.facts["client subscriptions"] == ["check.events", "check.stream"]


def test_server_checker_flags_a_broken_server(key, pipe_name):
    """A server that answers unknown methods with ok:true and never pongs."""
    from stintmate_ipc import _win32
    accepted = []

    def serve(h):
        accepted.append(h)

        def run():
            rc = RawConn(h, pipe_name)
            try:
                rc.server_handshake(key, decl={"methods": {}, "channels": {}})
                while True:
                    kind, m = rc.recv(timeout=30)  # never answers ping
                    if kind == "msg" and m["t"] == "req":
                        rc.send({"t": "res", "id": m["id"], "ok": True, "r": None})
            except Exception:  # noqa: BLE001
                pass
            finally:
                rc.close()
        threading.Thread(target=run, daemon=True).start()

    lis = _win32.Listener("\\\\.\\pipe\\" + pipe_name, serve).start()
    try:
        report = ServerChecker(pipe_name, key, slow=False).run()
    finally:
        lis.stop()
        for h in accepted:
            h.close()
    status = {r.id: r.status for r in report.results}
    assert not report.ok
    assert status["S07"] == "FAIL"  # no pong
    assert status["S09"] == "FAIL"  # unknown method answered ok
    assert status["S01"] == "PASS" and status["S02"] == "PASS"


def test_cli_server_json(endpoint, key, pipe_name, capsys):
    ep = endpoint()
    ep.start()
    code = cli.main(["server", pipe_name, "--key-hex", key.hex(), "--fast", "--json"])
    out = capsys.readouterr().out
    assert code == 0, out
    assert '"ok": true' in out


def test_cli_wrong_key_fails(endpoint, key, pipe_name, capsys):
    endpoint().start()
    code = cli.main(["server", pipe_name, "--key-hex", "00" * 32, "--fast"])
    out = capsys.readouterr().out
    assert code == 1 and "FAIL  S02" in out


def test_cli_keygen_and_list(tmp_path, endpoint, capsys):
    assert cli.main(["keygen", str(tmp_path / "ipc" / "key")]) == 0
    assert "created" in capsys.readouterr().out
    ep = endpoint(info=PeerInfo("listed"), registry=tmp_path / "ipc" / "endpoints").start()
    assert cli.main(["list", "--workspace", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert ep.path in out and "alive" in out


def test_cli_usage_errors(capsys, tmp_path):
    assert cli.main(["server", "x", "--key-file", str(tmp_path / "none")]) == 2
    assert "key file not found" in capsys.readouterr().err
    assert wire.PROTOCOL_VERSION == 2


def _run_cli(*args, timeout=60):
    import subprocess
    import sys
    return subprocess.run([sys.executable, "-m", "stintmate_ipc.check.cli", *args],
                          capture_output=True, text=True, timeout=timeout)


def test_cli_process_exits_cleanly(endpoint, key, pipe_name):
    ep = endpoint()
    ep.event_channel("t.ev")
    ep.start()
    r = _run_cli("server", pipe_name, "--key-hex", key.hex(), "--fast", "--listen", "0.3")
    assert "Observed" in r.stdout and r.returncode in (0, 1)


def test_cli_process_exits_when_the_endpoint_closes_first(endpoint, key, pipe_name):
    ep = endpoint()
    ep.event_channel("t.ev")
    ep.state_channel("t.st", 1)
    ep.start()
    threading.Timer(0.3, ep.stop).start()
    r = _run_cli("server", pipe_name, "--key-hex", key.hex(), "--listen", "0.5", "--json")
    assert r.returncode == 1 and r.stdout.strip().startswith("{")


def test_cli_client_mode_exits_when_no_client_comes(key, pipe_name):
    r = _run_cli("client", pipe_name, "--key-hex", key.hex(), "--timeout", "1",
                 "--serve", "0.2", "--reconnect", "0.5")
    assert r.returncode == 1
