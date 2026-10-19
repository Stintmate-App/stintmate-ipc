"""Key file (SPEC.md 8.1), names and the registry (SPEC.md 4)."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from stintmate_ipc import KeyFile, ensure_key, naming, read_key
from stintmate_ipc.errors import KeyFileError
from stintmate_ipc.keyfile import default_key_path, format_key, parse_key


def test_parse_and_format():
    k = bytes(range(32))
    text = format_key(k)
    assert text == k.hex().encode() + b"\r\n"
    assert parse_key(text) == k
    assert parse_key(k.hex().upper().encode()) == k
    assert parse_key(b"\xef\xbb\xbf" + k.hex().encode() + b"\n  ") == k
    for bad in (b"", b"00" * 31, b"zz" * 32, k.hex().encode() + b"00", b"0" * 63 + b" 0"):
        with pytest.raises(KeyFileError):
            parse_key(bad)


def test_ensure_key_creates_once(tmp_path: Path):
    p = tmp_path / "ipc" / "key"
    k1 = ensure_key(p)
    assert len(k1) == 32 and p.exists()
    assert ensure_key(p) == k1
    assert read_key(p) == k1
    assert not list(p.parent.glob("*.tmp"))


def test_ensure_key_is_user_only(tmp_path: Path):
    p = tmp_path / "key"
    ensure_key(p)
    out = subprocess.run(["icacls", str(p)], capture_output=True, text=True).stdout
    # One ACE (the user), not inherited (no "(I)").
    aces = [line for line in out.splitlines()[0:-2] if ":" in line.split(str(p))[-1]]
    assert aces and all("(I)" not in a for a in aces)
    assert "Everyone" not in out and "BUILTIN\\Users" not in out


def test_concurrent_creation_agrees(tmp_path: Path):
    p = tmp_path / "key"
    keys: list[bytes] = []
    barrier = threading.Barrier(8)

    def make() -> None:
        barrier.wait()
        keys.append(ensure_key(p))

    threads = [threading.Thread(target=make) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(keys)) == 1
    code = "import sys; from stintmate_ipc import ensure_key; print(ensure_key(sys.argv[1]).hex())"
    procs = [subprocess.Popen([sys.executable, "-c", code, str(tmp_path / "k2")],
                              stdout=subprocess.PIPE, text=True) for _ in range(4)]
    outs = {pr.communicate(timeout=30)[0].strip() for pr in procs}
    assert len(outs) == 1


def test_keyfile_rereads_on_change(tmp_path: Path):
    p = tmp_path / "key"
    kf = KeyFile(p)
    with pytest.raises(KeyFileError, match="not found"):
        kf()
    p.write_bytes(format_key(b"\x01" * 32))
    assert kf() == b"\x01" * 32
    p.write_bytes(format_key(b"\x02" * 32) + b"\n")
    assert kf() == b"\x02" * 32
    assert KeyFile(tmp_path / "new", create=True)() == read_key(tmp_path / "new")


def test_default_key_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STINTMATE_WORKSPACE", str(tmp_path))
    assert default_key_path() == tmp_path / "ipc" / "key"
    monkeypatch.delenv("STINTMATE_WORKSPACE")
    pointer = tmp_path / "settings.json"
    pointer.write_text('{"schema": 1, "workspace": "C:\\\\ws"}', encoding="utf-8-sig")
    monkeypatch.setenv("STINTMATE_SETTINGS_FILE", str(pointer))
    assert naming.find_workspace() == Path("C:\\ws")
    pointer.unlink()
    with pytest.raises(KeyFileError, match="no Stintmate workspace"):
        default_key_path()


def test_names():
    assert naming.ascii_lower("LeÓna") == "leÓna"
    assert naming.short_name("pitwall", user="Leona") == "stintmate-pitwall-leona"
    assert naming.pipe_path("voice", "radio", user="leona") == \
        "\\\\.\\pipe\\stintmate-voice-radio-leona"
    assert naming.short_name("connector-ams2", "recorder", user="x") == \
        "stintmate-connector-ams2-recorder-x"
    for bad in ("", "Pitwall", "-x", "a" * 49, "a.b"):
        with pytest.raises(ValueError):
            naming.endpoint_id(bad)
    assert naming.split_pipe("stintmate-a-b") == ("\\\\.\\pipe\\stintmate-a-b", "stintmate-a-b")
    assert naming.split_pipe("\\\\.\\pipe\\x") == ("\\\\.\\pipe\\x", "x")
    assert naming.pipe_user() == naming.pipe_user().lower()


def test_method_and_channel_names():
    good = ["pitwall.write", "a", "a.b_c.d9"]
    bad = ["", "A.b", "a..b", ".a", "a.", "1a", "a-b", "a" * 65, 5]
    assert all(naming.valid_name(n) for n in good)
    assert not any(naming.valid_name(n) for n in bad)


def test_registry_roundtrip(tmp_path: Path, endpoint):
    reg = tmp_path / "endpoints"
    ep = endpoint(registry=reg).start()
    entries = naming.list_endpoints(reg)
    assert len(entries) == 1
    e = entries[0]
    assert e.pipe == ep.path and e.pid == os.getpid() and e.run == ep.run and e.alive()
    assert e.app == "testsrv" and e.versions == [2]
    ep.stop()
    assert naming.list_endpoints(reg, alive_only=False) == []


def test_registry_stale_entries(tmp_path: Path):
    reg = tmp_path / "endpoints"
    naming.register(reg, naming.RegistryEntry(
        endpoint="ghost", pipe="\\\\.\\pipe\\stintmate-ghost-nobody", app="ghost", process=None,
        pid=1, run="r", versions=[2], started_at="2026-01-01T00:00:00+00:00"))
    assert naming.list_endpoints(reg) == []
    assert len(naming.list_endpoints(reg, alive_only=False)) == 1
    old = reg / "ghost.json"
    os.utime(old, (0, 0))
    naming.list_endpoints(reg, prune=True)
    assert not old.exists()
    naming.unregister(reg, "ghost", "r")  # missing: no error
