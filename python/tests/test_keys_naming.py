"""Key file, naming, registry and layout: parity with the 0.1.4 package's tests."""

import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from stintmate_ipc import (KeyFile, KeyFileError, Layout, PeerInfo, RegistryEntry, default_key_path,
                           ensure_key, find_workspace, list_endpoints, pipe_path, pipe_user,
                           read_key, registry_dir, short_name)
from stintmate_ipc.keyfile import format_key, parse_key


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
    with pytest.raises(ValueError):
        format_key(b"short")


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
    assert len({pr.communicate(timeout=30)[0].strip() for pr in procs}) == 1


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
    assert kf.path == p and "KeyFile" in repr(kf)


def test_default_key_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STINTMATE_WORKSPACE", str(tmp_path))
    assert default_key_path() == tmp_path / "ipc" / "key"
    assert default_key_path(tmp_path / "x") == tmp_path / "x" / "ipc" / "key"
    monkeypatch.delenv("STINTMATE_WORKSPACE")
    pointer = tmp_path / "settings.json"
    pointer.write_text('{"schema": 1, "workspace": "C:\\\\ws"}', encoding="utf-8-sig")
    monkeypatch.setenv("STINTMATE_SETTINGS_FILE", str(pointer))
    assert find_workspace() == Path("C:\\ws")
    pointer.unlink()
    with pytest.raises(KeyFileError, match="no Stintmate workspace"):
        default_key_path()


def test_names():
    assert short_name("pitwall", user="Alex") == "stintmate-pitwall-alex"
    assert pipe_path("voice", "radio", user="alex") == "\\\\.\\pipe\\stintmate-voice-radio-alex"
    assert short_name("connector-ams2", "recorder", user="x") == \
        "stintmate-connector-ams2-recorder-x"
    for bad in ("", "Pitwall", "-x", "a" * 49, "a.b"):
        with pytest.raises(ValueError):
            short_name(bad, user="x")
    assert pipe_user() == pipe_user().lower()
    assert pipe_user("LeÓna") == "leÓna"
    assert pipe_path("hub").endswith("-" + pipe_user())


def test_registry_dir(tmp_path: Path):
    assert registry_dir(tmp_path) == tmp_path / "ipc" / "endpoints"


def test_registry_entries(tmp_path: Path):
    reg = tmp_path / "endpoints"
    reg.mkdir()
    e = RegistryEntry(endpoint="ghost", pipe="\\\\.\\pipe\\stintmate-ghost-nobody", app="ghost",
                      process=None, pid=1, run="r", versions=[2],
                      started_at="2026-01-01T00:00:00+00:00", extra={"k": 1})
    (reg / "ghost.json").write_text(json.dumps(e.to_json()), encoding="utf-8")
    assert list_endpoints(reg) == []
    got = list_endpoints(reg, alive_only=False)
    assert len(got) == 1 and got[0].extra == {"k": 1} and got[0].path == reg / "ghost.json"
    assert not got[0].alive()
    os.utime(reg / "ghost.json", (0, 0))
    list_endpoints(reg, prune=True)
    assert not (reg / "ghost.json").exists()


def test_registry_entry_alive(endpoint):
    assert RegistryEntry("x", endpoint.path, "t6test", None, os.getpid(), "r", [2], "").alive()


def test_peer_info():
    i = PeerInfo("pitwall", name="Pitwall", version="1.0", process="core", meta={"sim": "ams2"})
    assert (i.app, i.name, i.version, i.process, i.meta) == (
        "pitwall", "Pitwall", "1.0", "core", {"sim": "ams2"})
    assert PeerInfo("a").meta == {} and PeerInfo("a").process is None
    assert PeerInfo("a", name="n") == PeerInfo("a", name="n") != PeerInfo("a")


def test_layout_pack_unpack():
    lay = Layout([("speed", "f32"), ("gear", "i8"), ("flags", "u8"), ("rpm", "u16"),
                  ("temps", "f32", 4), ("t", "i64")], name="telemetry")
    assert lay.size == 4 + 1 + 1 + 2 + 16 + 8 and lay.name == "telemetry"
    body = lay.pack_dict({"speed": 1.5, "gear": -1, "flags": 3, "rpm": 7000,
                          "temps": (1.0, 2.0, 3.0, 4.0), "t": -9})
    assert len(body) == lay.size and body[:4] == b"\x00\x00\xc0\x3f"
    d = lay.unpack_dict(body)
    assert d["gear"] == -1 and d["temps"] == (1.0, 2.0, 3.0, 4.0)
    assert d["rpm"] == 7000 and d["t"] == -9
    flat = lay.unpack(body)
    assert flat[:2] == (1.5, -1) and len(flat) == 9
    assert lay.pack(*flat) == body
    assert lay.names == ["speed", "gear", "flags", "rpm", "temps", "t"]
    assert lay.fields[4] == ("temps", "f32", 4)
    w = lay.to_wire()
    assert w["fields"][4] == ["temps", "f32", 4] and w["fields"][0] == ["speed", "f32"]
    assert w["size"] == lay.size
    assert Layout.from_wire(w) == lay and hash(Layout.from_wire(w)) == hash(lay)


@pytest.mark.parametrize("fields", [[], [("a", "f16")], [("A", "u8")], [("a", "u8"), ("a", "u8")],
                                    [("a", "u8", 0)], [("a",)]])
def test_layout_rejects(fields):
    with pytest.raises(ValueError):
        Layout(fields)


def test_layout_from_wire_checks_size():
    with pytest.raises(ValueError):
        Layout.from_wire({"size": 5, "fields": [["a", "u32"]]})


def test_layout_value_range():
    lay = Layout([("a", "u8")])
    with pytest.raises(ValueError):
        lay.pack(256)
    with pytest.raises(ValueError):
        lay.pack(1, 2)
