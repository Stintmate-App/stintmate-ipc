"""Endpoint names, the workspace and the endpoint registry (SPEC.md section 4)."""

from __future__ import annotations

import json
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import _win32

PIPE_PREFIX = "\\\\.\\pipe\\"
APP_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
PROCESS_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$")  # methods and channels
MAX_NAME = 64


def ascii_lower(s: str) -> str:
    """Lowercase A-Z only (SPEC.md 4.1: identical in every language)."""
    return s.translate(_ASCII_LOWER)


_ASCII_LOWER = {c: c + 32 for c in range(ord("A"), ord("Z") + 1)}


def pipe_user(user: str | None = None) -> str:
    """The ``<user>`` part of endpoint names: the Windows user name, ASCII-lowercased."""
    return ascii_lower(user if user is not None else _win32.user_name())


def endpoint_id(app: str, process: str | None = None) -> str:
    if not APP_RE.match(app):
        raise ValueError(f"invalid app id {app!r} (^[a-z0-9][a-z0-9_-]{{0,47}}$)")
    if process is None:
        return app
    if not PROCESS_RE.match(process):
        raise ValueError(f"invalid process id {process!r} (^[a-z0-9][a-z0-9_-]{{0,31}}$)")
    return f"{app}-{process}"


def short_name(app: str, process: str | None = None, user: str | None = None) -> str:
    """``stintmate-<app>[-<process>]-<user>``: the pipe name without ``\\\\.\\pipe\\``.
    This exact string is bound into the handshake MACs."""
    return f"stintmate-{endpoint_id(app, process)}-{pipe_user(user)}"


def pipe_path(app: str, process: str | None = None, user: str | None = None) -> str:
    """``\\\\.\\pipe\\stintmate-<app>[-<process>]-<user>``."""
    return PIPE_PREFIX + short_name(app, process, user)


def split_pipe(name: str) -> tuple[str, str]:
    """``(path, short)`` for a full ``\\\\.\\pipe\\...`` path or a short name."""
    if name.lower().startswith(PIPE_PREFIX):
        short = name[len(PIPE_PREFIX):]
    else:
        short = name
    if not short or "\\" in short:
        raise ValueError(f"invalid pipe name {name!r}")
    return PIPE_PREFIX + short, short


def valid_name(name: Any) -> bool:
    """A method or channel name (SPEC.md 7.1)."""
    return isinstance(name, str) and len(name) <= MAX_NAME and bool(NAME_RE.match(name))


def new_run_id() -> str:
    """A random id per server start (``welcome.run``)."""
    return secrets.token_hex(8)


# -------------------------------------------------------------------------- workspace


def find_workspace() -> Path | None:
    """The Stintmate workspace, resolved as SPEC.md 4.3 says: ``STINTMATE_WORKSPACE``, then
    the pointer file ``%APPDATA%\\Stintmate\\settings.json`` (``STINTMATE_SETTINGS_FILE``
    replaces its path, for tests). ``None`` when neither gives one."""
    env = os.environ.get("STINTMATE_WORKSPACE", "").strip()
    if env:
        return Path(env)
    pointer = os.environ.get("STINTMATE_SETTINGS_FILE", "").strip()
    if pointer:
        p = Path(pointer)
    else:
        appdata = os.environ.get("APPDATA", "").strip()
        if not appdata:
            return None
        p = Path(appdata) / "Stintmate" / "settings.json"
    try:
        doc = json.loads(p.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None
    ws = doc.get("workspace") if isinstance(doc, dict) else None
    return Path(ws) if isinstance(ws, str) and ws.strip() else None


def ipc_dir(workspace: str | os.PathLike[str]) -> Path:
    return Path(workspace) / "ipc"


# --------------------------------------------------------------------------- registry


@dataclass
class RegistryEntry:
    """One ``<workspace>\\ipc\\endpoints\\<endpoint id>.json`` (SPEC.md 4.4)."""

    endpoint: str
    pipe: str
    app: str
    process: str | None
    pid: int
    run: str
    versions: list[int]
    started_at: str
    name: str = ""
    version: str = ""
    extra: dict[str, Any] = field(default_factory=dict)
    path: Path | None = None

    def to_json(self) -> dict[str, Any]:
        d = {"schema": 1, "endpoint": self.endpoint, "pipe": self.pipe, "app": self.app,
             "process": self.process, "pid": self.pid, "run": self.run,
             "versions": list(self.versions), "started_at": self.started_at,
             "name": self.name, "version": self.version}
        d.update(self.extra)
        return d

    @classmethod
    def from_json(cls, d: dict[str, Any], path: Path | None = None) -> RegistryEntry:
        known = {"schema", "endpoint", "pipe", "app", "process", "pid", "run", "versions",
                 "started_at", "name", "version"}
        return cls(endpoint=str(d["endpoint"]), pipe=str(d["pipe"]), app=str(d["app"]),
                   process=d.get("process"), pid=int(d["pid"]), run=str(d["run"]),
                   versions=[int(v) for v in d.get("versions", [])],
                   started_at=str(d.get("started_at", "")), name=str(d.get("name", "")),
                   version=str(d.get("version", "")),
                   extra={k: v for k, v in d.items() if k not in known}, path=path)

    def alive(self) -> bool:
        """The pipe exists (the only liveness test that counts; the PID may be reused)."""
        return _win32.pipe_exists(self.pipe)


def registry_dir(workspace: str | os.PathLike[str]) -> Path:
    return ipc_dir(workspace) / "endpoints"


def register(directory: str | os.PathLike[str], entry: RegistryEntry) -> Path:
    """Write the entry atomically (temporary file + replace)."""
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{entry.endpoint}.json"
    tmp = d / f".{entry.endpoint}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
    tmp.write_text(json.dumps(entry.to_json(), indent=2), encoding="utf-8")
    os.replace(tmp, path)
    entry.path = path
    return path


def unregister(directory: str | os.PathLike[str], endpoint: str, run: str) -> None:
    """Delete the entry if it is still ours (same ``run``)."""
    path = Path(directory) / f"{endpoint}.json"
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        if doc.get("run") == run:
            path.unlink()
    except (OSError, ValueError):
        pass


def list_endpoints(directory: str | os.PathLike[str], alive_only: bool = True,
                   prune: bool = False) -> list[RegistryEntry]:
    """Entries of the registry; with ``alive_only`` only those whose pipe exists; ``prune``
    deletes the stale ones (older than 10 s, so a server that is starting is spared)."""
    out: list[RegistryEntry] = []
    d = Path(directory)
    if not d.is_dir():
        return out
    for p in sorted(d.glob("*.json")):
        try:
            entry = RegistryEntry.from_json(json.loads(p.read_text(encoding="utf-8")), p)
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if alive_only or prune:
            ok = entry.alive()
            if not ok and prune:
                try:
                    if time.time() - p.stat().st_mtime > 10:
                        p.unlink()
                except OSError:
                    pass
            if not ok and alive_only:
                continue
        out.append(entry)
    return out
