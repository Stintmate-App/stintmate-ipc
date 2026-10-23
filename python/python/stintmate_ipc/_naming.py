"""Registry entries (SPEC 4.4): data class and listing on top of the native helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from stintmate_ipc import _native


@dataclass
class RegistryEntry:
    r"""One ``<workspace>\ipc\endpoints\<endpoint id>.json``."""

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
        return _native._pipe_exists(self.pipe)


def list_endpoints(directory: str | os.PathLike[str], alive_only: bool = True,
                   prune: bool = False) -> list[RegistryEntry]:
    """Entries of the registry; with ``alive_only`` only those whose pipe exists; ``prune``
    deletes the stale ones (older than 10 s, so a server that is starting is spared)."""
    base = Path(directory)
    return [RegistryEntry.from_json(d, base / f"{d['endpoint']}.json")
            for d in _native._list_endpoints(os.fspath(base), alive_only, prune)]
