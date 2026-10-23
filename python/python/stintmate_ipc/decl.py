"""Declarations: what a peer offers (SPEC.md section 9).

::

    {"methods":  {"pitwall.write": {"v": [1], "ordered": true, "idempotent": true,
                                    "doc": "..."}},
     "channels": {"pitwall.lap_completed": {"kind": "event", "v": [1]},
                  "pitwall.connectors":    {"kind": "state", "v": [1]},
                  "ams2.telemetry":        {"kind": "stream", "v": [1], "rate_hz": 200,
                                            "layout": {"size": 48, "fields": [...]}}}}
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from stintmate_ipc._native import Layout

EVENT = "event"
STATE = "state"
STREAM = "stream"
KINDS = (EVENT, STATE, STREAM)
MAX_VERSION = 0xFFFF
_NAME_RE = re.compile(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*\Z")


def valid_name(name: Any) -> bool:
    """A method or channel name (SPEC.md 7.1)."""
    return isinstance(name, str) and len(name) <= 64 and bool(_NAME_RE.match(name))


def _versions(v: Any, what: str) -> list[int]:
    if not isinstance(v, list) or not v:
        raise ValueError(f"{what}: 'v' must be a non-empty list of versions")
    out = []
    for x in v:
        if not isinstance(x, int) or isinstance(x, bool) or not 1 <= x <= MAX_VERSION:
            raise ValueError(f"{what}: version {x!r} must be an integer 1..{MAX_VERSION}")
        out.append(x)
    return sorted(set(out))


@dataclass
class MethodDecl:
    name: str
    versions: list[int] = field(default_factory=lambda: [1])
    ordered: bool = False
    idempotent: bool = False
    doc: str | None = None
    params: dict[str, Any] | None = None  # JSON Schema (informative)
    result: dict[str, Any] | None = None

    def to_wire(self) -> dict[str, Any]:
        d: dict[str, Any] = {"v": list(self.versions)}
        if self.ordered:
            d["ordered"] = True
        if self.idempotent:
            d["idempotent"] = True
        if self.doc:
            d["doc"] = self.doc
        if self.params is not None:
            d["params"] = self.params
        if self.result is not None:
            d["result"] = self.result
        return d

    @classmethod
    def from_wire(cls, name: str, d: Any) -> MethodDecl:
        if not isinstance(d, dict):
            raise ValueError(f"method {name}: must be a map")
        return cls(name=name, versions=_versions(d.get("v"), f"method {name}"),
                   ordered=d.get("ordered") is True, idempotent=d.get("idempotent") is True,
                   doc=d.get("doc") if isinstance(d.get("doc"), str) else None,
                   params=d.get("params") if isinstance(d.get("params"), dict) else None,
                   result=d.get("result") if isinstance(d.get("result"), dict) else None)


@dataclass
class ChannelDecl:
    name: str
    kind: str
    versions: list[int] = field(default_factory=lambda: [1])
    doc: str | None = None
    layout: Layout | None = None  # streams: the layout of the highest version
    rate_hz: float | None = None  # streams: the nominal rate
    data: dict[str, Any] | None = None  # events/state: JSON Schema of `d` (informative)

    def to_wire(self) -> dict[str, Any]:
        d: dict[str, Any] = {"kind": self.kind, "v": list(self.versions)}
        if self.doc:
            d["doc"] = self.doc
        if self.layout is not None:
            d["layout"] = self.layout.to_wire()
        if self.rate_hz is not None:
            d["rate_hz"] = self.rate_hz
        if self.data is not None:
            d["data"] = self.data
        return d

    @classmethod
    def from_wire(cls, name: str, d: Any) -> ChannelDecl:
        if not isinstance(d, dict):
            raise ValueError(f"channel {name}: must be a map")
        kind = d.get("kind")
        if kind not in KINDS:
            raise ValueError(f"channel {name}: kind must be one of {KINDS}")
        layout = None
        if kind == STREAM:
            if "layout" not in d:
                raise ValueError(f"stream {name}: 'layout' is required")
            layout = Layout.from_wire(d["layout"])
        rate = d.get("rate_hz")
        return cls(name=name, kind=kind, versions=_versions(d.get("v"), f"channel {name}"),
                   doc=d.get("doc") if isinstance(d.get("doc"), str) else None,
                   layout=layout,
                   rate_hz=float(rate) if isinstance(rate, (int, float))
                   and not isinstance(rate, bool) else None,
                   data=d.get("data") if isinstance(d.get("data"), dict) else None)


@dataclass
class Declarations:
    methods: dict[str, MethodDecl] = field(default_factory=dict)
    channels: dict[str, ChannelDecl] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        return {"methods": {n: m.to_wire() for n, m in self.methods.items()},
                "channels": {n: c.to_wire() for n, c in self.channels.items()}}

    @classmethod
    def from_wire(cls, d: Any) -> Declarations:
        """Parse and validate; raises ``ValueError`` (the receiver answers
        ``protocol_error``). Unknown keys are ignored."""
        if d is None:
            return cls()
        if not isinstance(d, dict):
            raise ValueError("decl must be a map")
        out = cls()
        methods = d.get("methods", {})
        channels = d.get("channels", {})
        if not isinstance(methods, dict) or not isinstance(channels, dict):
            raise ValueError("decl.methods and decl.channels must be maps")
        for name, m in methods.items():
            if not valid_name(name):
                raise ValueError(f"invalid method name {name!r}")
            out.methods[name] = MethodDecl.from_wire(name, m)
        for name, c in channels.items():
            if not valid_name(name):
                raise ValueError(f"invalid channel name {name!r}")
            out.channels[name] = ChannelDecl.from_wire(name, c)
        return out
