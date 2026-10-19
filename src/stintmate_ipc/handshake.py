"""Handshake MACs and messages (SPEC.md section 8)."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass, field
from typing import Any

from .wire import PROTOCOL_NAME

NONCE_SIZE = 32
MAC_SIZE = 32
HANDSHAKE_TIMEOUT_S = 5.0


def _label(version: int, side: str) -> bytes:
    return f"stintmate-ipc/{version} {side}".encode("ascii") + b"\x00"


def client_mac(key: bytes, version: int, server_nonce: bytes, client_nonce: bytes,
               pipe_short_name: str) -> bytes:
    """``HMAC-SHA256(key, "stintmate-ipc/<v> client" 0x00 | s_nonce | c_nonce | name)``."""
    msg = _label(version, "client") + server_nonce + client_nonce \
        + pipe_short_name.encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).digest()


def server_mac(key: bytes, version: int, server_nonce: bytes, client_nonce: bytes,
               pipe_short_name: str) -> bytes:
    """``HMAC-SHA256(key, "stintmate-ipc/<v> server" 0x00 | c_nonce | s_nonce | name)``."""
    msg = _label(version, "server") + client_nonce + server_nonce \
        + pipe_short_name.encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).digest()


def new_nonce() -> bytes:
    return secrets.token_bytes(NONCE_SIZE)


def choose_version(offered: Any, supported: tuple[int, ...]) -> int | None:
    if not isinstance(offered, list):
        return None
    common = [v for v in offered if isinstance(v, int) and not isinstance(v, bool)
              and v in supported]
    return max(common) if common else None


@dataclass
class PeerInfo:
    """Who a peer says it is (``auth.peer`` / ``welcome.peer``, SPEC.md 8.4).

    ``app``: the app id as in endpoint names (``pitwall``, ``voice``, ``hub``,
    ``connector-<id>``); ``process``: which process of the app; ``name``/``version``: for
    people; ``meta``: anything app-specific (a connector's ``sim``, say)."""

    app: str
    name: str = ""
    version: str = ""
    process: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        d: dict[str, Any] = {"app": self.app, "name": self.name, "version": self.version}
        if self.process is not None:
            d["process"] = self.process
        if self.meta:
            d["meta"] = self.meta
        return d

    @classmethod
    def from_wire(cls, d: Any) -> PeerInfo:
        if not isinstance(d, dict) or not isinstance(d.get("app"), str):
            raise ValueError("peer must be a map with a string 'app'")
        meta = d.get("meta")
        proc = d.get("process")
        return cls(app=d["app"], name=d.get("name") if isinstance(d.get("name"), str) else "",
                   version=d.get("version") if isinstance(d.get("version"), str) else "",
                   process=proc if isinstance(proc, str) else None,
                   meta=meta if isinstance(meta, dict) else {})


def hello_message(nonce: bytes, max_frame: int, features: list[str] | None = None,
                  clock: tuple[int, int] | None = None) -> dict[str, Any]:
    m: dict[str, Any] = {"t": "hello", "proto": PROTOCOL_NAME, "versions": [2],
                         "nonce": nonce, "max_frame": max_frame,
                         "features": list(features or [])}
    if clock is not None:
        m["clock"] = list(clock)
    return m
