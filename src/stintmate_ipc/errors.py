"""Error codes (SPEC.md section 11) and the exceptions of the package."""

from __future__ import annotations

from typing import Any

# Codes of the protocol (lowercase snake case). Applications add their own codes.
BAD_REQUEST = "bad_request"
UNAUTHORIZED = "unauthorized"
VERSION_UNSUPPORTED = "version_unsupported"
UNKNOWN_METHOD = "unknown_method"
UNKNOWN_CHANNEL = "unknown_channel"
BUSY = "busy"
UNAVAILABLE = "unavailable"
TIMEOUT = "timeout"
CANCELLED = "cancelled"
OVERLOADED = "overloaded"
TOO_LARGE = "too_large"
INTERNAL = "internal"
PROTOCOL_ERROR = "protocol_error"
SHUTTING_DOWN = "shutting_down"
NORMAL = "normal"
CHANNEL_CLOSED = "channel_closed"
DISCONNECTED = "disconnected"  # local only: never sent on the wire

CODES = frozenset({
    BAD_REQUEST, UNAUTHORIZED, VERSION_UNSUPPORTED, UNKNOWN_METHOD, UNKNOWN_CHANNEL, BUSY,
    UNAVAILABLE, TIMEOUT, CANCELLED, OVERLOADED, TOO_LARGE, INTERNAL, PROTOCOL_ERROR,
    SHUTTING_DOWN, NORMAL, CHANNEL_CLOSED,
})


class IpcError(Exception):
    """Base of every error raised by the package."""


class PipeError(IpcError, OSError):
    """A Win32 pipe operation failed (``errno`` is the Win32 error code)."""


class PipeNotFound(PipeError):
    """No server listens on that pipe name (the app is not running)."""


class PipeBusy(PipeError):
    """Every instance of the pipe stayed busy."""


class PipeClosed(PipeError):
    """The pipe is closed or broke."""


class EndpointInUse(PipeError):
    """Another process already serves this endpoint name (``FILE_FLAG_FIRST_PIPE_INSTANCE``)."""


class KeyFileError(IpcError):
    """The key file is missing or malformed."""


class ProtocolError(IpcError):
    """The peer broke the protocol; the connection is closed with ``bye protocol_error``."""

    code = PROTOCOL_ERROR


class HandshakeError(IpcError):
    """The handshake failed (``code``: ``unauthorized``, ``version_unsupported``,
    ``protocol_error``, ``timeout`` or a ``bye`` code the peer sent)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class NotConnected(IpcError):
    """No connection to the endpoint now."""


class ConnectionLost(IpcError):
    """The connection closed while a call was waiting for its response (its fate on the
    server is unknown: retry only with an idempotency key)."""


class CallTimeout(IpcError, TimeoutError):
    """No response within the call's timeout (a ``cancel`` was sent)."""


class RemoteError(IpcError):
    """An error response (``res`` with ``ok: false``).

    Raised by :meth:`Peer.call` when the peer answered an error, and raised **by handlers**
    to answer one: ``raise RemoteError("game_not_running", "AMS2 is not running")``.
    """

    def __init__(self, code: str, message: str = "", details: dict[str, Any] | None = None,
                 retry_ms: int | None = None) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.details = details
        self.retry_ms = retry_ms

    def to_wire(self) -> dict[str, Any]:
        e: dict[str, Any] = {"code": self.code, "msg": self.message}
        if self.details is not None:
            e["details"] = self.details
        if self.retry_ms is not None:
            e["retry_ms"] = int(self.retry_ms)
        return e

    @classmethod
    def from_wire(cls, e: Any) -> RemoteError:
        if not isinstance(e, dict) or not isinstance(e.get("code"), str):
            return cls(INTERNAL, "malformed error response", {"raw": repr(e)[:200]})
        details = e.get("details")
        retry = e.get("retry_ms")
        return cls(e["code"], e.get("msg") if isinstance(e.get("msg"), str) else "",
                   details if isinstance(details, dict) else None,
                   retry if isinstance(retry, int) else None)
