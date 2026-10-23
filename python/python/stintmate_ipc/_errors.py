"""The exceptions of the package (same hierarchy and attributes as 0.1.x).

The Rust binding builds them by name from this module, so they are ordinary Python classes.
"""

from __future__ import annotations

from typing import Any


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
    """Another process already serves this endpoint name."""


class KeyFileError(IpcError):
    """The key file is missing or malformed."""


class ProtocolError(IpcError):
    """The peer broke the protocol; the connection is closed with ``bye protocol_error``."""

    code = "protocol_error"


class HandshakeError(IpcError):
    """The handshake failed (``code``: ``unauthorized``, ``version_unsupported``,
    ``protocol_error``, ``timeout`` or a ``bye`` code the peer sent)."""

    def __init__(self, code: str, message: str = "") -> None:
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


class CallCancelled(IpcError):
    """The caller cancelled the call. Not a timeout: it is not a :class:`CallTimeout`."""

    code = "cancelled"


class RemoteError(IpcError):
    """An error response (``res`` with ``ok: false``), raised by :meth:`Peer.call` and
    raised by handlers to answer one. ``data`` (also ``details``) is the machine-readable
    part, ``retry_ms`` says when retrying makes sense."""

    def __init__(self, code: str, message: str = "", data: Any = None,
                 retry_ms: int | None = None, *, details: Any = None) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.data = data if data is not None else details
        self.retry_ms = retry_ms

    @property
    def details(self) -> Any:
        return self.data
