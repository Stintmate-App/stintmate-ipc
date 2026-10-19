"""The shared key file ``<workspace>\\ipc\\key`` (SPEC.md section 8.1).

Format: 64 hexadecimal characters (32 bytes), written lowercase, optionally followed by ASCII
whitespace (a line end). Created by whoever needs it first, atomically and never replaced:
a temporary file with a user-only DACL in the same folder, then a rename that fails if
``key`` exists (in which case the existing key wins).
"""

from __future__ import annotations

import os
import secrets
import threading
from pathlib import Path

from . import _win32
from .errors import KeyFileError
from .naming import find_workspace, ipc_dir

KEY_SIZE = 32


def default_key_path(workspace: str | os.PathLike[str] | None = None) -> Path:
    """``<workspace>\\ipc\\key``; the workspace is found with :func:`find_workspace` when not
    given. Raises :class:`KeyFileError` when there is no workspace."""
    ws = Path(workspace) if workspace is not None else find_workspace()
    if ws is None:
        raise KeyFileError("no Stintmate workspace (set STINTMATE_WORKSPACE or run the hub's "
                           "onboarding), so no key file")
    return ipc_dir(ws) / "key"


def parse_key(text: bytes) -> bytes:
    """The 32 key bytes from the file's contents."""
    if text.startswith(b"\xef\xbb\xbf"):
        text = text[3:]
    s = text.strip(b" \t\r\n")
    if len(s) != 2 * KEY_SIZE:
        raise KeyFileError(f"the key file must hold {2 * KEY_SIZE} hex characters, "
                           f"found {len(s)}")
    try:
        return bytes.fromhex(s.decode("ascii"))
    except (UnicodeDecodeError, ValueError):
        raise KeyFileError("the key file holds characters that are not hexadecimal") from None


def format_key(key: bytes) -> bytes:
    if len(key) != KEY_SIZE:
        raise ValueError(f"a key is {KEY_SIZE} bytes")
    return key.hex().encode("ascii") + b"\r\n"


def read_key(path: str | os.PathLike[str]) -> bytes:
    p = Path(path)
    try:
        data = p.read_bytes()
    except FileNotFoundError:
        raise KeyFileError(f"key file not found: {p}") from None
    except OSError as exc:
        raise KeyFileError(f"cannot read the key file {p}: {exc}") from None
    try:
        return parse_key(data)
    except KeyFileError as exc:
        raise KeyFileError(f"{p}: {exc}") from None


def ensure_key(path: str | os.PathLike[str]) -> bytes:
    """Read the key, creating the file first if it does not exist (atomic, no replace)."""
    p = Path(path)
    if p.exists():
        return read_key(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f".{p.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    _win32.write_new_file_user_only(str(tmp), format_key(secrets.token_bytes(KEY_SIZE)))
    try:
        os.rename(tmp, p)  # Windows: fails if p exists (MoveFileExW without REPLACE)
    except FileExistsError:
        pass
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    return read_key(p)


class KeyFile:
    """A key source that re-reads the file when it changes (cheap ``stat`` per handshake),
    so a replaced key takes effect without a restart. ``create``: create it if missing."""

    def __init__(self, path: str | os.PathLike[str], create: bool = False) -> None:
        self.path = Path(path)
        self.create = create
        self._lock = threading.Lock()
        self._stamp: tuple[int, int] | None = None
        self._key = b""

    def __call__(self) -> bytes:
        with self._lock:
            try:
                st = self.path.stat()
            except FileNotFoundError:
                if not self.create:
                    raise KeyFileError(f"key file not found: {self.path}") from None
                self._key = ensure_key(self.path)
                st = self.path.stat()
                self._stamp = (st.st_mtime_ns, st.st_size)
                return self._key
            stamp = (st.st_mtime_ns, st.st_size)
            if stamp != self._stamp:
                self._key = read_key(self.path)
                self._stamp = stamp
            return self._key

    def __repr__(self) -> str:
        return f"KeyFile({str(self.path)!r})"
