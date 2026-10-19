"""Win32 named-pipe I/O through ctypes (no pywin32), SPEC.md section 3.

Every handle is opened for **overlapped I/O**: a connection's reader thread waits in the
kernel for bytes (no polling, no CPU while idle) and a write on the same handle never waits
for that read. The GIL is released while a thread waits.

- :class:`PipeHandle`: one connected handle (either end): :meth:`~PipeHandle.read`,
  :meth:`~PipeHandle.write`, :meth:`~PipeHandle.close`, the peer's process id.
- :class:`Listener`: creates the server instances of one pipe name (user-only DACL, remote
  clients rejected, the first one with ``FILE_FLAG_FIRST_PIPE_INSTANCE``) and hands every
  connected client to a callback.
- :func:`open_pipe`: the client end.
"""

from __future__ import annotations

import ctypes
import logging
import os
import threading
from collections.abc import Callable
from ctypes import wintypes

from .errors import EndpointInUse, PipeBusy, PipeClosed, PipeError, PipeNotFound

log = logging.getLogger(__name__)

READ_CHUNK = 1024 * 1024
PIPE_BUFFER = 64 * 1024

ERROR_FILE_NOT_FOUND = 2
ERROR_ACCESS_DENIED = 5
ERROR_INVALID_HANDLE = 6
ERROR_BROKEN_PIPE = 109
ERROR_SEM_TIMEOUT = 121
ERROR_PIPE_BUSY = 231
ERROR_NO_DATA = 232
ERROR_PIPE_NOT_CONNECTED = 233
ERROR_MORE_DATA = 234
ERROR_PIPE_CONNECTED = 535
ERROR_OPERATION_ABORTED = 995
ERROR_IO_PENDING = 997

PIPE_ACCESS_DUPLEX = 0x3
FILE_FLAG_OVERLAPPED = 0x40000000
FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000
PIPE_TYPE_BYTE = 0x0
PIPE_READMODE_BYTE = 0x0
PIPE_WAIT = 0x0
PIPE_REJECT_REMOTE_CLIENTS = 0x8
PIPE_UNLIMITED_INSTANCES = 255
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
CREATE_NEW = 1
FILE_ATTRIBUTE_NORMAL = 0x80
SECURITY_SQOS_PRESENT = 0x00100000
SECURITY_IDENTIFICATION = 0x00010000
TOKEN_QUERY = 0x0008
TOKEN_USER_CLASS = 1
SDDL_REVISION_1 = 1
INFINITE = 0xFFFFFFFF
WAIT_OBJECT_0 = 0
WAIT_TIMEOUT = 0x102
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
STILL_ACTIVE = 259
INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

_k32 = ctypes.WinDLL("kernel32", use_last_error=True)
_adv = ctypes.WinDLL("advapi32", use_last_error=True)
HANDLE = wintypes.HANDLE


class OVERLAPPED(ctypes.Structure):
    _fields_ = [("Internal", ctypes.c_void_p), ("InternalHigh", ctypes.c_void_p),
                ("Offset", wintypes.DWORD), ("OffsetHigh", wintypes.DWORD),
                ("hEvent", HANDLE)]


class SECURITY_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("nLength", wintypes.DWORD), ("lpSecurityDescriptor", ctypes.c_void_p),
                ("bInheritHandle", wintypes.BOOL)]


class SID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("Sid", ctypes.c_void_p), ("Attributes", wintypes.DWORD)]


def _fn(lib, name: str, restype, *argtypes):
    f = getattr(lib, name)
    f.restype = restype
    f.argtypes = argtypes
    return f


_POV = ctypes.POINTER(OVERLAPPED)
_PDW = ctypes.POINTER(wintypes.DWORD)
_CreateNamedPipeW = _fn(_k32, "CreateNamedPipeW", HANDLE, wintypes.LPCWSTR, wintypes.DWORD,
                        wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD,
                        wintypes.DWORD, ctypes.POINTER(SECURITY_ATTRIBUTES))
_ConnectNamedPipe = _fn(_k32, "ConnectNamedPipe", wintypes.BOOL, HANDLE, _POV)
_CreateFileW = _fn(_k32, "CreateFileW", HANDLE, wintypes.LPCWSTR, wintypes.DWORD,
                   wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, HANDLE)
_WaitNamedPipeW = _fn(_k32, "WaitNamedPipeW", wintypes.BOOL, wintypes.LPCWSTR, wintypes.DWORD)
_ReadFile = _fn(_k32, "ReadFile", wintypes.BOOL, HANDLE, ctypes.c_void_p, wintypes.DWORD,
                _PDW, _POV)
_WriteFile = _fn(_k32, "WriteFile", wintypes.BOOL, HANDLE, ctypes.c_void_p, wintypes.DWORD,
                 _PDW, _POV)
_GetOverlappedResult = _fn(_k32, "GetOverlappedResult", wintypes.BOOL, HANDLE, _POV, _PDW,
                           wintypes.BOOL)
_CancelIoEx = _fn(_k32, "CancelIoEx", wintypes.BOOL, HANDLE, _POV)
_CreateEventW = _fn(_k32, "CreateEventW", HANDLE, ctypes.c_void_p, wintypes.BOOL,
                    wintypes.BOOL, wintypes.LPCWSTR)
_SetEvent = _fn(_k32, "SetEvent", wintypes.BOOL, HANDLE)
_ResetEvent = _fn(_k32, "ResetEvent", wintypes.BOOL, HANDLE)
_WaitForSingleObject = _fn(_k32, "WaitForSingleObject", wintypes.DWORD, HANDLE,
                           wintypes.DWORD)
_WaitForMultipleObjects = _fn(_k32, "WaitForMultipleObjects", wintypes.DWORD, wintypes.DWORD,
                              ctypes.POINTER(HANDLE), wintypes.BOOL, wintypes.DWORD)
_CloseHandle = _fn(_k32, "CloseHandle", wintypes.BOOL, HANDLE)
_GetCurrentProcess = _fn(_k32, "GetCurrentProcess", HANDLE)
_LocalFree = _fn(_k32, "LocalFree", ctypes.c_void_p, ctypes.c_void_p)
_GetNamedPipeClientProcessId = _fn(_k32, "GetNamedPipeClientProcessId", wintypes.BOOL, HANDLE,
                                   ctypes.POINTER(wintypes.ULONG))
_GetNamedPipeServerProcessId = _fn(_k32, "GetNamedPipeServerProcessId", wintypes.BOOL, HANDLE,
                                   ctypes.POINTER(wintypes.ULONG))
_OpenProcess = _fn(_k32, "OpenProcess", HANDLE, wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
_GetExitCodeProcess = _fn(_k32, "GetExitCodeProcess", wintypes.BOOL, HANDLE, _PDW)
_OpenProcessToken = _fn(_adv, "OpenProcessToken", wintypes.BOOL, HANDLE, wintypes.DWORD,
                        ctypes.POINTER(HANDLE))
_GetTokenInformation = _fn(_adv, "GetTokenInformation", wintypes.BOOL, HANDLE, ctypes.c_int,
                           ctypes.c_void_p, wintypes.DWORD, _PDW)
_ConvertSidToStringSidW = _fn(_adv, "ConvertSidToStringSidW", wintypes.BOOL, ctypes.c_void_p,
                              ctypes.POINTER(wintypes.LPWSTR))
_ConvertSD = _fn(_adv, "ConvertStringSecurityDescriptorToSecurityDescriptorW", wintypes.BOOL,
                 wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p),
                 ctypes.c_void_p)
_GetUserNameW = _fn(_adv, "GetUserNameW", wintypes.BOOL, wintypes.LPWSTR, _PDW)


def _fail(what: str, cls: type[PipeError] = PipeError) -> PipeError:
    err = ctypes.get_last_error()
    return cls(err, f"{what} failed: {ctypes.FormatError(err).strip()} ({err})")


def user_name() -> str:
    """The Windows user name of this process (``GetUserNameW``)."""
    size = wintypes.DWORD(257)
    buf = ctypes.create_unicode_buffer(size.value)
    if _GetUserNameW(buf, ctypes.byref(size)):
        return buf.value
    return os.environ.get("USERNAME", "user")


def current_user_sid() -> str:
    token = HANDLE()
    if not _OpenProcessToken(_GetCurrentProcess(), TOKEN_QUERY, ctypes.byref(token)):
        raise _fail("OpenProcessToken")
    try:
        size = wintypes.DWORD()
        _GetTokenInformation(token, TOKEN_USER_CLASS, None, 0, ctypes.byref(size))
        buf = ctypes.create_string_buffer(size.value)
        if not _GetTokenInformation(token, TOKEN_USER_CLASS, buf, size, ctypes.byref(size)):
            raise _fail("GetTokenInformation")
        sid = SID_AND_ATTRIBUTES.from_buffer(buf).Sid
        text = wintypes.LPWSTR()
        if not _ConvertSidToStringSidW(sid, ctypes.byref(text)):
            raise _fail("ConvertSidToStringSidW")
        try:
            return text.value
        finally:
            _LocalFree(text)
    finally:
        _CloseHandle(token)


def user_only_sddl() -> str:
    """Protected DACL: full access for the current user only (SPEC.md 3.2)."""
    return f"D:P(A;;GA;;;{current_user_sid()})"


class _SecurityAttributes:
    """A ``SECURITY_ATTRIBUTES`` for an SDDL string, freed on :meth:`free`."""

    def __init__(self, sddl: str) -> None:
        self._psd = ctypes.c_void_p()
        if not _ConvertSD(sddl, SDDL_REVISION_1, ctypes.byref(self._psd), None):
            raise _fail("ConvertStringSecurityDescriptorToSecurityDescriptorW")
        self.sa = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), self._psd, False)

    def free(self) -> None:
        if self._psd:
            _LocalFree(self._psd)
            self._psd = ctypes.c_void_p()


def _event() -> int:
    h = _CreateEventW(None, True, False, None)
    if not h:
        raise _fail("CreateEventW")
    return h


def pid_alive(pid: int) -> bool:
    h = _OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not h:
        return ctypes.get_last_error() == ERROR_ACCESS_DENIED
    try:
        code = wintypes.DWORD()
        return bool(_GetExitCodeProcess(h, ctypes.byref(code))) and code.value == STILL_ACTIVE
    finally:
        _CloseHandle(h)


def write_new_file_user_only(path: str, data: bytes) -> None:
    """Create ``path`` (must not exist) with a user-only protected DACL and write ``data``."""
    sa = _SecurityAttributes(user_only_sddl())
    try:
        h = _CreateFileW(path, GENERIC_WRITE, 0, ctypes.byref(sa.sa), CREATE_NEW,
                         FILE_ATTRIBUTE_NORMAL, None)
    finally:
        sa.free()
    if not h or h == INVALID_HANDLE_VALUE:
        err = ctypes.get_last_error()
        raise OSError(err, f"cannot create {path}: {ctypes.FormatError(err).strip()}")
    try:
        written = wintypes.DWORD()
        if not _WriteFile(h, data, len(data), ctypes.byref(written), None) \
                or written.value != len(data):
            err = ctypes.get_last_error()
            raise OSError(err, f"cannot write {path}")
    finally:
        _CloseHandle(h)


# ------------------------------------------------------------------------- connection


class PipeHandle:
    """One connected pipe handle (either end), opened for overlapped I/O.

    One thread reads (:meth:`read`) and one thread writes (:meth:`write`) at a time; either
    may be called while the other waits. :meth:`close` may be called from any thread: it
    cancels the pending I/O and closes the handle once both are out of the kernel.
    """

    def __init__(self, handle: int, server_side: bool) -> None:
        self.handle = handle
        self.server_side = server_side
        self._lock = threading.Lock()
        self._closing = False
        self._busy = 0
        self._rov = OVERLAPPED()
        self._rov.hEvent = _event()
        self._wov = OVERLAPPED()
        self._wov.hEvent = _event()
        self._rbuf = ctypes.create_string_buffer(READ_CHUNK)
        self._rview = memoryview(self._rbuf).cast("B")
        self._got = wintypes.DWORD()
        self._put = wintypes.DWORD()
        self._rovp = ctypes.byref(self._rov)
        self._wovp = ctypes.byref(self._wov)
        self._gotp = ctypes.byref(self._got)
        self._putp = ctypes.byref(self._put)
        self.peer_pid: int | None = None
        pid = wintypes.ULONG()
        get = _GetNamedPipeClientProcessId if server_side else _GetNamedPipeServerProcessId
        if get(handle, ctypes.byref(pid)):
            self.peer_pid = int(pid.value)

    @property
    def closed(self) -> bool:
        return self._closing

    def _enter(self) -> int:
        with self._lock:
            if self._closing or not self.handle:
                raise PipeClosed("pipe closed")
            self._busy += 1
            return self.handle

    def _leave(self) -> None:
        with self._lock:
            self._busy -= 1
            if self._closing and self._busy == 0:
                self._release()

    # Hot path: every ctypes call releases and retakes the GIL, and with another thread
    # waiting for it each one is a context switch, so these use as few calls as possible.
    # ReadFile/WriteFile reset the OVERLAPPED's event themselves, and an operation that
    # completes at once leaves its byte count in ``InternalHigh``.

    def read(self, timeout_ms: int = INFINITE) -> bytes:
        """The bytes available (at least one); raises :class:`PipeClosed` at the end of the
        stream and :class:`TimeoutError` when nothing came within ``timeout_ms``."""
        h = self._enter()
        try:
            ov = self._rov
            while True:
                if _ReadFile(h, self._rbuf, READ_CHUNK, None, self._rovp):
                    n = ov.InternalHigh or 0
                else:
                    e = ctypes.get_last_error()
                    if e == ERROR_MORE_DATA:
                        n = ov.InternalHigh or 0
                    elif e != ERROR_IO_PENDING:
                        raise PipeClosed(f"read failed ({e})")
                    else:
                        if timeout_ms != INFINITE and \
                                _WaitForSingleObject(ov.hEvent, timeout_ms) == WAIT_TIMEOUT:
                            _CancelIoEx(h, self._rovp)
                            if _GetOverlappedResult(h, self._rovp, self._gotp, True) \
                                    and self._got.value:
                                return self._rview[:self._got.value].tobytes()
                            raise TimeoutError("no data")
                        if not _GetOverlappedResult(h, self._rovp, self._gotp, True):
                            e = ctypes.get_last_error()
                            if e != ERROR_MORE_DATA:
                                raise PipeClosed(f"read failed ({e})")
                        n = self._got.value
                if n:
                    return self._rview[:n].tobytes()
                if self._closing:
                    raise PipeClosed("pipe closed")
        finally:
            self._leave()

    def write(self, data: bytes, timeout_ms: int = 30_000) -> None:
        """Write all of ``data``. Raises :class:`PipeClosed` when the pipe broke or the peer
        did not read for ``timeout_ms``."""
        if not self.write_begin(data):
            self.write_finish(timeout_ms)

    def write_begin(self, data: bytes) -> bool:
        """Start writing ``data``: True when it completed at once (the usual case: the pipe
        buffer had room), False when it is pending and :meth:`write_finish` must be called
        (possibly from another thread) before the next write."""
        h = self._enter()
        try:
            self._wdata = data
            self._wbase = ctypes.cast(ctypes.c_char_p(data), ctypes.c_void_p).value
            self._woff = 0
            if self._issue(h):
                return False
        except BaseException:
            self._wdata = b""
            self._leave()
            raise
        self._wdata = b""
        self._leave()
        return True

    def _issue(self, h: int) -> bool:
        """Write from ``_woff`` until done (False) or an operation is pending (True)."""
        ov = self._wov
        total = len(self._wdata)
        while self._woff < total:
            if not _WriteFile(h, self._wbase + self._woff, total - self._woff, None,
                              self._wovp):
                e = ctypes.get_last_error()
                if e == ERROR_IO_PENDING:
                    return True
                raise PipeClosed(f"write failed ({e})")
            self._woff += ov.InternalHigh or 0
        return False

    def write_finish(self, timeout_ms: int = 30_000) -> None:
        """Complete the pending write started by :meth:`write_begin`."""
        try:
            h = self.handle
            ov = self._wov
            while True:
                if _WaitForSingleObject(ov.hEvent, timeout_ms) == WAIT_TIMEOUT:
                    _CancelIoEx(h, self._wovp)
                    _GetOverlappedResult(h, self._wovp, self._putp, True)
                    raise PipeClosed(f"the peer did not read for {timeout_ms} ms")
                if not _GetOverlappedResult(h, self._wovp, self._putp, True):
                    raise PipeClosed(f"write failed ({ctypes.get_last_error()})")
                self._woff += self._put.value
                if not self._issue(h):
                    return
        finally:
            self._wdata = b""
            self._leave()

    def close(self) -> None:
        with self._lock:
            if self._closing:
                return
            self._closing = True
            if self.handle:
                _CancelIoEx(self.handle, None)
            if self._busy == 0:
                self._release()

    def _release(self) -> None:  # with self._lock
        if self.handle:
            _CloseHandle(self.handle)
            self.handle = 0
        for ov in (self._rov, self._wov):
            if ov.hEvent:
                _CloseHandle(ov.hEvent)
                ov.hEvent = 0


def open_pipe(path: str, busy_wait_ms: int = 1000) -> PipeHandle:
    """Open the client end of ``path``. Raises :class:`PipeNotFound` when no server listens
    and :class:`PipeBusy` when every instance stayed busy for ``busy_wait_ms``.

    Opened with ``SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION`` so the server cannot
    impersonate this process beyond identification (SPEC.md 3.3)."""
    waited = False
    while True:
        h = _CreateFileW(path, GENERIC_READ | GENERIC_WRITE, 0, None, OPEN_EXISTING,
                         FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION,
                         None)
        if h and h != INVALID_HANDLE_VALUE:
            return PipeHandle(h, server_side=False)
        err = ctypes.get_last_error()
        if err == ERROR_FILE_NOT_FOUND:
            raise PipeNotFound(err, f"no server on {path}")
        if err == ERROR_PIPE_BUSY and not waited:
            waited = True
            if _WaitNamedPipeW(path, max(1, busy_wait_ms)):
                continue
            if ctypes.get_last_error() == ERROR_FILE_NOT_FOUND:
                raise PipeNotFound(err, f"no server on {path}")
            raise PipeBusy(err, f"{path}: every instance busy")
        if err == ERROR_PIPE_BUSY:
            raise PipeBusy(err, f"{path}: every instance busy")
        raise PipeError(err, f"cannot open {path}: {ctypes.FormatError(err).strip()} ({err})")


def pipe_exists(path: str) -> bool:
    """Whether a server listens on ``path`` (does not take an instance)."""
    if _WaitNamedPipeW(path, 1):
        return True
    return ctypes.get_last_error() != ERROR_FILE_NOT_FOUND


# ----------------------------------------------------------------------------- server


class Listener:
    """Serves one pipe name: one instance waits at a time (overlapped ``ConnectNamedPipe``,
    so :meth:`stop` wakes it at once); every client is handed to ``on_connection``."""

    def __init__(self, path: str, on_connection: Callable[[PipeHandle], None],
                 buffer_size: int = PIPE_BUFFER) -> None:
        self.path = path
        self.on_connection = on_connection
        self.buffer_size = buffer_size
        self.sddl = user_only_sddl()
        self._stop = _event()
        self._stopping = False
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._error: BaseException | None = None

    def _instance(self, first: bool) -> int:
        sa = _SecurityAttributes(self.sddl)
        try:
            mode = PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED \
                | (FILE_FLAG_FIRST_PIPE_INSTANCE if first else 0)
            h = _CreateNamedPipeW(
                self.path, mode,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                PIPE_UNLIMITED_INSTANCES, self.buffer_size, self.buffer_size, 0,
                ctypes.byref(sa.sa))
        finally:
            sa.free()
        if not h or h == INVALID_HANDLE_VALUE:
            err = ctypes.get_last_error()
            if first and err in (ERROR_ACCESS_DENIED, ERROR_PIPE_BUSY):
                raise EndpointInUse(err, f"{self.path} is already served by another process")
            raise PipeError(err, f"CreateNamedPipeW({self.path}) failed: "
                                 f"{ctypes.FormatError(err).strip()} ({err})")
        return h

    def _loop(self) -> None:
        # The next instance is created *before* a connected one is handed over, so the name
        # always has an instance: a client that connects, leaves and comes back at once
        # must never find the pipe gone (ERROR_FILE_NOT_FOUND).
        ov = OVERLAPPED()
        ov.hEvent = _event()
        got = wintypes.DWORD()
        handles = (HANDLE * 2)(ov.hEvent, self._stop)
        h = 0
        try:
            try:
                h = self._instance(True)
            except PipeError as exc:
                self._error = exc
                self._ready.set()
                return
            self._ready.set()
            while not self._stopping:
                _ResetEvent(ov.hEvent)
                connected = bool(_ConnectNamedPipe(h, ctypes.byref(ov)))
                if not connected:
                    e = ctypes.get_last_error()
                    if e == ERROR_PIPE_CONNECTED:
                        connected = True
                    elif e == ERROR_IO_PENDING:
                        r = _WaitForMultipleObjects(2, handles, False, INFINITE)
                        if r != WAIT_OBJECT_0:  # stop
                            _CancelIoEx(h, ctypes.byref(ov))
                            _GetOverlappedResult(h, ctypes.byref(ov), ctypes.byref(got), True)
                            return
                        connected = bool(_GetOverlappedResult(h, ctypes.byref(ov),
                                                              ctypes.byref(got), False))
                if self._stopping:
                    return
                nxt = 0
                while not nxt and not self._stopping:
                    try:
                        nxt = self._instance(False)
                    except PipeError as exc:
                        log.error("Cannot create a pipe instance: %s", exc)
                        if _WaitForSingleObject(self._stop, 1000) == WAIT_OBJECT_0:
                            break
                if connected:
                    conn = PipeHandle(h, server_side=True)
                    try:
                        self.on_connection(conn)
                    except Exception:  # noqa: BLE001
                        log.exception("on_connection failed")
                        conn.close()
                else:
                    _CloseHandle(h)
                h = nxt
        finally:
            if h:
                _CloseHandle(h)
            _CloseHandle(ov.hEvent)

    def start(self) -> Listener:
        self._thread = threading.Thread(target=self._loop, daemon=True, name="ipc-listen")
        self._thread.start()
        self._ready.wait(5.0)
        if self._error is not None:
            raise self._error
        return self

    def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        _SetEvent(self._stop)
        if self._thread is not None:
            self._thread.join(5.0)
        _CloseHandle(self._stop)
        self._stop = 0
