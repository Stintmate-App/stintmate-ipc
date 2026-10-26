//! The C ABI of Stintmate IPC v2 (PRY-085 T8, PRY-095), over the blocking façade.
//!
//! The rules (also at the top of the generated header):
//!
//! 1. **Status:** every fallible function returns an `int32_t`: `STINTMATE_IPC_OK` (0) or a
//!    negative `STINTMATE_IPC_E_*`. Queries that cannot fail return their value. On failure the
//!    calling thread's error record is set (`stintmate_ipc_last_error`).
//! 2. **Payloads** are MessagePack. Buffers returned by the library (`stintmate_ipc_buffer`)
//!    belong to the caller and are released with `stintmate_ipc_free_buffer`; a buffer of
//!    length 0 may have a null `ptr`. Pointers passed into the library are borrowed for the
//!    duration of the call (the library copies what it keeps); pointers passed to a callback
//!    are valid only until the callback returns.
//! 3. **Handles** are opaque pointers, each released with its own `_free`, exactly once, from
//!    any thread.
//! 4. **Callbacks** never run on a runtime (Tokio) thread: method handlers on the handler
//!    pool (`INLINE`: the connection's lane), deliveries on the connection's lane, in order;
//!    hooks and releases on the library's hooks thread; logs on the log thread. A callback may
//!    call any function except `stintmate_ipc_shutdown`; a callback that blocks stalls its
//!    lane or thread.
//! 5. **Lifetime of `user_data`:** every registration takes `user_data` and an optional
//!    `release`, which runs exactly once, on any thread, after the last callback that can
//!    receive that `user_data`.
//! 6. **Structs the caller fills** begin with `size_t struct_size` (set to `sizeof`); the
//!    library reads only the fields that fit, so fields can be added at the end in a minor
//!    version. Structs the library fills may grow at the end.
//! 7. **Timeouts** are milliseconds; `STINTMATE_IPC_FOREVER` (-1) means none.
//! 8. **Panics** never cross the boundary (`E_PANIC` or an `internal` answer).
//! 9. **Strings** are NUL-terminated UTF-8 in; out as `stintmate_ipc_buffer` (not
//!    NUL-terminated).
//! 10. **Versioning:** `stintmate_ipc_abi_version()` returns `(MAJOR << 16) | MINOR`, equal to
//!     `STINTMATE_IPC_ABI_MAJOR` / `_MINOR`. A caller accepts a DLL with the same MAJOR and a
//!     MINOR at least its own. MINOR grows when functions, constants, struct fields at the end
//!     or MessagePack map keys are added; MAJOR on any removal, rename, signature or meaning
//!     change. The crate version (`stintmate_ipc_version`) is independent.

mod client;
mod endpoint;
mod keys;
mod node;
mod peer;
mod runtime;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicIsize, Ordering};

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::Error;

// ------------------------------------------------------------------------ versions

/// The ABI major version: changes on any removal, rename, signature or meaning change.
pub const STINTMATE_IPC_ABI_MAJOR: u32 = 1;
/// The ABI minor version: grows when functions, constants or struct fields are added.
pub const STINTMATE_IPC_ABI_MINOR: u32 = 0;
/// A timeout of "none".
pub const STINTMATE_IPC_FOREVER: i64 = -1;

static VERSION: &CStr =
    match CStr::from_bytes_with_nul(concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes()) {
        Ok(v) => v,
        Err(_) => panic!("bad version"),
    };

static SPEC_REVISION: std::sync::LazyLock<std::ffi::CString> = std::sync::LazyLock::new(|| {
    std::ffi::CString::new(stintmate_ipc::SPEC_REVISION).unwrap_or_default()
});

/// `(MAJOR << 16) | MINOR` of the ABI this library implements.
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_abi_version() -> u32 {
    (STINTMATE_IPC_ABI_MAJOR << 16) | STINTMATE_IPC_ABI_MINOR
}

/// The library's (crate) version as a static NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_version() -> *const c_char {
    VERSION.as_ptr()
}

/// The revision of `SPEC.md` this library follows, as a static NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_spec_revision() -> *const c_char {
    SPEC_REVISION.as_ptr()
}

/// The protocol version the library speaks (the highest it offers in the handshake).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_protocol_version() -> u32 {
    stintmate_ipc::handshake::SUPPORTED_VERSIONS
        .iter()
        .copied()
        .max()
        .unwrap_or(0)
}

// ------------------------------------------------------------------------ status codes

/// Success.
pub const STINTMATE_IPC_OK: i32 = 0;
/// Nobody serves the pipe.
pub const STINTMATE_IPC_E_NOT_FOUND: i32 = -1;
/// Every instance of the pipe is busy.
pub const STINTMATE_IPC_E_BUSY: i32 = -2;
/// Another process already serves the endpoint.
pub const STINTMATE_IPC_E_IN_USE: i32 = -3;
/// The handshake failed or timed out, or the peer closed with `bye`.
pub const STINTMATE_IPC_E_HANDSHAKE: i32 = -4;
/// The key file is missing, unreadable or invalid.
pub const STINTMATE_IPC_E_KEYFILE: i32 = -5;
/// There is no connection.
pub const STINTMATE_IPC_E_NOT_CONNECTED: i32 = -6;
/// The connection was lost.
pub const STINTMATE_IPC_E_LOST: i32 = -7;
/// The call got no answer in time.
pub const STINTMATE_IPC_E_TIMEOUT: i32 = -8;
/// The call was cancelled.
pub const STINTMATE_IPC_E_CANCELLED: i32 = -9;
/// A protocol violation.
pub const STINTMATE_IPC_E_PROTOCOL: i32 = -10;
/// An argument is null, malformed or out of range.
pub const STINTMATE_IPC_E_INVALID_ARG: i32 = -11;
/// A panic was caught inside the library.
pub const STINTMATE_IPC_E_PANIC: i32 = -12;
/// The other end answered with an error: the details are in the error record
/// (`stintmate_ipc_last_error`).
pub const STINTMATE_IPC_E_REMOTE: i32 = -13;
/// The message is larger than the peer accepts.
pub const STINTMATE_IPC_E_TOO_LARGE: i32 = -14;
/// An operating system error.
pub const STINTMATE_IPC_E_IO: i32 = -15;
/// An internal failure.
pub const STINTMATE_IPC_E_INTERNAL: i32 = -16;
/// `stintmate_ipc_call_wait` (or shutdown) gave up waiting.
pub const STINTMATE_IPC_E_WAIT_TIMEOUT: i32 = -17;
/// A value cannot be encoded.
pub const STINTMATE_IPC_E_ENCODE: i32 = -18;
/// The handle was stopped or closed.
pub const STINTMATE_IPC_E_CLOSED: i32 = -19;
/// The library was shut down (`stintmate_ipc_shutdown`).
pub const STINTMATE_IPC_E_SHUTDOWN: i32 = -20;

/// The status of a library error.
pub(crate) fn status_of(e: &Error) -> i32 {
    match e {
        Error::PipeNotFound(_) => STINTMATE_IPC_E_NOT_FOUND,
        Error::PipeBusy(_) => STINTMATE_IPC_E_BUSY,
        Error::EndpointInUse(_) => STINTMATE_IPC_E_IN_USE,
        Error::Handshake(_) | Error::HandshakeTimeout | Error::PeerBye { .. } => {
            STINTMATE_IPC_E_HANDSHAKE
        }
        Error::KeyFile(_) => STINTMATE_IPC_E_KEYFILE,
        Error::NotConnected => STINTMATE_IPC_E_NOT_CONNECTED,
        Error::ConnectionLost { .. } => STINTMATE_IPC_E_LOST,
        Error::CallTimeout => STINTMATE_IPC_E_TIMEOUT,
        Error::CallCancelled => STINTMATE_IPC_E_CANCELLED,
        Error::Remote(_) => STINTMATE_IPC_E_REMOTE,
        Error::TooLarge { .. } => STINTMATE_IPC_E_TOO_LARGE,
        Error::Encode(_) => STINTMATE_IPC_E_ENCODE,
        Error::Protocol(_) => STINTMATE_IPC_E_PROTOCOL,
        Error::Invalid(_) => STINTMATE_IPC_E_INVALID_ARG,
        Error::Io(_) => STINTMATE_IPC_E_IO,
    }
}

// ------------------------------------------------------------------------ the error record

/// The last failure of a thread.
#[derive(Default)]
struct ErrorRecord {
    status: i32,
    message: String,
    code: Option<String>,
    data: Option<Value>,
    retry_ms: Option<u64>,
    os_error: Option<i32>,
}

thread_local! {
    static LAST_ERROR: RefCell<ErrorRecord> = RefCell::new(ErrorRecord::default());
}

/// Records a failure of this thread.
pub(crate) fn record_error(status: i32, message: impl Into<String>) {
    let rec = ErrorRecord {
        status,
        message: message.into(),
        ..ErrorRecord::default()
    };
    let _ = LAST_ERROR.try_with(|e| *e.borrow_mut() = rec);
}

/// Records `e` (with its remote code, details, retry hint or OS error) and returns its status.
pub(crate) fn fail(e: &Error) -> i32 {
    let status = status_of(e);
    let mut rec = ErrorRecord {
        status,
        message: e.to_string(),
        ..ErrorRecord::default()
    };
    match e {
        Error::Remote(r) => {
            rec.message.clone_from(&r.message);
            rec.code = Some(r.code.clone());
            rec.data.clone_from(&r.details);
            rec.retry_ms = r.retry_ms;
        }
        Error::PeerBye { code, .. } => rec.code = Some(code.clone()),
        Error::ConnectionLost { code, .. } => rec.code.clone_from(code),
        Error::Io(io) => rec.os_error = io.raw_os_error(),
        _ => {}
    }
    let _ = LAST_ERROR.try_with(|l| *l.borrow_mut() = rec);
    status
}

/// Records a failure with all its details (the peer and client code keep errors as data).
pub(crate) fn record_detail(
    status: i32,
    message: String,
    code: Option<String>,
    data: Option<Value>,
    retry_ms: Option<u64>,
    os_error: Option<i32>,
) {
    let rec = ErrorRecord {
        status,
        message,
        code,
        data,
        retry_ms,
        os_error,
    };
    let _ = LAST_ERROR.try_with(|l| *l.borrow_mut() = rec);
}

/// Records an invalid argument.
pub(crate) fn invalid(message: &str) -> i32 {
    record_error(STINTMATE_IPC_E_INVALID_ARG, message);
    STINTMATE_IPC_E_INVALID_ARG
}

/// Runs `f`; a panic becomes [`STINTMATE_IPC_E_PANIC`], and after shutdown nothing runs.
pub(crate) fn guard(f: impl FnOnce() -> i32) -> i32 {
    if runtime::is_shutdown() {
        return runtime::shutdown_status();
    }
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(status) => status,
        Err(payload) => {
            record_error(
                STINTMATE_IPC_E_PANIC,
                format!("panic: {}", panic_message(payload.as_ref())),
            );
            STINTMATE_IPC_E_PANIC
        }
    }
}

pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown".to_owned()
    }
}

/// Copies the message of the last failure on this thread into `buf` (NUL-terminated,
/// truncated to `len` bytes) and returns the full length of the message without the NUL.
/// `buf` may be null with `len` 0 to learn the length.
///
/// # Safety
/// `buf` must be null or valid for writes of `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_last_error_message(buf: *mut c_char, len: usize) -> usize {
    LAST_ERROR
        .try_with(|e| {
            let e = e.borrow();
            let bytes = e.message.as_bytes();
            if !buf.is_null() && len > 0 {
                let n = bytes.len().min(len - 1);
                // SAFETY: the caller guarantees `buf` is valid for `len` bytes and n < len.
                unsafe {
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.cast::<u8>(), n);
                    *buf.add(n) = 0;
                }
            }
            bytes.len()
        })
        .unwrap_or(0)
}

/// The calling thread's last failure as a MessagePack map `{status, message, code?, data?,
/// retry_ms?, os_error?}` (`code`/`data`/`retry_ms` of a remote error or a `bye`, `os_error`
/// the Win32 code). With no failure recorded: `{status: 0, message: ""}`.
///
/// # Safety
/// `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_last_error(out: *mut stintmate_ipc_buffer) -> i32 {
    if out.is_null() {
        return invalid("out is null");
    }
    let map = LAST_ERROR
        .try_with(|e| {
            let e = e.borrow();
            let mut pairs: Vec<(&str, Value)> = vec![
                ("status", Value::from(i64::from(e.status))),
                ("message", Value::from(e.message.as_str())),
            ];
            if let Some(c) = &e.code {
                pairs.push(("code", Value::from(c.as_str())));
            }
            if let Some(d) = &e.data {
                pairs.push(("data", d.clone()));
            }
            if let Some(r) = e.retry_ms {
                pairs.push(("retry_ms", Value::from(r)));
            }
            if let Some(o) = e.os_error {
                pairs.push(("os_error", Value::from(i64::from(o))));
            }
            codec::map(pairs)
        })
        .unwrap_or(Value::Nil);
    // SAFETY: `out` per the contract.
    unsafe { put_value(out, &map) }
}

// ------------------------------------------------------------------------ buffers

/// A byte buffer owned by the library: release it with `stintmate_ipc_free_buffer`.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_buffer {
    /// The bytes (null when `len` is 0).
    pub ptr: *mut u8,
    /// The number of bytes.
    pub len: usize,
}

static LIVE_BUFFERS: AtomicIsize = AtomicIsize::new(0);

/// Hands `bytes` to the caller through `out`.
///
/// # Safety
/// `out` must be null or valid for a write.
pub(crate) unsafe fn put_buffer(out: *mut stintmate_ipc_buffer, bytes: Vec<u8>) {
    if out.is_null() {
        return;
    }
    let value = if bytes.is_empty() {
        stintmate_ipc_buffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        }
    } else {
        LIVE_BUFFERS.fetch_add(1, Ordering::Relaxed);
        let boxed = bytes.into_boxed_slice();
        let len = boxed.len();
        stintmate_ipc_buffer {
            ptr: Box::into_raw(boxed).cast::<u8>(),
            len,
        }
    };
    // SAFETY: `out` is valid for a write (caller's contract).
    unsafe { out.write(value) };
}

/// Hands the MessagePack of `value` to the caller.
///
/// # Safety
/// As [`put_buffer`].
pub(crate) unsafe fn put_value(out: *mut stintmate_ipc_buffer, value: &Value) -> i32 {
    match codec::encode(value) {
        Ok(bytes) => {
            // SAFETY: forwarded contract.
            unsafe { put_buffer(out, bytes) };
            STINTMATE_IPC_OK
        }
        Err(e) => fail(&Error::Encode(e)),
    }
}

/// Releases a buffer returned by the library (a zeroed or already empty buffer is fine).
///
/// # Safety
/// `buf` must come from the library and not be freed twice.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_free_buffer(buf: stintmate_ipc_buffer) {
    if buf.ptr.is_null() {
        return;
    }
    LIVE_BUFFERS.fetch_sub(1, Ordering::Relaxed);
    // SAFETY: the pointer and length are those of a boxed slice made by `put_buffer`.
    drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(buf.ptr, buf.len)) });
}

/// The number of buffers handed out and not yet freed (a leak check for tests).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_live_buffers() -> isize {
    LIVE_BUFFERS.load(Ordering::Relaxed)
}

// ------------------------------------------------------------------------ argument helpers

/// A required UTF-8 string argument.
///
/// # Safety
/// `p` must be null or a NUL-terminated string.
pub(crate) unsafe fn cstr<'a>(p: *const c_char, what: &str) -> Result<&'a str, i32> {
    if p.is_null() {
        return Err(invalid(&format!("{what} is null")));
    }
    // SAFETY: non-null and NUL-terminated per the caller's contract.
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| invalid(&format!("{what} is not UTF-8")))
}

/// An optional UTF-8 string argument (null is `None`).
///
/// # Safety
/// As [`cstr`].
pub(crate) unsafe fn opt_cstr<'a>(p: *const c_char, what: &str) -> Result<Option<&'a str>, i32> {
    if p.is_null() {
        Ok(None)
    } else {
        // SAFETY: forwarded contract.
        unsafe { cstr(p, what) }.map(Some)
    }
}

/// A MessagePack argument; zero bytes are `nil`.
///
/// # Safety
/// `p` must be valid for reads of `len` bytes (or null with `len` 0).
pub(crate) unsafe fn value_arg(p: *const u8, len: usize, what: &str) -> Result<Value, i32> {
    if len == 0 {
        return Ok(Value::Nil);
    }
    if p.is_null() {
        return Err(invalid(&format!("{what} is null")));
    }
    // SAFETY: valid for `len` bytes per the caller's contract.
    let bytes = unsafe { std::slice::from_raw_parts(p, len) };
    codec::decode(bytes).map_err(|e| invalid(&format!("{what}: {e}")))
}

/// A list of versions (null/0 is `[1]`).
///
/// # Safety
/// `p` must be valid for reads of `n` values (or null with `n` 0).
pub(crate) unsafe fn versions_arg(p: *const u32, n: usize) -> Vec<u32> {
    if p.is_null() || n == 0 {
        vec![1]
    } else {
        // SAFETY: valid for `n` values per the caller's contract.
        unsafe { std::slice::from_raw_parts(p, n) }.to_vec()
    }
}

/// An opaque `void*` handed back to callbacks.
#[derive(Clone, Copy)]
pub(crate) struct UserData(pub(crate) *mut std::ffi::c_void);

// SAFETY: the embedder owns the pointee and promises it can be used from library threads.
unsafe impl Send for UserData {}
// SAFETY: as above.
unsafe impl Sync for UserData {}

impl UserData {
    pub(crate) fn get(self) -> *mut std::ffi::c_void {
        self.0
    }
}
