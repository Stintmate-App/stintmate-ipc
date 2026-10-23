//! The C ABI of Stintmate IPC v2 (PRY-085 T8), over the blocking façade.
//!
//! * Every function returns an `int32_t` status: `0` is success, negative values are the
//!   `STINTMATE_IPC_E_*` codes. The message of the last failure on the calling thread is read
//!   with `stintmate_ipc_last_error_message`.
//! * Payloads are **MessagePack bytes** in and out. A buffer returned by the library
//!   (`stintmate_ipc_buffer`) is released with `stintmate_ipc_free_buffer`.
//! * Callbacks get a `void* user_data`. Method handlers run on the handler pool (or on the
//!   connection's callback lane for `INLINE` methods), subscriber callbacks on the
//!   connection's callback lane, in order. **Never on a runtime thread**, but a callback must
//!   not block for long: the lane serves every subscription of the connection.
//! * A Rust panic never crosses the boundary: it becomes `STINTMATE_IPC_E_PANIC` (or an
//!   `internal` answer when it happens in a handler).
//! * Pointers passed to a callback are valid only until it returns.

mod client;
mod endpoint;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicIsize, Ordering};

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::Error;

static VERSION: &CStr = c"0.2.0";

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
/// A protocol violation or a message that cannot be encoded.
pub const STINTMATE_IPC_E_PROTOCOL: i32 = -10;
/// An argument is null, malformed or out of range.
pub const STINTMATE_IPC_E_INVALID_ARG: i32 = -11;
/// A panic was caught inside the library.
pub const STINTMATE_IPC_E_PANIC: i32 = -12;
/// The other end answered with an error: the result buffer holds the MessagePack map
/// `{code, msg, details?, retry_ms?}`.
pub const STINTMATE_IPC_E_REMOTE: i32 = -13;
/// The message is larger than the peer accepts.
pub const STINTMATE_IPC_E_TOO_LARGE: i32 = -14;
/// An operating system error.
pub const STINTMATE_IPC_E_IO: i32 = -15;
/// An internal failure.
pub const STINTMATE_IPC_E_INTERNAL: i32 = -16;
/// `stintmate_ipc_call_wait` gave up waiting; the call is still in flight.
pub const STINTMATE_IPC_E_WAIT_TIMEOUT: i32 = -17;

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
        Error::Protocol(_) | Error::Encode(_) => STINTMATE_IPC_E_PROTOCOL,
        Error::Invalid(_) => STINTMATE_IPC_E_INVALID_ARG,
        Error::Io(_) => STINTMATE_IPC_E_IO,
    }
}

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Records the message of the last failure of this thread.
pub(crate) fn set_error(message: impl Into<String>) {
    let message = message.into();
    LAST_ERROR.with(|e| *e.borrow_mut() = message);
}

/// Records `e` and returns its status.
pub(crate) fn fail(e: &Error) -> i32 {
    set_error(e.to_string());
    status_of(e)
}

/// Records an invalid argument.
pub(crate) fn invalid(message: &str) -> i32 {
    set_error(message);
    STINTMATE_IPC_E_INVALID_ARG
}

/// Runs `f`; a panic becomes [`STINTMATE_IPC_E_PANIC`].
pub(crate) fn guard(f: impl FnOnce() -> i32) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(status) => status,
        Err(payload) => {
            set_error(format!("panic: {}", panic_message(payload.as_ref())));
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
    LAST_ERROR.with(|e| {
        let e = e.borrow();
        let bytes = e.as_bytes();
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
}

/// The library's version as a static NUL-terminated string (never freed by the caller).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_version() -> *const c_char {
    VERSION.as_ptr()
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
    codec::decode(bytes).map_err(|e| {
        set_error(format!("{what}: {e}"));
        STINTMATE_IPC_E_INVALID_ARG
    })
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

// ------------------------------------------------------------------------ naming and keys

/// Writes the pipe path of `app` (and `process`, may be null) for the current user, UTF-8
/// without NUL, into `out`.
///
/// # Safety
/// Strings NUL-terminated; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_pipe_path(
    app: *const c_char,
    process: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let (app, process) = unsafe {
            match (cstr(app, "app"), opt_cstr(process, "process")) {
                (Ok(a), Ok(p)) => (a, p),
                (Err(s), _) | (_, Err(s)) => return s,
            }
        };
        match stintmate_ipc::naming::pipe_path(app, process) {
            Ok(path) => {
                // SAFETY: `out` per the contract.
                unsafe { put_buffer(out, path.into_bytes()) };
                STINTMATE_IPC_OK
            }
            Err(e) => {
                set_error(e.to_string());
                STINTMATE_IPC_E_INVALID_ARG
            }
        }
    })
}

/// Writes the default key file path (`<workspace>\ipc\key`) into `out`; `workspace` may be
/// null to find it (`STINTMATE_WORKSPACE`, then the settings file).
///
/// # Safety
/// As [`stintmate_ipc_pipe_path`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_default_key_path(
    workspace: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let ws = match unsafe { opt_cstr(workspace, "workspace") } {
            Ok(w) => w,
            Err(s) => return s,
        };
        match stintmate_ipc::keyfile::default_key_path(ws.map(std::path::Path::new)) {
            Ok(p) => {
                // SAFETY: `out` per the contract.
                unsafe { put_buffer(out, p.to_string_lossy().into_owned().into_bytes()) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&Error::KeyFile(e)),
        }
    })
}

/// Creates the key file (32 random bytes as hex, private to the user) when it does not exist.
///
/// # Safety
/// `path` NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_ensure_key(path: *const c_char) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let path = match unsafe { cstr(path, "path") } {
            Ok(p) => p,
            Err(s) => return s,
        };
        match stintmate_ipc::keyfile::ensure_key(std::path::Path::new(path)) {
            Ok(_) => STINTMATE_IPC_OK,
            Err(e) => fail(&Error::KeyFile(e)),
        }
    })
}

/// 1 when `app` is a valid app id, else 0.
///
/// # Safety
/// `app` NUL-terminated or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_valid_app(app: *const c_char) -> i32 {
    // SAFETY: the contract of this function.
    match unsafe { cstr(app, "app") } {
        Ok(a) => i32::from(stintmate_ipc::naming::valid_app(a)),
        Err(_) => 0,
    }
}
