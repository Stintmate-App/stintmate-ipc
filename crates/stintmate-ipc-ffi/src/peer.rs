//! Peers, calls and subscriptions (spec 2.6): the handle of one connection, calls in flight
//! with their `on_done` hook, and subscriptions (of a peer, or kept by a client).

use std::ffi::{CString, c_char, c_void};
use std::mem::{MaybeUninit, offset_of, size_of, size_of_val};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::ThreadId;
use std::time::Duration;

use stintmate_ipc::blocking::{self, Client, Peer, PendingCall};
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::error::Error;
use stintmate_ipc::{Delivery, SubscribeOptions, Subscription, SubscriptionHandle};

use crate::runtime::{
    Handle, Registration, enter, in_callback, post_hook, stintmate_ipc_release_cb,
};
use crate::{
    STINTMATE_IPC_E_REMOTE, STINTMATE_IPC_E_WAIT_TIMEOUT, STINTMATE_IPC_OK, cstr, fail, guard,
    invalid, opt_cstr, put_value, record_detail, record_error, status_of, stintmate_ipc_buffer,
    value_arg,
};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Runs `f` under [`guard`]: an `Err` carries a status that is already recorded.
pub(crate) fn run(f: impl FnOnce() -> Result<(), i32>) -> i32 {
    guard(|| f().map_or_else(|s| s, |()| STINTMATE_IPC_OK))
}

/// A wait in milliseconds (`FOREVER` and any negative value: as long as it takes).
pub(crate) fn wait_arg(ms: i64) -> Option<Duration> {
    u64::try_from(ms).ok().map(Duration::from_millis)
}

/// The default deadline of a call or a subscription answer when the caller gives no options.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
/// "No deadline" for the options that need a duration.
const LONG: Duration = Duration::from_secs(60 * 60 * 24 * 365);

// ------------------------------------------------------------------------ struct arguments

/// Copies the fields of a caller-filled struct that fit its `struct_size`; the rest is zero.
/// Returns the struct and the number of bytes the caller provided.
///
/// # Safety
/// `p` must be null or valid for reads of its `struct_size` bytes.
pub(crate) unsafe fn read_struct<T>(p: *const T) -> Option<(T, usize)> {
    if p.is_null() {
        return None;
    }
    // SAFETY: every caller-filled struct begins with its `size_t struct_size`.
    let size = unsafe { p.cast::<usize>().read_unaligned() };
    if size < size_of::<usize>() {
        return None;
    }
    let n = size.min(size_of::<T>());
    let mut v = MaybeUninit::<T>::zeroed();
    // SAFETY: `p` is valid for `size >= n` bytes and `v` for `size_of::<T>() >= n`; the structs
    // read here hold only integers, raw pointers and `Option` of function pointers, for which
    // all-zero is a valid value.
    unsafe {
        std::ptr::copy_nonoverlapping(p.cast::<u8>(), v.as_mut_ptr().cast::<u8>(), n);
        Some((v.assume_init(), n))
    }
}

/// Whether a field (at `offset`, `size` bytes) is within the `provided` bytes of a struct.
pub(crate) fn present(provided: usize, offset: usize, size: usize) -> bool {
    provided >= offset + size
}

// ------------------------------------------------------------------------ errors as data

/// An error kept as data: for `client_last_error`, `on_connect_error` and the error record.
#[derive(Clone)]
pub(crate) struct ErrInfo {
    status: i32,
    message: String,
    code: Option<String>,
    data: Option<Value>,
    retry_ms: Option<u64>,
    os_error: Option<i32>,
}

impl ErrInfo {
    pub(crate) fn of(e: &Error) -> Self {
        let mut i = Self {
            status: status_of(e),
            message: e.to_string(),
            code: None,
            data: None,
            retry_ms: None,
            os_error: None,
        };
        match e {
            Error::Remote(r) => {
                i.message.clone_from(&r.message);
                i.code = Some(r.code.clone());
                i.data.clone_from(&r.details);
                i.retry_ms = r.retry_ms;
            }
            Error::Handshake(f) => {
                i.message.clone_from(&f.message);
                i.code = Some(f.code.as_str().to_owned());
            }
            Error::HandshakeTimeout => i.code = Some("timeout".to_owned()),
            Error::PeerBye { code, .. } => i.code = Some(code.clone()),
            Error::ConnectionLost { code, .. } => i.code.clone_from(code),
            Error::Io(io) => i.os_error = io.raw_os_error(),
            _ => {}
        }
        i
    }

    /// The same shape as `stintmate_ipc_last_error`.
    pub(crate) fn to_value(&self) -> Value {
        let mut pairs: Vec<(&str, Value)> = vec![
            ("status", Value::from(i64::from(self.status))),
            ("message", Value::from(self.message.as_str())),
        ];
        if let Some(c) = &self.code {
            pairs.push(("code", Value::from(c.as_str())));
        }
        if let Some(d) = &self.data {
            pairs.push(("data", d.clone()));
        }
        if let Some(r) = self.retry_ms {
            pairs.push(("retry_ms", Value::from(r)));
        }
        if let Some(o) = self.os_error {
            pairs.push(("os_error", Value::from(i64::from(o))));
        }
        codec::map(pairs)
    }

    /// Makes this the calling thread's last failure and returns its status.
    pub(crate) fn record(&self) -> i32 {
        record_detail(
            self.status,
            self.message.clone(),
            self.code.clone(),
            self.data.clone(),
            self.retry_ms,
            self.os_error,
        );
        self.status
    }

    /// As [`record`](Self::record) with the status and a prefix of "not connected".
    pub(crate) fn record_as_cause(&self, status: i32, what: &str) -> i32 {
        record_detail(
            status,
            format!("{what}: {}", self.message),
            self.code.clone(),
            None,
            None,
            self.os_error,
        );
        status
    }
}

/// Hands `value` to the caller through `out` (null: dropped).
///
/// # Safety
/// `out` null or valid for a write.
unsafe fn put_opt(out: *mut stintmate_ipc_buffer, value: &Value) -> i32 {
    // SAFETY: forwarded contract.
    unsafe { put_value(out, value) }
}

/// Writes the outcome of a call: the result, or records the error.
///
/// # Safety
/// `out` null or valid for a write.
pub(crate) unsafe fn finish(result: Result<Value, Error>, out: *mut stintmate_ipc_buffer) -> i32 {
    match result {
        // SAFETY: forwarded contract.
        Ok(v) => unsafe { put_opt(out, &v) },
        Err(e) => fail(&e),
    }
}

// ------------------------------------------------------------------------ the peer handle

/// Closes the client of a `connect()` when the last reference to its peer goes.
struct OwnerClient(Client);

impl Drop for OwnerClient {
    fn drop(&mut self) {
        let c = self.0.clone();
        // The close blocks on the runtime: never from one of its threads.
        let _ = catch_unwind(AssertUnwindSafe(move || c.close()));
    }
}

/// One connection (a reference to it: `stintmate_ipc_peer_ref` makes more).
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_peer {
    pub(crate) peer: Peer,
    owner: Option<Arc<OwnerClient>>,
    #[allow(dead_code)]
    handle: Handle,
}

impl stintmate_ipc_peer {
    /// A new reference to `peer`, for the caller to release with `stintmate_ipc_peer_free`.
    pub(crate) fn new_ref(peer: Peer) -> *mut Self {
        Box::into_raw(Box::new(Self {
            peer,
            owner: None,
            handle: Handle::new(),
        }))
    }

    /// A reference to the connection of a client made by `connect()`: the client closes when
    /// the last reference is released (and at shutdown).
    pub(crate) fn new_owned(peer: Peer, client: Client) -> *mut Self {
        let closer = client.clone();
        Box::into_raw(Box::new(Self {
            peer,
            owner: Some(Arc::new(OwnerClient(client))),
            handle: Handle::with_closer(move || closer.close()),
        }))
    }
}

/// The peer behind a pointer.
///
/// # Safety
/// `p` null or a live `stintmate_ipc_peer`.
unsafe fn peer_of<'a>(p: *const stintmate_ipc_peer) -> Result<&'a stintmate_ipc_peer, i32> {
    // SAFETY: the contract of the callers.
    unsafe { p.as_ref() }.ok_or_else(|| invalid("peer is null"))
}

/// A new owned reference to the same connection (null for null).
///
/// # Safety
/// `peer` null or a live peer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_ref(
    peer: *mut stintmate_ipc_peer,
) -> *mut stintmate_ipc_peer {
    crate::run(std::ptr::null_mut(), || {
        // SAFETY: the contract of this function.
        let Some(p) = (unsafe { peer.as_ref() }) else {
            return std::ptr::null_mut();
        };
        let owner = p.owner.clone();
        let handle = match &owner {
            Some(o) => {
                let closer = o.0.clone();
                Handle::with_closer(move || closer.close())
            }
            None => Handle::new(),
        };
        Box::into_raw(Box::new(stintmate_ipc_peer {
            peer: p.peer.clone(),
            owner,
            handle,
        }))
    })
}

/// Releases a reference to a connection (the connection stays up while other references or its
/// owner exist). Null is ignored.
///
/// # Safety
/// `peer` from the library, not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_free(peer: *mut stintmate_ipc_peer) {
    if !peer.is_null() {
        // SAFETY: made by `Box::into_raw` in this module.
        let b = unsafe { Box::from_raw(peer) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(b)));
    }
}

/// An id equal for the references of one connection (0 for null).
///
/// # Safety
/// `peer` null or a live peer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_id(peer: *const stintmate_ipc_peer) -> u64 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        unsafe { peer.as_ref() }.map_or(0, |p| p.peer.inner().id())
    })
}

/// Who is on the other end: `{app, name, version, process?, meta, pid?, run}`. (Named
/// `peer_get_info` because the C struct `stintmate_ipc_peer_info` has the other name.)
///
/// # Safety
/// `peer` live; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_get_info(
    peer: *const stintmate_ipc_peer,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let p = unsafe { peer_of(peer) }?;
        let i = p.peer.info();
        let mut pairs: Vec<(&str, Value)> = vec![
            ("app", Value::from(i.app.as_str())),
            ("name", Value::from(i.name.as_str())),
            ("version", Value::from(i.version.as_str())),
        ];
        if let Some(pr) = &i.process {
            pairs.push(("process", Value::from(pr.as_str())));
        }
        let meta = i
            .meta
            .clone()
            .filter(Value::is_map)
            .unwrap_or_else(|| Value::Map(Vec::new()));
        pairs.push(("meta", meta));
        if let Some(pid) = p.peer.pid() {
            pairs.push(("pid", Value::from(u64::from(pid))));
        }
        pairs.push((
            "run",
            Value::from(p.peer.inner().connection().info().run.as_str()),
        ));
        // SAFETY: `out` per the contract.
        match unsafe { put_value(out, &codec::map(pairs)) } {
            STINTMATE_IPC_OK => Ok(()),
            s => Err(s),
        }
    })
}

/// What the other end declares now (`{methods, channels}`).
///
/// # Safety
/// `peer` live; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_decl(
    peer: *const stintmate_ipc_peer,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let p = unsafe { peer_of(peer) }?;
        // SAFETY: `out` per the contract.
        match unsafe { put_value(out, &p.peer.decl().to_value()) } {
            STINTMATE_IPC_OK => Ok(()),
            s => Err(s),
        }
    })
}

/// 1 while the connection is up, else 0.
///
/// # Safety
/// `peer` null or a live peer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_is_open(peer: *const stintmate_ipc_peer) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        unsafe { peer.as_ref() }.map_or(0, |p| i32::from(p.peer.is_open()))
    })
}

/// Waits until the connection is closing or closed: 0, or `E_WAIT_TIMEOUT`.
///
/// # Safety
/// `peer` live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_wait_closed(
    peer: *const stintmate_ipc_peer,
    timeout_ms: i64,
) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        let p = unsafe { peer_of(peer) }?;
        let conn = p.peer.inner().connection().clone();
        if blocking::wait_for(conn.wait_closing(), wait_arg(timeout_ms)) {
            Ok(())
        } else {
            record_error(STINTMATE_IPC_E_WAIT_TIMEOUT, "the connection is still open");
            Err(STINTMATE_IPC_E_WAIT_TIMEOUT)
        }
    })
}

/// `{code, message}` once the connection is closed, nil while it is open.
///
/// # Safety
/// `peer` live; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_close_reason(
    peer: *const stintmate_ipc_peer,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let p = unsafe { peer_of(peer) }?;
        let v = p
            .peer
            .inner()
            .connection()
            .close_info()
            .map_or(Value::Nil, |c| {
                codec::map([
                    ("code", Value::from(c.code.as_str())),
                    ("message", Value::from(c.message.as_str())),
                ])
            });
        // SAFETY: `out` per the contract.
        match unsafe { put_value(out, &v) } {
            STINTMATE_IPC_OK => Ok(()),
            s => Err(s),
        }
    })
}

/// Ends the connection with `bye` (`code` null: `normal`; `message` null: empty).
///
/// # Safety
/// `peer` live; strings NUL-terminated or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_close(
    peer: *mut stintmate_ipc_peer,
    code: *const c_char,
    message: *const c_char,
) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let p = peer_of(peer)?;
            let code = opt_cstr(code, "code")?.unwrap_or("normal");
            let message = opt_cstr(message, "message")?.unwrap_or("");
            p.peer.close(code, message);
        }
        Ok(())
    })
}

// ------------------------------------------------------------------------ calls

/// A progress callback: the MessagePack of one `prog` message.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_progress_cb =
    Option<unsafe extern "C-unwind" fn(user_data: *mut c_void, data: *const u8, len: usize)>;

/// Options of a call (spec 2.6); `struct_size` is `sizeof`.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_call_opts {
    /// `sizeof` this struct.
    pub struct_size: usize,
    /// The method version (0: 1).
    pub version: u32,
    /// The deadline in milliseconds (`STINTMATE_IPC_FOREVER`: none; 0: do not wait).
    pub timeout_ms: i64,
    /// The idempotency key (null: none).
    pub idempotency_key: *const c_char,
    /// Called with each progress message, on the hooks thread, in order (null: none).
    pub on_progress: stintmate_ipc_progress_cb,
    /// Passed to `on_progress`.
    pub user_data: *mut c_void,
    /// Releases `user_data`; runs once on every return after the options were read.
    pub release: stintmate_ipc_release_cb,
}

/// Counts the progress deliveries posted to the hooks thread and not yet run.
#[derive(Default)]
struct ProgressSync {
    pending: Mutex<usize>,
    idle: Condvar,
}

/// One posted progress delivery; dropping it (it ran, or never will) counts it off.
struct Tick(Arc<ProgressSync>);

impl Tick {
    fn new(s: &Arc<ProgressSync>) -> Self {
        *lock(&s.pending) += 1;
        Self(s.clone())
    }
}

impl Drop for Tick {
    fn drop(&mut self) {
        let mut n = lock(&self.0.pending);
        *n -= 1;
        if *n == 0 {
            self.0.idle.notify_all();
        }
    }
}

impl ProgressSync {
    fn wait_drained(&self) {
        let mut n = lock(&self.pending);
        while *n > 0 {
            n = self.idle.wait(n).unwrap_or_else(|p| p.into_inner());
        }
    }
}

pub(crate) struct Built {
    opts: CallOptions,
    progress: Option<Arc<ProgressSync>>,
}

/// Reads the call options (null: the defaults).
///
/// # Safety
/// `p` null or a `stintmate_ipc_call_opts` whose pointers follow the field docs.
pub(crate) unsafe fn call_opts_arg(p: *const stintmate_ipc_call_opts) -> Result<Built, i32> {
    if p.is_null() {
        return Ok(Built {
            opts: CallOptions::default(),
            progress: None,
        });
    }
    // SAFETY: the contract of this function.
    let Some((o, n)) = (unsafe { read_struct(p) }) else {
        return Err(invalid("opts.struct_size is invalid"));
    };
    let reg = Registration::new(o.user_data, o.release);
    let version = if o.version == 0 { 1 } else { o.version };
    let timeout = if present(
        n,
        offset_of!(stintmate_ipc_call_opts, timeout_ms),
        size_of_val(&o.timeout_ms),
    ) {
        wait_arg(o.timeout_ms)
    } else {
        Some(DEFAULT_TIMEOUT)
    };
    let mut opts = CallOptions::new().version(version);
    opts = match timeout {
        Some(t) => opts.timeout(t),
        None => opts.no_timeout(),
    };
    // SAFETY: a NUL-terminated string per the field docs.
    if let Some(k) = unsafe { opt_cstr(o.idempotency_key, "opts.idempotency_key") }? {
        opts = opts.idempotency_key(k);
    }
    let mut progress = None;
    if let Some(cb) = o.on_progress {
        let sync = Arc::new(ProgressSync::default());
        let s = sync.clone();
        opts = opts.on_progress(move |v: Value| {
            let bytes = codec::encode(&v).unwrap_or_default();
            let (tick, reg) = (Tick::new(&s), reg.clone());
            post_hook(move || {
                let _tick = tick;
                // SAFETY: the caller's callback and `user_data` (kept by `reg`).
                let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
                    cb(reg.user_data(), bytes.as_ptr(), bytes.len());
                }));
            });
        });
        progress = Some(sync);
    }
    Ok(Built { opts, progress })
}

/// A synchronous call through `f` (a peer's or a client's).
///
/// # Safety
/// The pointers per the function docs.
pub(crate) unsafe fn call_sync(
    method: *const c_char,
    params: *const u8,
    len: usize,
    opts: *const stintmate_ipc_call_opts,
    out: *mut stintmate_ipc_buffer,
    f: impl FnOnce(&str, Value, CallOptions) -> Result<Value, Error>,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of the callers.
        let (built, method, params) = unsafe {
            let built = match call_opts_arg(opts) {
                Ok(b) => b,
                Err(s) => return s,
            };
            let method = match cstr(method, "method") {
                Ok(m) => m,
                Err(s) => return s,
            };
            let params = match value_arg(params, len, "params") {
                Ok(p) => p,
                Err(s) => return s,
            };
            (built, method, params)
        };
        let r = f(method, params, built.opts);
        // The last progress message is delivered before the call returns, unless this is the
        // hooks thread itself (which would wait for its own queue).
        if let Some(p) = &built.progress
            && !in_callback()
        {
            p.wait_drained();
        }
        // SAFETY: `out` per the contract.
        unsafe { finish(r, out) }
    })
}

/// What `on_done` of a call needs to know about the handle's lifetime.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
}

#[derive(Default)]
struct GateState {
    freed: bool,
    hook: bool,
}

/// A call in flight.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_pending {
    call: Arc<PendingCall>,
    gate: Arc<Gate>,
    #[allow(dead_code)]
    handle: Handle,
}

/// An asynchronous call through `f` (a peer's or a client's).
///
/// # Safety
/// The pointers per the function docs.
pub(crate) unsafe fn call_async_with(
    method: *const c_char,
    params: *const u8,
    len: usize,
    opts: *const stintmate_ipc_call_opts,
    out: *mut *mut stintmate_ipc_pending,
    f: impl FnOnce(&str, Value, CallOptions) -> Result<PendingCall, Error>,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of the callers.
        let (built, method, params) = unsafe {
            let built = match call_opts_arg(opts) {
                Ok(b) => b,
                Err(s) => return s,
            };
            let method = match cstr(method, "method") {
                Ok(m) => m,
                Err(s) => return s,
            };
            let params = match value_arg(params, len, "params") {
                Ok(p) => p,
                Err(s) => return s,
            };
            (built, method, params)
        };
        match f(method, params, built.opts) {
            Ok(call) => {
                // SAFETY: `out` per the contract.
                unsafe {
                    out.write(Box::into_raw(Box::new(stintmate_ipc_pending {
                        call: Arc::new(call),
                        gate: Arc::default(),
                        handle: Handle::new(),
                    })));
                }
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&e),
        }
    })
}

/// A synchronous call: returns after its last `on_progress` ran. `opts` may be null (version
/// 1, 5 s).
///
/// # Safety
/// `peer` live; `method` NUL-terminated; `params` valid for `len` bytes; `opts` per its struct;
/// `result_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_call(
    peer: *mut stintmate_ipc_peer,
    method: *const c_char,
    params: *const u8,
    len: usize,
    opts: *const stintmate_ipc_call_opts,
    result_out: *mut stintmate_ipc_buffer,
) -> i32 {
    // SAFETY: the contract of this function.
    let Some(p) = (unsafe { peer.as_ref() }) else {
        return guard(|| invalid("peer is null"));
    };
    // SAFETY: as above.
    unsafe {
        call_sync(method, params, len, opts, result_out, |m, v, o| {
            p.peer.call(m, v, o)
        })
    }
}

/// Sends a call and returns it in flight.
///
/// # Safety
/// As [`stintmate_ipc_peer_call`]; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_call_async(
    peer: *mut stintmate_ipc_peer,
    method: *const c_char,
    params: *const u8,
    len: usize,
    opts: *const stintmate_ipc_call_opts,
    out: *mut *mut stintmate_ipc_pending,
) -> i32 {
    // SAFETY: the contract of this function.
    let Some(p) = (unsafe { peer.as_ref() }) else {
        return guard(|| invalid("peer is null"));
    };
    // SAFETY: as above.
    unsafe {
        call_async_with(method, params, len, opts, out, |m, v, o| {
            p.peer.call_async(m, v, o)
        })
    }
}

/// The id of the request.
///
/// # Safety
/// `p` null or live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_id(p: *const stintmate_ipc_pending) -> u64 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        unsafe { p.as_ref() }.map_or(0, |p| p.call.id())
    })
}

/// 1 when the call has ended, else 0.
///
/// # Safety
/// `p` null or live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_is_done(p: *const stintmate_ipc_pending) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        unsafe { p.as_ref() }.map_or(0, |p| {
            i32::from(p.call.wait_timeout(Some(Duration::ZERO)).is_some())
        })
    })
}

/// Waits for a call in flight, at most `timeout_ms`: `E_WAIT_TIMEOUT` when it has not ended
/// (it stays in flight and can be waited for again); otherwise the call's own status and
/// result.
///
/// # Safety
/// `p` live; `result_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_wait(
    p: *mut stintmate_ipc_pending,
    timeout_ms: i64,
    result_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { p.as_ref() }) else {
            return invalid("call is null");
        };
        match c.call.wait_timeout(wait_arg(timeout_ms)) {
            // SAFETY: `result_out` per the contract.
            Some(r) => unsafe { finish(r, result_out) },
            None => {
                record_error(STINTMATE_IPC_E_WAIT_TIMEOUT, "the call has not ended");
                STINTMATE_IPC_E_WAIT_TIMEOUT
            }
        }
    })
}

/// Gives up on a call: sends `cancel`; the call then fails with `E_CANCELLED`. Returns 1 when
/// the cancel took effect, 0 when the answer had already won. `reason` is for the caller's
/// logs (the protocol's `cancel` carries none) and may be null.
///
/// # Safety
/// `p` live; `reason` null or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_cancel(
    p: *mut stintmate_ipc_pending,
    reason: *const c_char,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let Some(c) = p.as_ref() else {
                return invalid("call is null");
            };
            if let Err(s) = opt_cstr(reason, "reason") {
                return s;
            }
            i32::from(c.call.cancel())
        }
    })
}

/// Called once, on the hooks thread, when a call ends. `p` is the call's handle: it is valid
/// until the callback returns even if `stintmate_ipc_call_free` ran meanwhile (the free then
/// takes effect when the callback returns).
#[allow(non_camel_case_types)]
pub type stintmate_ipc_done_cb =
    Option<unsafe extern "C-unwind" fn(user_data: *mut c_void, p: *mut stintmate_ipc_pending)>;

/// Runs `cb` once on the hooks thread when the call ends (soon, when it already has). One
/// `on_done` per call; `release` runs after the callback (or at once when this fails).
///
/// # Safety
/// `p` live; `cb` and `user_data` usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_on_done(
    p: *mut stintmate_ipc_pending,
    cb: stintmate_ipc_done_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    guard(|| {
        let reg = Registration::new(user_data, release);
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { p.as_ref() }) else {
            return invalid("call is null");
        };
        let Some(cb) = cb else {
            return invalid("cb is null");
        };
        {
            let mut g = lock(&c.gate.state);
            if g.hook {
                return invalid("the call already has an on_done");
            }
            g.hook = true;
        }
        let done = DoneJob(Some((c.gate.clone(), p as usize)));
        c.call.on_done(move || {
            post_hook(move || {
                let mut done = done;
                let p = done
                    .0
                    .as_ref()
                    .map_or(std::ptr::null_mut(), |d| d.1 as *mut stintmate_ipc_pending);
                // SAFETY: the handle is alive: `call_free` waits for `hook` to clear.
                let _ = catch_unwind(AssertUnwindSafe(|| unsafe { cb(reg.user_data(), p) }));
                drop(reg);
                done.finish();
            });
        });
        STINTMATE_IPC_OK
    })
}

/// The `on_done` job of a call: clears the gate when it has run, or when it is dropped without
/// running (shutdown started first), so `call_free` never leaks the handle.
struct DoneJob(Option<(Arc<Gate>, usize)>);

impl DoneJob {
    fn finish(&mut self) {
        let Some((gate, addr)) = self.0.take() else {
            return;
        };
        let free_now = {
            let mut g = lock(&gate.state);
            g.hook = false;
            g.freed
        };
        if free_now {
            // SAFETY: freed by the caller while the job was pending or ran; made by
            // `Box::into_raw`.
            drop(unsafe { Box::from_raw(addr as *mut stintmate_ipc_pending) });
        }
    }
}

impl Drop for DoneJob {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Releases a call handle (the call itself is not cancelled). With an `on_done` still to run
/// the release happens when it has returned. Null is ignored.
///
/// # Safety
/// `p` from the library, not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_free(p: *mut stintmate_ipc_pending) {
    if p.is_null() {
        return;
    }
    // SAFETY: made by `Box::into_raw` in this module.
    let gate = unsafe { &*p }.gate.clone();
    let now = {
        let mut g = lock(&gate.state);
        g.freed = true;
        !g.hook
    };
    if now {
        // SAFETY: as above; nothing else refers to it.
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(p) })));
    }
}

// ------------------------------------------------------------------------ deliveries

/// What a subscriber callback receives; the pointers are valid only during the callback.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_delivery {
    /// 0: an event or state (`data` is MessagePack); 1: a stream sample (`data` is the raw
    /// packed layout body).
    pub kind: i32,
    /// Bit 0: the state snapshot of the subscription; bit 1: samples were dropped before this.
    pub flags: u32,
    /// The channel name, UTF-8, not NUL-terminated.
    pub channel: *const u8,
    /// The length of `channel`.
    pub channel_len: usize,
    /// The sequence number.
    pub seq: u64,
    /// The source timestamp in nanoseconds (valid when `has_ts` is not 0).
    pub ts_ns: i64,
    /// Whether `ts_ns` is set.
    pub has_ts: i32,
    /// The payload.
    pub data: *const u8,
    /// The length of `data`.
    pub len: usize,
}

/// `STINTMATE_IPC_DELIVERY_SNAPSHOT`: the state snapshot of the subscription.
pub const STINTMATE_IPC_DELIVERY_SNAPSHOT: u32 = 1;
/// `STINTMATE_IPC_DELIVERY_DROPPED`: samples were dropped before this one.
pub const STINTMATE_IPC_DELIVERY_DROPPED: u32 = 2;
/// `stintmate_ipc_delivery.kind` of an event or state.
pub const STINTMATE_IPC_KIND_EVENT: i32 = 0;
/// `stintmate_ipc_delivery.kind` of a stream sample.
pub const STINTMATE_IPC_KIND_SAMPLE: i32 = 1;

/// A subscriber callback; runs on the connection's callback lane, in order. It must not block
/// for long.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_delivery_cb = Option<
    unsafe extern "C-unwind" fn(user_data: *mut c_void, delivery: *const stintmate_ipc_delivery),
>;

/// Called when a subscription ends (the owner closed it or the channel went away). `sub` is
/// null when the subscription never started (`peer_subscribe` failed).
#[allow(non_camel_case_types)]
pub type stintmate_ipc_end_cb = Option<
    unsafe extern "C-unwind" fn(
        user_data: *mut c_void,
        sub: *mut stintmate_ipc_subscription,
        channel: *const c_char,
        code: *const c_char,
        message: *const c_char,
    ),
>;

/// Options of a subscription (spec 2.6); `struct_size` is `sizeof`.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_subscribe_opts {
    /// `sizeof` this struct.
    pub struct_size: usize,
    /// The channel version (0: 1).
    pub version: u32,
    /// A MessagePack map of subscribe options (null or length 0: none).
    pub options: *const u8,
    /// The length of `options`.
    pub options_len: usize,
    /// How long `peer_subscribe` waits for the answer (`STINTMATE_IPC_FOREVER`: no limit).
    pub timeout_ms: i64,
    /// Required: runs on the connection's lane, in order.
    pub on_delivery: stintmate_ipc_delivery_cb,
    /// Called when the subscription ends (null: none).
    pub on_end: stintmate_ipc_end_cb,
    /// Passed to the callbacks.
    pub user_data: *mut c_void,
    /// Releases `user_data` after the last callback (after `unsubscribe`); runs once on every
    /// return after the options were read.
    pub release: stintmate_ipc_release_cb,
}

struct RunState {
    closed: bool,
    threads: Vec<ThreadId>,
}

/// What the callbacks of a subscription share with its handle: the `user_data`, the pointer
/// of the handle (for `on_end`), and the callbacks running (so `unsubscribe` can wait).
struct SubState {
    ptr: AtomicPtr<stintmate_ipc_subscription>,
    user: Mutex<Option<Arc<Registration>>>,
    run: Mutex<RunState>,
    idle: Condvar,
}

struct RunGuard<'a>(&'a SubState);

impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        let me = std::thread::current().id();
        let mut r = lock(&self.0.run);
        if let Some(i) = r.threads.iter().position(|t| *t == me) {
            r.threads.swap_remove(i);
        }
        self.0.idle.notify_all();
    }
}

impl SubState {
    /// Starts a callback: `None` once the subscription is closed.
    fn begin(&self) -> Option<(Arc<Registration>, RunGuard<'_>)> {
        let mut r = lock(&self.run);
        if r.closed {
            return None;
        }
        let reg = lock(&self.user).clone()?;
        r.threads.push(std::thread::current().id());
        Some((reg, RunGuard(self)))
    }

    /// No callback starts after this; waits for those running on other threads, then lets go
    /// of the `user_data` (it is released after the last callback).
    fn finish(&self) {
        let me = std::thread::current().id();
        let mut r = lock(&self.run);
        r.closed = true;
        while r.threads.iter().any(|t| *t != me) {
            r = self.idle.wait(r).unwrap_or_else(|p| p.into_inner());
        }
        drop(r);
        drop(lock(&self.user).take());
    }
}

enum SubKind {
    Peer(Subscription),
    Client(Arc<SubscriptionHandle>),
}

/// A subscription (of a peer, or kept by a client across reconnections).
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_subscription {
    kind: Mutex<Option<SubKind>>,
    state: Arc<SubState>,
    channel: String,
    #[allow(dead_code)]
    handle: Handle,
}

type Callback = Arc<dyn Fn(Delivery) + Send + Sync>;

/// A subscription being made: the handle exists (so `on_end` can name it) before the core
/// subscribes.
pub(crate) struct Setup {
    state: Arc<SubState>,
    sub: *mut stintmate_ipc_subscription,
    pub(crate) opts: SubscribeOptions,
    pub(crate) callback: Callback,
    pub(crate) channel: String,
    on_end_user: stintmate_ipc_end_cb,
}

fn c_string(s: &str) -> CString {
    CString::new(s.replace('\0', "")).unwrap_or_default()
}

fn deliver(
    state: &SubState,
    cb: unsafe extern "C-unwind" fn(*mut c_void, *const stintmate_ipc_delivery),
    d: Delivery,
) {
    let Some(_g) = enter() else { return };
    let Some((reg, _run)) = state.begin() else {
        return;
    };
    let (kind, flags, channel, seq, ts, data) = match &d {
        Delivery::Event(e) => {
            let bytes = codec::encode(&e.data).unwrap_or_default();
            (
                STINTMATE_IPC_KIND_EVENT,
                if e.snapshot {
                    STINTMATE_IPC_DELIVERY_SNAPSHOT
                } else {
                    0
                },
                e.channel.as_str(),
                e.seq,
                e.ts,
                bytes,
            )
        }
        Delivery::Sample(s) => (
            STINTMATE_IPC_KIND_SAMPLE,
            if s.dropped {
                STINTMATE_IPC_DELIVERY_DROPPED
            } else {
                0
            },
            s.channel.as_str(),
            u64::from(s.seq),
            Some(s.t_ns),
            s.body.to_vec(),
        ),
    };
    let c = stintmate_ipc_delivery {
        kind,
        flags,
        channel: channel.as_ptr(),
        channel_len: channel.len(),
        seq,
        ts_ns: ts.unwrap_or(0),
        has_ts: i32::from(ts.is_some()),
        data: data.as_ptr(),
        len: data.len(),
    };
    // A panic of the callback is contained: the lane goes on with the next delivery.
    // SAFETY: `cb` and `reg` are the embedder's; the struct outlives the call.
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        cb(reg.user_data(), &raw const c);
    }));
}

/// Reads the options and allocates the handle.
///
/// # Safety
/// `channel` NUL-terminated; `opts` per its struct.
pub(crate) unsafe fn subscribe_setup(
    channel: *const c_char,
    opts: *const stintmate_ipc_subscribe_opts,
) -> Result<Setup, i32> {
    // SAFETY: the contract of this function.
    let Some((o, n)) = (unsafe { read_struct(opts) }) else {
        return Err(invalid("opts is null or its struct_size is invalid"));
    };
    let reg = Registration::new(o.user_data, o.release);
    // SAFETY: a NUL-terminated string per the contract.
    let channel = unsafe { cstr(channel, "channel") }?.to_owned();
    let Some(on_delivery) = o.on_delivery else {
        return Err(invalid("opts.on_delivery is null"));
    };
    // SAFETY: valid for `options_len` bytes per the field docs.
    let options = match unsafe { value_arg(o.options, o.options_len, "opts.options") }? {
        Value::Nil => None,
        v => Some(v),
    };
    let timeout = if present(
        n,
        offset_of!(stintmate_ipc_subscribe_opts, timeout_ms),
        size_of_val(&o.timeout_ms),
    ) {
        wait_arg(o.timeout_ms).unwrap_or(LONG)
    } else {
        DEFAULT_TIMEOUT
    };
    let state = Arc::new(SubState {
        ptr: AtomicPtr::new(std::ptr::null_mut()),
        user: Mutex::new(Some(reg)),
        run: Mutex::new(RunState {
            closed: false,
            threads: Vec::new(),
        }),
        idle: Condvar::new(),
    });
    let sub = Box::into_raw(Box::new(stintmate_ipc_subscription {
        kind: Mutex::new(None),
        state: state.clone(),
        channel: channel.clone(),
        handle: Handle::new(),
    }));
    state.ptr.store(sub, Ordering::Release);
    let mut sopts = SubscribeOptions {
        version: if o.version == 0 { 1 } else { o.version },
        options,
        timeout,
        ..SubscribeOptions::default()
    };
    if let Some(end) = o.on_end {
        let st = state.clone();
        sopts.on_end = Some(Arc::new(move |ch: &str, code: &str, msg: &str| {
            let Some(_g) = enter() else { return };
            let Some((reg, _run)) = st.begin() else {
                return;
            };
            let (ch, code, msg) = (c_string(ch), c_string(code), c_string(msg));
            let sub = st.ptr.load(Ordering::Acquire);
            // SAFETY: the embedder's callback; the strings outlive the call.
            let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
                end(
                    reg.user_data(),
                    sub,
                    ch.as_ptr(),
                    code.as_ptr(),
                    msg.as_ptr(),
                );
            }));
        }));
    }
    let st = state.clone();
    Ok(Setup {
        state,
        sub,
        opts: sopts,
        callback: Arc::new(move |d| deliver(&st, on_delivery, d)),
        channel,
        on_end_user: o.on_end,
    })
}

impl Setup {
    /// The subscription started: gives the handle to the caller.
    ///
    /// # Safety
    /// `out` valid for a write.
    unsafe fn attach(self, kind: SubKind, out: *mut *mut stintmate_ipc_subscription) {
        // SAFETY: `sub` was made by `Box::into_raw` in `subscribe_setup` and is still ours.
        *lock(&unsafe { &*self.sub }.kind) = Some(kind);
        // SAFETY: `out` per the contract.
        unsafe { out.write(self.sub) };
    }

    /// It never started: frees the handle; `on_end` hears of it with a null subscription.
    fn abort(self, code: &str, message: &str) {
        // Callbacks that start from now on see no handle; those already running (on_end on a
        // lane, a delivery) may still hold it, so it is freed only after they ended.
        self.state
            .ptr
            .store(std::ptr::null_mut(), Ordering::Release);
        let reg = lock(&self.state.user).clone();
        self.state.finish();
        // SAFETY: made by `Box::into_raw`; `finish` waited for every callback that could
        // have been given the pointer, and no new one starts.
        drop(unsafe { Box::from_raw(self.sub) });
        if let (Some(end), Some(reg)) = (self.on_end_user, reg) {
            let (ch, code, msg) = (c_string(&self.channel), c_string(code), c_string(message));
            post_hook(move || {
                // SAFETY: the embedder's callback; `reg` keeps `user_data` alive.
                let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
                    end(
                        reg.user_data(),
                        std::ptr::null_mut(),
                        ch.as_ptr(),
                        code.as_ptr(),
                        msg.as_ptr(),
                    );
                }));
            });
        }
    }
}

/// Subscribes through a peer and waits for the owner's answer; fails with its error.
///
/// # Safety
/// `peer` live; `channel` NUL-terminated; `opts` per its struct; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peer_subscribe(
    peer: *mut stintmate_ipc_peer,
    channel: *const c_char,
    opts: *const stintmate_ipc_subscribe_opts,
    out: *mut *mut stintmate_ipc_subscription,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        unsafe {
            let setup = match subscribe_setup(channel, opts) {
                Ok(s) => s,
                Err(s) => return s,
            };
            let Some(p) = peer.as_ref() else {
                setup.abort("invalid_argument", "peer is null");
                return invalid("peer is null");
            };
            match p.peer.subscribe(&setup.channel, setup.opts.clone(), {
                let cb = setup.callback.clone();
                move |d| cb(d)
            }) {
                Ok(sub) => {
                    setup.attach(SubKind::Peer(sub), out);
                    STINTMATE_IPC_OK
                }
                Err(e) => {
                    let info = ErrInfo::of(&e);
                    setup.abort(info.code.as_deref().unwrap_or(""), &info.message);
                    info.record()
                }
            }
        }
    })
}

/// Attaches the subscription a client made (`client_subscribe`).
///
/// # Safety
/// `out` valid for a write.
pub(crate) unsafe fn attach_client(
    setup: Setup,
    handle: SubscriptionHandle,
    out: *mut *mut stintmate_ipc_subscription,
) {
    // SAFETY: forwarded contract.
    unsafe { setup.attach(SubKind::Client(Arc::new(handle)), out) };
}

/// The subscription behind a pointer.
///
/// # Safety
/// `s` null or live.
unsafe fn sub_of<'a>(
    s: *mut stintmate_ipc_subscription,
) -> Result<&'a stintmate_ipc_subscription, i32> {
    // SAFETY: the contract of the callers.
    unsafe { s.as_ref() }.ok_or_else(|| invalid("subscription is null"))
}

fn kind_of(s: &stintmate_ipc_subscription) -> Option<SubKind> {
    lock(&s.kind).as_ref().map(|k| match k {
        SubKind::Peer(p) => SubKind::Peer(p.clone()),
        SubKind::Client(h) => SubKind::Client(h.clone()),
    })
}

/// `{channel, layout?, error?: {status, message, code?}}`: the layout of a stream once it is
/// live; the error of the last attempt of a client's subscription.
///
/// # Safety
/// `s` live; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_subscription_info(
    s: *mut stintmate_ipc_subscription,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let s = unsafe { sub_of(s) }?;
        let mut pairs: Vec<(&str, Value)> = vec![("channel", Value::from(s.channel.as_str()))];
        match kind_of(s) {
            Some(SubKind::Peer(p)) => {
                if let Some(l) = p.layout() {
                    pairs.push(("layout", l.to_value()));
                }
            }
            Some(SubKind::Client(h)) => {
                if let Some(l) = h.current().and_then(|c| c.layout().map(|l| l.to_value())) {
                    pairs.push(("layout", l));
                }
                if let Some(m) = h.error() {
                    pairs.push((
                        "error",
                        codec::map([
                            ("status", Value::from(i64::from(STINTMATE_IPC_E_REMOTE))),
                            ("message", Value::from(m.as_str())),
                        ]),
                    ));
                }
            }
            None => {}
        }
        // SAFETY: `out` per the contract.
        match unsafe { put_value(out, &codec::map(pairs)) } {
            STINTMATE_IPC_OK => Ok(()),
            st => Err(st),
        }
    })
}

/// 1 when the subscription is live on a connection, else 0.
///
/// # Safety
/// `s` null or live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_subscription_is_active(
    s: *mut stintmate_ipc_subscription,
) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        let Some(s) = (unsafe { s.as_ref() }) else {
            return 0;
        };
        i32::from(match kind_of(s) {
            Some(SubKind::Peer(p)) => !p.is_ended(),
            Some(SubKind::Client(h)) => h.is_subscribed(),
            None => false,
        })
    })
}

/// Waits for `what` (1: subscribed, 2: ended), at most `timeout_ms`: 0, or `E_WAIT_TIMEOUT`.
/// For a client's subscription "ended" means the current subscription on a connection ended
/// (or there is none).
///
/// # Safety
/// `s` live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_subscription_wait(
    s: *mut stintmate_ipc_subscription,
    what: i32,
    timeout_ms: i64,
) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        let s = unsafe { sub_of(s) }?;
        if !(1..=2).contains(&what) {
            return Err(invalid("what must be 1 (subscribed) or 2 (ended)"));
        }
        let limit = wait_arg(timeout_ms);
        let done = match (kind_of(s), what) {
            (Some(SubKind::Peer(p)), 2) => {
                blocking::wait_for(async move { p.wait_ended().await }, limit)
            }
            (Some(SubKind::Client(h)), 1) => {
                blocking::wait_for(async move { h.wait_subscribed().await }, limit)
            }
            (Some(SubKind::Client(h)), _) => match h.current() {
                Some(cur) => blocking::wait_for(async move { cur.wait_ended().await }, limit),
                None => true,
            },
            (Some(SubKind::Peer(_)), _) => true,
            (None, 2) => true,
            (None, _) => {
                // Not made yet: it will be, within the call that creates it.
                false
            }
        };
        if done {
            Ok(())
        } else {
            record_error(STINTMATE_IPC_E_WAIT_TIMEOUT, "the wait timed out");
            Err(STINTMATE_IPC_E_WAIT_TIMEOUT)
        }
    })
}

/// Ends the subscription and releases the handle: no callback starts after this returns and
/// running ones are waited for (unless called from one of them); then `release` runs. Null is
/// ignored.
///
/// # Safety
/// `s` from the library, not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_unsubscribe(s: *mut stintmate_ipc_subscription) {
    if s.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: made by `Box::into_raw` in `subscribe_setup`.
        let b = unsafe { Box::from_raw(s) };
        let kind = lock(&b.kind).take();
        match &kind {
            Some(SubKind::Peer(p)) => p.close(),
            Some(SubKind::Client(h)) => h.close(),
            None => {}
        }
        drop(kind);
        b.state.finish();
    }));
}
