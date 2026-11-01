//! Nodes: what an endpoint or a client offers to the peers connected to it.
//!
//! A node is borrowed from its owner (`stintmate_ipc_endpoint_node`,
//! `stintmate_ipc_client_node`): methods with options, requests, event/state/stream
//! channels with publish/accept, hooks, peers and declarations work the same on both.

use std::ffi::{c_char, c_void};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam_channel::{RecvTimeoutError, Sender, bounded};
use stintmate_ipc::MethodOpts;
use stintmate_ipc::blocking::{
    self, Context, EventChannel, Node, PublishOptions, StateChannel, StreamChannel,
    SubscribeRequest,
};
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::{Error, RemoteError};
use stintmate_ipc::layout::Layout;

use crate::peer::stintmate_ipc_peer;
use crate::runtime::{
    Handle, Registration, count_handle, enter, post_hook, stintmate_ipc_release_cb, uncount_handle,
};
use crate::{
    STINTMATE_IPC_E_WAIT_TIMEOUT, STINTMATE_IPC_OK, cstr, fail, guard, invalid, opt_cstr,
    put_value, record_error, stintmate_ipc_buffer, value_arg, versions_arg,
};

/// `STINTMATE_IPC_METHOD_ORDERED`: one call at a time per connection, in arrival order.
pub const STINTMATE_IPC_METHOD_ORDERED: u32 = 1;
/// `STINTMATE_IPC_METHOD_INLINE`: run the handler on the connection's callback lane (only for
/// handlers that answer from memory in microseconds).
pub const STINTMATE_IPC_METHOD_INLINE: u32 = 2;
/// `STINTMATE_IPC_METHOD_IDEMPOTENT`: declares the method idempotent (answers are kept by
/// idempotency key).
pub const STINTMATE_IPC_METHOD_IDEMPOTENT: u32 = 4;
/// `STINTMATE_IPC_METHOD_NO_IDEMPOTENCY_CACHE`: an idempotent method whose answers the library
/// does not keep (the handler deduplicates).
pub const STINTMATE_IPC_METHOD_NO_IDEMPOTENCY_CACHE: u32 = 8;
/// `ts_ns` value of `stintmate_ipc_publish` and `t_ns` of `stintmate_ipc_stream_send`
/// meaning "now".
pub const STINTMATE_IPC_TS_NOW: i64 = i64::MIN;
/// `event` of a peer hook: a connection was established.
pub const STINTMATE_IPC_PEER_CONNECT: i32 = 1;
/// `event` of a peer hook: a connection ended.
pub const STINTMATE_IPC_PEER_DISCONNECT: i32 = 2;
/// `event` of a peer hook: a peer sent new declarations.
pub const STINTMATE_IPC_PEER_REDECLARE: i32 = 3;

// ------------------------------------------------------------------------ small helpers

/// Reads a field of a struct the caller fills, when it lies inside `struct_size`.
///
/// # Safety
/// `base` points to a struct of at least `size` readable bytes whose field at `off` has type
/// `T`.
pub(crate) unsafe fn read_field<T: Copy>(
    base: *const u8,
    size: usize,
    off: usize,
    default: T,
) -> T {
    if size >= off + size_of::<T>() {
        // SAFETY: inside `struct_size`, per the contract.
        unsafe { base.add(off).cast::<T>().read_unaligned() }
    } else {
        default
    }
}

/// The wire form of an error for the error callbacks: `{status, message, code?, data?,
/// retry_ms?, os_error?}`, the shape of `stintmate_ipc_last_error`.
pub(crate) fn error_map(e: &Error) -> Value {
    let mut pairs: Vec<(&str, Value)> = vec![
        ("status", Value::from(i64::from(crate::status_of(e)))),
        ("message", Value::from(e.to_string())),
    ];
    match e {
        Error::Remote(r) => {
            pairs[1].1 = Value::from(r.message.as_str());
            pairs.push(("code", Value::from(r.code.as_str())));
            if let Some(d) = &r.details {
                pairs.push(("data", d.clone()));
            }
            if let Some(ms) = r.retry_ms {
                pairs.push(("retry_ms", Value::from(ms)));
            }
        }
        Error::Handshake(f) => {
            pairs[1].1 = Value::from(f.message.as_str());
            pairs.push(("code", Value::from(f.code.as_str())));
        }
        Error::HandshakeTimeout => pairs.push(("code", Value::from("timeout"))),
        Error::PeerBye { code, .. } => pairs.push(("code", Value::from(code.as_str()))),
        Error::ConnectionLost {
            code: Some(code), ..
        } => {
            pairs.push(("code", Value::from(code.as_str())));
        }
        Error::Io(io) => {
            if let Some(o) = io.raw_os_error() {
                pairs.push(("os_error", Value::from(i64::from(o))));
            }
        }
        _ => {}
    }
    codec::map(pairs)
}

fn versions_value(v: &[u32]) -> Value {
    Value::Array(v.iter().map(|x| Value::from(u64::from(*x))).collect())
}

fn ms_duration(ms: i64) -> Option<Duration> {
    (ms >= 0).then(|| Duration::from_millis(ms as u64))
}

// ------------------------------------------------------------------------ the node

/// What an endpoint or a client offers; borrowed from its owner, never freed by the caller.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_node {
    pub(crate) node: Node,
    pub(crate) regs: Regs,
}

impl stintmate_ipc_node {
    pub(crate) fn new(node: Node) -> Self {
        Self {
            node,
            regs: Regs::default(),
        }
    }
}

impl Drop for stintmate_ipc_node {
    /// The owner is freed: the core node (and the handlers in it) may live on in cycles, so the
    /// registrations are let go here; their `release` runs once the callbacks in flight end.
    fn drop(&mut self) {
        self.regs.release_all();
    }
}

/// The `user_data` of a method or hook, held by its handler; emptied when the owner is freed.
pub(crate) struct Cell(Mutex<Option<Arc<Registration>>>);

impl Cell {
    pub(crate) fn get(&self) -> Option<Arc<Registration>> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// The registrations of one node (and its channels), to release when its owner is freed.
#[derive(Clone, Default)]
pub(crate) struct Regs(Arc<Mutex<Vec<Weak<Cell>>>>);

impl Regs {
    pub(crate) fn add(&self, user: Arc<Registration>) -> Arc<Cell> {
        let cell = Arc::new(Cell(Mutex::new(Some(user))));
        let mut v = self.0.lock().unwrap_or_else(|p| p.into_inner());
        v.retain(|w| w.strong_count() > 0);
        v.push(Arc::downgrade(&cell));
        cell
    }

    fn release_all(&self) {
        let v = std::mem::take(&mut *self.0.lock().unwrap_or_else(|p| p.into_inner()));
        for c in v.iter().filter_map(Weak::upgrade) {
            drop(c.0.lock().unwrap_or_else(|p| p.into_inner()).take());
        }
    }
}

unsafe fn node_ref<'a>(node: *mut stintmate_ipc_node) -> Result<&'a stintmate_ipc_node, i32> {
    // SAFETY: the contract of the callers.
    unsafe { node.as_ref() }.ok_or_else(|| invalid("node is null"))
}

// ------------------------------------------------------------------------ methods

/// Options of a method; `struct_size` is `sizeof`.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_method_opts {
    /// `sizeof` this struct.
    pub struct_size: usize,
    /// `STINTMATE_IPC_METHOD_*` flags.
    pub flags: u32,
    /// The versions offered (null/0: version 1).
    pub versions: *const u32,
    /// The number of versions.
    pub n_versions: usize,
    /// How long answers of an idempotent method are kept; -1 (or 0): the default.
    pub idempotency_ttl_ms: i64,
    /// One line for people (null: none).
    pub doc: *const c_char,
    /// JSON Schema of the params as MessagePack (null: none).
    pub params_schema: *const u8,
    /// The length of `params_schema`.
    pub params_schema_len: usize,
    /// JSON Schema of the result as MessagePack (null: none).
    pub result_schema: *const u8,
    /// The length of `result_schema`.
    pub result_schema_len: usize,
}

/// Reads method options (null: the defaults).
///
/// # Safety
/// `opts` null or per `stintmate_ipc_method_opts`.
unsafe fn method_opts_arg(opts: *const stintmate_ipc_method_opts) -> Result<MethodOpts, i32> {
    use std::mem::offset_of as off;
    let mut m = MethodOpts::default();
    if opts.is_null() {
        return Ok(m);
    }
    // SAFETY: the first field is readable per the contract.
    let size = unsafe { opts.cast::<usize>().read_unaligned() };
    let base = opts.cast::<u8>();
    // SAFETY: each field is read only when it lies inside `struct_size`.
    unsafe {
        let flags = read_field(base, size, off!(stintmate_ipc_method_opts, flags), 0u32);
        let versions = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, versions),
            std::ptr::null::<u32>(),
        );
        let n = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, n_versions),
            0usize,
        );
        let ttl = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, idempotency_ttl_ms),
            -1i64,
        );
        let doc = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, doc),
            std::ptr::null::<c_char>(),
        );
        let p = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, params_schema),
            std::ptr::null::<u8>(),
        );
        let pl = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, params_schema_len),
            0usize,
        );
        let r = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, result_schema),
            std::ptr::null::<u8>(),
        );
        let rl = read_field(
            base,
            size,
            off!(stintmate_ipc_method_opts, result_schema_len),
            0usize,
        );
        m.versions = versions_arg(versions, n);
        m.ordered = flags & STINTMATE_IPC_METHOD_ORDERED != 0;
        m.inline = flags & STINTMATE_IPC_METHOD_INLINE != 0;
        m.idempotent = flags & STINTMATE_IPC_METHOD_IDEMPOTENT != 0;
        m.idempotency_cache = flags & STINTMATE_IPC_METHOD_NO_IDEMPOTENCY_CACHE == 0;
        if ttl > 0 {
            m.idempotency_ttl = Some(Duration::from_millis(ttl as u64));
        }
        m.doc = opt_cstr(doc, "opts.doc")?.map(str::to_owned);
        let schema = |p: *const u8, l: usize, what: &str| -> Result<Option<Value>, i32> {
            if l == 0 {
                return Ok(None);
            }
            value_arg(p, l, what).map(|v| (!v.is_nil()).then_some(v))
        };
        m.params = schema(p, pl, "opts.params_schema")?;
        m.result = schema(r, rl, "opts.result_schema")?;
    }
    Ok(m)
}

struct RequestInner {
    tx: Sender<Result<Value, RemoteError>>,
    ctx: Context,
    _handle: Handle,
}

/// A call being handled (one per call). Exactly one of `stintmate_ipc_respond` or
/// `stintmate_ipc_respond_error` must be called, from the handler or later from any thread; it
/// consumes the request.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_request(RequestInner);

/// A method handler. It receives the request and the params as MessagePack (valid only during
/// the call), and answers through the request. The handler's pool thread waits for the answer
/// (up to the caller's deadline), so answer promptly.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_handler_cb = Option<
    unsafe extern "C-unwind" fn(
        user_data: *mut c_void,
        req: *mut stintmate_ipc_request,
        params: *const u8,
        params_len: usize,
    ),
>;

/// Offers a method on a node (an endpoint's or a client's). The handler runs on the handler
/// pool (or the callback lane with `STINTMATE_IPC_METHOD_INLINE`), never on a runtime thread;
/// a panic that unwinds out of it answers `internal`. Offering a name again replaces the
/// method (the previous `user_data` is released).
///
/// # Safety
/// `node` valid; `name` NUL-terminated; `opts` null or per its struct; `user_data` usable from
/// library threads while the method is offered.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_method(
    node: *mut stintmate_ipc_node,
    name: *const c_char,
    opts: *const stintmate_ipc_method_opts,
    handler: stintmate_ipc_handler_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    guard(|| {
        let Some(cb) = handler else {
            return invalid("handler is null");
        };
        // SAFETY: the contract of this function.
        let (n, name, opts) = unsafe {
            let n = match node_ref(node) {
                Ok(n) => n,
                Err(s) => return s,
            };
            let name = match cstr(name, "name") {
                Ok(n) => n,
                Err(s) => return s,
            };
            let opts = match method_opts_arg(opts) {
                Ok(o) => o,
                Err(s) => return s,
            };
            (n, name, opts)
        };
        let cell = n.regs.add(Registration::new(user_data, release));
        let r = n
            .node
            .method(name, opts, move |ctx: &Context, params: Value| {
                let Some(_active) = enter() else {
                    return Err(RemoteError::new("unavailable", "the library was shut down"));
                };
                let Some(user) = cell.get() else {
                    return Err(RemoteError::new("unavailable", "the node was freed"));
                };
                let bytes = codec::encode(&params)
                    .map_err(|e| RemoteError::new("internal", e.to_string()))?;
                let (tx, rx) = bounded(1);
                let inner = Arc::new(stintmate_ipc_request(RequestInner {
                    tx,
                    ctx: ctx.clone(),
                    _handle: Handle::new(),
                }));
                // The callback owns one reference (released by respond); we keep ours.
                let handle = Arc::into_raw(inner.clone()).cast_mut();
                // SAFETY: the embedder's callback; `bytes` outlives the call.
                unsafe { cb(user.user_data(), handle, bytes.as_ptr(), bytes.len()) };
                loop {
                    match rx.recv_timeout(Duration::from_millis(20)) {
                        Ok(r) => return r,
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(RemoteError::new("internal", "the handler did not answer"));
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            if inner.0.ctx.expired() {
                                return Err(RemoteError::new(
                                    "timeout",
                                    "the handler answered late",
                                ));
                            }
                            if inner.0.ctx.is_cancelled() {
                                return Err(RemoteError::new("cancelled", "the caller gave up"));
                            }
                        }
                    }
                }
            });
        match r {
            Ok(()) => STINTMATE_IPC_OK,
            Err(e) => fail(&e),
        }
    })
}

/// Withdraws a method.
///
/// # Safety
/// `node` valid; `name` NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_remove_method(
    node: *mut stintmate_ipc_node,
    name: *const c_char,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let n = match node_ref(node) {
                Ok(n) => n,
                Err(s) => return s,
            };
            match cstr(name, "name") {
                Ok(name) => {
                    n.node.remove_method(name);
                    STINTMATE_IPC_OK
                }
                Err(s) => s,
            }
        }
    })
}

/// What the node offers now, as the `decl` map `{methods: {...}, channels: {...}}`.
///
/// # Safety
/// `node` valid; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_declarations(
    node: *mut stintmate_ipc_node,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let n = match unsafe { node_ref(node) } {
            Ok(n) => n,
            Err(s) => return s,
        };
        // SAFETY: `out` per the contract.
        unsafe { put_value(out, &n.node.declarations().to_value()) }
    })
}

/// Owned references to the open connections: at most `cap` are written to `out`; `*count` is
/// the total. Release each with `stintmate_ipc_peer_free`.
///
/// # Safety
/// `node` valid; `out` valid for `cap` writes (or null with `cap` 0); `count` valid for a
/// write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_peers(
    node: *mut stintmate_ipc_node,
    out: *mut *mut stintmate_ipc_peer,
    cap: usize,
    count: *mut usize,
) -> i32 {
    guard(|| {
        if count.is_null() {
            return invalid("count is null");
        }
        if out.is_null() && cap > 0 {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let n = match unsafe { node_ref(node) } {
            Ok(n) => n,
            Err(s) => return s,
        };
        let peers = n.node.peers();
        for (i, p) in peers.iter().take(cap).enumerate() {
            // SAFETY: `i < cap`.
            unsafe { out.add(i).write(stintmate_ipc_peer::new_ref(p.clone())) };
        }
        // SAFETY: `count` is valid for a write.
        unsafe { count.write(peers.len()) };
        STINTMATE_IPC_OK
    })
}

/// A peer hook: `event` is `STINTMATE_IPC_PEER_*`; `peer` is borrowed for the call
/// (`stintmate_ipc_peer_ref` keeps it).
#[allow(non_camel_case_types)]
pub type stintmate_ipc_peer_cb = Option<
    unsafe extern "C-unwind" fn(user_data: *mut c_void, event: i32, peer: *mut stintmate_ipc_peer),
>;

/// Sets the hook called, on the hooks thread and in order, when a connection is established,
/// ends, or its peer sends new declarations. Replaces the previous hook (releasing its
/// `user_data`); a null `cb` clears it.
///
/// # Safety
/// `node` valid; `user_data` usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_on_peer(
    node: *mut stintmate_ipc_node,
    cb: stintmate_ipc_peer_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let n = match unsafe { node_ref(node) } {
            Ok(n) => n,
            Err(s) => return s,
        };
        let cell = n.regs.add(Registration::new(user_data, release));
        let make = |event: i32| {
            let cell = cell.clone();
            move |peer: blocking::Peer| {
                let Some(cb) = cb else { return };
                if cell.get().is_none() {
                    return;
                }
                let cell = cell.clone();
                post_hook(move || {
                    // Checked again here: a free that ran meanwhile stops the callbacks.
                    let Some(user) = cell.get() else { return };
                    let p = stintmate_ipc_peer::new_ref(peer);
                    // SAFETY: the embedder's callback; `p` lives until it returns.
                    unsafe { cb(user.user_data(), event, p) };
                    // SAFETY: made by `new_ref` just above (borrowed by the callback).
                    drop(unsafe { Box::from_raw(p) });
                });
            }
        };
        n.node.on_connect(make(STINTMATE_IPC_PEER_CONNECT));
        n.node.on_disconnect(make(STINTMATE_IPC_PEER_DISCONNECT));
        n.node.on_redeclare(make(STINTMATE_IPC_PEER_REDECLARE));
        STINTMATE_IPC_OK
    })
}

// ------------------------------------------------------------------------ requests

/// `{id, method, version, idempotency_key?, remaining_ms?}` of a request.
///
/// # Safety
/// `req` valid and not yet answered; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_request_info(
    req: *const stintmate_ipc_request,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let Some(r) = (unsafe { req.as_ref() }) else {
            return invalid("request is null");
        };
        let c = &r.0.ctx;
        let mut pairs = vec![
            ("id", Value::from(c.id())),
            ("method", Value::from(c.method())),
            ("version", Value::from(c.version())),
        ];
        if let Some(k) = c.idempotency_key() {
            pairs.push(("idempotency_key", Value::from(k)));
        }
        if let Some(rem) = c.remaining() {
            pairs.push(("remaining_ms", Value::from(rem.as_millis() as u64)));
        }
        // SAFETY: `out` per the contract.
        unsafe { put_value(out, &codec::map(pairs)) }
    })
}

/// An owned reference to the connection the request arrived on (null on a bad request).
/// Release it with `stintmate_ipc_peer_free`.
///
/// # Safety
/// `req` valid and not yet answered.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_request_peer(
    req: *const stintmate_ipc_request,
) -> *mut stintmate_ipc_peer {
    crate::run(std::ptr::null_mut(), || {
        // SAFETY: the contract of this function.
        match unsafe { req.as_ref() } {
            Some(r) => stintmate_ipc_peer::new_ref(blocking::Peer::from(r.0.ctx.peer().clone())),
            None => std::ptr::null_mut(),
        }
    })
}

/// 1 when the caller sent `cancel` or went away (stop and answer anything), else 0.
///
/// # Safety
/// `req` valid and not yet answered.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_request_is_cancelled(
    req: *const stintmate_ipc_request,
) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        unsafe { req.as_ref() }.map_or(0, |r| i32::from(r.0.ctx.is_cancelled()))
    })
}

/// Waits until the request is cancelled (`STINTMATE_IPC_OK`) or `timeout_ms` passes
/// (`STINTMATE_IPC_E_WAIT_TIMEOUT`). Call it from a thread that is not a library thread's
/// lane unless the wait is short.
///
/// # Safety
/// `req` valid and not yet answered.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_request_wait_cancelled(
    req: *const stintmate_ipc_request,
    timeout_ms: i64,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(r) = (unsafe { req.as_ref() }) else {
            return invalid("request is null");
        };
        let c = &r.0.ctx;
        let cancelled = match ms_duration(timeout_ms) {
            Some(t) if t.is_zero() => c.is_cancelled(),
            Some(t) => c.wait_cancelled(t),
            None => loop {
                if c.wait_cancelled(Duration::from_secs(1)) {
                    break true;
                }
                if crate::runtime::is_shutdown() {
                    break false;
                }
            },
        };
        if cancelled {
            STINTMATE_IPC_OK
        } else {
            record_error(STINTMATE_IPC_E_WAIT_TIMEOUT, "the wait timed out");
            STINTMATE_IPC_E_WAIT_TIMEOUT
        }
    })
}

/// Milliseconds left before the caller's deadline (0 when passed), or -1 when there is none.
///
/// # Safety
/// `req` valid and not yet answered.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_request_remaining_ms(
    req: *const stintmate_ipc_request,
) -> i64 {
    crate::run(-1, || {
        // SAFETY: the contract of this function.
        unsafe { req.as_ref() }
            .and_then(|r| r.0.ctx.remaining())
            .map_or(-1, |d| d.as_millis() as i64)
    })
}

/// Tells the caller the request is alive: `data` is a report (MessagePack, length 0: none),
/// `extend_ms` gives the caller a new deadline that far from now (-1: no).
///
/// # Safety
/// `req` valid and not yet answered; `data` valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_request_progress(
    req: *mut stintmate_ipc_request,
    data: *const u8,
    len: usize,
    extend_ms: i64,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(r) = (unsafe { req.as_ref() }) else {
            return invalid("request is null");
        };
        // SAFETY: the contract of this function.
        let d = match unsafe { value_arg(data, len, "data") } {
            Ok(v) => (!v.is_nil()).then_some(v),
            Err(s) => return s,
        };
        r.0.ctx
            .progress(d, ms_duration(extend_ms).filter(|d| !d.is_zero()));
        STINTMATE_IPC_OK
    })
}

/// Consumes the request and delivers `outcome`.
///
/// # Safety
/// `req` from a handler callback, answered once.
unsafe fn answer(req: *mut stintmate_ipc_request, outcome: Result<Value, RemoteError>) -> i32 {
    if req.is_null() {
        return invalid("request is null");
    }
    // SAFETY: `Arc::into_raw` made it in the handler wrapper; this consumes that reference.
    let r = unsafe { Arc::from_raw(req.cast_const()) };
    // The handler may be gone (deadline, cancel): then nobody is waiting, which is fine.
    let _ = r.0.tx.send(outcome);
    STINTMATE_IPC_OK
}

/// After shutdown nobody waits for an answer: the request is still consumed (so it is not
/// leaked) and the call fails with `STINTMATE_IPC_E_SHUTDOWN`.
///
/// # Safety
/// `req` null or from a handler callback, answered once.
unsafe fn after_shutdown(req: *mut stintmate_ipc_request) -> Option<i32> {
    if !crate::runtime::is_shutdown() {
        return None;
    }
    if !req.is_null() {
        // SAFETY: `Arc::into_raw` made it in the handler wrapper; this consumes that reference.
        drop(unsafe { Arc::from_raw(req.cast_const()) });
    }
    Some(crate::runtime::shutdown_status())
}

/// Answers a call with a result (MessagePack; length 0: nil). Consumes the request.
///
/// # Safety
/// `req` from a handler callback, answered exactly once; `result` valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_respond(
    req: *mut stintmate_ipc_request,
    result: *const u8,
    len: usize,
) -> i32 {
    // SAFETY: the contract of this function.
    if let Some(status) = unsafe { after_shutdown(req) } {
        return status;
    }
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            match value_arg(result, len, "result") {
                Ok(v) => answer(req, Ok(v)),
                Err(s) => {
                    let msg = "the handler's result is not valid MessagePack";
                    answer(req, Err(RemoteError::new("internal", msg)));
                    s
                }
            }
        }
    })
}

/// Answers a call with an error `code` and `message`, optional MessagePack `details` (length
/// 0: none) and a retry hint (`retry_ms` -1: none). Consumes the request.
///
/// # Safety
/// As [`stintmate_ipc_respond`]; strings NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_respond_error(
    req: *mut stintmate_ipc_request,
    code: *const c_char,
    message: *const c_char,
    details: *const u8,
    details_len: usize,
    retry_ms: i64,
) -> i32 {
    // SAFETY: the contract of this function.
    if let Some(status) = unsafe { after_shutdown(req) } {
        return status;
    }
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let code = cstr(code, "code").unwrap_or("internal");
            let message = cstr(message, "message").unwrap_or("");
            let mut e = RemoteError::new(code, message);
            if let Ok(d) = value_arg(details, details_len, "details") {
                e.details = (!d.is_nil()).then_some(d);
            }
            e.retry_ms = (retry_ms >= 0).then_some(retry_ms as u64);
            answer(req, Err(e))
        }
    })
}

// ------------------------------------------------------------------------ channels

enum Kind {
    Event(EventChannel),
    State(StateChannel),
    Stream(StreamChannel),
}

/// A channel offered by a node.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_channel {
    kind: Kind,
    node: Node,
    regs: Regs,
    info: Value,
}

impl stintmate_ipc_channel {
    fn subscribers(&self) -> usize {
        match &self.kind {
            Kind::Event(c) => c.subscriber_count(),
            Kind::State(c) => c.subscriber_count(),
            Kind::Stream(c) => c.subscriber_count(),
        }
    }
}

/// Hands a new channel to the caller, with the info the node declared for it.
///
/// # Safety
/// `out` valid for a write.
unsafe fn channel_out(
    n: &stintmate_ipc_node,
    name: &str,
    ch: Result<Kind, Error>,
    out: *mut *mut stintmate_ipc_channel,
) -> i32 {
    let kind = match ch {
        Ok(k) => k,
        Err(e) => return fail(&e),
    };
    let decls = n.node.declarations();
    let mut pairs = vec![("name", Value::from(name))];
    if let Some(d) = decls.channels.get(name) {
        pairs.push(("kind", Value::from(d.kind.name())));
        pairs.push(("versions", versions_value(&d.versions)));
        if let Some(l) = &d.layout {
            pairs.push(("layout", l.to_value()));
        }
        if let Some(r) = d.rate_hz {
            pairs.push(("rate_hz", Value::F64(r)));
        }
    }
    count_handle();
    let ch = stintmate_ipc_channel {
        kind,
        node: n.node.clone(),
        regs: n.regs.clone(),
        info: codec::map(pairs),
    };
    // SAFETY: `out` is valid for a write.
    unsafe { out.write(Box::into_raw(Box::new(ch))) };
    STINTMATE_IPC_OK
}

/// Declares an event channel on a node.
///
/// # Safety
/// `node` valid; `name` NUL-terminated; `versions` valid for `n_versions` values; `out` valid
/// for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_event_channel(
    node: *mut stintmate_ipc_node,
    name: *const c_char,
    versions: *const u32,
    n_versions: usize,
    out: *mut *mut stintmate_ipc_channel,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        unsafe {
            let n = match node_ref(node) {
                Ok(n) => n,
                Err(s) => return s,
            };
            let name = match cstr(name, "name") {
                Ok(n) => n,
                Err(s) => return s,
            };
            channel_out(
                n,
                name,
                n.node
                    .event_channel(name, &versions_arg(versions, n_versions))
                    .map(Kind::Event),
                out,
            )
        }
    })
}

/// Declares a state channel with an initial value (MessagePack; length 0: nil).
///
/// # Safety
/// As [`stintmate_ipc_event_channel`]; `initial` valid for `initial_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_state_channel(
    node: *mut stintmate_ipc_node,
    name: *const c_char,
    initial: *const u8,
    initial_len: usize,
    versions: *const u32,
    n_versions: usize,
    out: *mut *mut stintmate_ipc_channel,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        unsafe {
            let n = match node_ref(node) {
                Ok(n) => n,
                Err(s) => return s,
            };
            let name = match cstr(name, "name") {
                Ok(n) => n,
                Err(s) => return s,
            };
            let initial = match value_arg(initial, initial_len, "initial") {
                Ok(v) => v,
                Err(s) => return s,
            };
            channel_out(
                n,
                name,
                n.node
                    .state_channel(name, initial, &versions_arg(versions, n_versions))
                    .map(Kind::State),
                out,
            )
        }
    })
}

/// Declares a stream channel. `layout` is the MessagePack form of the layout (see
/// `stintmate_ipc_layout_normalize`). `rate_hz` 0 declares no nominal rate (a positive value is the rate in Hz); `queue` 0 the default ring (one second at `rate_hz`, at least 64 samples; 256 without a rate).
///
/// # Safety
/// As [`stintmate_ipc_event_channel`]; `layout` valid for `layout_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_stream_channel(
    node: *mut stintmate_ipc_node,
    name: *const c_char,
    layout: *const u8,
    layout_len: usize,
    versions: *const u32,
    n_versions: usize,
    rate_hz: f64,
    queue: usize,
    out: *mut *mut stintmate_ipc_channel,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        unsafe {
            let n = match node_ref(node) {
                Ok(n) => n,
                Err(s) => return s,
            };
            let name = match cstr(name, "name") {
                Ok(n) => n,
                Err(s) => return s,
            };
            let layout = match value_arg(layout, layout_len, "layout") {
                Ok(v) => v,
                Err(s) => return s,
            };
            let layout = match Layout::from_value(&layout) {
                Ok(l) => l,
                Err(e) => return invalid(&e.to_string()),
            };
            channel_out(
                n,
                name,
                n.node
                    .stream_channel_with(
                        name,
                        layout,
                        &versions_arg(versions, n_versions),
                        (rate_hz > 0.0).then_some(rate_hz),
                        (queue > 0).then_some(queue),
                    )
                    .map(Kind::Stream),
                out,
            )
        }
    })
}

/// `{name, kind: "event"|"state"|"stream", versions, layout?, rate_hz?}` of the channel.
///
/// # Safety
/// `ch` valid; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_info(
    ch: *mut stintmate_ipc_channel,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { ch.as_ref() }) else {
            return invalid("channel is null");
        };
        // SAFETY: `out` per the contract.
        unsafe { put_value(out, &c.info) }
    })
}

/// The number of subscribers of the channel (negative on a bad handle).
///
/// # Safety
/// `ch` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_subscribers(ch: *mut stintmate_ipc_channel) -> i64 {
    crate::run(i64::from(crate::STINTMATE_IPC_E_PANIC), || {
        // SAFETY: the contract of this function.
        match unsafe { ch.as_ref() } {
            Some(c) => c.subscribers() as i64,
            None => i64::from(invalid("channel is null")),
        }
    })
}

/// 1 when anybody is subscribed, else 0.
///
/// # Safety
/// `ch` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_is_active(ch: *mut stintmate_ipc_channel) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        unsafe { ch.as_ref() }.map_or(0, |c| i32::from(c.subscribers() > 0))
    })
}

/// Ends every subscription (`end` with `code`, default `channel_closed`) and withdraws the channel.
///
/// # Safety
/// `ch` valid; strings NUL-terminated or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_close(
    ch: *mut stintmate_ipc_channel,
    code: *const c_char,
    message: *const c_char,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let Some(c) = ch.as_ref() else {
                return invalid("channel is null");
            };
            let code = match opt_cstr(code, "code") {
                Ok(c) => c.unwrap_or("channel_closed"),
                Err(s) => return s,
            };
            let message = match opt_cstr(message, "message") {
                Ok(m) => m.unwrap_or(""),
                Err(s) => return s,
            };
            match &c.kind {
                Kind::Event(x) => x.close(code, message),
                Kind::State(x) => x.close(code, message),
                Kind::Stream(x) => x.close(code, message),
            }
            STINTMATE_IPC_OK
        }
    })
}

/// Chooses subscriptions for `stintmate_ipc_publish`: runs on the calling thread once per
/// subscription with the view `{peer: {app, name, version, process?, meta?}, pid?, version,
/// options, connection: <peer id>}`; returns 1 to send. It must be quick.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_accept_cb =
    Option<unsafe extern "C-unwind" fn(user_data: *mut c_void, view: *const u8, len: usize) -> i32>;

/// Publishes on an event or state channel. Event channel: `data` is the MessagePack event;
/// `ts_ns` is its timestamp (`STINTMATE_IPC_TS_NOW`: now), `version` limits it to the
/// subscriptions of that channel version (0: every version) and `accept` (nullable) picks
/// subscriptions (it cannot be combined with `ts_ns`). State channel: `data` is the new state
/// (coalesced for slow subscribers); `ts_ns`, `version` and `accept` must be left at their
/// defaults. `queued_out` (nullable) receives how many subscriptions it was queued for.
///
/// # Safety
/// `ch` valid; `data` valid for `len` bytes; `accept` per its type with `user_data`;
/// `queued_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_publish(
    ch: *mut stintmate_ipc_channel,
    data: *const u8,
    len: usize,
    ts_ns: i64,
    version: u32,
    accept: stintmate_ipc_accept_cb,
    user_data: *mut c_void,
    queued_out: *mut usize,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(ch) = (unsafe { ch.as_ref() }) else {
            return invalid("channel is null");
        };
        let ts = (ts_ns != STINTMATE_IPC_TS_NOW).then_some(ts_ns);
        let version = (version != 0).then_some(version);
        // SAFETY: the contract of this function.
        let v = match unsafe { value_arg(data, len, "data") } {
            Ok(v) => v,
            Err(s) => return s,
        };
        let queued = match &ch.kind {
            Kind::Stream(_) => {
                return invalid("publish is for event and state channels: use stream_send");
            }
            Kind::Event(c) => match accept {
                None => c.publish_with(&v, PublishOptions { ts, version }),
                Some(cb) => {
                    if ts.is_some() {
                        return invalid("ts_ns cannot be combined with accept");
                    }
                    c.publish_filtered(&v, |view| {
                        if version.is_some_and(|x| view.version() != x) {
                            return false;
                        }
                        let mut pairs = vec![
                            ("peer", view.peer_info().to_value()),
                            ("version", Value::from(u64::from(view.version()))),
                            ("options", view.options().clone()),
                            ("connection", Value::from(view.connection().id())),
                        ];
                        if let Some(pid) = view.pid() {
                            pairs.push(("pid", Value::from(u64::from(pid))));
                        }
                        let Ok(bytes) = codec::encode(&codec::map(pairs)) else {
                            return false;
                        };
                        // SAFETY: the embedder's callback; `bytes` outlives the call.
                        unsafe { cb(user_data, bytes.as_ptr(), bytes.len()) == 1 }
                    })
                }
            },
            Kind::State(c) => {
                if accept.is_some() || version.is_some() || ts.is_some() {
                    return invalid("a state channel takes no ts_ns, version or accept");
                }
                c.set(&v).map(|()| c.subscriber_count())
            }
        };
        match queued {
            Ok(n) => {
                if !queued_out.is_null() {
                    // SAFETY: valid for a write per the contract.
                    unsafe { queued_out.write(n) };
                }
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&e),
        }
    })
}

/// The current value of a state channel (MessagePack).
///
/// # Safety
/// `ch` valid; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_state_get(
    ch: *mut stintmate_ipc_channel,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        match unsafe { ch.as_ref() }.map(|c| &c.kind) {
            Some(Kind::State(c)) => {
                // SAFETY: `out` per the contract.
                unsafe { put_value(out, &c.value()) }
            }
            Some(_) => invalid("not a state channel"),
            None => invalid("channel is null"),
        }
    })
}

/// Sends one sample on a stream channel: `body` is the packed layout body (exactly its size);
/// `t_ns` when it was taken (`STINTMATE_IPC_TS_NOW`: now). `sent_out` (nullable) receives how
/// many subscriptions it was queued for.
///
/// # Safety
/// `ch` valid; `body` valid for `len` bytes; `sent_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_stream_send(
    ch: *mut stintmate_ipc_channel,
    body: *const u8,
    len: usize,
    t_ns: i64,
    sent_out: *mut usize,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { ch.as_ref() }) else {
            return invalid("channel is null");
        };
        let Kind::Stream(s) = &c.kind else {
            return invalid("not a stream channel");
        };
        if body.is_null() && len > 0 {
            return invalid("body is null");
        }
        let bytes = if len == 0 {
            &[][..]
        } else {
            // SAFETY: valid for `len` bytes per the contract.
            unsafe { std::slice::from_raw_parts(body, len) }
        };
        match s.send(bytes, (t_ns != STINTMATE_IPC_TS_NOW).then_some(t_ns)) {
            Ok(n) => {
                if !sent_out.is_null() {
                    // SAFETY: valid for a write per the contract.
                    unsafe { sent_out.write(n) };
                }
                STINTMATE_IPC_OK
            }
            Err(e) => invalid(&e.to_string()),
        }
    })
}

/// The context of a subscribe hook, used to refuse.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_hook_ctx {
    refusal: Mutex<Option<RemoteError>>,
}

/// Runs before a subscription is accepted, on the handler pool, with
/// `{peer: <peer id>, info, version, options}`. Return 0 to accept; to refuse call
/// `stintmate_ipc_refuse` and return nonzero. `peer` is borrowed for the call.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_subscribe_cb = Option<
    unsafe extern "C-unwind" fn(
        user_data: *mut c_void,
        ctx: *mut stintmate_ipc_hook_ctx,
        peer: *mut stintmate_ipc_peer,
        req: *const u8,
        len: usize,
    ) -> i32,
>;

/// Sets the refusal a subscribe hook returns (the hook then returns nonzero).
///
/// # Safety
/// `ctx` from a subscribe hook, used during it; strings NUL-terminated; `details` valid for
/// `details_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_refuse(
    ctx: *mut stintmate_ipc_hook_ctx,
    code: *const c_char,
    message: *const c_char,
    details: *const u8,
    details_len: usize,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let Some(c) = ctx.as_ref() else {
                return invalid("ctx is null");
            };
            let code = match cstr(code, "code") {
                Ok(c) => c,
                Err(s) => return s,
            };
            let message = opt_cstr(message, "message").ok().flatten().unwrap_or("");
            let mut e = RemoteError::new(code, message);
            match value_arg(details, details_len, "details") {
                Ok(d) => e.details = (!d.is_nil()).then_some(d),
                Err(s) => return s,
            }
            *c.refusal.lock().unwrap_or_else(|p| p.into_inner()) = Some(e);
            STINTMATE_IPC_OK
        }
    })
}

/// Sets the hook that runs before a subscription to the channel is accepted. Replaces the
/// previous hook (releasing its `user_data`); a null `cb` accepts everyone.
///
/// # Safety
/// `ch` valid; `user_data` usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_on_subscribe(
    ch: *mut stintmate_ipc_channel,
    cb: stintmate_ipc_subscribe_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { ch.as_ref() }) else {
            return invalid("channel is null");
        };
        let cell = c.regs.add(Registration::new(user_data, release));
        let hook = move |req: SubscribeRequest| -> Result<(), RemoteError> {
            let Some(cb) = cb else { return Ok(()) };
            let Some(_active) = enter() else {
                return Err(RemoteError::new("unavailable", "the library was shut down"));
            };
            let Some(user) = cell.get() else {
                return Err(RemoteError::new("unavailable", "the node was freed"));
            };
            let peer = blocking::Peer::from(req.peer);
            let view = codec::map([
                ("peer", Value::from(peer.inner().id())),
                ("info", peer.info().to_value()),
                ("version", Value::from(u64::from(req.version))),
                ("options", req.options),
            ]);
            let bytes =
                codec::encode(&view).map_err(|e| RemoteError::new("internal", e.to_string()))?;
            let ctx = stintmate_ipc_hook_ctx {
                refusal: Mutex::new(None),
            };
            let p = stintmate_ipc_peer::new_ref(peer);
            // SAFETY: the embedder's callback; `ctx`, `p` and `bytes` outlive the call.
            let rc = unsafe {
                cb(
                    user.user_data(),
                    std::ptr::from_ref(&ctx).cast_mut(),
                    p,
                    bytes.as_ptr(),
                    bytes.len(),
                )
            };
            // SAFETY: made by `new_ref` just above (borrowed by the callback).
            drop(unsafe { Box::from_raw(p) });
            if rc == 0 {
                return Ok(());
            }
            let refusal = ctx.refusal.lock().unwrap_or_else(|p| p.into_inner()).take();
            Err(refusal
                .unwrap_or_else(|| RemoteError::new("refused", "the subscription was refused")))
        };
        match &c.kind {
            Kind::Event(x) => c.node.on_subscribe(x, hook),
            Kind::State(x) => c.node.on_subscribe(x, hook),
            Kind::Stream(x) => c.node.on_subscribe(x, hook),
        }
        STINTMATE_IPC_OK
    })
}

/// Called with 1 when the first subscriber comes and 0 when the last one leaves.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_active_cb =
    Option<unsafe extern "C-unwind" fn(user_data: *mut c_void, active: i32)>;

/// Sets the hook called, on the hooks thread and in order, when the channel gains its first
/// subscriber and loses its last. Replaces the previous hook (releasing its `user_data`).
///
/// # Safety
/// `ch` valid; `user_data` usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_on_active(
    ch: *mut stintmate_ipc_channel,
    cb: stintmate_ipc_active_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { ch.as_ref() }) else {
            return invalid("channel is null");
        };
        let cell = c.regs.add(Registration::new(user_data, release));
        let hook = move |active: bool| {
            let Some(cb) = cb else { return };
            if cell.get().is_none() {
                return;
            }
            let cell = cell.clone();
            post_hook(move || {
                // Checked again here: a free that ran meanwhile stops the callbacks.
                let Some(user) = cell.get() else { return };
                // SAFETY: the embedder's callback.
                unsafe { cb(user.user_data(), i32::from(active)) };
            });
        };
        match &c.kind {
            Kind::Event(x) => c.node.on_active(x, hook),
            Kind::State(x) => c.node.on_active(x, hook),
            Kind::Stream(x) => c.node.on_active(x, hook),
        }
        STINTMATE_IPC_OK
    })
}

/// Releases the channel handle (the channel itself lives as long as its node).
///
/// # Safety
/// `ch` from a channel constructor, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_free(ch: *mut stintmate_ipc_channel) {
    crate::run((), || {
        if ch.is_null() {
            return;
        }
        // SAFETY: made by `Box::into_raw` in a channel constructor.
        drop(unsafe { Box::from_raw(ch) });
        uncount_handle();
    })
}

/// The client's node, to offer methods and channels on to the endpoint it connects to (offer
/// before `stintmate_ipc_client_start` so the first handshake carries them). Borrowed: valid
/// until `stintmate_ipc_client_free`, never freed by the caller (null on a null client).
///
/// # Safety
/// `client` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_node(
    client: *mut crate::client::stintmate_ipc_client,
) -> *mut stintmate_ipc_node {
    crate::run(std::ptr::null_mut(), || {
        // SAFETY: the contract of this function.
        match unsafe { client.as_ref() } {
            Some(c) => std::ptr::from_ref(&c.3).cast_mut(),
            None => std::ptr::null_mut(),
        }
    })
}
