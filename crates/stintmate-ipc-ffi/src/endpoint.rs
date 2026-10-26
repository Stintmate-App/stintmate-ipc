//! The server side of the C ABI: the endpoint (spec 2.7, endpoint half). What it offers
//! (methods, channels, hooks) is on its node (`node.rs`).

use std::ffi::{c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::time::Duration;

use stintmate_ipc::blocking::{Endpoint, Node};
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::Error;
use stintmate_ipc::handshake::PeerInfo;

use crate::client::{peer_info_arg, stintmate_ipc_peer_info};
use crate::keys::{key_source_arg, stintmate_ipc_key_source};
use crate::node::{error_map, read_field, stintmate_ipc_node};
use crate::runtime::{Handle, Registration, post_hook, stintmate_ipc_release_cb};
use crate::{
    STINTMATE_IPC_OK, fail, guard, invalid, opt_cstr, put_value, stintmate_ipc_buffer, value_arg,
};

/// The server of one endpoint.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_endpoint {
    ep: Endpoint,
    node: stintmate_ipc_node,
    info: PeerInfo,
    registry: Option<String>,
    _handle: Handle,
}

/// Options of an endpoint (spec 2.7); `struct_size` is `sizeof`. Zero (or null) fields mean the
/// default.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_endpoint_opts {
    /// `sizeof` this struct.
    pub struct_size: usize,
    /// A full path or short pipe name; null: derived from the app id.
    pub pipe: *const c_char,
    /// The registry folder the endpoint writes its descriptor to (null: none).
    pub registry: *const c_char,
    /// How long a client has to finish the handshake; -1 (or 0): the default.
    pub handshake_timeout_ms: i64,
    /// The handler pool size; 0: 8.
    pub handler_threads: u32,
    /// The heartbeat period in milliseconds; 0: the default.
    pub heartbeat_ms: u32,
    /// Events queued per subscriber; 0: 1024.
    pub event_queue: usize,
    /// Bytes queued per subscriber; 0: 8 MiB.
    pub event_queue_bytes: usize,
    /// Requests in flight per connection; 0: the default.
    pub max_inflight: usize,
    /// A MessagePack array of feature names to offer (null: none).
    pub features: *const u8,
    /// The length of `features`.
    pub features_len: usize,
}

/// The options of the endpoint, as owned values.
struct Opts {
    pipe: Option<String>,
    registry: Option<String>,
    handshake_timeout: Option<Duration>,
    handler_threads: u32,
    heartbeat_ms: u32,
    event_queue: usize,
    event_queue_bytes: usize,
    max_inflight: usize,
    features: Vec<String>,
}

/// Reads the options (null options: all defaults).
///
/// # Safety
/// `opts` null or per `stintmate_ipc_endpoint_opts`.
unsafe fn opts_arg(opts: *const stintmate_ipc_endpoint_opts) -> Result<Opts, i32> {
    use std::mem::offset_of as off;
    let mut o = Opts {
        pipe: None,
        registry: None,
        handshake_timeout: None,
        handler_threads: 0,
        heartbeat_ms: 0,
        event_queue: 0,
        event_queue_bytes: 0,
        max_inflight: 0,
        features: Vec::new(),
    };
    if opts.is_null() {
        return Ok(o);
    }
    // SAFETY: the first field is readable per the contract.
    let size = unsafe { opts.cast::<usize>().read_unaligned() };
    let base = opts.cast::<u8>();
    // SAFETY: each field is read only when it lies inside `struct_size`.
    unsafe {
        let none = std::ptr::null::<c_char>();
        let pipe = read_field(base, size, off!(stintmate_ipc_endpoint_opts, pipe), none);
        let registry = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, registry),
            none,
        );
        let hs = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, handshake_timeout_ms),
            -1i64,
        );
        o.handler_threads = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, handler_threads),
            0u32,
        );
        o.heartbeat_ms = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, heartbeat_ms),
            0u32,
        );
        o.event_queue = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, event_queue),
            0usize,
        );
        o.event_queue_bytes = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, event_queue_bytes),
            0usize,
        );
        o.max_inflight = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, max_inflight),
            0usize,
        );
        let features = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, features),
            std::ptr::null::<u8>(),
        );
        let features_len = read_field(
            base,
            size,
            off!(stintmate_ipc_endpoint_opts, features_len),
            0usize,
        );
        o.pipe = opt_cstr(pipe, "opts.pipe")?.map(str::to_owned);
        o.registry = opt_cstr(registry, "opts.registry")?.map(str::to_owned);
        if hs > 0 {
            o.handshake_timeout = Some(Duration::from_millis(hs as u64));
        }
        if features_len > 0 {
            let v = value_arg(features, features_len, "opts.features")?;
            let Value::Array(items) = v else {
                return Err(invalid("opts.features is not an array of strings"));
            };
            for i in items {
                let Some(s) = i.as_str() else {
                    return Err(invalid("opts.features is not an array of strings"));
                };
                o.features.push(s.to_owned());
            }
        }
    }
    Ok(o)
}

/// Creates an endpoint. `key` null uses the default key file; `opts` may be null (the pipe is
/// derived from the app id and process; defaults for everything else).
///
/// # Safety
/// `info`, `key` and `opts` per their structs; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_new(
    info: *const stintmate_ipc_peer_info,
    key: *const stintmate_ipc_key_source,
    opts: *const stintmate_ipc_endpoint_opts,
    out: *mut *mut stintmate_ipc_endpoint,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let (o, info, key) = unsafe {
            let o = match opts_arg(opts) {
                Ok(o) => o,
                Err(s) => return s,
            };
            let info = match peer_info_arg(info) {
                Ok(i) => i,
                Err(s) => return s,
            };
            let key = match key_source_arg(key) {
                Ok(k) => k,
                Err(s) => return s,
            };
            (o, info, key)
        };
        let mut b = Endpoint::builder(info.clone(), key);
        if let Some(p) = &o.pipe {
            b = b.pipe(p.as_str());
        }
        if let Some(r) = &o.registry {
            b = b.registry(PathBuf::from(r));
        }
        if let Some(t) = o.handshake_timeout {
            b = b.handshake_timeout(t);
        }
        if o.handler_threads > 0 {
            b = b.handler_threads(o.handler_threads as usize);
        }
        if o.heartbeat_ms > 0 {
            b = b.heartbeat_ms(o.heartbeat_ms);
        }
        if o.event_queue > 0 || o.event_queue_bytes > 0 {
            let events = if o.event_queue > 0 {
                o.event_queue
            } else {
                1024
            };
            let bytes = if o.event_queue_bytes > 0 {
                o.event_queue_bytes
            } else {
                8 << 20
            };
            b = b.event_queue(events, bytes);
        }
        if o.max_inflight > 0 {
            b = b.max_inflight(o.max_inflight);
        }
        if !o.features.is_empty() {
            b = b.features(o.features);
        }
        match b.build() {
            Ok(e) => {
                let closer = e.clone();
                let handle = Handle::with_closer(move || closer.stop());
                let node = stintmate_ipc_node::new(Node::clone(&e));
                let ep = stintmate_ipc_endpoint {
                    ep: e,
                    node,
                    info,
                    registry: o.registry,
                    _handle: handle,
                };
                // SAFETY: `out` is valid for a write.
                unsafe { out.write(Box::into_raw(Box::new(ep))) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&e),
        }
    })
}

unsafe fn ep_ref<'a>(ep: *mut stintmate_ipc_endpoint) -> Result<&'a stintmate_ipc_endpoint, i32> {
    // SAFETY: the contract of the callers.
    unsafe { ep.as_ref() }.ok_or_else(|| invalid("endpoint is null"))
}

/// The endpoint's node, to offer methods and channels on. Borrowed: valid until
/// `stintmate_ipc_endpoint_free`, never freed by the caller (null on a null endpoint).
///
/// # Safety
/// `ep` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_node(
    ep: *mut stintmate_ipc_endpoint,
) -> *mut stintmate_ipc_node {
    crate::run(std::ptr::null_mut(), || {
        // SAFETY: the contract of this function.
        match unsafe { ep.as_ref() } {
            Some(e) => std::ptr::from_ref(&e.node).cast_mut(),
            None => std::ptr::null_mut(),
        }
    })
}

/// Creates the pipe and starts serving. Declare methods and channels before or after.
///
/// # Safety
/// `ep` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_start(ep: *mut stintmate_ipc_endpoint) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let e = match unsafe { ep_ref(ep) } {
            Ok(e) => e,
            Err(s) => return s,
        };
        match e.ep.start() {
            Ok(()) => STINTMATE_IPC_OK,
            Err(e) => fail(&e),
        }
    })
}

/// `{path, short_name, run, registry?, info: {app, name, version, process?, meta?}}`.
///
/// # Safety
/// `ep` valid; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_info(
    ep: *mut stintmate_ipc_endpoint,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let e = match unsafe { ep_ref(ep) } {
            Ok(e) => e,
            Err(s) => return s,
        };
        let mut pairs = vec![
            ("path", Value::from(e.ep.path())),
            ("short_name", Value::from(e.ep.short_name())),
            ("run", Value::from(e.ep.run_id())),
        ];
        if let Some(r) = &e.registry {
            pairs.push(("registry", Value::from(r.as_str())));
        }
        pairs.push(("info", e.info.to_value()));
        // SAFETY: `out` per the contract.
        unsafe { put_value(out, &codec::map(pairs)) }
    })
}

/// Stops serving, says `bye` to every peer (with `code`, default `shutting_down`, and
/// `message`) and ends the handler pool. The handle stays valid until
/// `stintmate_ipc_endpoint_free`.
///
/// # Safety
/// `ep` valid; strings NUL-terminated or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_stop(
    ep: *mut stintmate_ipc_endpoint,
    code: *const c_char,
    message: *const c_char,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let e = match ep_ref(ep) {
                Ok(e) => e,
                Err(s) => return s,
            };
            let code = match opt_cstr(code, "code") {
                Ok(c) => c.unwrap_or("shutting_down"),
                Err(s) => return s,
            };
            let message = match opt_cstr(message, "message") {
                Ok(m) => m.unwrap_or("the endpoint stops"),
                Err(s) => return s,
            };
            e.ep.stop_with(code, message);
        }
        STINTMATE_IPC_OK
    })
}

/// Called when a client fails its handshake: `pid` is its process id (-1: unknown), `error`
/// the error map (`{status, message, code?, ...}`, as `stintmate_ipc_last_error`), valid only
/// during the call.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_handshake_failed_cb = Option<
    unsafe extern "C-unwind" fn(user_data: *mut c_void, pid: i64, error: *const u8, len: usize),
>;

/// Sets the hook called, on the hooks thread and in order, when a client fails its handshake.
/// Replaces the previous hook (releasing its `user_data`); a null `cb` clears it.
///
/// # Safety
/// `ep` valid; `user_data` usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_on_handshake_failed(
    ep: *mut stintmate_ipc_endpoint,
    cb: stintmate_ipc_handshake_failed_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let e = match unsafe { ep_ref(ep) } {
            Ok(e) => e,
            Err(s) => return s,
        };
        let cell = e.node.regs.add(Registration::new(user_data, release));
        e.ep.on_handshake_failed(move |pid: Option<u32>, err: &Error| {
            let Some(cb) = cb else { return };
            if cell.get().is_none() {
                return;
            }
            let cell = cell.clone();
            let Ok(bytes) = codec::encode(&error_map(err)) else {
                return;
            };
            post_hook(move || {
                // Checked again here: a free that ran meanwhile stops the callbacks.
                let Some(user) = cell.get() else { return };
                // SAFETY: the embedder's callback; `bytes` outlives the call.
                unsafe {
                    cb(
                        user.user_data(),
                        pid.map_or(-1, i64::from),
                        bytes.as_ptr(),
                        bytes.len(),
                    );
                }
            });
        });
        STINTMATE_IPC_OK
    })
}

/// Stops the endpoint (when it still serves) and releases the handle (and its node).
///
/// # Safety
/// `ep` from `stintmate_ipc_endpoint_new`, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_free(ep: *mut stintmate_ipc_endpoint) {
    if ep.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: made by `Box::into_raw` in `stintmate_ipc_endpoint_new`.
        let e = unsafe { Box::from_raw(ep) };
        e.ep.stop();
    }));
}
