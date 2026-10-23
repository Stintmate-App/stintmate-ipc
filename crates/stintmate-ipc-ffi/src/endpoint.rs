//! The server side of the C ABI: endpoint, methods with responders, channels, publish.

use std::ffi::{c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{RecvTimeoutError, Sender, bounded};
use stintmate_ipc::MethodOpts;
use stintmate_ipc::blocking::{
    Context, Endpoint, EventChannel, PublishOptions, StateChannel, StreamChannel,
};
use stintmate_ipc::codec::Value;
use stintmate_ipc::error::{Error, RemoteError};
use stintmate_ipc::layout::Layout;

use crate::client::{key_arg, peer_info_arg, stintmate_ipc_peer_info};
use crate::{
    STINTMATE_IPC_E_INTERNAL, STINTMATE_IPC_E_INVALID_ARG, STINTMATE_IPC_OK, UserData, cstr, fail,
    guard, invalid, opt_cstr, put_buffer, set_error, value_arg, versions_arg,
};

/// `STINTMATE_IPC_METHOD_ORDERED`: one call at a time per connection, in arrival order.
pub const STINTMATE_IPC_METHOD_ORDERED: u32 = 1;
/// `STINTMATE_IPC_METHOD_INLINE`: run the handler on the connection's callback lane (only for
/// handlers that answer from memory in microseconds).
pub const STINTMATE_IPC_METHOD_INLINE: u32 = 2;
/// `STINTMATE_IPC_METHOD_IDEMPOTENT`: declares the method idempotent (answers are kept by
/// idempotency key).
pub const STINTMATE_IPC_METHOD_IDEMPOTENT: u32 = 4;

/// The server of one endpoint.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_endpoint(Endpoint);

/// A channel offered by an endpoint.
#[allow(non_camel_case_types)]
pub enum stintmate_ipc_channel {
    /// An event channel.
    Event(EventChannel),
    /// A state channel.
    State(StateChannel),
    /// A stream channel.
    Stream(StreamChannel),
}

struct ResponderInner {
    tx: Sender<Result<Value, RemoteError>>,
    ctx: Context,
}

/// The way a method handler answers: exactly one of `stintmate_ipc_respond` or
/// `stintmate_ipc_respond_error` must be called, from the handler or later from any thread.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_responder(ResponderInner);

/// A method handler. It receives the params as MessagePack (valid only during the call) and
/// the responder, and answers through it. The handler's pool thread waits for the answer (up
/// to the caller's deadline), so answer promptly.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_handler_cb = Option<
    unsafe extern "C-unwind" fn(
        user_data: *mut c_void,
        responder: *mut stintmate_ipc_responder,
        params: *const u8,
        params_len: usize,
    ),
>;

/// Creates an endpoint. `pipe` null derives it from the app id (and process); `key_path` null
/// uses the default key file; `handler_threads` 0 is the default (8).
///
/// # Safety
/// Strings NUL-terminated; `info` per `stintmate_ipc_peer_info`; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_new(
    pipe: *const c_char,
    info: *const stintmate_ipc_peer_info,
    key_path: *const c_char,
    handler_threads: u32,
    out: *mut *mut stintmate_ipc_endpoint,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let (pipe, info, key) = unsafe {
            let pipe = match opt_cstr(pipe, "pipe") {
                Ok(p) => p,
                Err(s) => return s,
            };
            let info = match peer_info_arg(info) {
                Ok(i) => i,
                Err(s) => return s,
            };
            let key = match key_arg(key_path) {
                Ok(k) => k,
                Err(s) => return s,
            };
            (pipe, info, key)
        };
        let mut b = Endpoint::builder(info, key);
        if let Some(p) = pipe {
            b = b.pipe(p);
        }
        if handler_threads > 0 {
            b = b.handler_threads(handler_threads as usize);
        }
        match b.build() {
            Ok(e) => {
                // SAFETY: `out` is valid for a write.
                unsafe { out.write(Box::into_raw(Box::new(stintmate_ipc_endpoint(e)))) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&e),
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
        let Some(e) = (unsafe { ep.as_ref() }) else {
            return invalid("endpoint is null");
        };
        match e.0.start() {
            Ok(()) => STINTMATE_IPC_OK,
            Err(e) => fail(&e),
        }
    })
}

/// The pipe path of the endpoint, UTF-8 without NUL.
///
/// # Safety
/// `ep` valid; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_path(
    ep: *mut stintmate_ipc_endpoint,
    out: *mut crate::stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(e) = (unsafe { ep.as_ref() }) else {
            return invalid("endpoint is null");
        };
        // SAFETY: `out` per the contract.
        unsafe { put_buffer(out, e.0.path().as_bytes().to_vec()) };
        STINTMATE_IPC_OK
    })
}

/// Stops serving, says `bye` to every peer and ends the handler pool.
///
/// # Safety
/// `ep` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_stop(ep: *mut stintmate_ipc_endpoint) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(e) = (unsafe { ep.as_ref() }) else {
            return invalid("endpoint is null");
        };
        e.0.stop();
        STINTMATE_IPC_OK
    })
}

/// Stops the endpoint (when it still serves) and releases the handle.
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
        e.0.stop();
    }));
}

/// Offers a method. `versions` null/0 offers version 1. The handler runs on the handler pool
/// (or the callback lane with `STINTMATE_IPC_METHOD_INLINE`), never on a runtime thread; a
/// panic that unwinds out of it answers `internal`.
///
/// # Safety
/// `ep` valid; `name` NUL-terminated; `versions` valid for `n_versions` values;
/// `user_data` usable from library threads while the method is offered.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_method(
    ep: *mut stintmate_ipc_endpoint,
    name: *const c_char,
    flags: u32,
    versions: *const u32,
    n_versions: usize,
    handler: stintmate_ipc_handler_cb,
    user_data: *mut c_void,
) -> i32 {
    guard(|| {
        let Some(cb) = handler else {
            return invalid("handler is null");
        };
        // SAFETY: the contract of this function.
        let (e, name, versions) = unsafe {
            let Some(e) = ep.as_ref() else {
                return invalid("endpoint is null");
            };
            let name = match cstr(name, "name") {
                Ok(n) => n,
                Err(s) => return s,
            };
            (e, name, versions_arg(versions, n_versions))
        };
        let opts = MethodOpts {
            versions,
            ordered: flags & STINTMATE_IPC_METHOD_ORDERED != 0,
            inline: flags & STINTMATE_IPC_METHOD_INLINE != 0,
            idempotent: flags & STINTMATE_IPC_METHOD_IDEMPOTENT != 0,
            ..MethodOpts::default()
        };
        let user = UserData(user_data);
        let r = e.0.method(name, opts, move |ctx: &Context, params: Value| {
            let bytes = stintmate_ipc::codec::encode(&params)
                .map_err(|e| RemoteError::new("internal", e.to_string()))?;
            let (tx, rx) = bounded(1);
            let inner = Arc::new(stintmate_ipc_responder(ResponderInner {
                tx,
                ctx: ctx.clone(),
            }));
            // The callback owns one reference (released by respond); we keep ours.
            let handle = Arc::into_raw(inner.clone()).cast_mut();
            // SAFETY: the embedder's callback; `bytes` outlives the call.
            unsafe { cb(user.get(), handle, bytes.as_ptr(), bytes.len()) };
            loop {
                match rx.recv_timeout(Duration::from_millis(20)) {
                    Ok(r) => return r,
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(RemoteError::new("internal", "the handler did not answer"));
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if inner.0.ctx.expired() {
                            return Err(RemoteError::new("timeout", "the handler answered late"));
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
/// `ep` valid; `name` NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_endpoint_remove_method(
    ep: *mut stintmate_ipc_endpoint,
    name: *const c_char,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let Some(e) = ep.as_ref() else {
                return invalid("endpoint is null");
            };
            match cstr(name, "name") {
                Ok(n) => {
                    e.0.remove_method(n);
                    STINTMATE_IPC_OK
                }
                Err(s) => s,
            }
        }
    })
}

/// 1 when the caller of this request sent `cancel` or went away (stop and answer anything).
///
/// # Safety
/// `responder` valid and not yet used to answer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_responder_is_cancelled(
    responder: *const stintmate_ipc_responder,
) -> i32 {
    // SAFETY: the contract of this function.
    unsafe { responder.as_ref() }.map_or(0, |r| i32::from(r.0.ctx.is_cancelled()))
}

/// Consumes the handle and delivers `outcome`.
///
/// # Safety
/// `responder` from a handler callback, answered once.
unsafe fn answer(
    responder: *mut stintmate_ipc_responder,
    outcome: Result<Value, RemoteError>,
) -> i32 {
    if responder.is_null() {
        return invalid("responder is null");
    }
    // SAFETY: `Arc::into_raw` made it in the handler wrapper; this consumes that reference.
    let r = unsafe { Arc::from_raw(responder.cast_const()) };
    // The handler may be gone (deadline, cancel): then nobody is waiting, which is fine.
    let _ = r.0.tx.send(outcome);
    STINTMATE_IPC_OK
}

/// Answers a call with a result (MessagePack; length 0: nil). Consumes the responder.
///
/// # Safety
/// `responder` from a handler callback, answered exactly once; `result` valid for
/// `result_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_respond(
    responder: *mut stintmate_ipc_responder,
    result: *const u8,
    result_len: usize,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            match value_arg(result, result_len, "result") {
                Ok(v) => answer(responder, Ok(v)),
                Err(s) => {
                    let msg = "the handler's result is not valid MessagePack";
                    answer(responder, Err(RemoteError::new("internal", msg)));
                    s
                }
            }
        }
    })
}

/// Answers a call with an error `code` and `message`, and optional MessagePack `details`
/// (length 0: none). Consumes the responder.
///
/// # Safety
/// As [`stintmate_ipc_respond`]; strings NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_respond_error(
    responder: *mut stintmate_ipc_responder,
    code: *const c_char,
    message: *const c_char,
    details: *const u8,
    details_len: usize,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let code = cstr(code, "code").unwrap_or("internal");
            let message = cstr(message, "message").unwrap_or("");
            let mut e = RemoteError::new(code, message);
            if let Ok(d) = value_arg(details, details_len, "details") {
                e.details = (!d.is_nil()).then_some(d);
            }
            answer(responder, Err(e))
        }
    })
}

// ------------------------------------------------------------------------ channels

unsafe fn channel_out(
    ch: Result<stintmate_ipc_channel, Error>,
    out: *mut *mut stintmate_ipc_channel,
) -> i32 {
    match ch {
        Ok(c) => {
            // SAFETY: `out` is valid for a write.
            unsafe { out.write(Box::into_raw(Box::new(c))) };
            STINTMATE_IPC_OK
        }
        Err(e) => fail(&e),
    }
}

/// Declares an event channel.
///
/// # Safety
/// `ep` valid; `name` NUL-terminated; `versions` valid for `n_versions` values; `out` valid
/// for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_event_channel(
    ep: *mut stintmate_ipc_endpoint,
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
            let Some(e) = ep.as_ref() else {
                return invalid("endpoint is null");
            };
            let name = match cstr(name, "name") {
                Ok(n) => n,
                Err(s) => return s,
            };
            channel_out(
                e.0.event_channel(name, &versions_arg(versions, n_versions))
                    .map(stintmate_ipc_channel::Event),
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
    ep: *mut stintmate_ipc_endpoint,
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
            let Some(e) = ep.as_ref() else {
                return invalid("endpoint is null");
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
                e.0.state_channel(name, initial, &versions_arg(versions, n_versions))
                    .map(stintmate_ipc_channel::State),
                out,
            )
        }
    })
}

/// Declares a stream channel. `layout` is the MessagePack form of the layout (the `layout`
/// map of a stream declaration). `rate_hz` 0 means unlimited; `queue` 0 the default ring.
///
/// # Safety
/// As [`stintmate_ipc_event_channel`]; `layout` valid for `layout_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_stream_channel(
    ep: *mut stintmate_ipc_endpoint,
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
            let Some(e) = ep.as_ref() else {
                return invalid("endpoint is null");
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
                Err(e) => {
                    set_error(e.to_string());
                    return STINTMATE_IPC_E_INVALID_ARG;
                }
            };
            channel_out(
                e.0.stream_channel_with(
                    name,
                    layout,
                    &versions_arg(versions, n_versions),
                    (rate_hz > 0.0).then_some(rate_hz),
                    (queue > 0).then_some(queue),
                )
                .map(stintmate_ipc_channel::Stream),
                out,
            )
        }
    })
}

/// `ts_ns` value of `stintmate_ipc_publish` meaning "now".
pub const STINTMATE_IPC_TS_NOW: i64 = i64::MIN;

/// Publishes on a channel. Event channel: `data` is the MessagePack event. State channel:
/// `data` is the MessagePack new state (coalesced for slow subscribers). Stream channel:
/// `data` is the raw packed body of the layout. `ts_ns` is the source timestamp
/// (`STINTMATE_IPC_TS_NOW`: now; ignored by state channels).
///
/// # Safety
/// `ch` valid; `data` valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_publish(
    ch: *mut stintmate_ipc_channel,
    data: *const u8,
    len: usize,
    ts_ns: i64,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(ch) = (unsafe { ch.as_ref() }) else {
            return invalid("channel is null");
        };
        let ts = (ts_ns != STINTMATE_IPC_TS_NOW).then_some(ts_ns);
        // SAFETY: the contract of this function.
        unsafe {
            match ch {
                stintmate_ipc_channel::Stream(s) => {
                    if data.is_null() && len > 0 {
                        return invalid("data is null");
                    }
                    let body = if len == 0 {
                        &[][..]
                    } else {
                        std::slice::from_raw_parts(data, len)
                    };
                    match s.send(body, ts) {
                        Ok(_) => STINTMATE_IPC_OK,
                        Err(e) => {
                            set_error(e.to_string());
                            STINTMATE_IPC_E_INVALID_ARG
                        }
                    }
                }
                stintmate_ipc_channel::Event(c) => match value_arg(data, len, "data") {
                    Ok(v) => match c.publish_with(&v, PublishOptions { ts, version: None }) {
                        Ok(_) => STINTMATE_IPC_OK,
                        Err(e) => fail(&e),
                    },
                    Err(s) => s,
                },
                stintmate_ipc_channel::State(c) => match value_arg(data, len, "data") {
                    Ok(v) => match c.set(&v) {
                        Ok(()) => STINTMATE_IPC_OK,
                        Err(e) => fail(&e),
                    },
                    Err(s) => s,
                },
            }
        }
    })
}

/// The number of subscribers of the channel (negative on a bad handle).
///
/// # Safety
/// `ch` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_subscribers(ch: *mut stintmate_ipc_channel) -> i64 {
    // SAFETY: the contract of this function.
    match unsafe { ch.as_ref() } {
        Some(stintmate_ipc_channel::Event(c)) => c.subscriber_count() as i64,
        Some(stintmate_ipc_channel::State(c)) => c.subscriber_count() as i64,
        Some(stintmate_ipc_channel::Stream(c)) => c.subscriber_count() as i64,
        None => i64::from(STINTMATE_IPC_E_INTERNAL),
    }
}

/// Releases the channel handle (the channel itself lives as long as the endpoint).
///
/// # Safety
/// `ch` from a channel constructor, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_channel_free(ch: *mut stintmate_ipc_channel) {
    if ch.is_null() {
        return;
    }
    // SAFETY: made by `Box::into_raw` in a channel constructor.
    drop(unsafe { Box::from_raw(ch) });
}
