//! The client side of the C ABI: connect, call, call_async, subscribe.

use std::ffi::{c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use stintmate_ipc::blocking::{Client, PendingCall};
use stintmate_ipc::codec::Value;
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::error::Error;
use stintmate_ipc::handshake::PeerInfo;
use stintmate_ipc::{Delivery, SubscribeOptions, SubscriptionHandle};

use crate::keys::{key_source_arg, stintmate_ipc_key_source};
use crate::runtime::{Handle, Registration, enter, stintmate_ipc_release_cb};
use crate::{
    STINTMATE_IPC_E_INTERNAL, STINTMATE_IPC_E_WAIT_TIMEOUT, STINTMATE_IPC_OK, cstr, fail, guard,
    invalid, opt_cstr, put_value, stintmate_ipc_buffer, value_arg,
};

/// Who this process is, announced in the handshake.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_peer_info {
    /// The app id (`pitwall`, `voice`, `connector-<id>`); required.
    pub app: *const c_char,
    /// A display name (null: the app id).
    pub name: *const c_char,
    /// The program's version (null: empty).
    pub version: *const c_char,
    /// Which process of the app (null: none).
    pub process: *const c_char,
    /// A MessagePack map of app-specific facts (null or `meta_len` 0: none).
    pub meta: *const u8,
    /// The length of `meta`.
    pub meta_len: usize,
}

/// Reads a peer info.
///
/// # Safety
/// `info` must be null or point to a valid struct whose pointers follow the field docs.
pub(crate) unsafe fn peer_info_arg(info: *const stintmate_ipc_peer_info) -> Result<PeerInfo, i32> {
    if info.is_null() {
        return Err(invalid("info is null"));
    }
    // SAFETY: valid per the caller's contract.
    let i = unsafe { &*info };
    // SAFETY: the strings follow the field docs.
    unsafe {
        let app = cstr(i.app, "info.app")?;
        let mut p = PeerInfo::new(app);
        p.name = opt_cstr(i.name, "info.name")?.unwrap_or(app).to_owned();
        p.version = opt_cstr(i.version, "info.version")?
            .unwrap_or("")
            .to_owned();
        p.process = opt_cstr(i.process, "info.process")?.map(str::to_owned);
        if i.meta_len > 0 {
            p.meta = Some(value_arg(i.meta, i.meta_len, "info.meta")?);
        }
        Ok(p)
    }
}

/// A client that stays connected (reconnecting every few seconds).
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_client(Client, #[allow(dead_code)] Handle);

/// Options of a client (spec 2.7); `struct_size` is `sizeof`. T3 applies them.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_client_opts {
    /// `sizeof` this struct.
    pub struct_size: usize,
    /// 1 (default): reconnect after a lost connection.
    pub reconnect: i32,
    /// The event queue of the connection (0: 1024).
    pub event_queue: usize,
}

/// A call in flight.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_pending(PendingCall, #[allow(dead_code)] Handle);

/// A subscription.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_subscription(SubscriptionHandle, #[allow(dead_code)] Handle);

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

/// Called when a subscription ends (the owner closed it or the channel went away).
#[allow(non_camel_case_types)]
pub type stintmate_ipc_end_cb = Option<
    unsafe extern "C-unwind" fn(
        user_data: *mut c_void,
        channel: *const c_char,
        code: *const c_char,
        message: *const c_char,
    ),
>;

fn timeout_arg(ms: u32) -> Option<Duration> {
    (ms != 0).then(|| Duration::from_millis(u64::from(ms)))
}

/// Creates a client of the endpoint at `pipe` (see `stintmate_ipc_pipe_path`); it does not
/// connect until `stintmate_ipc_client_start`. `key` null uses the default key file; `opts`
/// may be null.
///
/// # Safety
/// Strings NUL-terminated; `info`, `key` and `opts` per their structs; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_new(
    pipe: *const c_char,
    info: *const stintmate_ipc_peer_info,
    key: *const stintmate_ipc_key_source,
    _opts: *const stintmate_ipc_client_opts,
    out: *mut *mut stintmate_ipc_client,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        let (pipe, info, key) = unsafe {
            let pipe = match cstr(pipe, "pipe") {
                Ok(p) => p,
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
            (pipe, info, key)
        };
        match Client::new(pipe, info, key) {
            Ok(c) => {
                let closer = c.clone();
                let handle = Handle::with_closer(move || closer.close());
                // SAFETY: `out` is valid for a write.
                unsafe { out.write(Box::into_raw(Box::new(stintmate_ipc_client(c, handle)))) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&e),
        }
    })
}

/// Starts connecting in the background.
///
/// # Safety
/// `client` from `stintmate_ipc_client_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_start(client: *mut stintmate_ipc_client) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        match unsafe { client.as_ref() } {
            Some(c) => {
                c.0.start();
                STINTMATE_IPC_OK
            }
            None => invalid("client is null"),
        }
    })
}

/// Waits until connected, at most `timeout_ms`.
///
/// # Safety
/// `client` from `stintmate_ipc_client_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_wait_connected(
    client: *mut stintmate_ipc_client,
    timeout_ms: u32,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { client.as_ref() }) else {
            return invalid("client is null");
        };
        match c
            .0
            .wait_connected(Duration::from_millis(u64::from(timeout_ms)))
        {
            Ok(_) => STINTMATE_IPC_OK,
            Err(e) => fail(&e),
        }
    })
}

/// 1 when the connection is up, else 0.
///
/// # Safety
/// `client` from `stintmate_ipc_client_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_is_connected(
    client: *mut stintmate_ipc_client,
) -> i32 {
    // SAFETY: the contract of this function.
    unsafe { client.as_ref() }.map_or(0, |c| i32::from(c.0.is_connected()))
}

/// Closes the connection, stops reconnecting and releases the handle.
///
/// # Safety
/// `client` from `stintmate_ipc_client_new`, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_free(client: *mut stintmate_ipc_client) {
    if client.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: made by `Box::into_raw` in `stintmate_ipc_client_new`.
        let c = unsafe { Box::from_raw(client) };
        c.0.close();
    }));
}

fn call_options(version: u32, timeout_ms: u32) -> CallOptions {
    CallOptions {
        version,
        timeout: timeout_arg(timeout_ms),
        ..CallOptions::default()
    }
}

/// Writes the outcome of a call: the result, or records the error (a remote error's details
/// are in the error record).
///
/// # Safety
/// `out` null or valid for a write.
unsafe fn finish(result: Result<Value, Error>, out: *mut stintmate_ipc_buffer) -> i32 {
    match result {
        // SAFETY: forwarded contract.
        Ok(v) => unsafe { put_value(out, &v) },
        Err(e) => fail(&e),
    }
}

/// Calls a method and waits for the result. `params`/`params_len` are MessagePack (length 0:
/// nil). `timeout_ms` is the deadline (0: none). The result is written to `result_out`, to be freed with `stintmate_ipc_free_buffer`.
///
/// # Safety
/// `client` valid; `method` NUL-terminated; `params` valid for `params_len` bytes;
/// `result_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call(
    client: *mut stintmate_ipc_client,
    method: *const c_char,
    version: u32,
    params: *const u8,
    params_len: usize,
    timeout_ms: u32,
    result_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        unsafe {
            let Some(c) = client.as_ref() else {
                return invalid("client is null");
            };
            let method = match cstr(method, "method") {
                Ok(m) => m,
                Err(s) => return s,
            };
            let params = match value_arg(params, params_len, "params") {
                Ok(p) => p,
                Err(s) => return s,
            };
            finish(
                c.0.call_with(method, params, call_options(version, timeout_ms)),
                result_out,
            )
        }
    })
}

/// Sends a call and returns it in flight; see `stintmate_ipc_call_wait` and
/// `stintmate_ipc_call_cancel`.
///
/// # Safety
/// As [`stintmate_ipc_call`]; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_async(
    client: *mut stintmate_ipc_client,
    method: *const c_char,
    version: u32,
    params: *const u8,
    params_len: usize,
    timeout_ms: u32,
    out: *mut *mut stintmate_ipc_pending,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        // SAFETY: the contract of this function.
        unsafe {
            let Some(c) = client.as_ref() else {
                return invalid("client is null");
            };
            let method = match cstr(method, "method") {
                Ok(m) => m,
                Err(s) => return s,
            };
            let params = match value_arg(params, params_len, "params") {
                Ok(p) => p,
                Err(s) => return s,
            };
            match c
                .0
                .call_async(method, params, call_options(version, timeout_ms))
            {
                Ok(p) => {
                    out.write(Box::into_raw(Box::new(stintmate_ipc_pending(
                        p,
                        Handle::new(),
                    ))));
                    STINTMATE_IPC_OK
                }
                Err(e) => fail(&e),
            }
        }
    })
}

/// Waits for a call in flight, at most `timeout_ms` (`UINT32_MAX`: as long as it takes).
/// `STINTMATE_IPC_E_WAIT_TIMEOUT` when the wait ended first; the call stays in flight and can
/// be waited for again. The outcome is as in `stintmate_ipc_call`.
///
/// # Safety
/// `call` valid; `result_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_wait(
    call: *mut stintmate_ipc_pending,
    timeout_ms: u32,
    result_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let Some(c) = (unsafe { call.as_ref() }) else {
            return invalid("call is null");
        };
        let wait = (timeout_ms != u32::MAX).then(|| Duration::from_millis(u64::from(timeout_ms)));
        match c.0.wait_timeout(wait) {
            // SAFETY: `result_out` per the contract.
            Some(r) => unsafe { finish(r, result_out) },
            None => STINTMATE_IPC_E_WAIT_TIMEOUT,
        }
    })
}

/// Gives up on a call: sends `cancel`; the call then fails with `STINTMATE_IPC_E_CANCELLED`.
/// Returns 1 when the cancel took effect, 0 when the answer had already won.
///
/// # Safety
/// `call` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_cancel(call: *mut stintmate_ipc_pending) -> i32 {
    // SAFETY: the contract of this function.
    match unsafe { call.as_ref() } {
        Some(c) => catch_unwind(AssertUnwindSafe(|| i32::from(c.0.cancel())))
            .unwrap_or(STINTMATE_IPC_E_INTERNAL),
        None => invalid("call is null"),
    }
}

/// Releases a call handle (the call itself is not cancelled).
///
/// # Safety
/// `call` from `stintmate_ipc_call_async`, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_call_free(call: *mut stintmate_ipc_pending) {
    if !call.is_null() {
        // SAFETY: made by `Box::into_raw` in `stintmate_ipc_call_async`.
        drop(unsafe { Box::from_raw(call) });
    }
}

fn deliver(
    user: &Registration,
    cb: unsafe extern "C-unwind" fn(*mut c_void, *const stintmate_ipc_delivery),
    d: Delivery,
) {
    let (kind, flags, channel, seq, ts, data) = match &d {
        Delivery::Event(e) => {
            let bytes = stintmate_ipc::codec::encode(&e.data).unwrap_or_default();
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
    // SAFETY: `cb` and `user` are the embedder's; the struct outlives the call.
    let Some(_g) = enter() else { return };
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        cb(user.user_data(), &raw const c)
    }));
}

/// Subscribes to a channel of the server, now and after every reconnect. `options` is a
/// MessagePack map (length 0: none). `on_delivery` runs on the connection's callback lane, in
/// order; `on_end` (may be null) when the subscription ends. Subscription happens in the
/// background: see `stintmate_ipc_subscription_is_active`.
///
/// # Safety
/// `client` valid; `channel` NUL-terminated; `options` valid for `options_len` bytes; `out`
/// valid for a write; `user_data` usable from library threads until `unsubscribe`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_subscribe(
    client: *mut stintmate_ipc_client,
    channel: *const c_char,
    version: u32,
    options: *const u8,
    options_len: usize,
    on_delivery: stintmate_ipc_delivery_cb,
    on_end: stintmate_ipc_end_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
    out: *mut *mut stintmate_ipc_subscription,
) -> i32 {
    guard(|| {
        if out.is_null() {
            return invalid("out is null");
        }
        let Some(cb) = on_delivery else {
            return invalid("on_delivery is null");
        };
        // SAFETY: the contract of this function.
        unsafe {
            let Some(c) = client.as_ref() else {
                return invalid("client is null");
            };
            let channel = match cstr(channel, "channel") {
                Ok(c) => c,
                Err(s) => return s,
            };
            let options = match value_arg(options, options_len, "options") {
                Ok(Value::Nil) => None,
                Ok(v) => Some(v),
                Err(s) => return s,
            };
            let user = Registration::new(user_data, release);
            let mut opts = SubscribeOptions {
                version,
                options,
                ..SubscribeOptions::default()
            };
            if let Some(end) = on_end {
                let user = user.clone();
                opts.on_end = Some(Arc::new(move |ch: &str, code: &str, msg: &str| {
                    let Some(_g) = enter() else { return };
                    let to_c =
                        |s: &str| std::ffi::CString::new(s.replace('\0', "")).unwrap_or_default();
                    let (ch, code, msg) = (to_c(ch), to_c(code), to_c(msg));
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        end(user.user_data(), ch.as_ptr(), code.as_ptr(), msg.as_ptr());
                    }));
                }));
            }
            let handle = c.0.subscribe(channel, opts, move |d| deliver(&user, cb, d));
            out.write(Box::into_raw(Box::new(stintmate_ipc_subscription(
                handle,
                Handle::new(),
            ))));
            STINTMATE_IPC_OK
        }
    })
}

/// 1 when the subscription is established, else 0.
///
/// # Safety
/// `sub` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_subscription_is_active(
    sub: *mut stintmate_ipc_subscription,
) -> i32 {
    // SAFETY: the contract of this function.
    unsafe { sub.as_ref() }.map_or(0, |s| i32::from(s.0.is_subscribed()))
}

/// Ends the subscription and releases the handle. A delivery already running may finish
/// after this returns.
///
/// # Safety
/// `sub` from `stintmate_ipc_subscribe`, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_unsubscribe(sub: *mut stintmate_ipc_subscription) {
    if sub.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: made by `Box::into_raw` in `stintmate_ipc_subscribe`.
        let s = unsafe { Box::from_raw(sub) };
        s.0.close();
    }));
}
