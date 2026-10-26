//! The client side of the C ABI (spec 2.7): the client that stays connected, its calls and
//! subscriptions, `connect` and the reconnection hooks.

use std::ffi::{CStr, c_char, c_void};
use std::mem::{offset_of, size_of_val};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use stintmate_ipc::blocking::Client;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::Error;
use stintmate_ipc::handshake::PeerInfo;

use crate::keys::{key_source_arg, stintmate_ipc_key_source};
use crate::peer::{
    ErrInfo, attach_client, call_async_with, call_sync, present, read_struct, run,
    stintmate_ipc_call_opts, stintmate_ipc_peer, stintmate_ipc_pending,
    stintmate_ipc_subscribe_opts, stintmate_ipc_subscription, subscribe_setup, wait_arg,
};
use crate::runtime::{Handle, Registration, post_hook, stintmate_ipc_release_cb};
use crate::{
    STINTMATE_IPC_E_NOT_CONNECTED, STINTMATE_IPC_OK, cstr, guard, invalid, opt_cstr, put_value,
    record_error, stintmate_ipc_buffer, value_arg,
};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

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

/// Called with each failed connection attempt: the error as `stintmate_ipc_last_error` has it.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_error_cb =
    Option<unsafe extern "C-unwind" fn(user_data: *mut c_void, error: *const u8, len: usize)>;

type ErrorHook = (
    unsafe extern "C-unwind" fn(*mut c_void, *const u8, usize),
    Arc<Registration>,
);

/// What the connection attempts leave behind: the last error and the caller's hook.
#[derive(Default)]
pub(crate) struct ClientShared {
    last: Mutex<Option<ErrInfo>>,
    hook: Mutex<Option<ErrorHook>>,
    reconnect: bool,
}

/// A client that stays connected (reconnecting with backoff), or makes one connection.
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_client(
    pub(crate) Client,
    #[allow(dead_code)] pub(crate) Handle,
    pub(crate) Arc<ClientShared>,
    pub(crate) crate::node::stintmate_ipc_node,
);

/// Options of a client (spec 2.7); `struct_size` is `sizeof`.
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

/// Builds a client and wires its error bookkeeping.
fn build_client(
    pipe: &str,
    info: PeerInfo,
    key: stintmate_ipc::KeySource,
    reconnect: bool,
    event_queue: usize,
) -> Result<(Client, Arc<ClientShared>), Error> {
    let shared = Arc::new(ClientShared {
        reconnect,
        ..ClientShared::default()
    });
    let eq = if event_queue == 0 { 1024 } else { event_queue };
    let client = Client::with(pipe, info, key, |b| b.reconnect(reconnect).event_queue(eq))?;
    let s = shared.clone();
    client.on_connect_error(move |e: &Error| {
        let info = ErrInfo::of(e);
        let hook = lock(&s.hook).clone();
        if let Some((cb, reg)) = hook {
            let bytes = codec::encode(&info.to_value()).unwrap_or_default();
            post_hook(move || {
                // SAFETY: the caller's callback; `reg` keeps its `user_data` alive.
                let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
                    cb(reg.user_data(), bytes.as_ptr(), bytes.len());
                }));
            });
        }
        *lock(&s.last) = Some(info);
    });
    Ok((client, shared))
}

/// Records why a client is not connected: with `raw`, the last attempt's own error; otherwise
/// `E_NOT_CONNECTED` with that error as its cause.
fn not_connected(shared: &ClientShared, raw: bool) -> i32 {
    match lock(&shared.last).clone() {
        Some(l) if raw => l.record(),
        Some(l) => l.record_as_cause(STINTMATE_IPC_E_NOT_CONNECTED, "not connected"),
        None => {
            record_error(STINTMATE_IPC_E_NOT_CONNECTED, "not connected");
            STINTMATE_IPC_E_NOT_CONNECTED
        }
    }
}

/// Waits for the connection and hands out a reference to it.
fn wait_peer(
    c: &Client,
    shared: &ClientShared,
    timeout_ms: i64,
    raw: bool,
    peer_out: *mut *mut stintmate_ipc_peer,
) -> Result<(), i32> {
    let wait = wait_arg(timeout_ms).unwrap_or(Duration::from_secs(60 * 60 * 24 * 365));
    match c.wait_connected(wait) {
        Ok(p) => {
            if !peer_out.is_null() {
                // SAFETY: `peer_out` is valid for a write per the callers' contract.
                unsafe { peer_out.write(stintmate_ipc_peer::new_ref(p)) };
            }
            Ok(())
        }
        Err(Error::NotConnected) => Err(not_connected(shared, raw)),
        Err(e) => Err(crate::fail(&e)),
    }
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
    opts: *const stintmate_ipc_client_opts,
    out: *mut *mut stintmate_ipc_client,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let (pipe, info, key, (reconnect, event_queue)) = unsafe {
            (
                cstr(pipe, "pipe")?,
                peer_info_arg(info)?,
                key_source_arg(key)?,
                client_opts_arg(opts)?,
            )
        };
        let (c, shared) =
            build_client(pipe, info, key, reconnect, event_queue).map_err(|e| crate::fail(&e))?;
        let closer = c.clone();
        let handle = Handle::with_closer(move || closer.close());
        // SAFETY: `out` is valid for a write.
        unsafe {
            out.write(Box::into_raw(Box::new(stintmate_ipc_client(
                c.clone(),
                handle,
                shared,
                crate::node::stintmate_ipc_node::new(stintmate_ipc::blocking::Node::clone(&c)),
            ))))
        };
        Ok(())
    })
}

/// The client options: reconnect (default 1) and the event queue.
///
/// # Safety
/// `opts` null or a `stintmate_ipc_client_opts`.
unsafe fn client_opts_arg(opts: *const stintmate_ipc_client_opts) -> Result<(bool, usize), i32> {
    if opts.is_null() {
        return Ok((true, 0));
    }
    // SAFETY: the contract of the caller.
    let Some((o, n)) = (unsafe { read_struct(opts) }) else {
        return Err(invalid("opts.struct_size is invalid"));
    };
    let reconnect = !present(
        n,
        offset_of!(stintmate_ipc_client_opts, reconnect),
        size_of_val(&o.reconnect),
    ) || o.reconnect != 0;
    Ok((reconnect, o.event_queue))
}

/// The client behind a pointer.
///
/// # Safety
/// `c` null or live.
unsafe fn client_of<'a>(c: *mut stintmate_ipc_client) -> Result<&'a stintmate_ipc_client, i32> {
    // SAFETY: the contract of the callers.
    unsafe { c.as_ref() }.ok_or_else(|| invalid("client is null"))
}

/// Starts connecting in the background.
///
/// # Safety
/// `c` from `stintmate_ipc_client_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_start(c: *mut stintmate_ipc_client) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        unsafe { client_of(c) }?.0.start();
        Ok(())
    })
}

/// Waits until connected, at most `timeout_ms` (`STINTMATE_IPC_FOREVER`: as long as it takes).
/// `E_NOT_CONNECTED` otherwise, with the last attempt's error as its cause in the error record.
/// `peer_out` may be null: an owned reference to the connection (`stintmate_ipc_peer_free`).
///
/// # Safety
/// `c` live; `peer_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_wait_connected(
    c: *mut stintmate_ipc_client,
    timeout_ms: i64,
    peer_out: *mut *mut stintmate_ipc_peer,
) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        let c = unsafe { client_of(c) }?;
        wait_peer(&c.0, &c.2, timeout_ms, false, peer_out)
    })
}

/// Starts connecting and waits for the first connection (`Client.connect`).
///
/// # Safety
/// As [`stintmate_ipc_client_wait_connected`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_connect(
    c: *mut stintmate_ipc_client,
    timeout_ms: i64,
    peer_out: *mut *mut stintmate_ipc_peer,
) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        let c = unsafe { client_of(c) }?;
        c.0.start();
        wait_peer(&c.0, &c.2, timeout_ms, false, peer_out)
    })
}

/// 1 when the connection is up, else 0.
///
/// # Safety
/// `c` null or live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_is_connected(c: *mut stintmate_ipc_client) -> i32 {
    // SAFETY: the contract of this function.
    unsafe { c.as_ref() }.map_or(0, |c| i32::from(c.0.is_connected()))
}

/// An owned reference to the current connection; `E_NOT_CONNECTED` when there is none.
///
/// # Safety
/// `c` live; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_peer(
    c: *mut stintmate_ipc_client,
    out: *mut *mut stintmate_ipc_peer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let c = unsafe { client_of(c) }?;
        match c.0.peer() {
            Some(p) => {
                // SAFETY: `out` per the contract.
                unsafe { out.write(stintmate_ipc_peer::new_ref(p)) };
                Ok(())
            }
            None => Err(not_connected(&c.2, false)),
        }
    })
}

/// The error of the last failed connection attempt, as `stintmate_ipc_last_error` has it; nil
/// when the last attempt succeeded (or none was made).
///
/// # Safety
/// `c` live; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_last_error(
    c: *mut stintmate_ipc_client,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        let c = unsafe { client_of(c) }?;
        let v = if c.0.last_error().is_some() {
            lock(&c.2.last)
                .as_ref()
                .map_or(Value::Nil, ErrInfo::to_value)
        } else {
            Value::Nil
        };
        // SAFETY: `out` per the contract.
        match unsafe { put_value(out, &v) } {
            STINTMATE_IPC_OK => Ok(()),
            s => Err(s),
        }
    })
}

/// Sets the hook called, on the hooks thread, after each failed connection attempt, with the
/// error map. Replaces the previous hook (releasing its `user_data`); a null `cb` clears it.
///
/// # Safety
/// `c` live; `cb` and `user_data` usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_on_connect_error(
    c: *mut stintmate_ipc_client,
    cb: stintmate_ipc_error_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
) -> i32 {
    run(|| {
        let reg = Registration::new(user_data, release);
        // SAFETY: the contract of this function.
        let c = unsafe { client_of(c) }?;
        let old = std::mem::replace(&mut *lock(&c.2.hook), cb.map(|cb| (cb, reg)));
        drop(old);
        Ok(())
    })
}

/// Stops reconnecting and closes the connection; the handle stays valid until
/// `stintmate_ipc_client_free`.
///
/// # Safety
/// `c` live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_close(c: *mut stintmate_ipc_client) -> i32 {
    run(|| {
        // SAFETY: the contract of this function.
        unsafe { client_of(c) }?.0.close();
        Ok(())
    })
}

/// Closes the connection, stops reconnecting and releases the handle.
///
/// # Safety
/// `c` from `stintmate_ipc_client_new`, not used afterwards; null is ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_free(c: *mut stintmate_ipc_client) {
    if c.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: made by `Box::into_raw` in `stintmate_ipc_client_new`.
        let b = unsafe { Box::from_raw(c) };
        b.0.close();
        drop(lock(&b.2.hook).take());
    }));
}

/// How long the client waits (milliseconds, before jitter) before the next attempt, chosen by
/// the `bye` code that ended the last connection or attempt (`code` may be null) and the
/// failures in a row; -1: give up.
///
/// # Safety
/// `code` null or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_reconnect_delay_ms(
    code: *const c_char,
    attempt: u32,
) -> i64 {
    let code = if code.is_null() {
        None
    } else {
        // SAFETY: NUL-terminated per the contract.
        unsafe { CStr::from_ptr(code) }.to_str().ok()
    };
    stintmate_ipc::client::reconnect_delay(code, attempt)
        .map_or(-1, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// One connection without reconnection (`connect()`): handshake and return the peer. With a
/// `node` (a client made with `reconnect` 0 whose methods and channels this connection
/// offers) the connection is that client's; otherwise the connection owns a client of its own
/// that closes when the last reference to the peer is released. Failures are the attempt's own
/// error (no `E_NOT_CONNECTED` wrapping).
///
/// # Safety
/// Strings NUL-terminated; `info` and `key` per their structs; `node` null or live; `out`
/// valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_connect(
    pipe: *const c_char,
    info: *const stintmate_ipc_peer_info,
    key: *const stintmate_ipc_key_source,
    timeout_ms: i64,
    node: *mut stintmate_ipc_client,
    event_queue: usize,
    out: *mut *mut stintmate_ipc_peer,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        if !node.is_null() {
            // SAFETY: the contract of this function.
            let n = unsafe { client_of(node) }?;
            if n.2.reconnect {
                return Err(invalid("node must be a client made with reconnect = 0"));
            }
            n.0.start();
            return wait_peer(&n.0, &n.2, timeout_ms, true, out);
        }
        // SAFETY: the contract of this function.
        let (pipe, info, key) = unsafe {
            (
                cstr(pipe, "pipe")?,
                peer_info_arg(info)?,
                key_source_arg(key)?,
            )
        };
        let (c, shared) =
            build_client(pipe, info, key, false, event_queue).map_err(|e| crate::fail(&e))?;
        c.start();
        let wait = wait_arg(timeout_ms).unwrap_or(Duration::from_secs(60 * 60 * 24 * 365));
        match c.wait_connected(wait) {
            Ok(p) => {
                // SAFETY: `out` per the contract.
                unsafe { out.write(stintmate_ipc_peer::new_owned(p, c)) };
                Ok(())
            }
            Err(e) => {
                let status = match e {
                    Error::NotConnected => not_connected(&shared, true),
                    other => crate::fail(&other),
                };
                c.close();
                Err(status)
            }
        }
    })
}

/// A synchronous call on the client's current connection; `E_NOT_CONNECTED` when there is
/// none. See `stintmate_ipc_peer_call`.
///
/// # Safety
/// `c` live; the rest as `stintmate_ipc_peer_call`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_call(
    c: *mut stintmate_ipc_client,
    method: *const c_char,
    params: *const u8,
    len: usize,
    opts: *const stintmate_ipc_call_opts,
    result_out: *mut stintmate_ipc_buffer,
) -> i32 {
    // SAFETY: the contract of this function.
    let Some(c) = (unsafe { c.as_ref() }) else {
        return guard(|| invalid("client is null"));
    };
    // SAFETY: as above.
    unsafe {
        call_sync(method, params, len, opts, result_out, |m, v, o| {
            c.0.call_with(m, v, o)
        })
    }
}

/// An asynchronous call on the client's current connection.
///
/// # Safety
/// As [`stintmate_ipc_client_call`]; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_call_async(
    c: *mut stintmate_ipc_client,
    method: *const c_char,
    params: *const u8,
    len: usize,
    opts: *const stintmate_ipc_call_opts,
    out: *mut *mut stintmate_ipc_pending,
) -> i32 {
    // SAFETY: the contract of this function.
    let Some(c) = (unsafe { c.as_ref() }) else {
        return guard(|| invalid("client is null"));
    };
    // SAFETY: as above.
    unsafe {
        call_async_with(method, params, len, opts, out, |m, v, o| {
            c.0.call_async(m, v, o)
        })
    }
}

/// Subscribes to a channel of the server now and after every reconnection, until
/// unsubscribed. Does not wait: see `stintmate_ipc_subscription_wait` and
/// `stintmate_ipc_subscription_info` (the last error).
///
/// # Safety
/// `c` live; `channel` NUL-terminated; `opts` per its struct; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_client_subscribe(
    c: *mut stintmate_ipc_client,
    channel: *const c_char,
    opts: *const stintmate_ipc_subscribe_opts,
    out: *mut *mut stintmate_ipc_subscription,
) -> i32 {
    run(|| {
        if out.is_null() {
            return Err(invalid("out is null"));
        }
        // SAFETY: the contract of this function.
        unsafe {
            let client = client_of(c)?;
            let setup = subscribe_setup(channel, opts)?;
            let handle = client.0.subscribe(&setup.channel, setup.opts.clone(), {
                let cb = setup.callback.clone();
                move |d| cb(d)
            });
            attach_client(setup, handle, out);
        }
        Ok(())
    })
}
