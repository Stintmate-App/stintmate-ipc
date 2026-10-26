//! The peer, call, subscription and client functions of the DLL (spec 2.6, 2.7 client half),
//! resolved by name the way a C caller does. The library is the one `common::api()` builds
//! and loads; loading it again only adds a reference.
#![allow(dead_code, non_camel_case_types, clippy::missing_transmute_annotations)]

use std::ffi::{CStr, CString, c_char, c_void};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use windows_sys::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryW};

use crate::common::{self, Buffer, KeySource, NO_BUFFER, PeerInfo, c, key_source, take};

pub type ReleaseCb = Option<unsafe extern "C-unwind" fn(*mut c_void)>;
pub type ProgressCb = Option<unsafe extern "C-unwind" fn(*mut c_void, *const u8, usize)>;
pub type DoneCb = Option<unsafe extern "C-unwind" fn(*mut c_void, *mut c_void)>;
pub type DeliveryCb = Option<unsafe extern "C-unwind" fn(*mut c_void, *const Delivery)>;
pub type EndCb = Option<
    unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut c_void,
        *const c_char,
        *const c_char,
        *const c_char,
    ),
>;
pub type ErrorCb = Option<unsafe extern "C-unwind" fn(*mut c_void, *const u8, usize)>;

pub const OK: i32 = 0;
pub const E_NOT_FOUND: i32 = -1;
pub const E_HANDSHAKE: i32 = -4;
pub const E_NOT_CONNECTED: i32 = -6;
pub const E_TIMEOUT: i32 = -8;
pub const E_CANCELLED: i32 = -9;
pub const E_INVALID_ARG: i32 = -11;
pub const E_REMOTE: i32 = -13;
pub const E_WAIT_TIMEOUT: i32 = -17;
pub const FOREVER: i64 = -1;

#[repr(C)]
pub struct CallOpts {
    pub struct_size: usize,
    pub version: u32,
    pub timeout_ms: i64,
    pub idempotency_key: *const c_char,
    pub on_progress: ProgressCb,
    pub user_data: *mut c_void,
    pub release: ReleaseCb,
}

impl CallOpts {
    pub fn new(timeout_ms: i64) -> Self {
        Self {
            struct_size: size_of::<Self>(),
            version: 0,
            timeout_ms,
            idempotency_key: std::ptr::null(),
            on_progress: None,
            user_data: std::ptr::null_mut(),
            release: None,
        }
    }
}

#[repr(C)]
pub struct SubscribeOpts {
    pub struct_size: usize,
    pub version: u32,
    pub options: *const u8,
    pub options_len: usize,
    pub timeout_ms: i64,
    pub on_delivery: DeliveryCb,
    pub on_end: EndCb,
    pub user_data: *mut c_void,
    pub release: ReleaseCb,
}

impl SubscribeOpts {
    pub fn new(on_delivery: DeliveryCb, user_data: *mut c_void) -> Self {
        Self {
            struct_size: size_of::<Self>(),
            version: 0,
            options: std::ptr::null(),
            options_len: 0,
            timeout_ms: 5000,
            on_delivery,
            on_end: None,
            user_data,
            release: None,
        }
    }
}

#[repr(C)]
pub struct ClientOpts {
    pub struct_size: usize,
    pub reconnect: i32,
    pub event_queue: usize,
}

#[repr(C)]
pub struct Delivery {
    pub kind: i32,
    pub flags: u32,
    pub channel: *const u8,
    pub channel_len: usize,
    pub seq: u64,
    pub ts_ns: i64,
    pub has_ts: i32,
    pub data: *const u8,
    pub len: usize,
}

macro_rules! papi {
    ($( fn $name:ident($($a:ident: $t:ty),*) $(-> $r:ty)?; )*) => {
        pub struct PApi {
            $( pub $name: unsafe extern "C" fn($($t),*) $(-> $r)?, )*
        }
        impl PApi {
            fn load(module: *mut c_void) -> PApi {
                PApi {
                    $( $name: unsafe {
                        let name = concat!("stintmate_ipc_", stringify!($name), "\0");
                        let p = GetProcAddress(module, name.as_ptr());
                        assert!(p.is_some(), "stintmate_ipc.dll does not export {name}");
                        std::mem::transmute::<_, unsafe extern "C" fn($($t),*) $(-> $r)?>(p)
                    }, )*
                }
            }
        }
    };
}

papi! {
    fn live_buffers() -> isize;
    fn live_handles() -> isize;
    fn last_error(out: *mut Buffer) -> i32;
    fn peer_ref(peer: *mut c_void) -> *mut c_void;
    fn peer_free(peer: *mut c_void);
    fn peer_id(peer: *const c_void) -> u64;
    fn peer_get_info(peer: *const c_void, out: *mut Buffer) -> i32;
    fn peer_decl(peer: *const c_void, out: *mut Buffer) -> i32;
    fn peer_is_open(peer: *const c_void) -> i32;
    fn peer_wait_closed(peer: *const c_void, timeout_ms: i64) -> i32;
    fn peer_close_reason(peer: *const c_void, out: *mut Buffer) -> i32;
    fn peer_close(peer: *mut c_void, code: *const c_char, message: *const c_char) -> i32;
    fn peer_call(peer: *mut c_void, method: *const c_char, params: *const u8, len: usize, opts: *const CallOpts, out: *mut Buffer) -> i32;
    fn peer_call_async(peer: *mut c_void, method: *const c_char, params: *const u8, len: usize, opts: *const CallOpts, out: *mut *mut c_void) -> i32;
    fn client_call(c: *mut c_void, method: *const c_char, params: *const u8, len: usize, opts: *const CallOpts, out: *mut Buffer) -> i32;
    fn client_call_async(c: *mut c_void, method: *const c_char, params: *const u8, len: usize, opts: *const CallOpts, out: *mut *mut c_void) -> i32;
    fn call_id(p: *const c_void) -> u64;
    fn call_is_done(p: *const c_void) -> i32;
    fn call_wait(p: *mut c_void, timeout_ms: i64, out: *mut Buffer) -> i32;
    fn call_cancel(p: *mut c_void, reason: *const c_char) -> i32;
    fn call_on_done(p: *mut c_void, cb: DoneCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn call_free(p: *mut c_void);
    fn peer_subscribe(peer: *mut c_void, channel: *const c_char, opts: *const SubscribeOpts, out: *mut *mut c_void) -> i32;
    fn client_subscribe(c: *mut c_void, channel: *const c_char, opts: *const SubscribeOpts, out: *mut *mut c_void) -> i32;
    fn subscription_info(s: *mut c_void, out: *mut Buffer) -> i32;
    fn subscription_is_active(s: *mut c_void) -> i32;
    fn subscription_wait(s: *mut c_void, what: i32, timeout_ms: i64) -> i32;
    fn unsubscribe(s: *mut c_void);
    fn client_new(pipe: *const c_char, info: *const PeerInfo, key: *const KeySource, opts: *const ClientOpts, out: *mut *mut c_void) -> i32;
    fn client_start(c: *mut c_void) -> i32;
    fn client_wait_connected(c: *mut c_void, timeout_ms: i64, peer_out: *mut *mut c_void) -> i32;
    fn client_connect(c: *mut c_void, timeout_ms: i64, peer_out: *mut *mut c_void) -> i32;
    fn client_is_connected(c: *mut c_void) -> i32;
    fn client_peer(c: *mut c_void, out: *mut *mut c_void) -> i32;
    fn client_last_error(c: *mut c_void, out: *mut Buffer) -> i32;
    fn on_connect_error(c: *mut c_void, cb: ErrorCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn client_close(c: *mut c_void) -> i32;
    fn client_free(c: *mut c_void);
    fn reconnect_delay_ms(code: *const c_char, attempt: u32) -> i64;
    fn connect(pipe: *const c_char, info: *const PeerInfo, key: *const KeySource, timeout_ms: i64, node: *mut c_void, event_queue: usize, out: *mut *mut c_void) -> i32;
}

/// The functions of this file (builds and loads the library on first use).
pub fn p() -> &'static PApi {
    static API: OnceLock<PApi> = OnceLock::new();
    API.get_or_init(|| {
        let _ = common::api();
        let exe = std::env::current_exe().unwrap();
        let dll: PathBuf = exe
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("stintmate_ipc.dll");
        let wide: Vec<u16> = dll
            .to_str()
            .unwrap()
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let module = unsafe { LoadLibraryW(wide.as_ptr()) };
        assert!(!module.is_null(), "cannot load {}", dll.display());
        PApi::load(module)
    })
}

// ------------------------------------------------------------------------ helpers

/// The decoded MessagePack of a returned buffer.
pub fn value(buf: Buffer) -> stintmate_ipc::codec::Value {
    stintmate_ipc::codec::decode(&take(buf)).unwrap()
}

pub fn mp(v: &stintmate_ipc::codec::Value) -> Vec<u8> {
    stintmate_ipc::codec::encode(v).unwrap()
}

/// The error map of the calling thread's last failure.
pub fn last_map() -> stintmate_ipc::codec::Value {
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (p().last_error)(&mut b) }, 0);
    value(b)
}

pub fn get_str(v: &stintmate_ipc::codec::Value, key: &str) -> Option<String> {
    stintmate_ipc::codec::get(v, key)
        .and_then(|x| x.as_str())
        .map(str::to_owned)
}

pub fn get_i64(v: &stintmate_ipc::codec::Value, key: &str) -> Option<i64> {
    stintmate_ipc::codec::get(v, key).and_then(stintmate_ipc::codec::Value::as_i64)
}

pub fn peer_info(app: &CStr) -> PeerInfo {
    PeerInfo {
        app: app.as_ptr(),
        name: std::ptr::null(),
        version: std::ptr::null(),
        process: std::ptr::null(),
        meta: std::ptr::null(),
        meta_len: 0,
    }
}

/// A raw key and its key source.
pub fn new_key() -> [u8; 32] {
    stintmate_ipc::handshake::new_nonce()
}

/// A client on `pipe` with `key` (not started).
pub fn new_client(pipe: &str, key: &[u8; 32], reconnect: bool) -> *mut c_void {
    let (pipe, app) = (c(pipe), c("client"));
    let ks = key_source(key);
    let opts = ClientOpts {
        struct_size: size_of::<ClientOpts>(),
        reconnect: i32::from(reconnect),
        event_queue: 0,
    };
    let mut out = std::ptr::null_mut();
    let st = unsafe { (p().client_new)(pipe.as_ptr(), &peer_info(&app), &ks, &opts, &mut out) };
    assert_eq!(st, 0, "{}", common::last_message());
    out
}

pub fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Both counters back to zero (the handles are released from library threads: poll).
pub fn settle() {
    wait_until("handles to be released", || {
        (unsafe { (p().live_handles)() }) == 0
    });
    assert_eq!(unsafe { (p().live_buffers)() }, 0);
}

/// One delivery: kind, flags, seq, data.
pub type Seen = (i32, u32, u64, Vec<u8>);

/// Counts what the callbacks of a test saw.
#[derive(Default)]
pub struct Probe {
    pub released: AtomicUsize,
    pub progress: AtomicUsize,
    pub done: AtomicUsize,
    pub ends: AtomicUsize,
    pub errors: AtomicUsize,
    pub deliveries: std::sync::Mutex<Vec<Seen>>,
    pub last: std::sync::Mutex<Vec<u8>>,
    pub thread: std::sync::Mutex<Option<String>>,
    pub text: std::sync::Mutex<String>,
    pub null_sub_on_end: std::sync::atomic::AtomicBool,
    pub done_ptr: std::sync::atomic::AtomicPtr<c_void>,
}

impl Probe {
    pub fn leak() -> &'static Probe {
        Box::leak(Box::default())
    }
    pub fn ptr(&'static self) -> *mut c_void {
        std::ptr::from_ref(self).cast_mut().cast()
    }
    pub fn count(&self, f: impl Fn(&Probe) -> &AtomicUsize) -> usize {
        f(self).load(Ordering::SeqCst)
    }
}

pub unsafe extern "C-unwind" fn release(ud: *mut c_void) {
    unsafe { &*ud.cast::<Probe>() }
        .released
        .fetch_add(1, Ordering::SeqCst);
}

pub unsafe extern "C-unwind" fn on_progress(ud: *mut c_void, data: *const u8, len: usize) {
    let pr = unsafe { &*ud.cast::<Probe>() };
    *pr.last.lock().unwrap() = unsafe { std::slice::from_raw_parts(data, len) }.to_vec();
    pr.progress.fetch_add(1, Ordering::SeqCst);
}

pub unsafe extern "C-unwind" fn on_done(ud: *mut c_void, call: *mut c_void) {
    let pr = unsafe { &*ud.cast::<Probe>() };
    *pr.thread.lock().unwrap() = Some(format!("{:?}", std::thread::current().id()));
    pr.done_ptr.store(call, Ordering::SeqCst);
    // The handle is valid inside the callback: ask for the outcome.
    let mut out = NO_BUFFER;
    let st = unsafe { (p().call_wait)(call, 0, &mut out) };
    *pr.text.lock().unwrap() = format!("{st}");
    *pr.last.lock().unwrap() = take(out);
    pr.done.fetch_add(1, Ordering::SeqCst);
}

pub unsafe extern "C-unwind" fn on_delivery(ud: *mut c_void, d: *const Delivery) {
    let pr = unsafe { &*ud.cast::<Probe>() };
    let d = unsafe { &*d };
    let data = if d.data.is_null() {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(d.data, d.len) }.to_vec()
    };
    pr.deliveries
        .lock()
        .unwrap()
        .push((d.kind, d.flags, d.seq, data));
}

pub unsafe extern "C-unwind" fn on_end(
    ud: *mut c_void,
    sub: *mut c_void,
    channel: *const c_char,
    code: *const c_char,
    message: *const c_char,
) {
    let pr = unsafe { &*ud.cast::<Probe>() };
    let s = |p: *const c_char| unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    *pr.text.lock().unwrap() = format!("{}|{}|{}", s(channel), s(code), s(message));
    pr.null_sub_on_end.store(sub.is_null(), Ordering::SeqCst);
    pr.ends.fetch_add(1, Ordering::SeqCst);
}

pub unsafe extern "C-unwind" fn on_error(ud: *mut c_void, data: *const u8, len: usize) {
    let pr = unsafe { &*ud.cast::<Probe>() };
    *pr.last.lock().unwrap() = unsafe { std::slice::from_raw_parts(data, len) }.to_vec();
    pr.errors.fetch_add(1, Ordering::SeqCst);
}

/// A string for a C argument.
pub fn cs(s: &str) -> CString {
    c(s)
}
