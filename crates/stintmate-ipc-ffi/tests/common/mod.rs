//! Loads `stintmate_ipc.dll` the way a C caller (ctypes, P/Invoke) does: by name, resolving
//! every function from the export table. The tests below call only these extern functions.

#![allow(dead_code, non_camel_case_types, clippy::missing_transmute_annotations)]

use std::ffi::{CStr, CString, c_char, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use windows_sys::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryW};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
}

pub const NO_BUFFER: Buffer = Buffer {
    ptr: std::ptr::null_mut(),
    len: 0,
};

#[repr(C)]
pub struct PeerInfo {
    pub app: *const c_char,
    pub name: *const c_char,
    pub version: *const c_char,
    pub process: *const c_char,
    pub meta: *const u8,
    pub meta_len: usize,
}

#[repr(C)]
pub struct KeySource {
    pub struct_size: usize,
    pub path: *const c_char,
    pub key: *const u8,
    pub key_len: usize,
    pub create: i32,
}

#[repr(C)]
pub struct EndpointOpts {
    pub struct_size: usize,
    pub pipe: *const c_char,
    pub registry: *const c_char,
    pub handshake_timeout_ms: i64,
    pub handler_threads: u32,
    pub heartbeat_ms: u32,
    pub event_queue: usize,
    pub event_queue_bytes: usize,
    pub max_inflight: usize,
    pub features: *const u8,
    pub features_len: usize,
}

impl EndpointOpts {
    pub fn new(pipe: *const c_char, handler_threads: u32) -> Self {
        Self {
            struct_size: size_of::<Self>(),
            pipe,
            registry: std::ptr::null(),
            handshake_timeout_ms: -1,
            handler_threads,
            heartbeat_ms: 0,
            event_queue: 0,
            event_queue_bytes: 0,
            max_inflight: 0,
            features: std::ptr::null(),
            features_len: 0,
        }
    }
}

#[repr(C)]
pub struct MethodOpts {
    pub struct_size: usize,
    pub flags: u32,
    pub versions: *const u32,
    pub n_versions: usize,
    pub idempotency_ttl_ms: i64,
    pub doc: *const c_char,
    pub params_schema: *const u8,
    pub params_schema_len: usize,
    pub result_schema: *const u8,
    pub result_schema_len: usize,
}

pub type PeerCb = Option<unsafe extern "C-unwind" fn(*mut c_void, i32, *mut c_void)>;
pub type AcceptCb = Option<unsafe extern "C-unwind" fn(*mut c_void, *const u8, usize) -> i32>;
pub type SubscribeCb = Option<
    unsafe extern "C-unwind" fn(*mut c_void, *mut c_void, *mut c_void, *const u8, usize) -> i32,
>;
pub type ActiveCb = Option<unsafe extern "C-unwind" fn(*mut c_void, i32)>;
pub type HandshakeFailedCb =
    Option<unsafe extern "C-unwind" fn(*mut c_void, i64, *const u8, usize)>;

pub type ReleaseCb = Option<unsafe extern "C-unwind" fn(*mut c_void)>;
pub type ThreadCb = Option<unsafe extern "C-unwind" fn(*mut c_void)>;
pub type LogCb =
    Option<unsafe extern "C-unwind" fn(*mut c_void, i32, *const c_char, *const c_char)>;
pub type HandlerCb =
    Option<unsafe extern "C-unwind" fn(*mut c_void, *mut c_void, *const u8, usize)>;

macro_rules! api {
    ($( fn $name:ident($($a:ident: $t:ty),*) $(-> $r:ty)?; )*) => {
        pub struct Api {
            $( pub $name: unsafe extern "C" fn($($t),*) $(-> $r)?, )*
        }
        impl Api {
            fn load(module: *mut c_void) -> Api {
                Api {
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

api! {
    fn abi_version() -> u32;
    fn version() -> *const c_char;
    fn spec_revision() -> *const c_char;
    fn protocol_version() -> u32;
    fn free_buffer(buf: Buffer);
    fn live_buffers() -> isize;
    fn live_handles() -> isize;
    fn last_error_message(buf: *mut c_char, len: usize) -> usize;
    fn last_error(out: *mut Buffer) -> i32;
    fn set_log_callback(cb: LogCb, user_data: *mut c_void, release: ReleaseCb, level: i32) -> i32;
    fn set_log_level(level: i32) -> i32;
    fn set_thread_hooks(on_start: ThreadCb, on_stop: ThreadCb, user_data: *mut c_void) -> i32;
    fn shutdown(timeout_ms: i64) -> i32;
    fn ensure_key(path: *const c_char, key_out: *mut Buffer) -> i32;
    fn read_key(path: *const c_char, key_out: *mut Buffer) -> i32;
    fn parse_key(text: *const u8, len: usize, key_out: *mut Buffer) -> i32;
    fn format_key(key: *const u8, len: usize, text_out: *mut Buffer) -> i32;
    fn default_key_path(workspace: *const c_char, out: *mut Buffer) -> i32;
    fn pipe_user(user: *const c_char, out: *mut Buffer) -> i32;
    fn short_name(app: *const c_char, process: *const c_char, user: *const c_char, out: *mut Buffer) -> i32;
    fn pipe_path(app: *const c_char, process: *const c_char, user: *const c_char, out: *mut Buffer) -> i32;
    fn find_workspace(out: *mut Buffer) -> i32;
    fn registry_dir(workspace: *const c_char, out: *mut Buffer) -> i32;
    fn valid_app(app: *const c_char) -> i32;
    fn valid_name(name: *const c_char) -> i32;
    fn layout_normalize(layout: *const u8, len: usize, out: *mut Buffer) -> i32;
    fn endpoint_new(info: *const PeerInfo, key: *const KeySource, opts: *const EndpointOpts, out: *mut *mut c_void) -> i32;
    fn endpoint_start(ep: *mut c_void) -> i32;
    fn endpoint_stop(ep: *mut c_void, code: *const c_char, message: *const c_char) -> i32;
    fn endpoint_node(ep: *mut c_void) -> *mut c_void;
    fn endpoint_info(ep: *mut c_void, out: *mut Buffer) -> i32;
    fn on_handshake_failed(ep: *mut c_void, cb: HandshakeFailedCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn client_node(client: *mut c_void) -> *mut c_void;
    fn endpoint_free(ep: *mut c_void);
    fn method(node: *mut c_void, name: *const c_char, opts: *const MethodOpts, handler: HandlerCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn remove_method(node: *mut c_void, name: *const c_char) -> i32;
    fn declarations(node: *mut c_void, out: *mut Buffer) -> i32;
    fn peers(node: *mut c_void, out: *mut *mut c_void, cap: usize, count: *mut usize) -> i32;
    fn peer_free(peer: *mut c_void);
    fn on_peer(node: *mut c_void, cb: PeerCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn request_info(req: *const c_void, out: *mut Buffer) -> i32;
    fn request_peer(req: *const c_void) -> *mut c_void;
    fn request_is_cancelled(req: *const c_void) -> i32;
    fn request_wait_cancelled(req: *const c_void, timeout_ms: i64) -> i32;
    fn request_remaining_ms(req: *const c_void) -> i64;
    fn request_progress(req: *mut c_void, data: *const u8, len: usize, extend_ms: i64) -> i32;
    fn respond(req: *mut c_void, result: *const u8, len: usize) -> i32;
    fn respond_error(req: *mut c_void, code: *const c_char, message: *const c_char, details: *const u8, details_len: usize, retry_ms: i64) -> i32;
    fn event_channel(node: *mut c_void, name: *const c_char, versions: *const u32, n_versions: usize, out: *mut *mut c_void) -> i32;
    fn state_channel(node: *mut c_void, name: *const c_char, initial: *const u8, initial_len: usize, versions: *const u32, n_versions: usize, out: *mut *mut c_void) -> i32;
    fn stream_channel(node: *mut c_void, name: *const c_char, layout: *const u8, layout_len: usize, versions: *const u32, n_versions: usize, rate_hz: f64, queue: usize, out: *mut *mut c_void) -> i32;
    fn channel_info(ch: *mut c_void, out: *mut Buffer) -> i32;
    fn channel_subscribers(ch: *mut c_void) -> i64;
    fn channel_is_active(ch: *mut c_void) -> i32;
    fn channel_close(ch: *mut c_void, code: *const c_char, message: *const c_char) -> i32;
    fn channel_free(ch: *mut c_void);
    fn publish(ch: *mut c_void, data: *const u8, len: usize, ts_ns: i64, version: u32, accept: AcceptCb, user_data: *mut c_void, queued_out: *mut usize) -> i32;
    fn state_get(ch: *mut c_void, out: *mut Buffer) -> i32;
    fn stream_send(ch: *mut c_void, body: *const u8, len: usize, t_ns: i64, sent_out: *mut usize) -> i32;
    fn refuse(ctx: *mut c_void, code: *const c_char, message: *const c_char, details: *const u8, details_len: usize) -> i32;
    fn on_subscribe(ch: *mut c_void, cb: SubscribeCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn on_active(ch: *mut c_void, cb: ActiveCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn client_new(pipe: *const c_char, info: *const PeerInfo, key: *const KeySource, opts: *const c_void, out: *mut *mut c_void) -> i32;
    fn client_start(client: *mut c_void) -> i32;
    fn client_wait_connected(client: *mut c_void, timeout_ms: i64, peer_out: *mut *mut c_void) -> i32;
    fn client_free(client: *mut c_void);
    fn client_call(client: *mut c_void, method: *const c_char, params: *const u8, params_len: usize, opts: *const CallOpts, result_out: *mut Buffer) -> i32;
}

fn profile_dir() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    exe.parent().unwrap().parent().unwrap().to_path_buf()
}

/// The loaded library (built first when `cargo test` did not build the cdylib).
pub fn api() -> &'static Api {
    static API: OnceLock<Api> = OnceLock::new();
    API.get_or_init(|| {
        let dir = profile_dir();
        let release = dir.file_name().is_some_and(|n| n == "release");
        let mut build = std::process::Command::new(env!("CARGO"));
        build
            .args(["build", "-p", "stintmate-ipc-ffi", "--manifest-path"])
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("..")
                    .join("Cargo.toml"),
            );
        if release {
            build.arg("--release");
        }
        assert!(build.status().unwrap().success(), "cargo build failed");
        let dll = dir.join("stintmate_ipc.dll");
        let wide: Vec<u16> = dll
            .to_str()
            .unwrap()
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let module = unsafe { LoadLibraryW(wide.as_ptr()) };
        assert!(!module.is_null(), "cannot load {}", dll.display());
        Api::load(module)
    })
}

pub fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

/// Takes the bytes of a returned buffer and frees it.
pub fn take(buf: Buffer) -> Vec<u8> {
    let v = if buf.ptr.is_null() {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(buf.ptr, buf.len) }.to_vec()
    };
    unsafe { (api().free_buffer)(buf) };
    v
}

pub fn text(buf: Buffer) -> String {
    String::from_utf8(take(buf)).unwrap()
}

/// The message of the calling thread's last failure.
pub fn last_message() -> String {
    let mut b = vec![0 as c_char; 512];
    unsafe { (api().last_error_message)(b.as_mut_ptr(), b.len()) };
    unsafe { CStr::from_ptr(b.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

pub fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The counters are global: tests that count run one at a time.
pub fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

pub fn random_tag() -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let nonce = stintmate_ipc::handshake::new_nonce();
    format!(
        "{}-{}-{:02x}{:02x}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed),
        nonce[0],
        nonce[1]
    )
}

/// An endpoint and a client on a random pipe with a random raw key.
pub struct Rig {
    pub ep: *mut c_void,
    pub client: *mut c_void,
    pub pipe: CString,
}

pub fn key_source(key: &[u8; 32]) -> KeySource {
    KeySource {
        struct_size: size_of::<KeySource>(),
        path: std::ptr::null(),
        key: key.as_ptr(),
        key_len: 32,
        create: 0,
    }
}

impl Rig {
    pub fn new() -> Rig {
        let a = api();
        let tag = random_tag();
        let pipe = c(&format!("stintmate-ffi-t1-{tag}"));
        let key: [u8; 32] = std::array::from_fn(|i| stintmate_ipc::handshake::new_nonce()[i % 32]);
        let ks = key_source(&key);
        let (app_s, app_c) = (c("server"), c("client"));
        let info = |app: &CString| PeerInfo {
            app: app.as_ptr(),
            name: std::ptr::null(),
            version: std::ptr::null(),
            process: std::ptr::null(),
            meta: std::ptr::null(),
            meta_len: 0,
        };
        let opts = EndpointOpts::new(pipe.as_ptr(), 2);
        let mut ep = std::ptr::null_mut();
        let st = unsafe { (a.endpoint_new)(&info(&app_s), &ks, &opts, &mut ep) };
        assert_eq!(st, 0, "{}", last_message());
        let mut client = std::ptr::null_mut();
        let st = unsafe {
            (a.client_new)(
                pipe.as_ptr(),
                &info(&app_c),
                &ks,
                std::ptr::null(),
                &mut client,
            )
        };
        assert_eq!(st, 0, "{}", last_message());
        Rig { ep, client, pipe }
    }

    /// Starts both sides and waits for the connection.
    pub fn go(&self) {
        let a = api();
        assert_eq!(
            unsafe { (a.endpoint_start)(self.ep) },
            0,
            "{}",
            last_message()
        );
        assert_eq!(unsafe { (a.client_start)(self.client) }, 0);
        let st = unsafe { (a.client_wait_connected)(self.client, 10_000, std::ptr::null_mut()) };
        assert_eq!(st, 0, "{}", last_message());
    }

    pub fn method(&self, name: &str, cb: HandlerCb, user: *mut c_void, release: ReleaseCb) {
        let n = c(name);
        let st = unsafe {
            (api().method)(
                (api().endpoint_node)(self.ep),
                n.as_ptr(),
                std::ptr::null(),
                cb,
                user,
                release,
            )
        };
        assert_eq!(st, 0, "{}", last_message());
    }

    /// Calls `name` with no params; the status and the result bytes.
    pub fn call(&self, name: &str, timeout_ms: u32) -> (i32, Vec<u8>) {
        let n = c(name);
        let mut out = NO_BUFFER;
        let st = unsafe {
            (api().client_call)(
                self.client,
                n.as_ptr(),
                std::ptr::null(),
                0,
                &call_opts(i64::from(timeout_ms)),
                &mut out,
            )
        };
        (st, take(out))
    }

    pub fn free(self) {
        unsafe {
            (api().client_free)(self.client);
            (api().endpoint_free)(self.ep);
        }
    }
}

#[repr(C)]
pub struct CallOpts {
    pub struct_size: usize,
    pub version: u32,
    pub timeout_ms: i64,
    pub idempotency_key: *const c_char,
    pub on_progress: *const c_void,
    pub user_data: *mut c_void,
    pub release: ReleaseCb,
}

/// Call options with a deadline (`-1`: none).
pub fn call_opts(timeout_ms: i64) -> CallOpts {
    CallOpts {
        struct_size: size_of::<CallOpts>(),
        version: 0,
        timeout_ms,
        idempotency_key: std::ptr::null(),
        on_progress: std::ptr::null(),
        user_data: std::ptr::null_mut(),
        release: None,
    }
}

// ---------------------------------------------------------------------------- core counterparts

pub use stintmate_ipc::blocking as ipc;
pub use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::handshake::PeerInfo as CorePeerInfo;
use stintmate_ipc::keyfile::KeySource as CoreKeySource;

pub fn mp(v: &Value) -> Vec<u8> {
    codec::encode(v).unwrap()
}

pub fn val(buf: Buffer) -> Value {
    codec::decode(&take(buf)).unwrap()
}

pub fn key_of(v: &Value, k: &str) -> Value {
    codec::get(v, k).cloned().unwrap_or(Value::Nil)
}

pub fn new_key() -> [u8; 32] {
    std::array::from_fn(|i| stintmate_ipc::handshake::new_nonce()[i % 32])
}

pub fn peer_info(app: &CString) -> PeerInfo {
    PeerInfo {
        app: app.as_ptr(),
        name: std::ptr::null(),
        version: std::ptr::null(),
        process: std::ptr::null(),
        meta: std::ptr::null(),
        meta_len: 0,
    }
}

/// A log the callbacks write to (its address is the `user_data`).
#[derive(Default)]
pub struct Log {
    pub lines: Mutex<Vec<String>>,
    pub ptrs: Mutex<Vec<usize>>,
    pub released: AtomicUsize,
}

impl Log {
    pub fn ud(self: &std::sync::Arc<Self>) -> *mut c_void {
        std::sync::Arc::as_ptr(self) as *mut c_void
    }
    pub fn push(&self, s: impl Into<String>) {
        self.lines.lock().unwrap().push(s.into());
    }
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }
}

/// # Safety
/// `p` is the address of a live `Log`.
pub unsafe fn log<'a>(p: *mut c_void) -> &'a Log {
    unsafe { &*(p as *const Log) }
}

pub unsafe extern "C-unwind" fn release_log(p: *mut c_void) {
    unsafe { log(p) }.released.fetch_add(1, Ordering::SeqCst);
}

/// A library endpoint (through the DLL) and a core client of it.
pub struct EpFix {
    pub ep: *mut c_void,
    pub node: *mut c_void,
    pub core: ipc::Client,
    pub pipe: CString,
    pub key: [u8; 32],
}

impl EpFix {
    pub fn new() -> EpFix {
        Self::with(|_| {})
    }

    pub fn with(tune: impl FnOnce(&mut EndpointOpts)) -> EpFix {
        let a = api();
        let pipe = c(&format!("stintmate-ffi-t2-{}", random_tag()));
        let key = new_key();
        let ks = key_source(&key);
        let app = c("server");
        let mut opts = EndpointOpts::new(pipe.as_ptr(), 2);
        tune(&mut opts);
        let mut ep = std::ptr::null_mut();
        let st = unsafe { (a.endpoint_new)(&peer_info(&app), &ks, &opts, &mut ep) };
        assert_eq!(st, 0, "{}", last_message());
        let node = unsafe { (a.endpoint_node)(ep) };
        assert!(!node.is_null());
        let core = ipc::Client::new(
            pipe.to_str().unwrap(),
            CorePeerInfo::new("tester"),
            CoreKeySource::Bytes(key),
        )
        .unwrap();
        EpFix {
            ep,
            node,
            core,
            pipe,
            key,
        }
    }

    /// Starts the endpoint and the core client; returns the client's peer.
    pub fn start(&self) -> ipc::Peer {
        assert_eq!(
            unsafe { (api().endpoint_start)(self.ep) },
            0,
            "{}",
            last_message()
        );
        self.core.start();
        self.core.wait_connected(Duration::from_secs(10)).unwrap()
    }

    pub fn free(self) {
        self.core.close();
        unsafe { (api().endpoint_free)(self.ep) };
    }
}

/// A core endpoint and a library client (through the DLL) of it.
pub struct ClientFix {
    pub core: ipc::Endpoint,
    pub client: *mut c_void,
    pub node: *mut c_void,
}

impl ClientFix {
    pub fn new() -> ClientFix {
        let a = api();
        let pipe = c(&format!("stintmate-ffi-t2c-{}", random_tag()));
        let key = new_key();
        let core = ipc::Endpoint::builder(CorePeerInfo::new("tester"), CoreKeySource::Bytes(key))
            .pipe(pipe.to_str().unwrap())
            .build()
            .unwrap();
        let ks = key_source(&key);
        let app = c("offerer");
        let mut client = std::ptr::null_mut();
        let st = unsafe {
            (a.client_new)(
                pipe.as_ptr(),
                &peer_info(&app),
                &ks,
                std::ptr::null(),
                &mut client,
            )
        };
        assert_eq!(st, 0, "{}", last_message());
        let node = unsafe { (a.client_node)(client) };
        assert!(!node.is_null());
        ClientFix { core, client, node }
    }

    /// Starts both and returns the core endpoint's peer (the client).
    pub fn start(&self) -> ipc::Peer {
        self.core.start().unwrap();
        assert_eq!(unsafe { (api().client_start)(self.client) }, 0);
        wait_for("the client", || !self.core.peers().is_empty());
        self.core.peers().remove(0)
    }

    pub fn free(self) {
        unsafe { (api().client_free)(self.client) };
        self.core.stop();
    }
}

/// The leak counters must be back to zero.
pub fn assert_clean() {
    wait_for("no live handles", || unsafe { (api().live_handles)() } == 0);
    assert_eq!(unsafe { (api().live_buffers)() }, 0);
}
