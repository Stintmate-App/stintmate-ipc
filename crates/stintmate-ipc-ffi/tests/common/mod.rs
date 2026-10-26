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
    pub handler_threads: u32,
}

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
    fn endpoint_stop(ep: *mut c_void) -> i32;
    fn endpoint_free(ep: *mut c_void);
    fn endpoint_method(ep: *mut c_void, name: *const c_char, flags: u32, versions: *const u32, n_versions: usize, handler: HandlerCb, user_data: *mut c_void, release: ReleaseCb) -> i32;
    fn endpoint_remove_method(ep: *mut c_void, name: *const c_char) -> i32;
    fn respond(responder: *mut c_void, result: *const u8, len: usize) -> i32;
    fn client_new(pipe: *const c_char, info: *const PeerInfo, key: *const KeySource, opts: *const c_void, out: *mut *mut c_void) -> i32;
    fn client_start(client: *mut c_void) -> i32;
    fn client_wait_connected(client: *mut c_void, timeout_ms: u32) -> i32;
    fn client_free(client: *mut c_void);
    fn call(client: *mut c_void, method: *const c_char, version: u32, params: *const u8, params_len: usize, timeout_ms: u32, result_out: *mut Buffer) -> i32;
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
        let opts = EndpointOpts {
            struct_size: size_of::<EndpointOpts>(),
            pipe: pipe.as_ptr(),
            handler_threads: 2,
        };
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
        let st = unsafe { (a.client_wait_connected)(self.client, 10_000) };
        assert_eq!(st, 0, "{}", last_message());
    }

    pub fn method(&self, name: &str, cb: HandlerCb, user: *mut c_void, release: ReleaseCb) {
        let n = c(name);
        let st = unsafe {
            (api().endpoint_method)(
                self.ep,
                n.as_ptr(),
                0,
                std::ptr::null(),
                0,
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
            (api().call)(
                self.client,
                n.as_ptr(),
                1,
                std::ptr::null(),
                0,
                timeout_ms,
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
