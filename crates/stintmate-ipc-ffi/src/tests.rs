//! The C API exercised through its `extern "C"` functions, against a real endpoint and client
//! over named pipes (random pipe names and temporary key files).

use std::ffi::{CStr, CString, c_char, c_void};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::layout::{FieldType, Layout};

use crate::client::*;
use crate::endpoint::*;
use crate::*;

/// The buffer counter is global: tests that count run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn mp(v: &Value) -> Vec<u8> {
    codec::encode(v).unwrap()
}

fn take(buf: stintmate_ipc_buffer) -> Vec<u8> {
    let v = if buf.ptr.is_null() {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(buf.ptr, buf.len) }.to_vec()
    };
    unsafe { stintmate_ipc_free_buffer(buf) };
    v
}

fn empty() -> stintmate_ipc_buffer {
    stintmate_ipc_buffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    }
}

fn last_error() -> String {
    let mut buf = vec![0 as c_char; 512];
    let n = unsafe { stintmate_ipc_last_error_message(buf.as_mut_ptr(), buf.len()) };
    let s = unsafe { CStr::from_ptr(buf.as_ptr()) }.to_str().unwrap();
    assert_eq!(s.len(), n.min(511));
    s.to_owned()
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Rig {
    ep: *mut stintmate_ipc_endpoint,
    client: *mut stintmate_ipc_client,
    key: PathBuf,
}

impl Rig {
    fn new() -> Rig {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tag = format!(
            "{}-{n}-{:x}",
            std::process::id(),
            stintmate_ipc::handshake::new_nonce()[0]
        );
        let key = std::env::temp_dir().join(format!("stintmate-ffi-key-{tag}"));
        let pipe = c(&format!("stintmate-ffi-{tag}"));
        let keyc = c(key.to_str().unwrap());
        assert_eq!(unsafe { stintmate_ipc_ensure_key(keyc.as_ptr()) }, 0);
        let (app_s, app_c) = (c("server"), c("client"));
        let info = |app: &CString| stintmate_ipc_peer_info {
            app: app.as_ptr(),
            name: std::ptr::null(),
            version: std::ptr::null(),
            process: std::ptr::null(),
            meta: std::ptr::null(),
            meta_len: 0,
        };
        let mut ep = std::ptr::null_mut();
        let st = unsafe {
            stintmate_ipc_endpoint_new(pipe.as_ptr(), &info(&app_s), keyc.as_ptr(), 4, &mut ep)
        };
        assert_eq!(st, 0, "{}", last_error());
        let mut client = std::ptr::null_mut();
        let st = unsafe {
            stintmate_ipc_client_new(pipe.as_ptr(), &info(&app_c), keyc.as_ptr(), &mut client)
        };
        assert_eq!(st, 0, "{}", last_error());
        Rig { ep, client, key }
    }

    /// Methods and channels first, then `go`.
    fn go(&self) {
        assert_eq!(
            unsafe { stintmate_ipc_endpoint_start(self.ep) },
            0,
            "{}",
            last_error()
        );
        assert_eq!(unsafe { stintmate_ipc_client_start(self.client) }, 0);
        assert_eq!(
            unsafe { stintmate_ipc_client_wait_connected(self.client, 5000) },
            0
        );
    }

    fn call(&self, method: &str, params: &Value, timeout_ms: u32) -> (i32, Vec<u8>) {
        let m = c(method);
        let p = mp(params);
        let mut out = empty();
        let st = unsafe {
            stintmate_ipc_call(
                self.client,
                m.as_ptr(),
                1,
                p.as_ptr(),
                p.len(),
                timeout_ms,
                &mut out,
            )
        };
        (st, take(out))
    }

    fn method(&self, name: &str, cb: stintmate_ipc_handler_cb, user: *mut c_void) {
        let n = c(name);
        let st = unsafe {
            stintmate_ipc_endpoint_method(self.ep, n.as_ptr(), 0, std::ptr::null(), 0, cb, user)
        };
        assert_eq!(st, 0, "{}", last_error());
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        unsafe {
            stintmate_ipc_client_free(self.client);
            stintmate_ipc_endpoint_free(self.ep);
        }
        let _ = std::fs::remove_file(&self.key);
    }
}

unsafe extern "C-unwind" fn echo(
    _user: *mut c_void,
    r: *mut stintmate_ipc_responder,
    params: *const u8,
    len: usize,
) {
    unsafe { stintmate_ipc_respond(r, params, len) };
}

unsafe extern "C-unwind" fn fail_with(
    _user: *mut c_void,
    r: *mut stintmate_ipc_responder,
    _params: *const u8,
    _len: usize,
) {
    let (code, msg) = (c("nope"), c("not today"));
    unsafe { stintmate_ipc_respond_error(r, code.as_ptr(), msg.as_ptr(), std::ptr::null(), 0) };
}

unsafe extern "C-unwind" fn panics(
    _user: *mut c_void,
    _r: *mut stintmate_ipc_responder,
    _params: *const u8,
    _len: usize,
) {
    panic!("a handler panic");
}

/// Never answers on its own: parks the responder for the test to answer later.
unsafe extern "C-unwind" fn park(
    user: *mut c_void,
    r: *mut stintmate_ipc_responder,
    _params: *const u8,
    _len: usize,
) {
    let parked = unsafe { &*(user as *const Mutex<Vec<usize>>) };
    parked.lock().unwrap().push(r as usize);
}

fn text(v: &str) -> Value {
    Value::from(v)
}

#[test]
fn version_matches_the_crate() {
    let v = unsafe { CStr::from_ptr(stintmate_ipc_version()) };
    assert_eq!(v.to_str().unwrap(), stintmate_ipc::VERSION);
}

#[test]
fn echo_and_remote_errors() {
    let _s = serial();
    let rig = Rig::new();
    rig.method("t.echo", Some(echo), std::ptr::null_mut());
    rig.method("t.fail", Some(fail_with), std::ptr::null_mut());
    rig.go();

    let params = codec::map([("a", Value::from(1)), ("b", text("two"))]);
    let (st, out) = rig.call("t.echo", &params, 2000);
    assert_eq!(st, STINTMATE_IPC_OK);
    assert_eq!(codec::decode(&out).unwrap(), params);

    let (st, out) = rig.call("t.fail", &Value::Nil, 2000);
    assert_eq!(st, STINTMATE_IPC_E_REMOTE);
    let e = codec::decode(&out).unwrap();
    assert_eq!(
        codec::get(&e, "code").and_then(|v| v.as_str()),
        Some("nope")
    );
    assert!(last_error().contains("not today"));

    let (st, _) = rig.call("t.missing", &Value::Nil, 2000);
    assert_eq!(st, STINTMATE_IPC_E_REMOTE);
}

#[test]
fn invalid_arguments_are_statuses_not_crashes() {
    let _s = serial();
    let mut out = std::ptr::null_mut();
    let st = unsafe {
        stintmate_ipc_client_new(
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, STINTMATE_IPC_E_INVALID_ARG);
    assert!(last_error().contains("pipe"));
    assert_eq!(
        unsafe { stintmate_ipc_call_cancel(std::ptr::null_mut()) },
        STINTMATE_IPC_E_INVALID_ARG
    );
    unsafe { stintmate_ipc_free_buffer(empty()) };
    assert_eq!(unsafe { stintmate_ipc_valid_app(c("Bad App").as_ptr()) }, 0);
    assert_eq!(unsafe { stintmate_ipc_valid_app(c("pitwall").as_ptr()) }, 1);
    let mut p = empty();
    assert_eq!(
        unsafe { stintmate_ipc_pipe_path(c("pitwall").as_ptr(), std::ptr::null(), &mut p) },
        0
    );
    assert!(
        String::from_utf8(take(p))
            .unwrap()
            .starts_with("\\\\.\\pipe\\")
    );
}

#[test]
fn a_panic_is_a_status() {
    let _s = serial();
    let st = guard(|| panic!("inside"));
    assert_eq!(st, STINTMATE_IPC_E_PANIC);
    assert!(last_error().contains("inside"));
}

#[test]
fn timeout_then_a_late_answer_is_harmless() {
    let _s = serial();
    let rig = Rig::new();
    let parked: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    rig.method("t.park", Some(park), &parked as *const _ as *mut c_void);
    rig.go();

    let started = Instant::now();
    // Room for a loaded machine: a request that waits in the pool past its deadline is answered
    // with the remote `timeout` (SPEC 10.3) instead of reaching the handler.
    let (st, _) = rig.call("t.park", &Value::Nil, 1000);
    assert_eq!(st, STINTMATE_IPC_E_TIMEOUT);
    assert!(started.elapsed() < Duration::from_secs(3));
    wait_for("the handler to be called", || {
        !parked.lock().unwrap().is_empty()
    });
    // Answering after the deadline must not fail or leak a pending state.
    let r = parked.lock().unwrap().pop().unwrap() as *mut stintmate_ipc_responder;
    assert_eq!(unsafe { stintmate_ipc_respond(r, std::ptr::null(), 0) }, 0);
}

#[test]
fn cancel_and_wait() {
    let _s = serial();
    let rig = Rig::new();
    let parked: Mutex<Vec<usize>> = Mutex::new(Vec::new());
    rig.method("t.park", Some(park), &parked as *const _ as *mut c_void);
    rig.method("t.echo", Some(echo), std::ptr::null_mut());
    rig.go();

    let m = c("t.park");
    let mut call = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            stintmate_ipc_call_async(rig.client, m.as_ptr(), 1, std::ptr::null(), 0, 0, &mut call)
        },
        0
    );
    let mut out = empty();
    assert_eq!(
        unsafe { stintmate_ipc_call_wait(call, 100, &mut out) },
        STINTMATE_IPC_E_WAIT_TIMEOUT
    );
    assert_eq!(unsafe { stintmate_ipc_call_cancel(call) }, 1);
    assert_eq!(
        unsafe { stintmate_ipc_call_wait(call, u32::MAX, &mut out) },
        STINTMATE_IPC_E_CANCELLED
    );
    unsafe { stintmate_ipc_call_free(call) };
    // The handler learns the caller gave up.
    wait_for("the handler to be called", || {
        !parked.lock().unwrap().is_empty()
    });
    let r = parked.lock().unwrap().pop().unwrap() as *mut stintmate_ipc_responder;
    wait_for(
        "the cancel to arrive",
        || unsafe { stintmate_ipc_responder_is_cancelled(r) } == 1,
    );
    unsafe { stintmate_ipc_respond(r, std::ptr::null(), 0) };

    // An async call that completes.
    let m = c("t.echo");
    let p = mp(&text("hi"));
    assert_eq!(
        unsafe {
            stintmate_ipc_call_async(
                rig.client,
                m.as_ptr(),
                1,
                p.as_ptr(),
                p.len(),
                2000,
                &mut call,
            )
        },
        0
    );
    assert_eq!(
        unsafe { stintmate_ipc_call_wait(call, u32::MAX, &mut out) },
        0
    );
    assert_eq!(codec::decode(&take(out)).unwrap(), text("hi"));
    unsafe { stintmate_ipc_call_free(call) };
}

#[derive(Default)]
struct Collector {
    seen: Mutex<Vec<(i32, u64, Vec<u8>)>>,
    panic_first: bool,
}

unsafe extern "C-unwind" fn collect(user: *mut c_void, d: *const stintmate_ipc_delivery) {
    let col = unsafe { &*(user as *const Collector) };
    let d = unsafe { &*d };
    let data = unsafe { std::slice::from_raw_parts(d.data, d.len) }.to_vec();
    let mut seen = col.seen.lock().unwrap();
    let first = seen.is_empty();
    seen.push((d.kind, d.seq, data));
    if col.panic_first && first {
        drop(seen);
        panic!("a subscriber panic");
    }
}

fn subscribe(rig: &Rig, channel: &str, col: &Collector) -> *mut stintmate_ipc_subscription {
    let ch = c(channel);
    let mut sub = std::ptr::null_mut();
    let st = unsafe {
        stintmate_ipc_subscribe(
            rig.client,
            ch.as_ptr(),
            1,
            std::ptr::null(),
            0,
            Some(collect),
            None,
            col as *const _ as *mut c_void,
            &mut sub,
        )
    };
    assert_eq!(st, 0, "{}", last_error());
    wait_for(
        "the subscription",
        || unsafe { stintmate_ipc_subscription_is_active(sub) } == 1,
    );
    sub
}

#[test]
fn subscribe_in_order_and_survive_a_panicking_callback() {
    let _s = serial();
    let rig = Rig::new();
    let name = c("t.events");
    let mut ev = std::ptr::null_mut();
    assert_eq!(
        unsafe { stintmate_ipc_event_channel(rig.ep, name.as_ptr(), std::ptr::null(), 0, &mut ev) },
        0
    );
    rig.go();

    let col = Collector {
        panic_first: true,
        ..Collector::default()
    };
    let sub = subscribe(&rig, "t.events", &col);
    for i in 0..200i64 {
        let p = mp(&Value::from(i));
        assert_eq!(
            unsafe { stintmate_ipc_publish(ev, p.as_ptr(), p.len(), STINTMATE_IPC_TS_NOW) },
            0
        );
    }
    wait_for("200 events", || col.seen.lock().unwrap().len() >= 200);
    let seen = col.seen.lock().unwrap();
    for (i, (kind, seq, data)) in seen.iter().enumerate() {
        assert_eq!(*kind, STINTMATE_IPC_KIND_EVENT);
        assert_eq!(*seq, i as u64 + 1);
        assert_eq!(codec::decode(data).unwrap().as_i64(), Some(i as i64));
    }
    drop(seen);
    unsafe {
        stintmate_ipc_unsubscribe(sub);
        stintmate_ipc_channel_free(ev);
    }
}

#[test]
fn state_snapshot_and_stream_samples() {
    let _s = serial();
    let rig = Rig::new();
    let (sname, tname) = (c("t.state"), c("t.stream"));
    let initial = mp(&text("start"));
    let mut state = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            stintmate_ipc_state_channel(
                rig.ep,
                sname.as_ptr(),
                initial.as_ptr(),
                initial.len(),
                std::ptr::null(),
                0,
                &mut state,
            )
        },
        0
    );
    let layout = mp(&Layout::new([("n", FieldType::U32, 1)], Some("t"))
        .unwrap()
        .to_value());
    let mut stream = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            stintmate_ipc_stream_channel(
                rig.ep,
                tname.as_ptr(),
                layout.as_ptr(),
                layout.len(),
                std::ptr::null(),
                0,
                0.0,
                0,
                &mut stream,
            )
        },
        0,
        "{}",
        last_error()
    );
    rig.go();

    let states = Collector::default();
    let s1 = subscribe(&rig, "t.state", &states);
    wait_for("the snapshot", || !states.seen.lock().unwrap().is_empty());
    assert_eq!(
        codec::decode(&states.seen.lock().unwrap()[0].2).unwrap(),
        text("start")
    );

    let samples = Collector::default();
    let s2 = subscribe(&rig, "t.stream", &samples);
    for n in 0u32..5 {
        let body = n.to_le_bytes();
        assert_eq!(
            unsafe {
                stintmate_ipc_publish(stream, body.as_ptr(), body.len(), 1_000 + i64::from(n))
            },
            0
        );
    }
    wait_for("5 samples", || samples.seen.lock().unwrap().len() >= 5);
    for (i, (kind, _, data)) in samples.seen.lock().unwrap().iter().enumerate() {
        assert_eq!(*kind, STINTMATE_IPC_KIND_SAMPLE);
        assert_eq!(data.as_slice(), (i as u32).to_le_bytes());
    }
    assert_eq!(unsafe { stintmate_ipc_channel_subscribers(stream) }, 1);
    unsafe {
        stintmate_ipc_unsubscribe(s1);
        stintmate_ipc_unsubscribe(s2);
        stintmate_ipc_channel_free(state);
        stintmate_ipc_channel_free(stream);
    }
}

#[test]
fn a_panicking_handler_answers_internal() {
    let _s = serial();
    let rig = Rig::new();
    rig.method("t.panic", Some(panics), std::ptr::null_mut());
    rig.method("t.echo", Some(echo), std::ptr::null_mut());
    rig.go();
    let (st, out) = rig.call("t.panic", &Value::Nil, 2000);
    assert_eq!(st, STINTMATE_IPC_E_REMOTE);
    let e = codec::decode(&out).unwrap();
    assert_eq!(
        codec::get(&e, "code").and_then(|v| v.as_str()),
        Some("internal")
    );
    // The endpoint is still alive.
    assert_eq!(rig.call("t.echo", &text("ok"), 2000).0, 0);
}

#[test]
fn ten_thousand_calls_leak_no_buffers() {
    let _s = serial();
    let rig = Rig::new();
    rig.method("t.echo", Some(echo), std::ptr::null_mut());
    rig.go();
    let before = stintmate_ipc_live_buffers();
    let params = codec::map([("n", Value::from(7)), ("s", text("payload"))]);
    for _ in 0..10_000 {
        let (st, out) = rig.call("t.echo", &params, 5000);
        assert_eq!(st, 0);
        assert!(!out.is_empty());
    }
    assert_eq!(stintmate_ipc_live_buffers(), before);
}
