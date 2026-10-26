//! Thread hooks: `on_start` before the first callback of each library thread, `on_stop` when
//! it ends, and the rule that they are set before the first handle. Runs in its own process
//! (shutdown at the end ends the library).

mod common;

use std::ffi::{c_char, c_void};
use std::sync::Mutex;
use std::thread::ThreadId;
use std::time::Duration;

use common::*;

static EVENTS: Mutex<Vec<(ThreadId, &'static str)>> = Mutex::new(Vec::new());

fn push(what: &'static str) {
    EVENTS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push((std::thread::current().id(), what));
}

fn events() -> Vec<(ThreadId, &'static str)> {
    EVENTS.lock().unwrap_or_else(|p| p.into_inner()).clone()
}

unsafe extern "C-unwind" fn on_start(_: *mut c_void) {
    push("start");
}

unsafe extern "C-unwind" fn on_stop(_: *mut c_void) {
    push("stop");
}

unsafe extern "C-unwind" fn echo(
    _: *mut c_void,
    responder: *mut c_void,
    params: *const u8,
    len: usize,
) {
    push("handler");
    unsafe { (api().respond)(responder, params, len) };
}

unsafe extern "C-unwind" fn log_cb(_: *mut c_void, _: i32, _: *const c_char, _: *const c_char) {
    push("log");
}

#[test]
fn hooks_run_once_per_library_thread_before_its_first_callback() {
    let a = api();
    assert_eq!(
        unsafe { (a.set_thread_hooks)(Some(on_start), Some(on_stop), std::ptr::null_mut()) },
        0
    );
    assert_eq!(
        unsafe { (a.set_log_callback)(Some(log_cb), std::ptr::null_mut(), None, 10) },
        0
    );
    let rig = Rig::new();

    // Too late for hooks once a handle exists.
    assert_eq!(
        unsafe { (a.set_thread_hooks)(None, None, std::ptr::null_mut()) },
        -11
    );
    assert!(
        last_message().contains("before the first handle"),
        "{}",
        last_message()
    );

    rig.method("t.echo", Some(echo), std::ptr::null_mut(), None);
    rig.go();
    assert_eq!(rig.call("t.echo", 5000).0, 0, "{}", last_message());
    // A failed handshake makes the server log.
    let wrong = key_source(&[9u8; 32]);
    let info = PeerInfo {
        app: c"intruder".as_ptr(),
        name: std::ptr::null(),
        version: std::ptr::null(),
        process: std::ptr::null(),
        meta: std::ptr::null(),
        meta_len: 0,
    };
    let mut intruder = std::ptr::null_mut();
    unsafe {
        (a.client_new)(
            rig.pipe.as_ptr(),
            &info,
            &wrong,
            std::ptr::null(),
            &mut intruder,
        )
    };
    unsafe { (a.client_start)(intruder) };
    wait_for("a log record", || events().iter().any(|e| e.1 == "log"));
    unsafe { (a.client_free)(intruder) };

    let ev = events();
    let mut callback_threads = Vec::new();
    for (tid, what) in &ev {
        if *what == "handler" || *what == "log" {
            if !callback_threads.contains(tid) {
                callback_threads.push(*tid);
            }
            let first_cb = ev
                .iter()
                .position(|e| e.0 == *tid && e.1 != "start")
                .unwrap();
            let starts: Vec<_> = ev
                .iter()
                .enumerate()
                .filter(|(_, e)| e.0 == *tid && e.1 == "start")
                .map(|(i, _)| i)
                .collect();
            assert_eq!(starts.len(), 1, "on_start once per thread");
            assert!(starts[0] < first_cb, "on_start before the first callback");
        }
    }
    assert!(
        callback_threads.len() >= 2,
        "the handler pool and the log thread: {ev:?}"
    );
    assert!(!callback_threads.contains(&std::thread::current().id()));

    // Shutdown ends the library threads, with their on_stop, before it returns.
    let log_thread = ev.iter().find(|e| e.1 == "log").unwrap().0;
    assert_eq!(unsafe { (a.shutdown)(5000) }, 0, "{}", last_message());
    let ev = events();
    assert!(
        ev.iter().any(|e| e.0 == log_thread && e.1 == "stop"),
        "the log thread stopped"
    );
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        events().len(),
        ev.len(),
        "no hook or callback after shutdown returned"
    );
    rig.free();
}
