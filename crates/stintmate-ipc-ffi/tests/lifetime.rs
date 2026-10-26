//! Lifetimes through the C functions: `release` runs exactly once after the last callback,
//! nothing is called after a free, a handle can be freed from inside its own callback.
//! (Shutdown, which ends the whole library for the process, is in `shutdown.rs`.)

mod common;

use std::ffi::{CStr, c_char, c_void};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicUsize, Ordering};
use std::time::Duration;

use common::*;

#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    released: AtomicUsize,
    ep: AtomicPtr<c_void>,
    freed: AtomicBool,
    last_level: AtomicI32,
    log_thread_is_test_thread: AtomicBool,
}

fn probe() -> &'static Probe {
    Box::leak(Box::default())
}

fn ptr(p: &'static Probe) -> *mut c_void {
    std::ptr::from_ref(p).cast_mut().cast()
}

unsafe extern "C-unwind" fn release(ud: *mut c_void) {
    unsafe { &*ud.cast::<Probe>() }
        .released
        .fetch_add(1, Ordering::SeqCst);
}

unsafe extern "C-unwind" fn echo(
    ud: *mut c_void,
    responder: *mut c_void,
    params: *const u8,
    len: usize,
) {
    let p = unsafe { &*ud.cast::<Probe>() };
    p.calls.fetch_add(1, Ordering::SeqCst);
    unsafe { (api().respond)(responder, params, len) };
}

/// Answers, then frees the endpoint it runs on, from inside its own callback.
unsafe extern "C-unwind" fn echo_and_free(
    ud: *mut c_void,
    responder: *mut c_void,
    params: *const u8,
    len: usize,
) {
    let p = unsafe { &*ud.cast::<Probe>() };
    p.calls.fetch_add(1, Ordering::SeqCst);
    unsafe { (api().respond)(responder, params, len) };
    let ep = p.ep.swap(std::ptr::null_mut(), Ordering::SeqCst);
    if !ep.is_null() {
        unsafe { (api().endpoint_free)(ep) };
        p.freed.store(true, Ordering::SeqCst);
    }
}

fn settle() {
    let a = api();
    wait_for(
        "handles to be released",
        || unsafe { (a.live_handles)() } == 0,
    );
    assert_eq!(unsafe { (a.live_buffers)() }, 0);
}

#[test]
fn release_runs_once_after_the_last_callback_and_nothing_runs_after_free() {
    let _s = serial();
    let a = api();
    let p = probe();
    let rig = Rig::new();
    rig.method("t.echo", Some(echo), ptr(p), Some(release));
    rig.go();
    for _ in 0..3 {
        assert_eq!(rig.call("t.echo", 5000).0, 0, "{}", last_message());
    }
    assert_eq!(p.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        p.released.load(Ordering::SeqCst),
        0,
        "released while the method is offered"
    );

    unsafe { (a.endpoint_free)(rig.ep) };
    wait_for("the release", || p.released.load(Ordering::SeqCst) == 1);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(p.released.load(Ordering::SeqCst), 1, "released twice");

    // The server is gone: no call reaches the handler any more.
    assert_ne!(rig.call("t.echo", 500).0, 0);
    assert_eq!(p.calls.load(Ordering::SeqCst), 3);
    unsafe { (a.client_free)(rig.client) };
    settle();
}

#[test]
fn remove_method_releases_user_data() {
    let _s = serial();
    let a = api();
    let p = probe();
    let rig = Rig::new();
    rig.method("t.echo", Some(echo), ptr(p), Some(release));
    rig.go();
    assert_eq!(rig.call("t.echo", 5000).0, 0);
    assert_eq!(
        unsafe { (a.endpoint_remove_method)(rig.ep, c"t.echo".as_ptr()) },
        0
    );
    wait_for("the release", || p.released.load(Ordering::SeqCst) == 1);
    assert_eq!(
        rig.call("t.echo", 5000).0,
        -13,
        "a removed method answers a remote error"
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(p.released.load(Ordering::SeqCst), 1);
    rig.free();
    settle();
}

#[test]
fn a_handle_can_be_freed_from_inside_its_own_callback() {
    let _s = serial();
    let a = api();
    let p = probe();
    let rig = Rig::new();
    p.ep.store(rig.ep, Ordering::SeqCst);
    rig.method("t.free", Some(echo_and_free), ptr(p), Some(release));
    rig.go();
    // The endpoint stops inside the handler, so the answer may or may not get out first.
    let _ = rig.call("t.free", 5000);
    wait_for("the free inside the callback", || {
        p.freed.load(Ordering::SeqCst)
    });
    wait_for("the release", || p.released.load(Ordering::SeqCst) == 1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(p.released.load(Ordering::SeqCst), 1);
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    unsafe { (a.client_free)(rig.client) };
    settle();
}

unsafe extern "C-unwind" fn log_record(
    ud: *mut c_void,
    level: i32,
    target: *const c_char,
    message: *const c_char,
) {
    let p = unsafe { &*ud.cast::<Probe>() };
    assert!(!unsafe { CStr::from_ptr(target) }.to_bytes().is_empty());
    assert!(!unsafe { CStr::from_ptr(message) }.to_bytes().is_empty());
    p.last_level.store(level, Ordering::SeqCst);
    p.log_thread_is_test_thread.store(
        std::thread::current()
            .name()
            .is_some_and(|n| n.starts_with("log_")),
        Ordering::SeqCst,
    );
    p.calls.fetch_add(1, Ordering::SeqCst);
}

/// A client with the wrong key: the server logs a failed handshake.
fn provoke_a_log(rig: &Rig) {
    let a = api();
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
    assert_eq!(
        unsafe {
            (a.client_new)(
                rig.pipe.as_ptr(),
                &info,
                &wrong,
                std::ptr::null(),
                &mut intruder,
            )
        },
        0
    );
    unsafe { (a.client_start)(intruder) };
    let _ = unsafe { (a.client_wait_connected)(intruder, 1500, std::ptr::null_mut()) };
    unsafe { (a.client_free)(intruder) };
}

#[test]
fn log_callback_gets_records_on_its_own_thread_and_is_released_when_replaced() {
    let _s = serial();
    let a = api();
    let (first, second) = (probe(), probe());
    assert_eq!(
        unsafe { (a.set_log_callback)(Some(log_record), ptr(first), Some(release), 10) },
        0
    );
    let rig = Rig::new();
    rig.go();
    provoke_a_log(&rig);
    wait_for("a log record", || first.calls.load(Ordering::SeqCst) > 0);
    assert!((10..=40).contains(&first.last_level.load(Ordering::SeqCst)));
    assert!(
        !first.log_thread_is_test_thread.load(Ordering::SeqCst),
        "records come on the log thread"
    );
    assert_eq!(first.released.load(Ordering::SeqCst), 0);

    // Replacing the callback releases the previous user_data once; it gets nothing afterwards.
    assert_eq!(
        unsafe { (a.set_log_callback)(Some(log_record), ptr(second), Some(release), 10) },
        0
    );
    wait_for("the release of the first", || {
        first.released.load(Ordering::SeqCst) == 1
    });
    let seen = first.calls.load(Ordering::SeqCst);
    provoke_a_log(&rig);
    wait_for("a record for the second", || {
        second.calls.load(Ordering::SeqCst) > 0
    });
    assert_eq!(
        first.calls.load(Ordering::SeqCst),
        seen,
        "a replaced callback was called"
    );
    assert_eq!(first.released.load(Ordering::SeqCst), 1);

    // The level filter: errors only, so a handshake failure (info) is not delivered.
    assert_eq!(unsafe { (a.set_log_level)(40) }, 0);
    std::thread::sleep(Duration::from_millis(100));
    let seen = second.calls.load(Ordering::SeqCst);
    provoke_a_log(&rig);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(second.calls.load(Ordering::SeqCst), seen);
    assert_eq!(unsafe { (a.set_log_level)(41) }, -11);

    // A null callback clears it and releases the user_data.
    assert_eq!(
        unsafe { (a.set_log_callback)(None, std::ptr::null_mut(), None, 0) },
        0
    );
    wait_for("the release of the second", || {
        second.released.load(Ordering::SeqCst) == 1
    });
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(second.released.load(Ordering::SeqCst), 1);
    rig.free();
    settle();
}
