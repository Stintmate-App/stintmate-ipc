//! Shutdown with callbacks running. Runs in its own process: shutdown ends the library.

mod common;

use std::ffi::{c_char, c_void};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::*;

static BLOCK_LOG: AtomicBool = AtomicBool::new(false);
static LOG_ENTERED: AtomicBool = AtomicBool::new(false);
static LOG_CALLS: AtomicUsize = AtomicUsize::new(0);
static LOG_RELEASED: AtomicUsize = AtomicUsize::new(0);
static HANDLER_ENTERED: AtomicBool = AtomicBool::new(false);
static HANDLER_DONE: AtomicBool = AtomicBool::new(false);
static HANDLER_CALLS: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_IN_CALLBACK: AtomicI32 = AtomicI32::new(0);

unsafe extern "C-unwind" fn log_cb(_: *mut c_void, _: i32, _: *const c_char, _: *const c_char) {
    LOG_CALLS.fetch_add(1, Ordering::SeqCst);
    LOG_ENTERED.store(true, Ordering::SeqCst);
    while BLOCK_LOG.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(5));
    }
}

unsafe extern "C-unwind" fn log_release(_: *mut c_void) {
    LOG_RELEASED.fetch_add(1, Ordering::SeqCst);
}

unsafe extern "C-unwind" fn slow(
    _: *mut c_void,
    responder: *mut c_void,
    params: *const u8,
    len: usize,
) {
    HANDLER_CALLS.fetch_add(1, Ordering::SeqCst);
    HANDLER_ENTERED.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(400));
    // Shutdown is the one function a callback must not call.
    SHUTDOWN_IN_CALLBACK.store(unsafe { (api().shutdown)(0) }, Ordering::SeqCst);
    HANDLER_DONE.store(true, Ordering::SeqCst);
    unsafe { (api().respond)(responder, params, len) };
}

fn intruder_info() -> PeerInfo {
    PeerInfo {
        app: c"intruder".as_ptr(),
        name: std::ptr::null(),
        version: std::ptr::null(),
        process: std::ptr::null(),
        meta: std::ptr::null(),
        meta_len: 0,
    }
}

#[test]
fn shutdown_waits_for_running_callbacks_and_nothing_runs_after_it() {
    let a = api();
    BLOCK_LOG.store(true, Ordering::SeqCst);
    assert_eq!(
        unsafe { (a.set_log_callback)(Some(log_cb), std::ptr::null_mut(), Some(log_release), 10) },
        0
    );
    let rig = Rig::new();
    rig.method("t.slow", Some(slow), std::ptr::null_mut(), None);
    rig.go();

    // A log callback that blocks (a handshake with the wrong key makes the server log), and a
    // handler that is running.
    let wrong = key_source(&[9u8; 32]);
    let mut intruder = std::ptr::null_mut();
    unsafe {
        (a.client_new)(
            rig.pipe.as_ptr(),
            &intruder_info(),
            &wrong,
            std::ptr::null(),
            &mut intruder,
        )
    };
    unsafe { (a.client_start)(intruder) };
    wait_for("the log callback to block", || {
        LOG_ENTERED.load(Ordering::SeqCst)
    });
    let client = rig.client as usize;
    let caller = std::thread::spawn(move || {
        let mut out = NO_BUFFER;
        let st = unsafe {
            (api().client_call)(
                client as *mut c_void,
                c"t.slow".as_ptr(),
                std::ptr::null(),
                0,
                &call_opts(10_000),
                &mut out,
            )
        };
        take(out);
        st
    });
    wait_for("the handler", || HANDLER_ENTERED.load(Ordering::SeqCst));

    // A short wait gives up; the library is already shut down for new work.
    let t = Instant::now();
    assert_eq!(unsafe { (a.shutdown)(50) }, -17);
    assert!(t.elapsed() < Duration::from_secs(5));
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.pipe_user)(std::ptr::null(), &mut out) }, -20);
    assert!(last_message().contains("shut down"), "{}", last_message());
    assert_eq!(
        LOG_RELEASED.load(Ordering::SeqCst),
        0,
        "released while a callback runs"
    );

    // Let the log callback go; shutdown now waits for the handler and finishes.
    BLOCK_LOG.store(false, Ordering::SeqCst);
    assert_eq!(unsafe { (a.shutdown)(10_000) }, 0, "{}", last_message());
    assert!(
        HANDLER_DONE.load(Ordering::SeqCst),
        "shutdown returned before the handler ended"
    );
    assert_eq!(
        SHUTDOWN_IN_CALLBACK.load(Ordering::SeqCst),
        -11,
        "shutdown inside a callback"
    );
    assert_eq!(
        LOG_RELEASED.load(Ordering::SeqCst),
        1,
        "the log user_data is released once"
    );

    // Nothing is called any more.
    let (logs, handlers) = (
        LOG_CALLS.load(Ordering::SeqCst),
        HANDLER_CALLS.load(Ordering::SeqCst),
    );
    let _ = caller.join().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(LOG_CALLS.load(Ordering::SeqCst), logs);
    assert_eq!(HANDLER_CALLS.load(Ordering::SeqCst), handlers);
    assert_eq!(LOG_RELEASED.load(Ordering::SeqCst), 1);

    // Later calls fail with E_SHUTDOWN; shutdown is idempotent; frees still work.
    assert_eq!(
        unsafe { (a.set_log_callback)(None, std::ptr::null_mut(), None, 0) },
        -20
    );
    let mut ep = std::ptr::null_mut();
    let ks = key_source(&[1u8; 32]);
    let info = PeerInfo {
        app: c"x".as_ptr(),
        ..intruder_info()
    };
    assert_eq!(
        unsafe { (a.endpoint_new)(&info, &ks, std::ptr::null(), &mut ep) },
        -20
    );
    assert_eq!(unsafe { (a.shutdown)(0) }, 0);
    unsafe { (a.client_free)(intruder) };
    rig.free();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(unsafe { (a.live_handles)() }, 0);
    assert_eq!(unsafe { (a.live_buffers)() }, 0);
}
