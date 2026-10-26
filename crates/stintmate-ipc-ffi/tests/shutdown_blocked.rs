//! Shutdown honours its deadline even when a host handler never returns. Own process:
//! shutdown ends the library.

mod common;

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::*;

static RELEASE: AtomicBool = AtomicBool::new(false);
static ENTERED: AtomicBool = AtomicBool::new(false);

unsafe extern "C-unwind" fn parked(
    _: *mut c_void,
    responder: *mut c_void,
    params: *const u8,
    len: usize,
) {
    ENTERED.store(true, Ordering::SeqCst);
    while !RELEASE.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(5));
    }
    unsafe { (api().respond)(responder, params, len) };
}

#[test]
fn shutdown_returns_wait_timeout_while_a_handler_is_parked() {
    let a = api();
    let rig = Rig::new();
    rig.method("t.parked", Some(parked), std::ptr::null_mut(), None);
    rig.go();
    let client = rig.client as usize;
    let caller = std::thread::spawn(move || {
        let mut out = NO_BUFFER;
        let st = unsafe {
            (api().client_call)(
                client as *mut c_void,
                c"t.parked".as_ptr(),
                std::ptr::null(),
                0,
                &call_opts(20_000),
                &mut out,
            )
        };
        take(out);
        st
    });
    wait_for("the handler", || ENTERED.load(Ordering::SeqCst));

    // Run on a thread so a hang fails the test instead of blocking it forever.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let t = Instant::now();
        let st = unsafe { (api().shutdown)(100) };
        let _ = tx.send((st, t.elapsed()));
    });
    let (st, took) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown(100) returned within its deadline");
    assert_eq!(st, -17, "E_WAIT_TIMEOUT");
    assert!(took < Duration::from_secs(5), "{took:?}");

    // Let the handler go; a second call finishes the work.
    RELEASE.store(true, Ordering::SeqCst);
    assert_eq!(unsafe { (a.shutdown)(10_000) }, 0, "{}", last_message());
    let _ = caller.join().unwrap();
    rig.free();
}
