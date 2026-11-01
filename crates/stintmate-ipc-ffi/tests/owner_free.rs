//! Two guarantees through the C functions: every id the library reports for a connection is
//! the one `stintmate_ipc_peer_id` returns, and freeing the owner of a node (an endpoint or a
//! client) runs the `release` of every method and hook registered on it exactly once.

mod common;
#[path = "common/peer_api.rs"]
mod papi;

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use stintmate_ipc::codec::{self, Value};

fn released(l: &Arc<Log>) -> usize {
    l.released.load(Ordering::SeqCst)
}

unsafe extern "C-unwind" fn never(_: *mut c_void, _: *mut c_void, _: *const u8, _: usize) {}

unsafe extern "C-unwind" fn on_peer(user: *mut c_void, event: i32, peer: *mut c_void) {
    let id = unsafe { (papi::p().peer_id)(peer) };
    unsafe { log(user) }.push(format!("peer:{event}:{id}"));
}

unsafe extern "C-unwind" fn on_sub(
    user: *mut c_void,
    _ctx: *mut c_void,
    peer: *mut c_void,
    view: *const u8,
    len: usize,
) -> i32 {
    let v = codec::decode(unsafe { std::slice::from_raw_parts(view, len) }).unwrap();
    let in_view = key_of(&v, "peer").as_u64().unwrap();
    let id = unsafe { (papi::p().peer_id)(peer) };
    unsafe { log(user) }.push(format!("sub:{in_view}:{id}"));
    0
}

unsafe extern "C-unwind" fn accept(user: *mut c_void, view: *const u8, len: usize) -> i32 {
    let v = codec::decode(unsafe { std::slice::from_raw_parts(view, len) }).unwrap();
    let conn = key_of(&v, "connection").as_u64().unwrap();
    unsafe { log(user) }.push(format!("conn:{conn}"));
    1
}

unsafe extern "C-unwind" fn on_active(_: *mut c_void, _: i32) {}

fn new_event(fx: &EpFix, name: &std::ffi::CStr) -> *mut c_void {
    let mut ch = std::ptr::null_mut();
    let st = unsafe { (api().event_channel)(fx.node, name.as_ptr(), [1u32].as_ptr(), 1, &mut ch) };
    assert_eq!(st, 0, "{}", last_message());
    ch
}

fn field(lines: &[String], prefix: &str) -> Vec<u64> {
    lines
        .iter()
        .filter_map(|s| s.strip_prefix(prefix))
        .flat_map(|s| s.split(':').map(|n| n.parse::<u64>().unwrap()))
        .collect()
}

#[test]
fn every_connection_id_is_the_peer_id() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    let ch = new_event(&fx, c"ev");
    assert_eq!(
        unsafe { (a.on_peer)(fx.node, Some(on_peer), l.ud(), Some(release_log)) },
        0
    );
    assert_eq!(
        unsafe { (a.on_subscribe)(ch, Some(on_sub), l.ud(), Some(release_log)) },
        0
    );
    let peer = fx.start();
    let _sub = peer.subscribe("ev", Default::default(), |_| {}).unwrap();
    wait_for("the hooks", || {
        let lines = l.lines();
        lines.iter().any(|s| s.starts_with("sub:")) && lines.iter().any(|s| s.starts_with("peer:1:"))
    });

    // The peer from the peers list.
    let mut got = [std::ptr::null_mut::<c_void>(); 4];
    let mut count = 0usize;
    assert_eq!(
        unsafe { (a.peers)(fx.node, got.as_mut_ptr(), 4, &mut count) },
        0
    );
    assert_eq!(count, 1);
    let listed = unsafe { (papi::p().peer_id)(got[0]) };
    assert_ne!(listed, 0);

    let lines = l.lines();
    let hooked = field(&lines, "peer:1:");
    assert_eq!(hooked.len(), 1);
    assert_eq!(hooked[0], listed, "on_peer vs peers list");
    let sub = field(&lines, "sub:");
    assert_eq!(sub, vec![listed, listed], "subscribe hook: view and peer");

    // The connection in a view given to publish's accept.
    let bytes = mp(&Value::from("x"));
    let mut queued = 0usize;
    assert_eq!(
        unsafe {
            (a.publish)(
                ch,
                bytes.as_ptr(),
                bytes.len(),
                i64::MIN,
                0,
                Some(accept),
                l.ud(),
                &mut queued,
            )
        },
        0
    );
    let conns = field(&l.lines(), "conn:");
    assert_eq!(conns, vec![listed], "publish accept view");

    unsafe { (papi::p().peer_free)(got[0]) };
    unsafe { (a.channel_free)(ch) };
    fx.free();
    assert_clean();
}

#[test]
fn freeing_an_endpoint_releases_its_methods_and_hooks_once() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let logs: Vec<Arc<Log>> = (0..5).map(|_| Arc::new(Log::default())).collect();
    assert_eq!(
        unsafe {
            (a.method)(
                fx.node,
                c"t.m".as_ptr(),
                std::ptr::null(),
                Some(never),
                logs[0].ud(),
                Some(release_log),
            )
        },
        0
    );
    assert_eq!(
        unsafe { (a.on_peer)(fx.node, Some(on_peer), logs[1].ud(), Some(release_log)) },
        0
    );
    let ch = new_event(&fx, c"ev");
    assert_eq!(
        unsafe { (a.on_subscribe)(ch, None, logs[2].ud(), Some(release_log)) },
        0
    );
    assert_eq!(
        unsafe { (a.on_subscribe)(ch, Some(on_sub), logs[3].ud(), Some(release_log)) },
        0
    );
    assert_eq!(
        unsafe { (a.on_active)(ch, Some(on_active), logs[4].ud(), Some(release_log)) },
        0
    );
    // Connected, so handlers and hooks are in use when the endpoint goes.
    let _peer = fx.start();
    std::thread::sleep(Duration::from_millis(100));
    for (i, l) in logs.iter().enumerate() {
        assert!(released(l) <= usize::from(i == 2), "released early: {i}");
    }

    fx.free();
    wait_for("all the releases", || logs.iter().all(|l| released(l) >= 1));
    std::thread::sleep(Duration::from_millis(200));
    for (i, l) in logs.iter().enumerate() {
        assert_eq!(released(l), 1, "registration {i}");
    }

    // The channel outlives its endpoint; freeing it does not release anything again.
    unsafe { (a.channel_free)(ch) };
    std::thread::sleep(Duration::from_millis(100));
    for l in &logs {
        assert_eq!(released(l), 1);
    }
    assert_clean();
}

#[test]
fn freeing_a_client_releases_what_it_offers_once() {
    let _s = serial();
    let a = api();
    let fx = ClientFix::new();
    let (m, h) = (Arc::new(Log::default()), Arc::new(Log::default()));
    assert_eq!(
        unsafe {
            (a.method)(
                fx.node,
                c"c.m".as_ptr(),
                std::ptr::null(),
                Some(never),
                m.ud(),
                Some(release_log),
            )
        },
        0
    );
    assert_eq!(
        unsafe { (a.on_peer)(fx.node, Some(on_peer), h.ud(), Some(release_log)) },
        0
    );
    let _peer = fx.start();
    assert_eq!((released(&m), released(&h)), (0, 0));
    fx.free();
    wait_for("the releases", || released(&m) == 1 && released(&h) == 1);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!((released(&m), released(&h)), (1, 1));
    assert_clean();
}

static HOLD_CONNECT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Logs the event; the CONNECT callback parks the hooks thread until the test lets it go.
unsafe extern "C-unwind" fn on_peer_parking(user: *mut c_void, event: i32, _peer: *mut c_void) {
    unsafe { log(user) }.push(format!("peer:{event}"));
    if event == 1 {
        while HOLD_CONNECT.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

#[test]
fn hooks_queued_before_an_endpoint_free_are_not_delivered_after_it() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    assert_eq!(
        unsafe { (a.on_peer)(fx.node, Some(on_peer_parking), l.ud(), Some(release_log)) },
        0
    );
    let _peer = fx.start();
    wait_for("the connect callback", || !l.lines().is_empty());
    // The hooks thread is parked in CONNECT: the DISCONNECT of the close and of the free's own
    // stop queue up behind it, and the free returns before any of them runs.
    fx.free();
    HOLD_CONNECT.store(false, Ordering::SeqCst);
    wait_for("the release", || released(&l) == 1);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        l.lines(),
        vec!["peer:1".to_owned()],
        "no hook after the free"
    );
    assert_clean();
}
