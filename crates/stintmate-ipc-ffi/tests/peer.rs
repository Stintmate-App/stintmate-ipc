//! Peers, calls and subscriptions through the C functions (spec 2.6), against a core
//! endpoint on a random pipe with a random key. Counters are global: tests run one at a time.

mod common;
#[path = "common/peer_api.rs"]
mod papi;
#[path = "common/server.rs"]
mod server;

use std::ffi::c_void;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{NO_BUFFER, serial, take};
use papi::*;
use server::Server;
use stintmate_ipc::codec::{self, Value};

fn text(v: &str) -> Value {
    Value::from(v)
}

fn call_peer(
    peer: *mut c_void,
    method: &str,
    params: &[u8],
    opts: Option<&CallOpts>,
) -> (i32, Vec<u8>) {
    let m = cs(method);
    let mut out = NO_BUFFER;
    let o = opts.map_or(std::ptr::null(), std::ptr::from_ref);
    let st =
        unsafe { (p().peer_call)(peer, m.as_ptr(), params.as_ptr(), params.len(), o, &mut out) };
    (st, take(out))
}

#[test]
fn peer_identity_info_and_decl() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (client, peer) = srv.connect();

    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (a.peer_get_info)(peer, &mut b) }, 0);
    let info = value(b);
    assert_eq!(get_str(&info, "app").as_deref(), Some("server"));
    assert!(codec::get(&info, "run").is_some());
    assert!(codec::get(&info, "meta").is_some());

    assert_eq!(unsafe { (a.peer_decl)(peer, &mut b) }, 0);
    let decl = value(b);
    let methods = codec::get(&decl, "methods").unwrap();
    assert!(codec::get(methods, "t.echo").is_some());
    let channels = codec::get(&decl, "channels").unwrap();
    assert!(codec::get(channels, "t.sm").is_some());

    // References of one connection share an id; the client's own peer is the same one.
    let again = unsafe { (a.peer_ref)(peer) };
    let mut from_client = std::ptr::null_mut();
    assert_eq!(unsafe { (a.client_peer)(client, &mut from_client) }, 0);
    let id = unsafe { (a.peer_id)(peer) };
    assert_ne!(id, 0);
    assert_eq!(unsafe { (a.peer_id)(again) }, id);
    assert_eq!(unsafe { (a.peer_id)(from_client) }, id);
    assert_eq!(unsafe { (a.peer_is_open)(peer) }, 1);
    assert_eq!(unsafe { (a.peer_close_reason)(peer, &mut b) }, 0);
    assert!(value(b).is_nil());

    assert_eq!(
        unsafe { (a.peer_get_info)(std::ptr::null(), &mut b) },
        E_INVALID_ARG
    );
    unsafe {
        (a.peer_free)(again);
        (a.peer_free)(from_client);
        (a.peer_free)(peer);
        (a.client_free)(client);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn sync_calls_progress_and_errors() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (client, peer) = srv.connect();
    let pr = Probe::leak();

    // No options: version 1, five seconds.
    let (st, out) = call_peer(peer, "t.echo", &mp(&text("hi")), None);
    assert_eq!(st, 0, "{}", common::last_message());
    assert_eq!(codec::decode(&out).unwrap(), text("hi"));

    // Progress: all three were delivered before the call returned.
    let mut opts = CallOpts::new(5000);
    opts.on_progress = Some(on_progress);
    opts.user_data = pr.ptr();
    opts.release = Some(release);
    let (st, out) = call_peer(peer, "t.progress", &[], Some(&opts));
    assert_eq!(st, 0);
    assert_eq!(codec::decode(&out).unwrap(), text("finished"));
    assert_eq!(pr.progress.load(Ordering::SeqCst), 3);
    assert_eq!(*pr.last.lock().unwrap(), mp(&Value::from(2i64)));
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);

    // A remote error: the status, and the code and message in the error record.
    let mut out = NO_BUFFER;
    let st = unsafe {
        (a.client_call)(
            client,
            cs("t.fail").as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, E_REMOTE);
    let e = last_map();
    assert_eq!(get_str(&e, "code").as_deref(), Some("boom"));
    assert_eq!(get_str(&e, "message").as_deref(), Some("it broke"));
    assert_eq!(get_i64(&e, "status"), Some(i64::from(E_REMOTE)));

    // Unknown method, bad arguments.
    let (st, _) = call_peer(peer, "t.none", &[], None);
    assert_eq!(st, E_REMOTE);
    assert_eq!(
        get_str(&last_map(), "code").as_deref(),
        Some("unknown_method")
    );
    let st = unsafe {
        (a.peer_call)(
            peer,
            std::ptr::null(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, E_INVALID_ARG);
    let (st, _) = call_peer(std::ptr::null_mut(), "t.echo", &[], None);
    assert_eq!(st, E_INVALID_ARG);

    // A deadline: the server is still working when it passes.
    let (st, _) = call_peer(peer, "t.slow", &[], Some(&CallOpts::new(150)));
    assert_eq!(st, E_TIMEOUT);
    // Zero means do not wait.
    let (st, _) = call_peer(peer, "t.slow", &[], Some(&CallOpts::new(0)));
    assert_eq!(st, E_TIMEOUT);
    srv.release_slow.store(true, Ordering::SeqCst);

    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(client);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn async_calls_wait_cancel_and_on_done() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (client, peer) = srv.connect();
    let pr = Probe::leak();
    let mut out = NO_BUFFER;

    // A call that stays in flight: wait times out, cancel takes effect, the wait then fails.
    let mut call = std::ptr::null_mut();
    let st = unsafe {
        (a.peer_call_async)(
            peer,
            cs("t.slow").as_ptr(),
            std::ptr::null(),
            0,
            &CallOpts::new(FOREVER),
            &mut call,
        )
    };
    assert_eq!(st, 0, "{}", common::last_message());
    assert_ne!(unsafe { (a.call_id)(call) }, 0);
    assert_eq!(unsafe { (a.call_is_done)(call) }, 0);
    assert_eq!(unsafe { (a.call_wait)(call, 50, &mut out) }, E_WAIT_TIMEOUT);
    assert_eq!(
        unsafe { (a.call_cancel)(call, cs("changed my mind").as_ptr()) },
        1
    );
    assert_eq!(unsafe { (a.call_wait)(call, 5000, &mut out) }, E_CANCELLED);
    assert_eq!(unsafe { (a.call_is_done)(call) }, 1);
    assert_eq!(unsafe { (a.call_cancel)(call, std::ptr::null()) }, 0);
    unsafe { (a.call_free)(call) };

    // on_done registered before the call ends: runs once on the hooks thread, with the same
    // handle, which can be waited for at once; release follows.
    let params = mp(&text("later"));
    let mut call = std::ptr::null_mut();
    srv.release_slow.store(false, Ordering::SeqCst);
    let st = unsafe {
        (a.peer_call_async)(
            peer,
            cs("t.slow").as_ptr(),
            params.as_ptr(),
            params.len(),
            &CallOpts::new(FOREVER),
            &mut call,
        )
    };
    assert_eq!(st, 0);
    let st = unsafe { (a.call_on_done)(call, Some(on_done), pr.ptr(), Some(release)) };
    assert_eq!(st, 0);
    // One on_done per call.
    assert_eq!(
        unsafe { (a.call_on_done)(call, Some(on_done), pr.ptr(), None) },
        E_INVALID_ARG
    );
    assert_eq!(pr.done.load(Ordering::SeqCst), 0);
    srv.release_slow.store(true, Ordering::SeqCst);
    wait_until("on_done", || pr.done.load(Ordering::SeqCst) == 1);
    wait_until("the release", || pr.released.load(Ordering::SeqCst) >= 1);
    assert_eq!(pr.done_ptr.load(Ordering::SeqCst), call);
    assert_eq!(*pr.text.lock().unwrap(), "0");
    assert_eq!(*pr.last.lock().unwrap(), params);
    // A library thread, not this one.
    assert_ne!(
        pr.thread.lock().unwrap().clone().unwrap(),
        format!("{:?}", std::thread::current().id())
    );
    assert_eq!(unsafe { (a.call_wait)(call, 0, &mut out) }, 0);
    assert_eq!(value(out), text("later"));
    unsafe { (a.call_free)(call) };

    // on_done after the call has ended runs soon; freeing the handle first is deferred until
    // the callback returned.
    let mut call = std::ptr::null_mut();
    let st = unsafe {
        (a.peer_call_async)(
            peer,
            cs("t.echo").as_ptr(),
            params.as_ptr(),
            params.len(),
            std::ptr::null(),
            &mut call,
        )
    };
    assert_eq!(st, 0);
    assert_eq!(unsafe { (a.call_wait)(call, 5000, &mut out) }, 0);
    assert_eq!(value(out), text("later"));
    let before = pr.done.load(Ordering::SeqCst);
    assert_eq!(
        unsafe { (a.call_on_done)(call, Some(on_done), pr.ptr(), None) },
        0
    );
    unsafe { (a.call_free)(call) };
    wait_until("the late on_done", || {
        pr.done.load(Ordering::SeqCst) == before + 1
    });

    // A failed call reports its status through call_wait.
    let mut call = std::ptr::null_mut();
    let st = unsafe {
        (a.client_call_async)(
            client,
            cs("t.fail").as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            &mut call,
        )
    };
    assert_eq!(st, 0);
    assert_eq!(unsafe { (a.call_wait)(call, 5000, &mut out) }, E_REMOTE);
    assert_eq!(get_str(&last_map(), "code").as_deref(), Some("boom"));
    unsafe { (a.call_free)(call) };

    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(client);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn closing_a_peer() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (client, peer) = srv.connect();
    // The client closes: the connection ends (the handle outlives it).
    assert_eq!(unsafe { (a.client_close)(client) }, 0);
    assert_eq!(unsafe { (a.peer_wait_closed)(peer, 5000) }, 0);
    assert_eq!(unsafe { (a.peer_is_open)(peer) }, 0);
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (a.peer_close_reason)(peer, &mut b) }, 0);
    let r = value(b);
    assert!(get_str(&r, "code").is_some(), "{r:?}");
    let (st, _) = call_peer(peer, "t.echo", &[], None);
    assert!(st < 0);
    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(client);
    }

    // peer_close with a code of our own.
    let (client, peer) = srv.connect();
    assert_eq!(unsafe { (a.peer_wait_closed)(peer, 20) }, E_WAIT_TIMEOUT);
    assert_eq!(
        unsafe { (a.peer_close)(peer, cs("normal").as_ptr(), cs("bye").as_ptr()) },
        0
    );
    assert_eq!(unsafe { (a.peer_wait_closed)(peer, 5000) }, 0);
    assert_eq!(unsafe { (a.peer_is_open)(peer) }, 0);
    unsafe {
        (a.peer_free)(peer);
        (a.client_close)(client);
        (a.client_free)(client);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn peer_subscriptions_event_state_and_stream() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (client, peer) = srv.connect();
    let pr = Probe::leak();
    let ev = cs("t.ev");

    let mut opts = SubscribeOpts::new(Some(on_delivery), pr.ptr());
    opts.on_end = Some(on_end);
    opts.release = Some(release);
    let mut sub = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.peer_subscribe)(peer, ev.as_ptr(), &opts, &mut sub) },
        0,
        "{}",
        common::last_message()
    );
    assert_eq!(unsafe { (a.subscription_is_active)(sub) }, 1);
    assert_eq!(unsafe { (a.subscription_wait)(sub, 1, 1000) }, 0);
    for i in 0..20i64 {
        srv.ev.publish(&Value::from(i)).unwrap();
    }
    wait_until("20 events", || pr.deliveries.lock().unwrap().len() >= 20);
    for (i, (kind, flags, seq, data)) in pr.deliveries.lock().unwrap().iter().enumerate() {
        assert_eq!((*kind, *flags), (0, 0));
        assert_eq!(*seq, i as u64 + 1);
        assert_eq!(data.as_slice(), mp(&Value::from(i as i64)).as_slice());
    }
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (a.subscription_info)(sub, &mut b) }, 0);
    assert_eq!(get_str(&value(b), "channel").as_deref(), Some("t.ev"));
    unsafe { (a.unsubscribe)(sub) };
    // Nothing is delivered after unsubscribe returned, and the release ran.
    let n = pr.deliveries.lock().unwrap().len();
    srv.ev.publish(&Value::from(99i64)).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(pr.deliveries.lock().unwrap().len(), n);
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);

    // State: the snapshot comes first, flagged.
    let pr2 = Probe::leak();
    let opts = SubscribeOpts::new(Some(on_delivery), pr2.ptr());
    let st_name = cs("t.st");
    let mut sub = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.peer_subscribe)(peer, st_name.as_ptr(), &opts, &mut sub) },
        0
    );
    wait_until("the snapshot", || {
        !pr2.deliveries.lock().unwrap().is_empty()
    });
    {
        let d = pr2.deliveries.lock().unwrap();
        assert_eq!(d[0].1 & 1, 1, "snapshot flag");
        assert_eq!(d[0].3, mp(&text("start")));
    }
    unsafe { (a.unsubscribe)(sub) };

    // Stream: the layout in the info, samples with the raw body.
    let pr3 = Probe::leak();
    let opts = SubscribeOpts::new(Some(on_delivery), pr3.ptr());
    let sm = cs("t.sm");
    let mut sub = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.peer_subscribe)(peer, sm.as_ptr(), &opts, &mut sub) },
        0
    );
    assert_eq!(unsafe { (a.subscription_info)(sub, &mut b) }, 0);
    let info = value(b);
    assert!(codec::get(&info, "layout").is_some(), "{info:?}");
    for n in 0u32..5 {
        srv.sm
            .send(&n.to_le_bytes(), Some(1000 + i64::from(n)))
            .unwrap();
    }
    wait_until("5 samples", || pr3.deliveries.lock().unwrap().len() >= 5);
    for (i, (kind, _, _, data)) in pr3.deliveries.lock().unwrap().iter().enumerate() {
        assert_eq!(*kind, 1);
        assert_eq!(data.as_slice(), (i as u32).to_le_bytes());
    }
    unsafe { (a.unsubscribe)(sub) };

    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(client);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn a_failed_subscribe_ends_with_a_null_subscription_and_releases() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (client, peer) = srv.connect();
    let pr = Probe::leak();
    let mut opts = SubscribeOpts::new(Some(on_delivery), pr.ptr());
    opts.on_end = Some(on_end);
    opts.release = Some(release);
    let mut sub = std::ptr::null_mut::<c_void>();
    let st = unsafe { (a.peer_subscribe)(peer, cs("t.nothing").as_ptr(), &opts, &mut sub) };
    assert_eq!(st, E_REMOTE);
    assert!(sub.is_null());
    assert_eq!(
        get_str(&last_map(), "code").as_deref(),
        Some("unknown_channel")
    );
    wait_until("on_end", || pr.ends.load(Ordering::SeqCst) == 1);
    assert!(pr.null_sub_on_end.load(Ordering::SeqCst));
    assert!(
        pr.text
            .lock()
            .unwrap()
            .starts_with("t.nothing|unknown_channel|")
    );
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);

    // No on_delivery: refused, and the release still runs.
    let pr2 = Probe::leak();
    let mut opts = SubscribeOpts::new(None, pr2.ptr());
    opts.release = Some(release);
    let st = unsafe { (a.peer_subscribe)(peer, cs("t.ev").as_ptr(), &opts, &mut sub) };
    assert_eq!(st, E_INVALID_ARG);
    wait_until("the release", || pr2.released.load(Ordering::SeqCst) == 1);

    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(client);
    }
    srv.ep.stop();
    settle();
}
