//! Clients through the C functions (spec 2.7, client half): connect errors and hooks,
//! reconnection after the endpoint restarts, subscriptions that follow it, `connect`, calls
//! without a connection. Core endpoints on random pipes with random keys.

mod common;
#[path = "common/peer_api.rs"]
mod papi;
#[path = "common/server.rs"]
mod server;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{NO_BUFFER, serial};
use papi::*;
use server::{Server, random_pipe};
use stintmate_ipc::codec::{self, Value};

fn is_connected(c: *mut std::ffi::c_void) -> bool {
    unsafe { (p().client_is_connected)(c) == 1 }
}

#[test]
fn reconnect_delays_follow_the_bye_code() {
    let a = p();
    let d = |code: Option<&str>, attempt: u32| {
        let code = code.map(cs);
        unsafe {
            (a.reconnect_delay_ms)(
                code.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
                attempt,
            )
        }
    };
    assert_eq!(d(None, 0), 250);
    assert_eq!(d(None, 9), 2000);
    assert_eq!(d(Some("unauthorized"), 3), 10_000);
    assert_eq!(d(Some("shutting_down"), 0), 250);
    assert_eq!(d(Some("shutting_down"), 5), 500);
    assert_eq!(d(Some("version_unsupported"), 0), -1);
}

#[test]
fn calls_without_a_connection_fail_with_not_connected() {
    let _s = serial();
    let a = p();
    let c = new_client(&random_pipe(), &new_key(), true);
    assert_eq!(unsafe { (a.client_is_connected)(c) }, 0);
    let mut out = NO_BUFFER;
    let st = unsafe {
        (a.client_call)(
            c,
            cs("t.echo").as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, E_NOT_CONNECTED);
    let mut call = std::ptr::null_mut();
    let st = unsafe {
        (a.client_call_async)(
            c,
            cs("t.echo").as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            &mut call,
        )
    };
    assert_eq!(st, E_NOT_CONNECTED);
    assert!(call.is_null());
    let mut peer = std::ptr::null_mut();
    assert_eq!(unsafe { (a.client_peer)(c, &mut peer) }, E_NOT_CONNECTED);
    // Never started: no error to report yet.
    assert_eq!(unsafe { (a.client_last_error)(c, &mut out) }, 0);
    assert!(value(out).is_nil());
    assert_eq!(
        unsafe {
            (a.client_call)(
                std::ptr::null_mut(),
                cs("x").as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null(),
                &mut out,
            )
        },
        E_INVALID_ARG
    );
    unsafe { (a.client_free)(c) };
    settle();
}

#[test]
fn connect_errors_are_kept_reported_and_cleared() {
    let _s = serial();
    let a = p();
    let (pipe, key) = (random_pipe(), new_key());
    let c = new_client(&pipe, &key, true);
    let pr = Probe::leak();
    assert_eq!(
        unsafe { (a.on_connect_error)(c, Some(on_error), pr.ptr(), Some(release)) },
        0
    );
    assert_eq!(unsafe { (a.client_start)(c) }, 0);

    // Nobody serves the pipe: the hook hears of each attempt, the last error is kept.
    wait_until("a connect error", || pr.errors.load(Ordering::SeqCst) >= 1);
    let e = codec::decode(&pr.last.lock().unwrap()).unwrap();
    assert_eq!(get_i64(&e, "status"), Some(i64::from(E_NOT_FOUND)));
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (a.client_last_error)(c, &mut b) }, 0);
    assert_eq!(get_i64(&value(b), "status"), Some(i64::from(E_NOT_FOUND)));

    // Waiting fails with not-connected, the attempt's error as its cause.
    let st = unsafe { (a.client_wait_connected)(c, 200, std::ptr::null_mut()) };
    assert_eq!(st, E_NOT_CONNECTED);
    let m = last_map();
    assert!(
        get_str(&m, "message")
            .unwrap()
            .starts_with("not connected: "),
        "{m:?}"
    );

    // Replacing the hook releases the old user_data once.
    assert_eq!(
        unsafe { (a.on_connect_error)(c, None, std::ptr::null_mut(), None) },
        0
    );
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);

    // The server comes up: the client connects and the error is gone.
    let srv = Server::start(&pipe, key);
    let mut peer = std::ptr::null_mut();
    let st = unsafe { (a.client_wait_connected)(c, 10_000, &mut peer) };
    assert_eq!(st, 0, "{}", common::last_message());
    assert_eq!(unsafe { (a.client_last_error)(c, &mut b) }, 0);
    assert!(value(b).is_nil());
    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(c);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn reconnects_after_the_endpoint_restarts_and_subscriptions_follow() {
    let _s = serial();
    let a = p();
    let (pipe, key) = (random_pipe(), new_key());
    let srv = Server::start(&pipe, key);
    let c = new_client(&pipe, &key, true);
    // Subscribe before connecting: it starts with the first connection.
    let pr = Probe::leak();
    let mut opts = SubscribeOpts::new(Some(on_delivery), pr.ptr());
    opts.release = Some(release);
    let mut sub = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.client_subscribe)(c, cs("t.ev").as_ptr(), &opts, &mut sub) },
        0
    );
    assert_eq!(unsafe { (a.subscription_is_active)(sub) }, 0);
    assert_eq!(unsafe { (a.client_start)(c) }, 0);
    assert_eq!(unsafe { (a.subscription_wait)(sub, 1, 10_000) }, 0);
    assert_eq!(unsafe { (a.subscription_is_active)(sub) }, 1);
    let mut out = NO_BUFFER;
    let params = mp(&Value::from("one"));
    let st = unsafe {
        (a.client_call)(
            c,
            cs("t.echo").as_ptr(),
            params.as_ptr(),
            params.len(),
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, 0);
    assert_eq!(value(out), Value::from("one"));
    srv.ev.publish(&Value::from(1i64)).unwrap();
    wait_until("the first event", || {
        !pr.deliveries.lock().unwrap().is_empty()
    });

    // The endpoint goes away: the connection ends, the subscription is no longer live.
    srv.ep.stop();
    drop(srv);
    wait_until("the disconnection", || !is_connected(c));
    wait_until("the subscription to end", || unsafe {
        (a.subscription_is_active)(sub) == 0
    });
    assert_eq!(unsafe { (a.subscription_wait)(sub, 2, 1000) }, 0);

    // It comes back (same pipe, same key): the client connects again by itself.
    let srv = Server::start(&pipe, key);
    wait_until("the reconnection", || is_connected(c));
    assert_eq!(unsafe { (a.subscription_wait)(sub, 1, 10_000) }, 0);
    let before = pr.deliveries.lock().unwrap().len();
    srv.ev.publish(&Value::from(2i64)).unwrap();
    wait_until("an event after the reconnection", || {
        pr.deliveries.lock().unwrap().len() > before
    });
    let st = unsafe {
        (a.client_call)(
            c,
            cs("t.echo").as_ptr(),
            params.as_ptr(),
            params.len(),
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, 0);
    let _ = value(out);

    unsafe { (a.unsubscribe)(sub) };
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);
    unsafe { (a.client_free)(c) };
    srv.ep.stop();
    settle();
}

#[test]
fn client_subscriptions_state_stream_and_a_refused_channel() {
    let _s = serial();
    let a = p();
    let srv = Server::new();
    let (c, peer) = srv.connect();
    unsafe { (a.peer_free)(peer) };

    // State: the snapshot first.
    let pr = Probe::leak();
    let opts = SubscribeOpts::new(Some(on_delivery), pr.ptr());
    let mut s1 = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.client_subscribe)(c, cs("t.st").as_ptr(), &opts, &mut s1) },
        0
    );
    wait_until("the snapshot", || !pr.deliveries.lock().unwrap().is_empty());
    assert_eq!(pr.deliveries.lock().unwrap()[0].1 & 1, 1);

    // Stream: samples, and the layout in the info once live.
    let pr2 = Probe::leak();
    let opts = SubscribeOpts::new(Some(on_delivery), pr2.ptr());
    let mut s2 = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.client_subscribe)(c, cs("t.sm").as_ptr(), &opts, &mut s2) },
        0
    );
    assert_eq!(unsafe { (a.subscription_wait)(s2, 1, 10_000) }, 0);
    for n in 0u32..3 {
        srv.sm.send(&n.to_le_bytes(), Some(i64::from(n))).unwrap();
    }
    wait_until("3 samples", || pr2.deliveries.lock().unwrap().len() >= 3);
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (a.subscription_info)(s2, &mut b) }, 0);
    let info = value(b);
    assert!(codec::get(&info, "layout").is_some(), "{info:?}");
    assert!(codec::get(&info, "error").is_none());

    // A channel the server does not have: the call does not fail, the info says why.
    let pr3 = Probe::leak();
    let opts = SubscribeOpts::new(Some(on_delivery), pr3.ptr());
    let mut s3 = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.client_subscribe)(c, cs("t.nothing").as_ptr(), &opts, &mut s3) },
        0
    );
    assert_eq!(unsafe { (a.subscription_is_active)(s3) }, 0);
    assert_eq!(unsafe { (a.subscription_wait)(s3, 1, 100) }, E_WAIT_TIMEOUT);
    assert_eq!(unsafe { (a.subscription_info)(s3, &mut b) }, 0);
    let info = value(b);
    let err = codec::get(&info, "error").expect("error in the info");
    assert_eq!(get_i64(err, "status"), Some(i64::from(E_REMOTE)));
    assert!(get_str(err, "message").is_some());

    // Bad arguments.
    let mut bad = std::ptr::null_mut();
    let none = SubscribeOpts::new(None, std::ptr::null_mut());
    assert_eq!(
        unsafe { (a.client_subscribe)(c, cs("t.ev").as_ptr(), &none, &mut bad) },
        E_INVALID_ARG
    );
    assert_eq!(
        unsafe { (a.client_subscribe)(c, cs("t.ev").as_ptr(), std::ptr::null(), &mut bad) },
        E_INVALID_ARG
    );

    unsafe {
        (a.unsubscribe)(s1);
        (a.unsubscribe)(s2);
        (a.unsubscribe)(s3);
        (a.client_free)(c);
    }
    srv.ep.stop();
    settle();
}

#[test]
fn client_connect_and_the_one_shot_connect() {
    let _s = serial();
    let a = p();
    let srv = Server::new();

    // Client.connect: start and wait.
    let c = new_client(&srv.pipe, &srv.key, true);
    let mut peer = std::ptr::null_mut();
    assert_eq!(unsafe { (a.client_connect)(c, 10_000, &mut peer) }, 0);
    assert_eq!(unsafe { (a.peer_is_open)(peer) }, 1);
    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(c);
    }

    // connect(): one connection that owns its client, closed with the last reference.
    let (pipe, app) = (cs(&srv.pipe), cs("oneshot"));
    let ks = common::key_source(&srv.key);
    let mut peer = std::ptr::null_mut();
    let st = unsafe {
        (a.connect)(
            pipe.as_ptr(),
            &peer_info(&app),
            &ks,
            10_000,
            std::ptr::null_mut(),
            0,
            &mut peer,
        )
    };
    assert_eq!(st, 0, "{}", common::last_message());
    let params = mp(&Value::from("x"));
    let mut out = NO_BUFFER;
    let st = unsafe {
        (a.peer_call)(
            peer,
            cs("t.echo").as_ptr(),
            params.as_ptr(),
            params.len(),
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, 0);
    assert_eq!(value(out), Value::from("x"));
    let second = unsafe { (a.peer_ref)(peer) };
    unsafe { (a.peer_free)(peer) };
    // Still open: the second reference keeps the connection.
    assert_eq!(unsafe { (a.peer_is_open)(second) }, 1);
    unsafe { (a.peer_free)(second) };
    wait_until("the server to see it go", || srv.ep.peers().is_empty());

    // With a node: a client made with reconnect = 0 becomes the connection's owner.
    let node = new_client(&srv.pipe, &srv.key, false);
    let mut peer = std::ptr::null_mut();
    let st = unsafe {
        (a.connect)(
            pipe.as_ptr(),
            &peer_info(&app),
            &ks,
            10_000,
            node,
            0,
            &mut peer,
        )
    };
    assert_eq!(st, 0, "{}", common::last_message());
    assert_eq!(unsafe { (a.client_is_connected)(node) }, 1);
    unsafe {
        (a.peer_free)(peer);
        (a.client_free)(node);
    }
    // ... and a node that reconnects is refused.
    let node = new_client(&srv.pipe, &srv.key, true);
    let st = unsafe {
        (a.connect)(
            pipe.as_ptr(),
            &peer_info(&app),
            &ks,
            1000,
            node,
            0,
            &mut peer,
        )
    };
    assert_eq!(st, E_INVALID_ARG);
    unsafe { (a.client_free)(node) };

    srv.ep.stop();
    settle();
}

#[test]
fn connect_failures_carry_the_attempts_own_error() {
    let _s = serial();
    let a = p();
    let (pipe, app) = (random_pipe(), cs("oneshot"));
    let key = new_key();
    let ks = common::key_source(&key);
    let mut peer = std::ptr::null_mut();
    let pc = cs(&pipe);

    // Nobody serves the pipe.
    let st = unsafe {
        (a.connect)(
            pc.as_ptr(),
            &peer_info(&app),
            &ks,
            2000,
            std::ptr::null_mut(),
            0,
            &mut peer,
        )
    };
    assert_eq!(st, E_NOT_FOUND, "{}", common::last_message());
    assert!(peer.is_null());

    // A wrong key: the handshake fails with the endpoint's code.
    let srv = Server::start(&pipe, new_key());
    let st = unsafe {
        (a.connect)(
            pc.as_ptr(),
            &peer_info(&app),
            &ks,
            5000,
            std::ptr::null_mut(),
            0,
            &mut peer,
        )
    };
    assert_eq!(st, E_HANDSHAKE, "{}", common::last_message());
    assert_eq!(
        get_str(&last_map(), "code").as_deref(),
        Some("unauthorized")
    );
    srv.ep.stop();
    settle();
}

/// What a test's reconnect policy saw and what it answers.
struct PolicyProbe {
    calls: std::sync::Mutex<Vec<(Option<String>, u32, std::time::Instant)>>,
    delay_ms: i64,
    released: std::sync::atomic::AtomicUsize,
}

impl PolicyProbe {
    fn leak(delay_ms: i64) -> &'static PolicyProbe {
        Box::leak(Box::new(PolicyProbe {
            calls: Default::default(),
            delay_ms,
            released: Default::default(),
        }))
    }
    fn ptr(&'static self) -> *mut std::ffi::c_void {
        std::ptr::from_ref(self).cast_mut().cast()
    }
    fn n(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

unsafe extern "C-unwind" fn policy(
    ud: *mut std::ffi::c_void,
    code: *const std::ffi::c_char,
    attempt: u32,
) -> i64 {
    let pr = unsafe { &*ud.cast::<PolicyProbe>() };
    let code = (!code.is_null()).then(|| {
        unsafe { std::ffi::CStr::from_ptr(code) }
            .to_string_lossy()
            .into_owned()
    });
    pr.calls
        .lock()
        .unwrap()
        .push((code, attempt, std::time::Instant::now()));
    pr.delay_ms
}

unsafe extern "C-unwind" fn release_policy(ud: *mut std::ffi::c_void) {
    unsafe { &*ud.cast::<PolicyProbe>() }
        .released
        .fetch_add(1, Ordering::SeqCst);
}

#[test]
fn a_reconnect_policy_is_called_with_increasing_attempts_and_its_delay_is_honoured() {
    let _s = serial();
    let a = p();
    let (pipe, key) = (random_pipe(), new_key());
    let c = new_client(&pipe, &key, true);
    let pr = PolicyProbe::leak(150);
    assert_eq!(
        unsafe { (a.client_set_reconnect_policy)(c, Some(policy), pr.ptr(), Some(release_policy)) },
        0
    );
    assert_eq!(unsafe { (a.client_start)(c) }, 0);
    wait_until("four policy calls", || pr.n() >= 4);
    {
        let calls = pr.calls.lock().unwrap();
        for (i, (code, attempt, _)) in calls.iter().take(4).enumerate() {
            assert_eq!(*attempt, i as u32);
            assert_eq!(*code, None, "nobody serves the pipe: no bye code");
        }
        // 150 ms with -20 % jitter at the least; the built-in policy would wait 250..2000 ms
        // growing, so also check the upper side stays near the answer.
        for w in calls.windows(2).take(3) {
            let gap = w[1].2 - w[0].2;
            assert!(gap >= Duration::from_millis(110), "{gap:?}");
            assert!(gap < Duration::from_millis(600), "{gap:?}");
        }
    }
    // Replacing the policy with the built-in one releases the first once.
    assert_eq!(
        unsafe { (a.client_set_reconnect_policy)(c, None, std::ptr::null_mut(), None) },
        0
    );
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);
    unsafe { (a.client_free)(c) };
    settle();
    assert_eq!(pr.released.load(Ordering::SeqCst), 1);
}

#[test]
fn a_reconnect_policy_answering_minus_one_stops_reconnecting_and_sees_the_bye_code() {
    let _s = serial();
    let a = p();
    let (pipe, key) = (random_pipe(), new_key());
    // A server with another key: every handshake fails with `unauthorized`.
    let srv = Server::start(&pipe, new_key());
    let c = new_client(&pipe, &key, true);
    let ec = Probe::leak();
    assert_eq!(
        unsafe { (a.on_connect_error)(c, Some(on_error), ec.ptr(), Some(release)) },
        0
    );
    let pr = PolicyProbe::leak(-1);
    assert_eq!(
        unsafe { (a.client_set_reconnect_policy)(c, Some(policy), pr.ptr(), Some(release_policy)) },
        0
    );
    assert_eq!(unsafe { (a.client_start)(c) }, 0);
    wait_until("the policy call", || pr.n() >= 1);
    {
        let calls = pr.calls.lock().unwrap();
        assert_eq!(calls[0].0.as_deref(), Some("unauthorized"));
        assert_eq!(calls[0].1, 0);
    }
    // The error map of the attempt carries the code too.
    wait_until("the error hook", || ec.errors.load(Ordering::SeqCst) >= 1);
    let e = codec::decode(&ec.last.lock().unwrap()).unwrap();
    assert_eq!(get_str(&e, "code").as_deref(), Some("unauthorized"));
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (a.client_last_error)(c, &mut b) }, 0);
    assert_eq!(get_str(&value(b), "code").as_deref(), Some("unauthorized"));
    // No further attempt, however long we wait (the built-in delay would be 10 s; -1 stops).
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(pr.n(), 1);
    assert_eq!(ec.errors.load(Ordering::SeqCst), 1);
    assert_eq!(unsafe { (a.client_is_connected)(c) }, 0);
    unsafe { (a.client_free)(c) };
    srv.ep.stop();
    wait_until("the release", || pr.released.load(Ordering::SeqCst) == 1);
    settle();
    assert_eq!(pr.released.load(Ordering::SeqCst), 1);
}
