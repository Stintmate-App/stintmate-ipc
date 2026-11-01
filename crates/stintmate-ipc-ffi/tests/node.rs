//! Nodes: methods with options, requests, peers and declarations, on an endpoint
//! and on a client. The other end is the core's own blocking API.

mod common;

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::error::Error;

unsafe extern "C-unwind" fn echo(user: *mut c_void, req: *mut c_void, p: *const u8, len: usize) {
    if !user.is_null() {
        unsafe { log(user) }.push("echo");
    }
    unsafe { (api().respond)(req, p, len) };
}

unsafe extern "C-unwind" fn parked(user: *mut c_void, req: *mut c_void, _: *const u8, _: usize) {
    unsafe { log(user) }.ptrs.lock().unwrap().push(req as usize);
}

unsafe extern "C-unwind" fn fail_with_details(
    _: *mut c_void,
    req: *mut c_void,
    _: *const u8,
    _: usize,
) {
    let details = mp(&codec::map([("why", Value::from("because"))]));
    unsafe {
        (api().respond_error)(
            req,
            c"busy".as_ptr(),
            c"try later".as_ptr(),
            details.as_ptr(),
            details.len(),
            250,
        )
    };
}

fn no_opts() -> *const MethodOpts {
    std::ptr::null()
}

#[test]
fn method_options_reach_the_declarations_and_replacing_releases() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    let versions = [1u32, 2];
    let schema = mp(&codec::map([("type", Value::from("object"))]));
    let opts = MethodOpts {
        struct_size: size_of::<MethodOpts>(),
        flags: 1 | 4, // ORDERED | IDEMPOTENT
        versions: versions.as_ptr(),
        n_versions: 2,
        idempotency_ttl_ms: 1000,
        doc: c"adds things".as_ptr(),
        params_schema: schema.as_ptr(),
        params_schema_len: schema.len(),
        result_schema: std::ptr::null(),
        result_schema_len: 0,
    };
    let st = unsafe {
        (a.method)(
            fx.node,
            c"m.full".as_ptr(),
            &opts,
            Some(echo),
            l.ud(),
            Some(release_log),
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    // A struct that stops before the later fields: they take their defaults.
    let short = MethodOpts {
        struct_size: std::mem::offset_of!(MethodOpts, flags),
        ..opts
    };
    let st = unsafe {
        (a.method)(
            fx.node,
            c"m.short".as_ptr(),
            &short,
            Some(echo),
            l.ud(),
            None,
        )
    };
    assert_eq!(st, 0);
    // No handler, bad name: invalid arguments.
    let bad = unsafe { (a.method)(fx.node, c"m.x".as_ptr(), no_opts(), None, l.ud(), None) };
    assert_eq!(bad, -11);
    let bad = unsafe {
        (a.method)(
            fx.node,
            c"Bad Name".as_ptr(),
            no_opts(),
            Some(echo),
            l.ud(),
            None,
        )
    };
    assert!(bad < 0);

    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.declarations)(fx.node, &mut out) }, 0);
    let d = val(out);
    let ms = key_of(&d, "methods");
    let m = key_of(&ms, "m.full");
    assert_eq!(key_of(&m, "v"), Value::Array(vec![1.into(), 2.into()]));
    assert_eq!(key_of(&m, "ordered"), Value::Boolean(true));
    assert_eq!(key_of(&m, "idempotent"), Value::Boolean(true));
    assert_eq!(key_of(&m, "doc"), Value::from("adds things"));
    assert!(key_of(&m, "params").is_map());
    assert_eq!(key_of(&key_of(&ms, "m.short"), "ordered"), Value::Nil);

    let peer = fx.start();
    let r = peer
        .call("m.full", Value::from("hi"), CallOptions::default())
        .unwrap();
    assert_eq!(r, Value::from("hi"));
    assert_eq!(l.lines(), ["echo", "echo"][..1]);

    // Offering the name again replaces the method and releases the old user_data.
    let st = unsafe {
        (a.method)(
            fx.node,
            c"m.full".as_ptr(),
            no_opts(),
            Some(echo),
            std::ptr::null_mut(),
            None,
        )
    };
    assert_eq!(st, 0);
    wait_for("the release", || l.released.load(Ordering::SeqCst) == 1);
    assert_eq!(
        unsafe { (a.remove_method)(fx.node, c"m.short".as_ptr()) },
        0
    );
    fx.free();
    assert_clean();
}

#[test]
fn a_request_exposes_its_context_and_answers_errors_with_details() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    unsafe {
        assert_eq!(
            (a.method)(
                fx.node,
                c"m.park".as_ptr(),
                no_opts(),
                Some(parked),
                l.ud(),
                None
            ),
            0
        );
        assert_eq!(
            (a.method)(
                fx.node,
                c"m.fail".as_ptr(),
                no_opts(),
                Some(fail_with_details),
                l.ud(),
                None
            ),
            0
        );
    }
    let peer = fx.start();

    // An error with details and a retry hint.
    let e = peer
        .call("m.fail", Value::Nil, CallOptions::default())
        .unwrap_err();
    let Error::Remote(r) = e else { panic!("{e:?}") };
    assert_eq!(r.code, "busy");
    assert_eq!(r.message, "try later");
    assert_eq!(r.retry_ms, Some(250));
    assert_eq!(
        key_of(r.details.as_ref().unwrap(), "why"),
        Value::from("because")
    );

    // A parked request: context, progress, cancellation, a late answer.
    let pending = peer
        .call_async(
            "m.park",
            Value::from(7),
            CallOptions {
                timeout: Some(Duration::from_secs(20)),
                ..CallOptions::default()
            },
        )
        .unwrap();
    wait_for("the request", || !l.ptrs.lock().unwrap().is_empty());
    let req = l.ptrs.lock().unwrap()[0] as *mut c_void;
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.request_info)(req, &mut out) }, 0);
    let info = val(out);
    assert_eq!(key_of(&info, "method"), Value::from("m.park"));
    assert_eq!(key_of(&info, "version"), Value::from(1));
    assert!(key_of(&info, "remaining_ms").as_u64().unwrap() > 1000);
    let remaining = unsafe { (a.request_remaining_ms)(req) };
    assert!(remaining > 1000 && remaining <= 20_000, "{remaining}");
    let rp = unsafe { (a.request_peer)(req) };
    assert!(!rp.is_null());
    unsafe { (a.peer_free)(rp) };
    let report = mp(&Value::from("halfway"));
    assert_eq!(
        unsafe { (a.request_progress)(req, report.as_ptr(), report.len(), 30_000) },
        0
    );
    assert!(unsafe { (a.request_remaining_ms)(req) } > 20_000);

    assert_eq!(unsafe { (a.request_is_cancelled)(req) }, 0);
    assert_eq!(unsafe { (a.request_wait_cancelled)(req, 30) }, -17);
    assert_eq!(unsafe { (a.request_wait_cancelled)(req, 0) }, -17);
    assert!(pending.cancel());
    assert_eq!(unsafe { (a.request_wait_cancelled)(req, 5000) }, 0);
    assert_eq!(unsafe { (a.request_is_cancelled)(req) }, 1);
    // The answer comes from another thread, after the handler returned.
    let addr = req as usize;
    std::thread::spawn(move || unsafe {
        (api().respond)(addr as *mut c_void, std::ptr::null(), 0)
    })
    .join()
    .unwrap();
    fx.free();
    assert_clean();
}

unsafe extern "C-unwind" fn on_peer(user: *mut c_void, event: i32, peer: *mut c_void) {
    assert!(!peer.is_null());
    unsafe { log(user) }.push(format!("peer:{event}"));
}

#[test]
fn peers_and_the_peer_hook() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    assert_eq!(
        unsafe { (a.on_peer)(fx.node, Some(on_peer), l.ud(), Some(release_log)) },
        0
    );
    assert_eq!(unsafe { (a.endpoint_start)(fx.ep) }, 0);
    fx.core.start();
    fx.core.wait_connected(Duration::from_secs(10)).unwrap();
    wait_for("connect", || l.lines().contains(&"peer:1".to_owned()));

    let mut count = 0usize;
    assert_eq!(
        unsafe { (a.peers)(fx.node, std::ptr::null_mut(), 0, &mut count) },
        0
    );
    assert_eq!(count, 1);
    let mut got = [std::ptr::null_mut::<c_void>(); 4];
    assert_eq!(
        unsafe { (a.peers)(fx.node, got.as_mut_ptr(), 4, &mut count) },
        0
    );
    assert_eq!(count, 1);
    assert!(!got[0].is_null() && got[1].is_null());
    unsafe { (a.peer_free)(got[0]) };

    // The other end declares something new.
    fx.core
        .method("c.late", stintmate_ipc::MethodOpts::default(), |_, v| Ok(v))
        .unwrap();
    wait_for("redeclare", || l.lines().contains(&"peer:3".to_owned()));

    // Replacing the hook releases the old user_data.
    assert_eq!(
        unsafe { (a.on_peer)(fx.node, None, std::ptr::null_mut(), None) },
        0
    );
    wait_for("the release", || l.released.load(Ordering::SeqCst) == 1);
    fx.core.close();
    wait_for("no peers", || {
        let mut n = 9usize;
        unsafe { (a.peers)(fx.node, std::ptr::null_mut(), 0, &mut n) };
        n == 0
    });
    unsafe { (a.endpoint_free)(fx.ep) };
    assert_clean();
}

#[test]
fn a_client_offers_a_method_and_a_state_channel_to_its_endpoint() {
    let _s = serial();
    let a = api();
    let fx = ClientFix::new();
    let l = Arc::new(Log::default());
    unsafe {
        assert_eq!(
            (a.method)(
                fx.node,
                c"client.echo".as_ptr(),
                no_opts(),
                Some(echo),
                l.ud(),
                Some(release_log)
            ),
            0
        );
    }
    let initial = mp(&Value::from("start"));
    let mut ch = std::ptr::null_mut();
    let st = unsafe {
        (a.state_channel)(
            fx.node,
            c"client.state".as_ptr(),
            initial.as_ptr(),
            initial.len(),
            std::ptr::null(),
            0,
            &mut ch,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let peer = fx.start();
    wait_for("the client's declarations", || {
        peer.decl().methods.contains_key("client.echo")
            && peer.decl().channels.contains_key("client.state")
    });

    // The endpoint calls the client.
    let r = peer
        .call("client.echo", Value::from(41), CallOptions::default())
        .unwrap();
    assert_eq!(r, Value::from(41));

    // The endpoint subscribes to the client's state channel: snapshot, then updates.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s2 = seen.clone();
    let sub = peer
        .subscribe("client.state", Default::default(), move |d| {
            if let stintmate_ipc::Delivery::Event(e) = d {
                s2.lock().unwrap().push((e.data, e.snapshot));
            }
        })
        .unwrap();
    wait_for("the snapshot", || !seen.lock().unwrap().is_empty());
    let next = mp(&Value::from("next"));
    let st = unsafe {
        (a.publish)(
            ch,
            next.as_ptr(),
            next.len(),
            i64::MIN,
            0,
            None,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    wait_for("the update", || seen.lock().unwrap().len() >= 2);
    assert_eq!(seen.lock().unwrap()[0], (Value::from("start"), true));
    assert_eq!(seen.lock().unwrap()[1], (Value::from("next"), false));
    assert_eq!(unsafe { (a.channel_subscribers)(ch) }, 1);
    sub.close();
    unsafe { (a.channel_free)(ch) };
    fx.free();
    wait_for("the release", || l.released.load(Ordering::SeqCst) == 1);
    assert_clean();
}
