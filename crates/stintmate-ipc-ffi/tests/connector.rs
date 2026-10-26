//! A connector made only through the DLL (spec 2.7 with 2.5), as the connector protocol
//! section 2 has it: a client that declares and offers `connector.status` (state) and
//! `connector.action` (method) to Pitwall (played here by a core endpoint), and calls one of
//! Pitwall's write methods.

mod common;

use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::{Delivery, MethodOpts, SubscribeOptions};

/// The action's answer: the params as they came, proving the round trip of the bytes.
unsafe extern "C-unwind" fn action(_user: *mut c_void, req: *mut c_void, p: *const u8, len: usize) {
    unsafe { (api().respond)(req, p, len) };
}

fn status(running: bool, phase: Option<&str>) -> Value {
    codec::map([
        ("game_running", Value::from(running)),
        ("phase", phase.map_or(Value::Nil, Value::from)),
    ])
}

#[test]
fn a_connector_through_the_dll_serves_status_and_actions_and_writes() {
    let _s = serial();
    let a = api();
    let fx = ClientFix::new();

    // Pitwall's side: a write method.
    fx.core
        .method(
            "pitwall.write.session_start",
            MethodOpts::default(),
            |_c, p| {
                Ok(codec::map([
                    ("ok", Value::from(true)),
                    ("type", Value::from("session_start")),
                    ("session_id", Value::from(7)),
                    ("session_key", key_of(&p, "session_key")),
                ]))
            },
        )
        .unwrap();

    // The connector declares `connector.status` and offers `connector.action` before it starts.
    let initial = mp(&status(false, None));
    let mut ch = std::ptr::null_mut();
    let st = unsafe {
        (a.state_channel)(
            fx.node,
            c"connector.status".as_ptr(),
            initial.as_ptr(),
            initial.len(),
            [1u32].as_ptr(),
            1,
            &mut ch,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let st = unsafe {
        (a.method)(
            fx.node,
            c"connector.action".as_ptr(),
            std::ptr::null(),
            Some(action),
            std::ptr::null_mut(),
            None,
        )
    };
    assert_eq!(st, 0, "{}", last_message());

    let peer = fx.start();
    let decl = peer.decl();
    assert!(decl.channels.contains_key("connector.status"));
    assert!(decl.methods.contains_key("connector.action"));

    // Pitwall subscribes at once: the snapshot, then every change.
    let seen: Arc<Mutex<Vec<(Value, bool)>>> = Arc::default();
    let s2 = seen.clone();
    let sub = peer
        .subscribe("connector.status", SubscribeOptions::default(), move |d| {
            if let Delivery::Event(e) = d {
                s2.lock().unwrap().push((e.data, e.snapshot));
            }
        })
        .unwrap();
    wait_for("the snapshot", || !seen.lock().unwrap().is_empty());
    assert_eq!(
        seen.lock().unwrap()[0],
        (status(false, None), true),
        "the first delivery is the state snapshot"
    );
    let now = status(true, Some("on_track"));
    let bytes = mp(&now);
    let mut queued = 0usize;
    let st = unsafe {
        (a.publish)(
            ch,
            bytes.as_ptr(),
            bytes.len(),
            i64::MIN,
            0,
            None,
            std::ptr::null_mut(),
            &mut queued,
        )
    };
    assert_eq!((st, queued), (0, 1), "{}", last_message());
    wait_for("the update", || seen.lock().unwrap().len() >= 2);
    assert_eq!(seen.lock().unwrap()[1], (now.clone(), false));
    let mut buf = NO_BUFFER;
    assert_eq!(unsafe { (a.state_get)(ch, &mut buf) }, 0);
    assert_eq!(val(buf), now);

    // Pitwall calls the action; the DLL handler answers.
    let params = codec::map([("action", Value::from("flash")), ("n", Value::from(3))]);
    let answer = peer
        .call(
            "connector.action",
            params.clone(),
            CallOptions {
                timeout: Some(Duration::from_secs(5)),
                ..CallOptions::default()
            },
        )
        .unwrap();
    assert_eq!(answer, params);

    // The connector calls Pitwall's write method (with an idempotency key).
    let sk = mp(&codec::map([(
        "session_key",
        Value::from("00ab12cd34ef56a7"),
    )]));
    let mut opts = call_opts(5000);
    opts.idempotency_key = c"conn-1/00ab12cd34ef56a7/start".as_ptr();
    let mut out = NO_BUFFER;
    let st = unsafe {
        (a.client_call)(
            fx.client,
            c"pitwall.write.session_start".as_ptr(),
            sk.as_ptr(),
            sk.len(),
            &opts,
            &mut out,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let r = val(out);
    assert_eq!(key_of(&r, "session_id"), Value::from(7));
    assert_eq!(key_of(&r, "session_key"), Value::from("00ab12cd34ef56a7"));

    // A method Pitwall does not offer is a remote error, not a crash.
    let mut out = NO_BUFFER;
    let st = unsafe {
        (a.client_call)(
            fx.client,
            c"pitwall.write.nothing".as_ptr(),
            std::ptr::null(),
            0,
            &call_opts(5000),
            &mut out,
        )
    };
    assert_ne!(st, 0);
    drop(take(out));

    drop(sub);
    drop(peer);
    unsafe { (a.channel_free)(ch) };
    fx.free();
    assert_clean();
}
