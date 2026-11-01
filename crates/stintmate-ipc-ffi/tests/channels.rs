//! Channels: event, state and stream channels on a node, publish with accept,
//! state get, stream send, subscribe and active hooks, close. The subscriber is the core's
//! blocking API.

mod common;

use std::ffi::c_void;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use common::*;
use stintmate_ipc::layout::{FieldType, Layout};
use stintmate_ipc::{Delivery, SubscribeOptions};

type Seen = Arc<Mutex<Vec<(Value, bool)>>>;

fn collect(seen: &Seen) -> impl Fn(Delivery) + Send + Sync + 'static {
    let seen = seen.clone();
    move |d| {
        if let Delivery::Event(e) = d {
            seen.lock().unwrap().push((e.data, e.snapshot));
        }
    }
}

fn opts_with(want: &str) -> SubscribeOptions {
    SubscribeOptions {
        options: Some(codec::map([("want", Value::from(want))])),
        ..SubscribeOptions::default()
    }
}

fn new_event(fx: &EpFix, name: &std::ffi::CStr, versions: &[u32]) -> *mut c_void {
    let mut ch = std::ptr::null_mut();
    let st = unsafe {
        (api().event_channel)(
            fx.node,
            name.as_ptr(),
            versions.as_ptr(),
            versions.len(),
            &mut ch,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    ch
}

unsafe extern "C-unwind" fn accept_yes(user: *mut c_void, view: *const u8, len: usize) -> i32 {
    let v = codec::decode(unsafe { std::slice::from_raw_parts(view, len) }).unwrap();
    assert_eq!(key_of(&key_of(&v, "peer"), "app"), Value::from("tester"));
    assert!(key_of(&v, "connection").as_u64().unwrap() != 0);
    assert!(key_of(&v, "pid").as_u64().is_some());
    unsafe { log(user) }.push("accept");
    i32::from(key_of(&key_of(&v, "options"), "want") == Value::from("yes"))
}

#[test]
fn event_publish_with_accept_version_and_the_active_hook() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    let ch = new_event(&fx, c"ev", &[1, 2]);
    assert_eq!(
        unsafe { (a.on_active)(ch, Some(active_cb), l.ud(), Some(release_log)) },
        0
    );
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.channel_info)(ch, &mut out) }, 0);
    let info = val(out);
    assert_eq!(key_of(&info, "name"), Value::from("ev"));
    assert_eq!(key_of(&info, "kind"), Value::from("event"));
    assert_eq!(
        key_of(&info, "versions"),
        Value::Array(vec![1.into(), 2.into()])
    );
    assert_eq!(unsafe { (a.channel_is_active)(ch) }, 0);

    let peer = fx.start();
    let (yes, no, v2) = (Seen::default(), Seen::default(), Seen::default());
    let s1 = peer
        .subscribe("ev", opts_with("yes"), collect(&yes))
        .unwrap();
    // One subscription per connection and version: the second subscriber is another client.
    let other = ipc::Client::new(
        fx.pipe.to_str().unwrap(),
        stintmate_ipc::handshake::PeerInfo::new("tester"),
        stintmate_ipc::keyfile::KeySource::Bytes(fx.key),
    )
    .unwrap();
    other.start();
    let peer2 = other
        .wait_connected(std::time::Duration::from_secs(10))
        .unwrap();
    let s2 = peer2
        .subscribe("ev", opts_with("no"), collect(&no))
        .unwrap();
    let s3 = peer
        .subscribe(
            "ev",
            SubscribeOptions {
                version: 2,
                ..opts_with("yes")
            },
            collect(&v2),
        )
        .unwrap();
    wait_for("the active hook", || {
        l.lines().contains(&"active:1".to_owned())
    });
    assert_eq!(unsafe { (a.channel_is_active)(ch) }, 1);
    assert_eq!(unsafe { (a.channel_subscribers)(ch) }, 3);

    let publish = |data: &Value, ts: i64, version: u32, accept: AcceptCb| {
        let bytes = mp(data);
        let mut queued = 99usize;
        let st = unsafe {
            (a.publish)(
                ch,
                bytes.as_ptr(),
                bytes.len(),
                ts,
                version,
                accept,
                l.ud(),
                &mut queued,
            )
        };
        (st, queued)
    };
    // Everybody.
    assert_eq!(publish(&Value::from("all"), i64::MIN, 0, None), (0, 3));
    // Only the subscriptions of version 2.
    assert_eq!(publish(&Value::from("v2"), 123, 2, None), (0, 1));
    // The accept callback runs on this thread, once per subscription, and picks.
    assert_eq!(
        publish(&Value::from("picked"), i64::MIN, 0, Some(accept_yes)),
        (0, 2)
    );
    assert_eq!(l.lines().iter().filter(|s| *s == "accept").count(), 3);
    // accept and a timestamp do not combine.
    assert_eq!(publish(&Value::from("x"), 5, 0, Some(accept_yes)).0, -11);

    wait_for("deliveries", || {
        yes.lock().unwrap().len() == 2
            && no.lock().unwrap().len() == 1
            && v2.lock().unwrap().len() == 3
    });
    let names =
        |s: &Seen| -> Vec<Value> { s.lock().unwrap().iter().map(|x| x.0.clone()).collect() };
    assert_eq!(names(&yes), [Value::from("all"), Value::from("picked")]);
    assert_eq!(names(&no), [Value::from("all")]);
    assert_eq!(
        names(&v2),
        [Value::from("all"), Value::from("v2"), Value::from("picked")]
    );

    // A stream is not published to, a state takes no accept.
    let layout = mp(&Layout::new([("n", FieldType::U32, 1)], Some("t"))
        .unwrap()
        .to_value());
    let mut stream = std::ptr::null_mut();
    let st = unsafe {
        (a.stream_channel)(
            fx.node,
            c"st".as_ptr(),
            layout.as_ptr(),
            layout.len(),
            std::ptr::null(),
            0,
            25.0,
            8,
            &mut stream,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let one = mp(&Value::from(1));
    let st = unsafe {
        (a.publish)(
            stream,
            one.as_ptr(),
            one.len(),
            i64::MIN,
            0,
            None,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(st, -11);
    let st =
        unsafe { (a.stream_send)(ch, one.as_ptr(), one.len(), i64::MIN, std::ptr::null_mut()) };
    assert_eq!(st, -11);
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.state_get)(ch, &mut out) }, -11);
    unsafe { (a.channel_free)(stream) };

    s1.close();
    s2.close();
    s3.close();
    other.close();
    wait_for("inactive", || l.lines().contains(&"active:0".to_owned()));
    unsafe { (a.channel_free)(ch) };
    fx.free();
    wait_for("the release", || l.released.load(Ordering::SeqCst) == 1);
    assert_clean();
}

unsafe extern "C-unwind" fn active_cb(user: *mut c_void, active: i32) {
    unsafe { log(user) }.push(format!("active:{active}"));
}

#[test]
fn a_state_channel_keeps_its_value_and_gives_new_subscribers_a_snapshot() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let initial = mp(&Value::from("start"));
    let mut ch = std::ptr::null_mut();
    let st = unsafe {
        (a.state_channel)(
            fx.node,
            c"stt".as_ptr(),
            initial.as_ptr(),
            initial.len(),
            std::ptr::null(),
            0,
            &mut ch,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let get = || {
        let mut out = NO_BUFFER;
        assert_eq!(unsafe { (a.state_get)(ch, &mut out) }, 0);
        val(out)
    };
    assert_eq!(get(), Value::from("start"));
    let next = mp(&Value::from("second"));
    let mut queued = 9usize;
    let st = unsafe {
        (a.publish)(
            ch,
            next.as_ptr(),
            next.len(),
            i64::MIN,
            0,
            None,
            std::ptr::null_mut(),
            &mut queued,
        )
    };
    assert_eq!((st, queued), (0, 0));
    assert_eq!(get(), Value::from("second"));

    let peer = fx.start();
    let seen = Seen::default();
    let sub = peer
        .subscribe("stt", Default::default(), collect(&seen))
        .unwrap();
    wait_for("the snapshot", || !seen.lock().unwrap().is_empty());
    assert_eq!(seen.lock().unwrap()[0], (Value::from("second"), true));
    let last = mp(&Value::from("third"));
    let st = unsafe {
        (a.publish)(
            ch,
            last.as_ptr(),
            last.len(),
            i64::MIN,
            0,
            None,
            std::ptr::null_mut(),
            &mut queued,
        )
    };
    assert_eq!((st, queued), (0, 1));
    wait_for("the update", || seen.lock().unwrap().len() == 2);
    // A state channel takes no accept.
    let st = unsafe {
        (a.publish)(
            ch,
            last.as_ptr(),
            last.len(),
            i64::MIN,
            0,
            Some(accept_yes),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(st, -11);
    sub.close();
    unsafe { (a.channel_free)(ch) };
    fx.free();
    assert_clean();
}

#[test]
fn a_stream_channel_sends_samples_and_reports_its_layout() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let layout = mp(&Layout::new([("n", FieldType::U32, 1)], Some("t"))
        .unwrap()
        .to_value());
    let mut ch = std::ptr::null_mut();
    let st = unsafe {
        (a.stream_channel)(
            fx.node,
            c"smp".as_ptr(),
            layout.as_ptr(),
            layout.len(),
            std::ptr::null(),
            0,
            25.0,
            0,
            &mut ch,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.channel_info)(ch, &mut out) }, 0);
    let info = val(out);
    assert_eq!(key_of(&info, "kind"), Value::from("stream"));
    assert!(key_of(&info, "layout").is_map());
    assert_eq!(key_of(&info, "rate_hz").as_f64(), Some(25.0));

    let peer = fx.start();
    let got = Arc::new(Mutex::new(Vec::new()));
    let g2 = got.clone();
    let sub = peer
        .subscribe("smp", Default::default(), move |d| {
            if let Delivery::Sample(s) = d {
                g2.lock().unwrap().push((s.t_ns, s.body.to_vec()));
            }
        })
        .unwrap();
    wait_for(
        "the subscriber",
        || unsafe { (a.channel_subscribers)(ch) } == 1,
    );
    for n in 0u32..3 {
        let body = n.to_le_bytes();
        let mut sent = 9usize;
        let st = unsafe {
            (a.stream_send)(
                ch,
                body.as_ptr(),
                body.len(),
                1_000 + i64::from(n),
                &mut sent,
            )
        };
        assert_eq!((st, sent), (0, 1));
    }
    wait_for("samples", || got.lock().unwrap().len() == 3);
    assert_eq!(got.lock().unwrap()[2], (1_002, 2u32.to_le_bytes().to_vec()));
    // A body of the wrong size is an invalid argument.
    let st = unsafe { (a.stream_send)(ch, [0u8; 3].as_ptr(), 3, i64::MIN, std::ptr::null_mut()) };
    assert_eq!(st, -11);
    sub.close();
    unsafe { (a.channel_free)(ch) };
    fx.free();
    assert_clean();
}

unsafe extern "C-unwind" fn gate(
    user: *mut c_void,
    ctx: *mut c_void,
    peer: *mut c_void,
    req: *const u8,
    len: usize,
) -> i32 {
    assert!(!peer.is_null());
    let v = codec::decode(unsafe { std::slice::from_raw_parts(req, len) }).unwrap();
    assert!(key_of(&v, "peer").as_u64().unwrap() != 0);
    assert_eq!(key_of(&key_of(&v, "info"), "app"), Value::from("tester"));
    assert_eq!(key_of(&v, "version"), Value::from(1));
    unsafe { log(user) }.push("gate");
    if key_of(&key_of(&v, "options"), "want") == Value::from("yes") {
        return 0;
    }
    let details = mp(&Value::from("members only"));
    unsafe {
        (api().refuse)(
            ctx,
            c"forbidden".as_ptr(),
            c"not for you".as_ptr(),
            details.as_ptr(),
            details.len(),
        )
    };
    1
}

#[test]
fn the_subscribe_hook_accepts_or_refuses_and_close_ends_subscriptions() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    let ch = new_event(&fx, c"gated", &[1]);
    assert_eq!(
        unsafe { (a.on_subscribe)(ch, Some(gate), l.ud(), Some(release_log)) },
        0
    );
    let peer = fx.start();
    let seen = Seen::default();
    let refused = peer.subscribe("gated", opts_with("no"), collect(&seen));
    let Err(stintmate_ipc::error::Error::Remote(r)) = refused else {
        panic!("expected a refusal");
    };
    assert_eq!(
        (r.code.as_str(), r.message.as_str()),
        ("forbidden", "not for you")
    );
    assert_eq!(r.details, Some(Value::from("members only")));
    let ended = Arc::new(Mutex::new(Vec::new()));
    let e2 = ended.clone();
    let _sub = peer
        .subscribe(
            "gated",
            SubscribeOptions {
                on_end: Some(Arc::new(move |_ch: &str, code: &str, msg: &str| {
                    e2.lock().unwrap().push((code.to_owned(), msg.to_owned()));
                })),
                ..opts_with("yes")
            },
            collect(&seen),
        )
        .unwrap();
    assert_eq!(l.lines(), ["gate", "gate"]);
    assert_eq!(unsafe { (a.channel_subscribers)(ch) }, 1);
    assert_eq!(
        unsafe { (a.channel_close)(ch, c"retired".as_ptr(), c"going away".as_ptr()) },
        0
    );
    wait_for("the end", || !ended.lock().unwrap().is_empty());
    assert_eq!(ended.lock().unwrap()[0].0, "retired");
    unsafe { (a.channel_free)(ch) };
    fx.free();
    wait_for("the release", || l.released.load(Ordering::SeqCst) == 1);
    assert_clean();
}
