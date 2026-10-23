//! The blocking façade over real named pipes: handlers run on the pool and callbacks on the
//! connection's lane, never on a runtime thread, and in order (plan section 5).

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{key, ks, map, pipe_name};
use stintmate_ipc::blocking::{Client, Endpoint};
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::error::{Error, RemoteError};
use stintmate_ipc::handshake::PeerInfo;
use stintmate_ipc::layout::{FieldType, Layout, Scalar};
use stintmate_ipc::{Delivery, MethodOpts, SubscribeOptions};

fn thread_name() -> String {
    std::thread::current().name().unwrap_or("").to_owned()
}

fn wait_for(secs: u64, what: &str, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn pair() -> (Endpoint, Client) {
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .handler_threads(3)
        .build()
        .unwrap();
    ep.start().unwrap();
    let c = Client::new(&pipe, PeerInfo::new("client"), ks(k)).unwrap();
    c.start();
    c.wait_connected(Duration::from_secs(5)).unwrap();
    (ep, c)
}

fn layout() -> Layout {
    Layout::new([("n", FieldType::U32, 1)], Some("t")).unwrap()
}

#[test]
fn handlers_run_on_the_pool_never_on_a_runtime_thread() {
    let (ep, c) = pair();
    ep.method("t.who", MethodOpts::default(), |_ctx, _p| {
        Ok(Value::from(thread_name()))
    })
    .unwrap();
    ep.method(
        "t.panic",
        MethodOpts::default(),
        |_ctx, _p| -> Result<Value, RemoteError> { panic!("boom") },
    )
    .unwrap();
    let who = c.call("t.who", Value::Nil).unwrap();
    let who = who.as_str().unwrap().to_owned();
    assert!(who.starts_with("stintmate-ipc-handler-"), "{who}");
    match c.call("t.panic", Value::Nil) {
        Err(Error::Remote(e)) => assert_eq!(e.code, "internal"),
        other => panic!("{other:?}"),
    }
    // The pool survived the panic.
    assert!(c.call("t.who", Value::Nil).is_ok());
    c.close();
    ep.stop();
}

#[test]
fn ordered_methods_queue_per_connection_and_unordered_use_the_pool_in_parallel() {
    let (ep, c) = pair();
    let (active, max, log) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Mutex::new(Vec::new())),
    );
    for (name, ordered) in [("t.ord", true), ("t.par", false)] {
        let (active, max, log) = (active.clone(), max.clone(), log.clone());
        ep.method(
            name,
            MethodOpts::default().ordered(ordered),
            move |_c, p| {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                log.lock().unwrap().push(p.as_i64().unwrap());
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(p)
            },
        )
        .unwrap();
    }
    let pend: Vec<_> = (0..6)
        .map(|i| {
            c.call_async("t.ord", Value::from(i), CallOptions::new())
                .unwrap()
        })
        .collect();
    for p in &pend {
        p.wait().unwrap();
    }
    assert_eq!(*log.lock().unwrap(), (0..6).collect::<Vec<i64>>());
    assert_eq!(max.load(Ordering::SeqCst), 1);
    let pend: Vec<_> = (0..3)
        .map(|i| {
            c.call_async("t.par", Value::from(i), CallOptions::new())
                .unwrap()
        })
        .collect();
    for p in &pend {
        p.wait().unwrap();
    }
    assert!(max.load(Ordering::SeqCst) > 1);
    c.close();
    ep.stop();
}

#[test]
fn callbacks_run_on_one_lane_thread_in_order_and_may_call_the_peer() {
    let (ep, c) = pair();
    let ch = ep.event_channel("t.e", &[1]).unwrap();
    ep.method("t.echo", MethodOpts::default(), |_c, p| Ok(p))
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (s, cc) = (seen.clone(), c.clone());
    let _h = c.subscribe("t.e", SubscribeOptions::default(), move |d| {
        let Delivery::Event(e) = d else { return };
        // Blocking on the peer from a callback is allowed: the lane is not the reader.
        let echoed = cc.call("t.echo", e.data.clone()).unwrap();
        s.lock().unwrap().push((thread_name(), e.seq, echoed));
    });
    for i in 0..30 {
        ch.publish(&Value::from(i)).unwrap();
    }
    wait_for(5, "30 callbacks", || seen.lock().unwrap().len() == 30);
    let seen = seen.lock().unwrap();
    assert!(seen.iter().all(|(t, _, _)| t == "stintmate-ipc-callbacks"));
    assert_eq!(
        seen.iter().map(|(_, s, _)| *s).collect::<Vec<_>>(),
        (1..=30).collect::<Vec<u64>>()
    );
    assert_eq!(seen[5].2, Value::from(5));
    drop(seen);
    c.close();
    ep.stop();
}

#[test]
fn inline_methods_run_on_the_lane_and_hooks_on_the_pool() {
    let (ep, c) = pair();
    ep.method("t.in", MethodOpts::default().inline(true), |_c, _p| {
        Ok(Value::from(thread_name()))
    })
    .unwrap();
    let ch = ep.event_channel("t.e", &[1]).unwrap();
    let hook_thread = Arc::new(Mutex::new(String::new()));
    let h = hook_thread.clone();
    ep.on_subscribe(&ch, move |req| {
        *h.lock().unwrap() = thread_name();
        if codec::get(&req.options, "deny").is_some() {
            Err(RemoteError::new("forbidden", "no"))
        } else {
            Ok(())
        }
    });
    let r = c.call("t.in", Value::Nil).unwrap();
    assert_eq!(r.as_str(), Some("stintmate-ipc-callbacks"));
    let peer = c.peer().unwrap();
    let denied = peer.subscribe(
        "t.e",
        SubscribeOptions {
            options: Some(map([("deny", Value::from(1))])),
            ..SubscribeOptions::default()
        },
        |_| {},
    );
    assert!(matches!(denied, Err(Error::Remote(ref e)) if e.code == "forbidden"));
    assert!(
        hook_thread
            .lock()
            .unwrap()
            .starts_with("stintmate-ipc-handler-")
    );
    assert!(
        peer.subscribe("t.e", SubscribeOptions::default(), |_| {})
            .is_ok()
    );
    c.close();
    ep.stop();
}

#[test]
fn a_slow_callback_coalesces_state_drops_stream_samples_and_ends_events_locally() {
    let (ep, c) = pair();
    let st = ep.state_channel("t.s", Value::from(0), &[1]).unwrap();
    let sm = ep
        .stream_channel_with("t.x", layout(), &[1], Some(1.0), Some(100_000))
        .unwrap();
    let ev = ep.event_channel("t.e", &[1]).unwrap();
    let gate = Arc::new(AtomicBool::new(false));
    let (got_s, got_x) = (
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
    );
    let wait_gate = {
        let g = gate.clone();
        move || {
            while !g.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    };
    let (gs, w) = (got_s.clone(), wait_gate.clone());
    let _h1 = c.subscribe("t.s", SubscribeOptions::default(), move |d| {
        w();
        if let Delivery::Event(e) = d {
            gs.lock().unwrap().push((e.seq, e.data));
        }
    });
    let (gx, w) = (got_x.clone(), wait_gate.clone());
    let _h2 = c.subscribe("t.x", SubscribeOptions::default(), move |d| {
        w();
        if let Delivery::Sample(s) = d {
            gx.lock().unwrap().push((s.seq, s.dropped));
        }
    });
    let ended = Arc::new(Mutex::new(Vec::new()));
    let e2 = ended.clone();
    let _h3 = c.subscribe(
        "t.e",
        SubscribeOptions {
            on_end: Some(Arc::new(move |_c, code, _m| {
                e2.lock().unwrap().push(code.to_owned())
            })),
            ..SubscribeOptions::default()
        },
        move |_| wait_gate(),
    );
    wait_for(3, "subscriptions", || {
        st.subscriber_count() + sm.subscriber_count() + ev.subscriber_count() == 3
    });
    // The first state callback (the snapshot) blocks on the gate; meanwhile 20 values arrive.
    for i in 1..=20 {
        st.set(&Value::from(i)).unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    // The stream callback blocks on the first sample while 200 more arrive: its local queue
    // (max(64, rate) = 64) overflows and drops the oldest.
    for i in 0..200u32 {
        sm.send_values(&[Scalar::U32(i)], None).unwrap();
        std::thread::sleep(Duration::from_micros(300));
    }
    // 1025+ events overflow the local queue of 1024: the subscription ends `overloaded`.
    for i in 0..1100 {
        ev.publish(&Value::from(i)).unwrap();
    }
    // The `end` callback is queued behind the blocked callback on the same lane, in order.
    wait_for(10, "the overload", || ev.subscriber_count() == 0);
    gate.store(true, Ordering::SeqCst);
    wait_for(10, "the end callback", || !ended.lock().unwrap().is_empty());
    assert_eq!(ended.lock().unwrap()[0], "overloaded");
    wait_for(5, "the latest state", || {
        got_s
            .lock()
            .unwrap()
            .last()
            .is_some_and(|(_, v)| *v == Value::from(20))
    });
    let s = got_s.lock().unwrap().clone();
    assert!(
        s.len() < 21,
        "coalesced: {} deliveries for 21 values",
        s.len()
    );
    assert_eq!(s[0].1, Value::from(0), "the snapshot came first");
    wait_for(5, "the samples", || got_x.lock().unwrap().len() >= 2);
    std::thread::sleep(Duration::from_millis(200));
    let x = got_x.lock().unwrap().clone();
    assert!(
        x.len() < 200,
        "samples were dropped locally ({} delivered)",
        x.len()
    );
    assert!(
        x.iter().any(|(_, dropped)| *dropped),
        "the next sample says dropped"
    );
    let seqs: Vec<u32> = x.iter().map(|(s, _)| *s).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "in order");
    // The client re-subscribes the ended event subscription.
    wait_for(6, "re-subscription", || ev.subscriber_count() == 1);
    c.close();
    ep.stop();
}

#[test]
fn pending_calls_cancel_and_wait_without_giving_up() {
    let (ep, c) = pair();
    let seen = Arc::new(AtomicBool::new(false));
    let s = seen.clone();
    ep.method("t.slow", MethodOpts::default(), move |ctx, _p| {
        let end = Instant::now() + Duration::from_millis(600);
        while Instant::now() < end {
            if ctx.wait_cancelled(Duration::from_millis(20)) {
                s.store(true, Ordering::SeqCst);
                return Ok(Value::from("cancelled"));
            }
        }
        Ok(Value::from("finished"))
    })
    .unwrap();
    let p = c
        .call_async("t.slow", Value::Nil, CallOptions::new().no_timeout())
        .unwrap();
    assert!(
        p.wait_timeout(Some(Duration::from_millis(100))).is_none(),
        "still running"
    );
    assert!(
        matches!(p.wait(), Ok(v) if v == Value::from("finished")),
        "the short wait gave nothing up"
    );
    let p = c
        .call_async("t.slow", Value::Nil, CallOptions::new().no_timeout())
        .unwrap();
    std::thread::sleep(Duration::from_millis(80));
    assert!(p.cancel());
    assert!(matches!(p.wait(), Err(Error::CallCancelled)));
    wait_for(3, "the handler to see cancel", || {
        seen.load(Ordering::SeqCst)
    });
    c.close();
    ep.stop();
}

#[test]
fn reconnect_and_resubscribe_with_the_blocking_client() {
    let (pipe, k) = (pipe_name(), key());
    let build = || {
        Endpoint::builder(PeerInfo::new("server"), ks(k))
            .pipe(&pipe)
            .build()
            .unwrap()
    };
    let ep = build();
    let st = ep.state_channel("t.s", Value::from("one"), &[1]).unwrap();
    ep.start().unwrap();
    let c = Client::new(&pipe, PeerInfo::new("client"), ks(k)).unwrap();
    c.start();
    c.wait_connected(Duration::from_secs(5)).unwrap();
    let got = Arc::new(Mutex::new(Vec::new()));
    let g = got.clone();
    let h = c.subscribe("t.s", SubscribeOptions::default(), move |d| {
        if let Delivery::Event(e) = d {
            g.lock().unwrap().push(e.data);
        }
    });
    wait_for(3, "snapshot", || got.lock().unwrap().len() == 1);
    drop(st);
    ep.stop();
    wait_for(3, "disconnect", || !c.is_connected());
    let ep2 = build();
    let st2 = ep2.state_channel("t.s", Value::from("two"), &[1]).unwrap();
    ep2.start().unwrap();
    c.wait_connected(Duration::from_secs(10)).unwrap();
    wait_for(5, "snapshot again", || got.lock().unwrap().len() == 2);
    st2.set(&Value::from("three")).unwrap();
    wait_for(3, "change", || got.lock().unwrap().len() == 3);
    assert_eq!(got.lock().unwrap()[1], Value::from("two"));
    drop(h);
    wait_for(3, "unsub on drop", || st2.subscriber_count() == 0);
    c.close();
    ep2.stop();
}

/// SPEC 13.3 with a handler on the pool: the `evt`s it publishes before returning are written
/// before its `res`. The client is the async one, so what it sees is the wire order.
#[test]
fn events_published_by_a_pool_handler_arrive_before_its_res() {
    const N: usize = 20;
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .handler_threads(3)
        .build()
        .unwrap();
    let ch = ep.event_channel("t.ev", &[1]).unwrap();
    let chh = ch.clone();
    ep.method("t.burst", MethodOpts::default(), move |_c, _p| {
        for i in 0..N {
            chh.publish(&map([("i", Value::from(i as i64))])).unwrap();
        }
        Ok(Value::Nil)
    })
    .unwrap();
    ep.start().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let c = common::inline_client(&pipe, k);
        c.start();
        c.wait_connected(Duration::from_secs(5)).await.unwrap();
        let sink = common::Sink::default();
        let h = c
            .subscribe(
                "t.ev",
                stintmate_ipc::SubscribeOptions::default(),
                sink.callback(),
            )
            .await;
        assert!(h.is_subscribed());
        for round in 0..200usize {
            c.call("t.burst", Value::Nil).await.unwrap();
            assert_eq!(sink.len(), (round + 1) * N, "round {round}: evt after res");
        }
        c.close().await;
    });
    ep.stop();
}
