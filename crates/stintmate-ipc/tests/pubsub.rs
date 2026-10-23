//! Channels over real named pipes (the rules of the Python suite's `test_pubsub.py`): events,
//! state (snapshot and coalescing), streams (drop-oldest with `dropped`), slow subscribers,
//! refusals, `send_to`, filtered publish and the symmetric case (the client owns a channel).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::{Error, RemoteError};
use stintmate_ipc::layout::{FieldType, Layout, Scalar};
use stintmate_ipc::{Delivery, Endpoint, SubscribeOptions};

fn layout() -> Layout {
    Layout::new(
        [("n", FieldType::U32, 1), ("x", FieldType::F32, 1)],
        Some("t"),
    )
    .unwrap()
}

fn body(n: u32) -> Vec<u8> {
    let mut b = Vec::new();
    layout()
        .pack(&[Scalar::U32(n), Scalar::F32(n as f32)], &mut b)
        .unwrap();
    b
}

fn seqs(d: &[Delivery]) -> Vec<u64> {
    d.iter()
        .map(|d| match d {
            Delivery::Event(e) => e.seq,
            Delivery::Sample(s) => u64::from(s.seq),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_channel_order_and_fields() {
    let (ep, c) = pair().await;
    let ch = ep.event_channel("test.lap", &[1]).unwrap();
    assert!(!ch.is_active());
    let sink = Sink::default();
    let h = c
        .subscribe("test.lap", SubscribeOptions::default(), sink.callback())
        .await;
    assert!(h.error().is_none() && h.is_subscribed());
    assert_eq!(ch.subscriber_count(), 1);
    for i in 0..50 {
        assert_eq!(ch.publish(&map([("lap", Value::from(i))])).unwrap(), 1);
    }
    eventually(3, "50 events", || sink.len() == 50).await;
    let items = sink.items();
    assert_eq!(seqs(&items), (1..=50).collect::<Vec<u64>>());
    let Delivery::Event(first) = &items[0] else {
        panic!()
    };
    assert_eq!(first.channel, "test.lap");
    assert!(first.ts.is_some() && !first.snapshot);
    assert_eq!(sink.data()[7], map([("lap", Value::from(7))]));
    // Closing the handle unsubscribes: the owner forgets the subscription.
    h.close();
    eventually(3, "unsub", || ch.subscriber_count() == 0).await;
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_snapshot_then_changes_and_on_active() {
    let (ep, c) = pair().await;
    let ch = ep
        .state_channel("test.status", Value::from("idle"), &[1])
        .unwrap();
    let flips = Arc::new(std::sync::Mutex::new(Vec::new()));
    let f = flips.clone();
    ch.on_active(move |v| f.lock().unwrap().push(v));
    ch.set(&Value::from("a")).unwrap(); // nobody listens yet
    let sink = Sink::default();
    let h = c
        .subscribe("test.status", SubscribeOptions::default(), sink.callback())
        .await;
    eventually(3, "the snapshot", || sink.len() == 1).await;
    let Delivery::Event(snap) = &sink.items()[0] else {
        panic!()
    };
    assert!(snap.snapshot);
    assert_eq!(snap.data, Value::from("a"));
    ch.set(&Value::from("b")).unwrap();
    eventually(3, "the change", || sink.len() == 2).await;
    assert_eq!(sink.data(), vec![Value::from("a"), Value::from("b")]);
    assert_eq!(ch.value(), Value::from("b"));
    assert_eq!(*flips.lock().unwrap(), vec![true]);
    h.close();
    eventually(3, "inactive", || flips.lock().unwrap().len() == 2).await;
    assert_eq!(*flips.lock().unwrap(), vec![true, false]);
    c.close().await;
    ep.stop().await;
}

/// With a current-thread runtime nothing else runs while the test publishes without awaiting:
/// the pump has not taken anything yet, so the bounded queues decide (deterministic).
#[tokio::test]
async fn state_coalesces_events_overload_and_streams_drop_the_oldest_at_the_owner() {
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(stintmate_ipc::handshake::PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .event_queue(5, 1 << 20)
        .build()
        .unwrap();
    ep.start().await.unwrap();
    let state = ep.state_channel("test.s", Value::from(0), &[1]).unwrap();
    let events = ep.event_channel("test.e", &[1]).unwrap();
    let stream = ep
        .stream_channel_with("test.x", layout(), &[1], Some(100.0), Some(4))
        .unwrap();
    let c = client(&pipe, k);
    c.start();
    c.wait_connected(Duration::from_secs(5)).await.unwrap();
    let (ss, es, xs) = (Sink::default(), Sink::default(), Sink::default());
    let ended = Arc::new(std::sync::Mutex::new(Vec::new()));
    let e2 = ended.clone();
    let opts = SubscribeOptions {
        on_end: Some(Arc::new(move |ch, code, _m| {
            e2.lock().unwrap().push((ch.to_owned(), code.to_owned()));
        })),
        ..SubscribeOptions::default()
    };
    let _h1 = c
        .subscribe("test.s", SubscribeOptions::default(), ss.callback())
        .await;
    let _h2 = c.subscribe("test.e", opts, es.callback()).await;
    let _h3 = c
        .subscribe("test.x", SubscribeOptions::default(), xs.callback())
        .await;
    eventually(3, "the snapshot", || ss.len() == 1).await;

    for i in 1..=10 {
        state.set(&Value::from(i)).unwrap();
        stream.send(&body(i as u32), None).unwrap();
        events.publish(&Value::from(i)).unwrap();
    }
    // State: only the latest is pending (seq skips).
    eventually(3, "the latest state", || ss.len() == 2).await;
    let st = ss.items();
    assert_eq!(sink_values(&st), vec![Value::from(0), Value::from(10)]);
    assert_eq!(seqs(&st)[1], 10, "seq shows the coalesced values");
    // Stream: the ring holds 4; the first delivered says dropped; seq shows the loss.
    eventually(3, "4 samples", || xs.len() == 4).await;
    let xi = xs.items();
    assert_eq!(seqs(&xi), vec![7, 8, 9, 10]);
    let flags: Vec<bool> = xi
        .iter()
        .map(|d| matches!(d, Delivery::Sample(s) if s.dropped))
        .collect();
    assert_eq!(flags, vec![true, false, false, false]);
    let Delivery::Sample(s) = &xi[3] else {
        panic!()
    };
    assert_eq!(
        codec::get(&s.values().unwrap(), "n"),
        Some(&Value::from(10u32))
    );
    // Events: the sixth did not fit: the subscription is ended `overloaded`, not silently cut.
    eventually(3, "end overloaded", || !ended.lock().unwrap().is_empty()).await;
    assert_eq!(
        ended.lock().unwrap()[0],
        ("test.e".to_owned(), "overloaded".to_owned())
    );
    assert_eq!(events.subscriber_count(), 0);
    // The connection and the other subscriptions live on.
    assert_eq!(state.subscriber_count(), 1);
    // The client re-subscribes the overloaded one after a second.
    eventually(5, "the event subscription again", || {
        events.subscriber_count() == 1
    })
    .await;
    c.close().await;
    ep.stop().await;
}

fn sink_values(d: &[Delivery]) -> Vec<Value> {
    d.iter()
        .filter_map(|d| match d {
            Delivery::Event(e) => Some(e.data.clone()),
            Delivery::Sample(_) => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_samples_arrive_in_order_with_the_layout() {
    let (ep, c) = pair().await;
    let ch = ep.stream_channel("test.tel", layout(), &[1]).unwrap();
    let sink = Sink::default();
    let h = c
        .subscribe("test.tel", SubscribeOptions::default(), sink.callback())
        .await;
    assert_eq!(
        h.current().unwrap().layout().unwrap().size(),
        layout().size()
    );
    assert!(ch.send(&[0u8; 3], None).is_err(), "a wrong size is refused");
    for i in 0..20 {
        ch.send_values(
            &[Scalar::U32(i), Scalar::F32(0.5)],
            Some(1000 + i64::from(i)),
        )
        .unwrap();
    }
    eventually(3, "20 samples", || sink.len() == 20).await;
    for (i, d) in sink.items().iter().enumerate() {
        let Delivery::Sample(s) = d else { panic!() };
        assert_eq!(
            (s.seq, s.t_ns, s.dropped),
            (i as u32 + 1, 1000 + i as i64, false)
        );
        assert_eq!(
            codec::get(&s.values().unwrap(), "n"),
            Some(&Value::from(i as u32))
        );
    }
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_channel_version_refusals_and_options() {
    let (ep, c) = pair().await;
    let ch = ep.event_channel("test.e", &[1, 2]).unwrap();
    let peer = c.peer().unwrap();
    let sub = |name: &'static str, v: u32| {
        let peer = peer.clone();
        async move {
            peer.subscribe(
                name,
                SubscribeOptions {
                    version: v,
                    ..SubscribeOptions::default()
                },
                Arc::new(|_| {}),
            )
            .await
        }
    };
    let code = |r: Result<_, Error>| match r {
        Err(Error::Remote(e)) => e,
        Err(e) => panic!("{e:?}"),
        Ok(_) => panic!("accepted"),
    };
    assert_eq!(code(sub("test.nope", 1).await).code, "unknown_channel");
    let e = code(sub("test.e", 3).await);
    assert_eq!(e.code, "version_unsupported");
    assert_eq!(
        codec::get(e.details.as_ref().unwrap(), "versions"),
        Some(&Value::Array(vec![Value::from(1), Value::from(2)]))
    );
    // on_subscribe sees the options and can refuse.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = seen.clone();
    ch.on_subscribe(move |req| {
        s.lock().unwrap().push(req.options.clone());
        Box::pin(async move {
            if codec::get(&req.options, "deny").is_some() {
                Err(RemoteError::new("forbidden", "not for you"))
            } else {
                Ok(())
            }
        })
    });
    let with = |o: Value| SubscribeOptions {
        options: Some(o),
        ..SubscribeOptions::default()
    };
    let ok = peer
        .subscribe(
            "test.e",
            with(map([("mine", Value::from(1))])),
            Arc::new(|_| {}),
        )
        .await;
    assert!(ok.is_ok());
    let denied = peer
        .subscribe(
            "test.e",
            SubscribeOptions {
                version: 2, // the same channel and version again is the same subscription
                ..with(map([("deny", Value::from(true))]))
            },
            Arc::new(|_| {}),
        )
        .await;
    assert_eq!(code(denied).code, "forbidden");
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert_eq!(ch.subscriber_count(), 1);
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_subscription_twice_is_one_and_closing_a_channel_ends_it() {
    let (ep, c) = pair().await;
    let ch = ep.state_channel("test.s", Value::from(1), &[1]).unwrap();
    let peer = c.peer().unwrap();
    let (a, b) = (Sink::default(), Sink::default());
    let ended = Arc::new(std::sync::Mutex::new(Vec::new()));
    let e2 = ended.clone();
    let o = SubscribeOptions {
        on_end: Some(Arc::new(move |_c, code, _m| {
            e2.lock().unwrap().push(code.to_owned())
        })),
        ..SubscribeOptions::default()
    };
    let s1 = peer
        .subscribe(
            "test.s",
            SubscribeOptions::default(),
            Arc::new(a.callback()),
        )
        .await
        .unwrap();
    // The repeat takes the place of the first on this connection.
    let s2 = peer
        .subscribe("test.s", o, Arc::new(b.callback()))
        .await
        .unwrap();
    assert_eq!(s1.sid(), s2.sid());
    assert_eq!(ch.subscriber_count(), 1);
    assert_eq!(
        s2.snapshot(),
        Some(Value::from(1)),
        "the repeat gets the current value"
    );
    ch.close("channel_closed", "bye");
    eventually(3, "end", || !ended.lock().unwrap().is_empty()).await;
    assert_eq!(ended.lock().unwrap()[0], "channel_closed");
    eventually_async(3, "decl", || {
        let c = c.clone();
        async move { !c.peer().unwrap().decl().channels.contains_key("test.s") }
    })
    .await;
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_subscribers_and_callbacks_may_call_the_peer() {
    let (pipe, k) = (pipe_name(), key());
    let ep = endpoint(&pipe, k);
    ep.start().await.unwrap();
    let ch = ep.event_channel("test.e", &[1]).unwrap();
    ep.method(
        "test.echo",
        stintmate_ipc::MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    let got = Arc::new(AtomicUsize::new(0));
    let mut clients = Vec::new();
    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = client(&pipe, k);
        c.start();
        c.wait_connected(Duration::from_secs(5)).await.unwrap();
        let (g, cc) = (got.clone(), c.clone());
        let rt = tokio::runtime::Handle::current();
        handles.push(
            c.subscribe("test.e", SubscribeOptions::default(), move |d| {
                // The callback calls the peer (from a runtime task, never blocking the reader).
                let (g, cc) = (g.clone(), cc.clone());
                rt.spawn(async move {
                    let Delivery::Event(e) = d else { return };
                    if cc.call("test.echo", e.data).await.is_ok() {
                        g.fetch_add(1, Ordering::SeqCst);
                    }
                });
            })
            .await,
        );
        clients.push(c);
    }
    assert_eq!(ch.publish(&Value::from(1)).unwrap(), 20);
    eventually(5, "20 echoes", || got.load(Ordering::SeqCst) == 20).await;
    drop(handles);
    for c in clients {
        c.close().await;
    }
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_can_own_a_channel_the_server_subscribes_to() {
    let (ep, c) = pair().await;
    let ch = c.stream_channel("client.tel", layout(), &[1]).unwrap();
    eventually_async(3, "the server to see the channel", || {
        let ep = ep.clone();
        async move {
            ep.peers()
                .first()
                .is_some_and(|p| p.decl().channels.contains_key("client.tel"))
        }
    })
    .await;
    let peer = ep.peers().remove(0);
    let sink = Sink::default();
    peer.subscribe(
        "client.tel",
        SubscribeOptions::default(),
        Arc::new(sink.callback()),
    )
    .await
    .unwrap();
    ch.send(&body(3), None).unwrap();
    eventually(3, "the sample", || sink.len() == 1).await;
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_to_one_subscriber_and_filtered_publish() {
    let (pipe, k) = (pipe_name(), key());
    let ep = endpoint(&pipe, k);
    ep.start().await.unwrap();
    let ev = ep.event_channel("test.e", &[1]).unwrap();
    let st = ep.state_channel("test.s", Value::from(0), &[1]).unwrap();
    let mut cs = Vec::new();
    let mut sinks = Vec::new();
    let mut keep = Vec::new();
    for (i, app) in ["alpha", "beta", "alpha"].iter().enumerate() {
        let c =
            stintmate_ipc::Client::new(&pipe, stintmate_ipc::handshake::PeerInfo::new(*app), ks(k))
                .unwrap();
        c.start();
        c.wait_connected(Duration::from_secs(5)).await.unwrap();
        let sink = Sink::default();
        let opts = SubscribeOptions {
            options: Some(map([("n", Value::from(i as i64))])),
            ..SubscribeOptions::default()
        };
        keep.push(c.subscribe("test.e", opts, sink.callback()).await);
        keep.push(
            c.subscribe("test.s", SubscribeOptions::default(), |_| {})
                .await,
        );
        sinks.push(sink);
        cs.push(c);
    }
    assert_eq!(ev.subscriber_count(), 3);
    // By option.
    let n = ev
        .publish_filtered(&Value::from("x"), |v| {
            v.option("n") == Some(&Value::from(1))
        })
        .unwrap();
    assert_eq!(n, 1);
    // By peer app.
    let n = ev
        .publish_filtered(&Value::from("y"), |v| v.peer_info().app == "alpha")
        .unwrap();
    assert_eq!(n, 2);
    // To nobody: nothing is queued.
    assert_eq!(
        ev.publish_filtered(&Value::from("z"), |_| false).unwrap(),
        0
    );
    eventually(3, "deliveries", || {
        sinks.iter().map(Sink::len).sum::<usize>() == 3
    })
    .await;
    assert_eq!(sinks[0].data(), vec![Value::from("y")]);
    assert_eq!(sinks[1].data(), vec![Value::from("x")]);
    assert_eq!(sinks[2].data(), vec![Value::from("y")]);
    // Only to the connection that asked (view.connection() against a request's peer).
    let target = ep
        .peers()
        .into_iter()
        .find(|p| p.info().app == "beta")
        .unwrap();
    assert_eq!(ev.send_to(&target, &Value::from("only")).unwrap(), 1);
    let t2 = target.clone();
    assert_eq!(
        ev.publish_filtered(&Value::from("same"), move |v| *v.connection() == t2)
            .unwrap(),
        1
    );
    assert_eq!(st.send_to(&target, &Value::from("snap")).unwrap(), 1);
    assert_eq!(
        st.value(),
        Value::from(0),
        "the channel's own value is untouched"
    );
    eventually(3, "the replies", || sinks[1].len() == 3).await;
    assert_eq!(
        sinks[1].data()[1..],
        [Value::from("only"), Value::from("same")]
    );
    assert_eq!(sinks[0].len(), 1);
    // A filter that ends a subscription meanwhile is skipped.
    for c in cs {
        c.close().await;
    }
    ep.stop().await;
}
