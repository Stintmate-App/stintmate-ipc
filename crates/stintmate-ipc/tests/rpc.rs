//! Methods over real named pipes: context,
//! errors, deadlines and `cancel`, progress, `ordered`, idempotency (SPEC 10.4, 0.1.1 rules),
//! large payloads, calls in both directions, `decl` updates and `busy`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use stintmate_ipc::codec::Value;
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::error::{Error, RemoteError};
use stintmate_ipc::handshake::PeerInfo;
use stintmate_ipc::{Endpoint, MethodOpts};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_echo_and_context() {
    let (ep, c) = pair().await;
    ep.method(
        "test.ctx",
        MethodOpts::default().versions(&[1, 2]),
        |ctx, p| async move {
            Ok(map([
                ("app", Value::from(ctx.peer().info().app.as_str())),
                ("method", Value::from(ctx.method())),
                ("v", Value::from(ctx.version())),
                ("ik", ctx.idempotency_key().map_or(Value::Nil, Value::from)),
                ("pid_known", Value::Boolean(ctx.peer().pid().is_some())),
                ("p", p),
            ]))
        },
    )
    .unwrap();
    let r = c
        .call_with(
            "test.ctx",
            Value::from(7),
            CallOptions::new().version(2).idempotency_key("abc"),
        )
        .await
        .unwrap();
    let g = |k| stintmate_ipc::codec::get(&r, k).cloned().unwrap();
    assert_eq!(g("app"), Value::from("client"));
    assert_eq!(g("method"), Value::from("test.ctx"));
    assert_eq!(g("v"), Value::from(2));
    assert_eq!(g("ik"), Value::from("abc"));
    assert_eq!(g("pid_known"), Value::Boolean(true));
    assert_eq!(g("p"), Value::from(7));
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors() {
    let (ep, c) = pair().await;
    ep.method("test.fail", MethodOpts::default(), |_c, _p| async move {
        let mut e = RemoteError::new("no_such_lap", "lap 9 does not exist");
        e.details = Some(map([("lap", Value::from(9))]));
        e.retry_ms = Some(250);
        Err(e)
    })
    .unwrap();
    ep.method("test.panic", MethodOpts::default(), |_c, _p| async move {
        if true {
            panic!("boom");
        }
        Ok(Value::Nil)
    })
    .unwrap();
    let remote = |r: Result<Value, Error>| match r {
        Err(Error::Remote(e)) => e,
        other => panic!("expected a remote error, got {other:?}"),
    };
    let e = remote(c.call("test.nope", Value::Nil).await);
    assert_eq!(e.code, "unknown_method");
    let e = remote(
        c.call_with("test.fail", Value::Nil, CallOptions::new().version(3))
            .await,
    );
    assert_eq!(e.code, "version_unsupported");
    let versions = stintmate_ipc::codec::get(e.details.as_ref().unwrap(), "versions").unwrap();
    assert_eq!(versions, &Value::Array(vec![Value::from(1)]));
    let e = remote(c.call("test.fail", Value::Nil).await);
    assert_eq!((e.code.as_str(), e.retry_ms), ("no_such_lap", Some(250)));
    assert_eq!(e.message, "lap 9 does not exist");
    assert_eq!(
        stintmate_ipc::codec::get(e.details.as_ref().unwrap(), "lap"),
        Some(&Value::from(9))
    );
    // A panic fails that request only.
    let e = remote(c.call("test.panic", Value::Nil).await);
    assert_eq!(e.code, "internal");
    ep.method(
        "test.ok",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    assert_eq!(
        c.call("test.ok", Value::from(1)).await.unwrap(),
        Value::from(1)
    );
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_sends_cancel() {
    let (ep, c) = pair().await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    ep.method("test.slow", MethodOpts::default(), move |ctx, _p| {
        let flag = flag.clone();
        async move {
            ctx.cancelled().await;
            flag.store(true, Ordering::SeqCst);
            Ok(Value::Nil)
        }
    })
    .unwrap();
    let r = c
        .call_with(
            "test.slow",
            Value::Nil,
            CallOptions::new().timeout(Duration::from_millis(150)),
        )
        .await;
    assert!(matches!(r, Err(Error::CallTimeout)), "{r:?}");
    eventually(3, "the handler to see cancel", || {
        cancelled.load(Ordering::SeqCst)
    })
    .await;
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_wait_without_giving_up_and_progress_extends_the_deadline() {
    let (ep, c) = pair().await;
    ep.method("test.long", MethodOpts::default(), |ctx, _p| async move {
        for i in 0..4 {
            sleep(Duration::from_millis(120)).await;
            ctx.progress(Some(Value::from(i)), Some(Duration::from_millis(300)));
        }
        Ok(Value::from("done"))
    })
    .unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let s = seen.clone();
    let opts = CallOptions::new()
        .timeout(Duration::from_millis(250))
        .on_progress(move |_| {
            s.fetch_add(1, Ordering::SeqCst);
        });
    // 480 ms of work with a 250 ms deadline: only the extensions keep it alive.
    let r = c.call_with("test.long", Value::Nil, opts).await.unwrap();
    assert_eq!(r, Value::from("done"));
    assert_eq!(seen.load(Ordering::SeqCst), 4);
    // Awaiting a PendingCall with a shorter wait does not give the call up.
    let pending = c
        .call_async("test.long", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    let mut pending = Box::pin(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(pending.await.unwrap(), Value::from("done"));
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_methods_run_in_order_one_at_a_time_and_unordered_concurrently() {
    let (ep, c) = pair().await;
    let (active, max, log) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(std::sync::Mutex::new(Vec::new())),
    );
    for (name, ordered) in [("test.ord", true), ("test.par", false)] {
        let (active, max, log) = (active.clone(), max.clone(), log.clone());
        ep.method(
            name,
            MethodOpts::default().ordered(ordered),
            move |_c, p| {
                let (active, max, log) = (active.clone(), max.clone(), log.clone());
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(now, Ordering::SeqCst);
                    sleep(Duration::from_millis(30)).await;
                    log.lock().unwrap().push(p.as_i64().unwrap());
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(p)
                }
            },
        )
        .unwrap();
    }
    let calls: Vec<_> = (0..8)
        .map(|i| c.call_async("test.ord", Value::from(i), CallOptions::new()))
        .collect();
    let mut pend = Vec::new();
    for call in calls {
        pend.push(call.await.unwrap());
    }
    for p in pend {
        p.await.unwrap();
    }
    assert_eq!(*log.lock().unwrap(), (0..8).collect::<Vec<i64>>());
    assert_eq!(max.load(Ordering::SeqCst), 1);
    log.lock().unwrap().clear();
    let mut pend = Vec::new();
    for i in 0..4 {
        pend.push(
            c.call_async("test.par", Value::from(i), CallOptions::new())
                .await
                .unwrap(),
        );
    }
    for p in pend {
        p.await.unwrap();
    }
    assert!(max.load(Ordering::SeqCst) > 1, "unordered calls overlap");
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_whose_deadline_passed_in_the_queue_does_not_run() {
    let (ep, c) = pair().await;
    let ran = Arc::new(AtomicUsize::new(0));
    let r = ran.clone();
    ep.method(
        "test.q",
        MethodOpts::default().ordered(true),
        move |_c, p| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(400)).await;
                Ok(p)
            }
        },
    )
    .unwrap();
    let first = c
        .call_async("test.q", Value::from(1), CallOptions::new())
        .await
        .unwrap();
    let second = c
        .call_async(
            "test.q",
            Value::from(2),
            CallOptions::new().timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap();
    first.await.unwrap();
    // The caller gave up locally at its deadline; the owner must not have run it.
    assert!(matches!(second.await, Err(Error::CallTimeout)));
    sleep(Duration::from_millis(100)).await;
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_cache_and_the_0_1_1_rules() {
    let (ep, c) = pair().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let r = runs.clone();
    ep.method(
        "test.once",
        MethodOpts::default().idempotent(true),
        move |_c, _p| {
            let r = r.clone();
            async move { Ok(Value::from(r.fetch_add(1, Ordering::SeqCst) as i64)) }
        },
    )
    .unwrap();
    let call = |ik: &'static str| {
        let c = c.clone();
        async move {
            c.call_with(
                "test.once",
                Value::Nil,
                CallOptions::new().idempotency_key(ik),
            )
            .await
        }
    };
    assert_eq!(call("k1").await.unwrap(), Value::from(0));
    assert_eq!(
        call("k1").await.unwrap(),
        Value::from(0),
        "the repeat gets the first answer"
    );
    assert_eq!(call("k2").await.unwrap(), Value::from(1));
    // Without a key every call runs.
    assert_eq!(
        c.call("test.once", Value::Nil).await.unwrap(),
        Value::from(2)
    );
    assert_eq!(
        c.call("test.once", Value::Nil).await.unwrap(),
        Value::from(3)
    );

    // Retryable errors do not consume the key; permanent errors stay cached.
    let attempts = Arc::new(AtomicUsize::new(0));
    for (name, code) in [("test.retry", "writer_busy"), ("test.perm", "bad_lap")] {
        let a = attempts.clone();
        ep.method(
            name,
            MethodOpts::default().idempotent(true),
            move |_c, _p| {
                let a = a.clone();
                async move {
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    if n.is_multiple_of(2) {
                        Err(RemoteError::new(code, "no"))
                    } else {
                        Ok(Value::from("ok"))
                    }
                }
            },
        )
        .unwrap();
    }
    let opts = || CallOptions::new().idempotency_key("r1");
    assert!(c.call_with("test.retry", Value::Nil, opts()).await.is_err());
    assert_eq!(
        c.call_with("test.retry", Value::Nil, opts()).await.unwrap(),
        Value::from("ok"),
        "a retryable error ran again"
    );
    attempts.store(0, Ordering::SeqCst);
    for _ in 0..3 {
        let e = c
            .call_with("test.perm", Value::Nil, opts())
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Remote(ref r) if r.code == "bad_lap"));
    }
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a permanent error is remembered"
    );

    // idempotency_cache(false): declared, but the handler deduplicates.
    let runs2 = Arc::new(AtomicUsize::new(0));
    let r2 = runs2.clone();
    ep.method(
        "test.nocache",
        MethodOpts::default()
            .idempotent(true)
            .idempotency_cache(false),
        move |_c, _p| {
            let r = r2.clone();
            async move { Ok(Value::from(r.fetch_add(1, Ordering::SeqCst) as i64)) }
        },
    )
    .unwrap();
    for _ in 0..2 {
        c.call_with(
            "test.nocache",
            Value::Nil,
            CallOptions::new().idempotency_key("z"),
        )
        .await
        .unwrap();
    }
    assert_eq!(runs2.load(Ordering::SeqCst), 2);
    assert!(
        c.peer().unwrap().decl().methods["test.nocache"].idempotent,
        "still declared idempotent"
    );

    // ttl: an answer is forgotten after it.
    let runs3 = Arc::new(AtomicUsize::new(0));
    let r3 = runs3.clone();
    ep.method(
        "test.ttl",
        MethodOpts::default()
            .idempotent(true)
            .idempotency_ttl(Duration::from_millis(150)),
        move |_c, _p| {
            let r = r3.clone();
            async move { Ok(Value::from(r.fetch_add(1, Ordering::SeqCst) as i64)) }
        },
    )
    .unwrap();
    let t = || CallOptions::new().idempotency_key("t");
    c.call_with("test.ttl", Value::Nil, t()).await.unwrap();
    c.call_with("test.ttl", Value::Nil, t()).await.unwrap();
    assert_eq!(runs3.load(Ordering::SeqCst), 1);
    sleep(Duration::from_millis(250)).await;
    c.call_with("test.ttl", Value::Nil, t()).await.unwrap();
    assert_eq!(runs3.load(Ordering::SeqCst), 2);
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeat_while_the_first_runs_waits_then_gets_busy_or_the_answer() {
    let (ep, c) = pair().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let r = runs.clone();
    ep.method(
        "test.slowonce",
        MethodOpts::default().idempotent(true),
        move |_c, _p| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(400)).await;
                Ok(Value::from("first"))
            }
        },
    )
    .unwrap();
    let opts = |ms| {
        CallOptions::new()
            .timeout(Duration::from_millis(ms))
            .idempotency_key("slow")
    };
    let first = c
        .call_async("test.slowonce", Value::Nil, opts(3000))
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;
    // A repeat with a short deadline: busy with retry_ms (the owner never waits forever).
    let owner_err = c
        .peer()
        .unwrap()
        .connection()
        .call("test.slowonce", Value::Nil, {
            // Ask the owner directly with a deadline it will hit before the first finishes.
            let mut o = opts(3000);
            o.timeout = Some(Duration::from_millis(150));
            o
        })
        .await;
    // The caller's own deadline (150 ms) wins locally, but the owner's answer is busy.
    assert!(matches!(
        owner_err,
        Err(Error::CallTimeout) | Err(Error::Remote(_))
    ));
    // A repeat with a long deadline waits and gets the first answer.
    let again = c
        .call_with("test.slowonce", Value::Nil, opts(3000))
        .await
        .unwrap();
    assert_eq!(again, Value::from("first"));
    assert_eq!(first.await.unwrap(), Value::from("first"));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_payloads_both_ways_and_the_server_calls_the_client() {
    let (ep, c) = pair().await;
    ep.method(
        "test.echo",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    let big = Value::from("x".repeat(3 * 1024 * 1024));
    assert_eq!(c.call("test.echo", big.clone()).await.unwrap(), big);
    // The client offers a method the server calls.
    c.method(
        "client.hello",
        MethodOpts::default(),
        |ctx, _p| async move { Ok(Value::from(ctx.peer().info().app.as_str())) },
    )
    .unwrap();
    eventually_async(3, "the server to know the method", || {
        let ep = ep.clone();
        async move {
            ep.peers()
                .first()
                .is_some_and(|p| p.decl().methods.contains_key("client.hello"))
        }
    })
    .await;
    let peer = ep.peers().remove(0);
    let r = peer
        .call("client.hello", Value::Nil, CallOptions::new())
        .await
        .unwrap();
    assert_eq!(r, Value::from("server"));
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaring_later_sends_decl_and_ping_works() {
    let (ep, c) = pair().await;
    let changed = Arc::new(AtomicUsize::new(0));
    let n = changed.clone();
    c.on_redeclare(move |_| {
        n.fetch_add(1, Ordering::SeqCst);
    });
    assert!(!c.peer().unwrap().decl().methods.contains_key("late.m"));
    ep.method(
        "late.m",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    eventually_async(3, "the decl", || {
        let c = c.clone();
        async move { c.peer().unwrap().decl().methods.contains_key("late.m") }
    })
    .await;
    assert_eq!(changed.load(Ordering::SeqCst), 1);
    ep.remove_method("late.m");
    eventually_async(3, "the withdrawal", || {
        let c = c.clone();
        async move { !c.peer().unwrap().decl().methods.contains_key("late.m") }
    })
    .await;
    let rtt = c
        .peer()
        .unwrap()
        .connection()
        .ping(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(rtt < Duration::from_secs(1));
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_call_fails_when_the_connection_closes() {
    let (ep, c) = pair().await;
    ep.method("test.hang", MethodOpts::default(), |ctx, _p| async move {
        ctx.cancelled().await;
        Ok(Value::Nil)
    })
    .unwrap();
    let pending = c
        .call_async("test.hang", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;
    ep.stop().await;
    let r = pending.await;
    assert!(matches!(r, Err(Error::ConnectionLost { .. })), "{r:?}");
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn too_many_requests_in_flight_get_busy() {
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .max_inflight(2)
        .build()
        .unwrap();
    ep.start().await.unwrap();
    ep.method("test.hold", MethodOpts::default(), |ctx, _p| async move {
        ctx.cancelled().await;
        Ok(Value::Nil)
    })
    .unwrap();
    let c = client(&pipe, k);
    c.start();
    c.wait_connected(Duration::from_secs(5)).await.unwrap();
    let a = c
        .call_async("test.hold", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    let b = c
        .call_async("test.hold", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;
    let third = c
        .call_with(
            "test.hold",
            Value::Nil,
            CallOptions::new().timeout(Duration::from_secs(2)),
        )
        .await;
    match third {
        Err(Error::Remote(e)) => assert_eq!(e.code, "busy"),
        other => panic!("expected busy, got {other:?}"),
    }
    assert!(a.cancel() && b.cancel());
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_is_a_no_op_after_the_answer_and_idempotent() {
    let (ep, c) = pair().await;
    let seen = Arc::new(AtomicBool::new(false));
    let s = seen.clone();
    ep.method("test.c", MethodOpts::default(), move |ctx, _p| {
        let s = s.clone();
        async move {
            ctx.cancelled().await;
            s.store(true, Ordering::SeqCst);
            Ok(Value::Nil)
        }
    })
    .unwrap();
    ep.method(
        "test.fast",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    let pending = c
        .call_async("test.c", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;
    let handle = pending.handle();
    assert!(pending.cancel());
    assert!(!handle.cancel(), "the second cancel does nothing");
    assert!(matches!(pending.await, Err(Error::CallCancelled)));
    eventually(3, "the owner to see cancel", || seen.load(Ordering::SeqCst)).await;
    let done = c
        .call_async("test.fast", Value::from(5), CallOptions::new())
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;
    let h = done.handle();
    assert_eq!(done.await.unwrap(), Value::from(5));
    assert!(!h.cancel(), "cancel after the answer is a no-op");
    c.close().await;
    ep.stop().await;
}

/// A repeat of an idempotent request that is still running waits for the first run; past its
/// own deadline it times out, and the first run's answer is still cached.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeat_while_the_first_runs_waits_and_times_out() {
    let (ep, c) = pair().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let r = runs.clone();
    ep.method(
        "test.slow_once",
        MethodOpts::default().idempotent(true),
        move |_c, _p| {
            let r = r.clone();
            async move {
                sleep(Duration::from_millis(700)).await;
                Ok(Value::from(r.fetch_add(1, Ordering::SeqCst) as i64))
            }
        },
    )
    .unwrap();
    let first = {
        let c = c.clone();
        tokio::spawn(async move {
            c.call_with(
                "test.slow_once",
                Value::Nil,
                CallOptions::new().idempotency_key("k").no_timeout(),
            )
            .await
        })
    };
    sleep(Duration::from_millis(100)).await;
    let t0 = std::time::Instant::now();
    let second = c
        .call_with(
            "test.slow_once",
            Value::Nil,
            CallOptions::new()
                .idempotency_key("k")
                .timeout(Duration::from_millis(200)),
        )
        .await;
    assert!(matches!(second, Err(Error::CallTimeout)), "{second:?}");
    assert!(t0.elapsed() < Duration::from_millis(600));
    assert_eq!(first.await.unwrap().unwrap(), Value::from(0));
    // The repeat did not run the handler a second time, and now gets the cached answer.
    let third = c
        .call_with(
            "test.slow_once",
            Value::Nil,
            CallOptions::new().idempotency_key("k"),
        )
        .await
        .unwrap();
    assert_eq!(third, Value::from(0));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    c.close().await;
    ep.stop().await;
}
