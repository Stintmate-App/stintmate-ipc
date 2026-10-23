//! Connections that come and go (the rules of the Python suite's `test_lifecycle.py`):
//! reconnection with re-subscription, `bye` codes, hooks, the heartbeat, the registry.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use stintmate_ipc::codec::Value;
use stintmate_ipc::error::Error;
use stintmate_ipc::handshake::PeerInfo;
use stintmate_ipc::{Client, Delivery, Endpoint, MethodOpts, SubscribeOptions};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_reconnects_and_resubscribes() {
    let (pipe, k) = (pipe_name(), key());
    let ep = endpoint(&pipe, k);
    ep.start().await.unwrap();
    let st = ep
        .state_channel("test.s", Value::from("one"), &[1])
        .unwrap();
    ep.method(
        "test.echo",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    let c = client(&pipe, k);
    c.start();
    let first = c.wait_connected(Duration::from_secs(5)).await.unwrap();
    let run1 = first.connection().info().run.clone();
    let sink = Sink::default();
    let h = c
        .subscribe("test.s", SubscribeOptions::default(), sink.callback())
        .await;
    eventually(3, "snapshot", || sink.len() == 1).await;
    drop(st);

    ep.stop().await;
    eventually(3, "the client to notice", || !c.is_connected()).await;
    assert!(matches!(
        c.call("test.echo", Value::Nil).await,
        Err(Error::NotConnected)
    ));

    // A new server on the same name, with another value and another run id.
    let ep2 = endpoint(&pipe, k);
    let st2 = ep2
        .state_channel("test.s", Value::from("two"), &[1])
        .unwrap();
    ep2.method(
        "test.echo",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    ep2.start().await.unwrap();
    let second = c.wait_connected(Duration::from_secs(10)).await.unwrap();
    assert_ne!(
        second.connection().info().run,
        run1,
        "a different run: the server restarted"
    );
    eventually(5, "the snapshot again", || sink.len() == 2).await;
    assert_eq!(sink.data()[1], Value::from("two"));
    assert!(h.is_subscribed());
    st2.set(&Value::from("three")).unwrap();
    eventually(3, "the change", || sink.len() == 3).await;
    assert_eq!(
        c.call("test.echo", Value::from(1)).await.unwrap(),
        Value::from(1)
    );
    c.close().await;
    ep2.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_keeps_trying_until_the_server_starts() {
    let (pipe, k) = (pipe_name(), key());
    let c = client(&pipe, k);
    let errors = Arc::new(AtomicUsize::new(0));
    let e = errors.clone();
    c.on_connect_error(move |err| {
        assert!(matches!(err, Error::PipeNotFound(_)), "{err:?}");
        e.fetch_add(1, Ordering::SeqCst);
    });
    c.start();
    assert!(c.wait_connected(Duration::from_millis(400)).await.is_err());
    assert!(errors.load(Ordering::SeqCst) >= 1);
    assert!(c.last_error().is_some());
    let ep = endpoint(&pipe, k);
    ep.start().await.unwrap();
    c.wait_connected(Duration::from_secs(10)).await.unwrap();
    assert!(c.last_error().is_none());
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endpoint_stop_says_bye_and_the_client_close_is_seen_by_the_server() {
    let (ep, c) = pair().await;
    let connected = Arc::new(AtomicUsize::new(0));
    let gone = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (n, g) = (connected.clone(), gone.clone());
    ep.on_connect(move |p| {
        assert_eq!(p.info().app, "client");
        n.fetch_add(1, Ordering::SeqCst);
    });
    ep.on_disconnect(move |p| {
        g.lock()
            .unwrap()
            .push(p.connection().close_info().map(|i| i.code));
    });
    let c2 = client(ep.path(), match_key(&ep));
    drop(c2);
    // The pair's own client disconnects: the server hears of it.
    let peer = c.peer().unwrap();
    c.close().await;
    eventually(3, "on_disconnect", || !gone.lock().unwrap().is_empty()).await;
    assert!(!peer.is_open());
    // A second client sees `shutting_down` when the endpoint stops.
    let seen_code = Arc::new(std::sync::Mutex::new(None));
    let s = seen_code.clone();
    let (pipe2, k2) = (pipe_name(), key());
    let ep2 = endpoint(&pipe2, k2);
    ep2.start().await.unwrap();
    let c3 = client(&pipe2, k2);
    c3.on_disconnect(move |p| {
        *s.lock().unwrap() = p.connection().close_info().map(|i| i.code);
    });
    c3.start();
    c3.wait_connected(Duration::from_secs(5)).await.unwrap();
    ep2.stop().await;
    eventually(3, "the bye", || seen_code.lock().unwrap().is_some()).await;
    assert_eq!(seen_code.lock().unwrap().as_deref(), Some("shutting_down"));
    c3.close().await;
    ep.stop().await;
    assert_eq!(
        connected.load(Ordering::SeqCst),
        0,
        "hooks set late see only later peers"
    );
}

fn match_key(_ep: &Endpoint) -> stintmate_ipc::keyfile::Key {
    key()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_keeps_an_idle_connection_alive() {
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .heartbeat_ms(100)
        .build()
        .unwrap();
    ep.start().await.unwrap();
    let c = client(&pipe, k);
    c.start();
    let p = c.wait_connected(Duration::from_secs(5)).await.unwrap();
    assert_eq!(p.connection().info().heartbeat_ms, 100);
    sleep(Duration::from_millis(900)).await; // nine heartbeats of silence from the apps
    assert!(c.is_connected() && ep.peers().len() == 1);
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_key_is_refused_and_slows_the_client_down() {
    let (pipe, k) = (pipe_name(), key());
    let ep = endpoint(&pipe, k);
    let failed = Arc::new(AtomicUsize::new(0));
    let f = failed.clone();
    ep.on_handshake_failed(move |pid, e| {
        assert!(pid.is_some() && matches!(e, Error::Handshake(_)), "{e:?}");
        f.fetch_add(1, Ordering::SeqCst);
    });
    ep.start().await.unwrap();
    let c = client(&pipe, key());
    let code = Arc::new(std::sync::Mutex::new(String::new()));
    let cc = code.clone();
    c.on_connect_error(move |e| match e {
        Error::Handshake(f) => *cc.lock().unwrap() = f.code.as_str().to_owned(),
        Error::PeerBye { code, .. } => *cc.lock().unwrap() = code.clone(),
        _ => {}
    });
    c.start();
    eventually(5, "the refusal", || !code.lock().unwrap().is_empty()).await;
    assert_eq!(code.lock().unwrap().as_str(), "unauthorized");
    assert!(!c.is_connected());
    assert_eq!(failed.load(Ordering::SeqCst), 1);
    c.close().await;
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_endpoint_on_the_same_name_is_in_use_and_the_registry_is_kept() {
    let (pipe, k) = (pipe_name(), key());
    let dir = std::env::temp_dir().join(format!("stintmate-t4-reg-{pipe}"));
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .registry(&dir)
        .build()
        .unwrap();
    ep.start().await.unwrap();
    let id = pipe.strip_prefix("stintmate-").unwrap();
    let user = stintmate_ipc::naming::pipe_user().unwrap();
    let id = id.strip_suffix(&format!("-{user}")).unwrap_or(id);
    let file = dir.join(format!("{id}.json"));
    let doc: serde_json::Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    assert_eq!(doc["run"], ep.run_id());
    assert_eq!(doc["versions"], serde_json::json!([2]));
    assert_eq!(doc["pid"], std::process::id());

    let other = endpoint(&pipe, k);
    assert!(matches!(other.start().await, Err(Error::EndpointInUse(_))));
    assert!(ep.start().await.is_err(), "already started");

    ep.stop().await;
    assert!(!file.exists(), "the entry is withdrawn at stop");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_connected_before_close_gets_connection_lost_not_a_hang() {
    let (ep, c) = pair().await;
    ep.method("test.hold", MethodOpts::default(), |ctx, _| async move {
        ctx.cancelled().await;
        Ok(Value::Nil)
    })
    .unwrap();
    let hit = Arc::new(std::sync::Mutex::new(Vec::new()));
    let h = hit.clone();
    let _sub = c
        .subscribe("nope", SubscribeOptions::default(), move |d: Delivery| {
            h.lock().unwrap().push(format!("{d:?}"))
        })
        .await;
    assert!(_sub.error().is_some_and(|e| e.contains("unknown_channel")));
    let pending = c
        .call_async(
            "test.hold",
            Value::Nil,
            stintmate_ipc::connection::CallOptions::new().no_timeout(),
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;
    c.close().await;
    assert!(pending.await.is_err());
    ep.stop().await;
}

#[allow(dead_code)]
fn unused(_: Client) {}
