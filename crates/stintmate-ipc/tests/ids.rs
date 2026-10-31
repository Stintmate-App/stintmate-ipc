//! Ids in flight (SPEC 10.2, 11.2): a `req` and a `sub` of one sender share one id space, and
//! reusing an id that still waits for its `res` is a protocol error. Scripted peer over a real
//! pipe with a random name.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use stintmate_ipc::MethodOpts;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::connection::{CloseInfo, Connection, ConnectionOptions, NoHandler, connect};
use stintmate_ipc::decl::Declarations;
use stintmate_ipc::handshake::{ClientConfig, PeerInfo};
use tokio::sync::Notify;

async fn raw(pipe: &str, k: stintmate_ipc::keyfile::Key) -> Connection {
    let cfg = ClientConfig::new(pipe, PeerInfo::new("raw"), Declarations::default());
    connect(
        &stintmate_ipc::naming::split_pipe(pipe).unwrap().0,
        cfg,
        k,
        Arc::new(NoHandler),
        ConnectionOptions::default(),
    )
    .await
    .unwrap()
}

fn req(id: u64, m: &str) -> Value {
    map([
        ("t", Value::from("req")),
        ("id", Value::from(id)),
        ("m", Value::from(m)),
    ])
}

fn sub(id: u64, ch: &str) -> Value {
    map([
        ("t", Value::from("sub")),
        ("id", Value::from(id)),
        ("ch", Value::from(ch)),
    ])
}

async fn closed_by_protocol_error(c: &Connection) -> CloseInfo {
    tokio::time::timeout(Duration::from_secs(3), c.wait_closed())
        .await
        .expect("the owner closes the connection")
}

/// An endpoint with a method that never answers and a channel whose subscribe hook never
/// answers until `gate` opens.
async fn owner(
    gate: Arc<Notify>,
) -> (stintmate_ipc::Endpoint, String, stintmate_ipc::keyfile::Key) {
    let (pipe, k) = (pipe_name(), key());
    let ep = endpoint(&pipe, k);
    let hold = Arc::new(Notify::new());
    ep.method("wait", MethodOpts::default(), move |_c, _p| {
        let hold = hold.clone();
        async move {
            hold.notified().await;
            Ok(Value::Nil)
        }
    })
    .unwrap();
    let slow = ep.event_channel("test.slow", &[1]).unwrap();
    slow.on_subscribe(move |_| {
        let gate = gate.clone();
        Box::pin(async move {
            gate.notified().await;
            Ok(())
        })
    });
    ep.event_channel("test.fast", &[1]).unwrap();
    ep.start().await.unwrap();
    (ep, pipe, k)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sub_reusing_the_id_of_a_pending_req_is_a_protocol_error() {
    let (ep, pipe, k) = owner(Arc::new(Notify::new())).await;
    let c = raw(&pipe, k).await;
    c.send_message(&req(5, "wait")).await.unwrap();
    c.send_message(&sub(5, "test.fast")).await.unwrap();
    assert_eq!(closed_by_protocol_error(&c).await.code, "protocol_error");
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_req_reusing_the_id_of_a_pending_sub_is_a_protocol_error() {
    let (ep, pipe, k) = owner(Arc::new(Notify::new())).await;
    let c = raw(&pipe, k).await;
    c.send_message(&sub(6, "test.slow")).await.unwrap();
    c.send_message(&req(6, "wait")).await.unwrap();
    assert_eq!(closed_by_protocol_error(&c).await.code, "protocol_error");
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sub_reusing_the_id_of_a_pending_sub_is_a_protocol_error() {
    let (ep, pipe, k) = owner(Arc::new(Notify::new())).await;
    let c = raw(&pipe, k).await;
    c.send_message(&sub(7, "test.slow")).await.unwrap();
    c.send_message(&sub(7, "test.fast")).await.unwrap();
    assert_eq!(closed_by_protocol_error(&c).await.code, "protocol_error");
    ep.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_id_answered_is_free_again() {
    let (ep, pipe, k) = owner(Arc::new(Notify::new())).await;
    let c = raw(&pipe, k).await;
    c.send_message(&sub(8, "test.fast")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The `sub` was answered: the same number may now carry a `req`, and a `sub` again.
    c.send_message(&req(8, "nothing")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.send_message(&sub(8, "test.fast")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!c.is_closed(), "{:?}", c.close_info());
    assert_eq!(codec::get(&req(8, "x"), "id"), Some(&Value::from(8u64)));
    c.close("normal", "done");
    ep.stop().await;
}
