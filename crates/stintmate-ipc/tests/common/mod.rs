//! Helpers of the node tests: random pipe names and keys, a connected pair, polling.
#![allow(dead_code)]

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::handshake::{PeerInfo, new_nonce};
use stintmate_ipc::keyfile::Key;
use stintmate_ipc::{Client, Delivery, Endpoint, KeySource};

pub fn pipe_name() -> String {
    let n: String = new_nonce()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("stintmate-t4-{n}")
}

pub fn key() -> Key {
    new_nonce()
}

pub fn ks(k: Key) -> KeySource {
    KeySource::Bytes(k)
}

pub fn map<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    codec::map(pairs)
}

pub fn endpoint(pipe: &str, k: Key) -> Endpoint {
    Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(pipe)
        .build()
        .unwrap()
}

pub fn client(pipe: &str, k: Key) -> Client {
    Client::new(pipe, PeerInfo::new("client"), ks(k)).unwrap()
}

/// A started endpoint and a connected client.
pub async fn pair() -> (Endpoint, Client) {
    let (pipe, k) = (pipe_name(), key());
    let ep = endpoint(&pipe, k);
    ep.start().await.unwrap();
    let c = client(&pipe, k);
    c.start();
    c.wait_connected(Duration::from_secs(5)).await.unwrap();
    (ep, c)
}

/// Polls `cond` every 5 ms until it holds; panics after `secs`.
pub async fn eventually(secs: u64, what: &str, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub async fn eventually_async<F: Future<Output = bool>>(
    secs: u64,
    what: &str,
    mut cond: impl FnMut() -> F,
) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !cond().await {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Collects deliveries.
#[derive(Clone, Default)]
pub struct Sink(pub Arc<Mutex<Vec<Delivery>>>);

impl Sink {
    pub fn callback(&self) -> impl Fn(Delivery) + Send + Sync + 'static {
        let v = self.0.clone();
        move |d| v.lock().unwrap().push(d)
    }
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    pub fn items(&self) -> Vec<Delivery> {
        self.0.lock().unwrap().clone()
    }
    /// The `data` of the events collected.
    pub fn data(&self) -> Vec<Value> {
        self.items()
            .into_iter()
            .filter_map(|d| match d {
                Delivery::Event(e) => Some(e.data),
                Delivery::Sample(_) => None,
            })
            .collect()
    }
}
