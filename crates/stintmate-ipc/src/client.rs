//! A client that stays connected (SPEC 15.3): connects in the background, retries with backoff
//! chosen by the `bye` code, and subscribes again after every reconnect.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::codec::Value;
use crate::connection::{BoxFuture, CallOptions, PendingCall, connect};
use crate::error::Error;
use crate::handshake::{ClientConfig, PeerInfo};
use crate::keyfile::KeySource;
use crate::lane::LaneFactory;
use crate::naming;
use crate::node::{
    Delivery, DeliveryFn, EndFn, Node, NodeConfig, Peer, SubscribeOptions, Subscription,
};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

const BACKOFF: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];

/// How long a wrong key waits between attempts (SPEC 15.3).
pub const UNAUTHORIZED_DELAY: Duration = Duration::from_secs(10);

/// How long to wait (before jitter) for the next connection attempt, chosen by the `bye` code
/// that ended the last connection or attempt (SPEC 15.3), or `None` to stop reconnecting.
/// `attempt` counts the failures in a row.
pub fn reconnect_delay(code: Option<&str>, attempt: u32) -> Option<Duration> {
    match code {
        Some("version_unsupported") => None,
        Some("unauthorized") => Some(UNAUTHORIZED_DELAY),
        Some("shutting_down") => Some(BACKOFF[(attempt as usize).min(1)]),
        _ => Some(BACKOFF[(attempt as usize).min(BACKOFF.len() - 1)]),
    }
}

/// `d` with +-20 % jitter.
fn jitter(d: Duration) -> Duration {
    let mut b = [0u8; 4];
    let _ = getrandom::fill(&mut b);
    let f = f64::from(u32::from_le_bytes(b)) / f64::from(u32::MAX);
    d.mul_f64(0.8 + 0.4 * f)
}

/// Called with the error of each failed connection attempt.
pub type ConnectErrorHook = Arc<dyn Fn(&Error) + Send + Sync>;

/// Builds a [`Client`].
pub struct ClientBuilder {
    pipe: String,
    info: PeerInfo,
    key: KeySource,
    cfg: NodeConfig,
    reconnect: bool,
    handle: Option<Handle>,
    lanes: Option<LaneFactory>,
}

impl ClientBuilder {
    /// Whether to reconnect (default true).
    pub fn reconnect(mut self, on: bool) -> Self {
        self.reconnect = on;
        self
    }
    /// The bounds of an event subscription's local queue.
    pub fn event_queue(mut self, events: usize) -> Self {
        self.cfg.event_queue = events;
        self
    }
    /// Requests of the server in flight before `busy`.
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.cfg.max_inflight = n;
        self
    }
    /// The runtime to run on (default: the current one).
    pub fn runtime(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }
    /// Where subscription callbacks run (default: a Tokio task per connection).
    pub fn lanes(mut self, lanes: LaneFactory) -> Self {
        self.lanes = Some(lanes);
        self
    }
    /// Builds the client; it does not connect until [`Client::start`].
    pub fn build(self) -> Result<Client, Error> {
        let handle = match self.handle {
            Some(h) => h,
            None => Handle::try_current()
                .map_err(|_| Error::Invalid("no Tokio runtime: pass one with runtime()".into()))?,
        };
        let (path, short) =
            naming::split_pipe(&self.pipe).map_err(|e| Error::Invalid(e.to_string()))?;
        let node = Node::new(self.info, self.key, handle, self.cfg, self.lanes);
        let (up, _) = watch::channel(false);
        Ok(Client {
            node,
            inner: Arc::new(Inner {
                path,
                short: naming::ascii_lower(&short),
                reconnect: self.reconnect,
                peer: Mutex::new(None),
                up,
                stop: CancellationToken::new(),
                task: Mutex::new(None),
                subs: Mutex::new(Vec::new()),
                last_error: Mutex::new(None),
                on_error: Mutex::new(None),
            }),
        })
    }
}

struct ClientSub {
    channel: String,
    opts: SubscribeOptions,
    callback: DeliveryFn,
    current: Mutex<Option<(Peer, Subscription)>>,
    error: Mutex<Option<String>>,
    closed: AtomicBool,
    gate: tokio::sync::Mutex<()>,
}

struct Inner {
    path: String,
    short: String,
    reconnect: bool,
    peer: Mutex<Option<Peer>>,
    up: watch::Sender<bool>,
    stop: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    subs: Mutex<Vec<Arc<ClientSub>>>,
    last_error: Mutex<Option<String>>,
    on_error: Mutex<Option<ConnectErrorHook>>,
}

/// A client of one endpoint that keeps its connection. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    node: Node,
    inner: Arc<Inner>,
}

impl Deref for Client {
    type Target = Node;
    fn deref(&self) -> &Node {
        &self.node
    }
}

/// A subscription a [`Client`] keeps across reconnects (and after an `overloaded` end).
/// Dropping the handle unsubscribes.
pub struct SubscriptionHandle {
    cs: Arc<ClientSub>,
    inner: Arc<Inner>,
}

impl SubscriptionHandle {
    /// The error of the last attempt to subscribe (for example `unknown_channel`).
    pub fn error(&self) -> Option<String> {
        lock(&self.cs.error).clone()
    }
    /// Whether the subscription is live on the current connection.
    pub fn is_subscribed(&self) -> bool {
        lock(&self.cs.current)
            .as_ref()
            .is_some_and(|(_, s)| !s.is_ended())
    }
    /// The current subscription on the connection, if any.
    pub fn current(&self) -> Option<Subscription> {
        lock(&self.cs.current).as_ref().map(|(_, s)| s.clone())
    }
    /// Unsubscribes and stops re-subscribing.
    pub fn close(&self) {
        self.cs.closed.store(true, Ordering::Release);
        lock(&self.inner.subs).retain(|s| !Arc::ptr_eq(s, &self.cs));
        let cur = lock(&self.cs.current).take();
        if let Some((_, s)) = cur {
            s.close();
        }
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        self.close();
    }
}

impl Client {
    /// A client of the endpoint at `pipe` (a full path or a short name), reconnecting, on the
    /// current runtime.
    pub fn new(pipe: &str, info: PeerInfo, key: KeySource) -> Result<Client, Error> {
        Self::builder(pipe, info, key).build()
    }

    /// Starts a builder.
    pub fn builder(pipe: &str, info: PeerInfo, key: KeySource) -> ClientBuilder {
        ClientBuilder {
            pipe: pipe.to_owned(),
            info,
            key,
            cfg: NodeConfig::default(),
            reconnect: true,
            handle: None,
            lanes: None,
        }
    }

    /// Sets the hook called with the error of each failed connection attempt.
    pub fn on_connect_error(&self, hook: impl Fn(&Error) + Send + Sync + 'static) {
        *lock(&self.inner.on_error) = Some(Arc::new(hook));
    }

    /// Connects in the background (once).
    pub fn start(&self) {
        let mut t = lock(&self.inner.task);
        if t.is_none() {
            *t = Some(
                self.node
                    .core()
                    .handle
                    .spawn(run(self.node.clone(), self.inner.clone())),
            );
        }
    }

    /// The connection, when it is up.
    pub fn peer(&self) -> Option<Peer> {
        lock(&self.inner.peer).clone().filter(Peer::is_open)
    }

    /// Whether the connection is up.
    pub fn is_connected(&self) -> bool {
        self.peer().is_some()
    }

    /// The error of the last failed attempt (cleared on success).
    pub fn last_error(&self) -> Option<String> {
        lock(&self.inner.last_error).clone()
    }

    /// Waits until connected, at most `wait`; [`Error::NotConnected`] otherwise.
    pub async fn wait_connected(&self, wait: Duration) -> Result<Peer, Error> {
        let mut rx = self.inner.up.subscribe();
        let r = timeout(wait, async {
            loop {
                if let Some(p) = self.peer() {
                    return p;
                }
                if rx.changed().await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        })
        .await;
        r.map_err(|_| Error::NotConnected)
    }

    /// Calls a method of the endpoint with the defaults (version 1, 5 s).
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, Error> {
        self.call_with(method, params, CallOptions::default()).await
    }

    /// Calls a method with options. [`Error::NotConnected`] when not connected now.
    pub async fn call_with(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<Value, Error> {
        self.peer()
            .ok_or(Error::NotConnected)?
            .call(method, params, options)
            .await
    }

    /// Sends a call and returns it in flight (`cancel()`; progress through
    /// [`CallOptions::on_progress`]).
    pub async fn call_async(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<PendingCall, Error> {
        self.peer()
            .ok_or(Error::NotConnected)?
            .call_async(method, params, options)
            .await
    }

    /// Subscribes now if connected, and again after every reconnect. Errors of an attempt are
    /// kept in [`SubscriptionHandle::error`].
    pub async fn subscribe(
        &self,
        channel: &str,
        opts: SubscribeOptions,
        callback: impl Fn(Delivery) + Send + Sync + 'static,
    ) -> SubscriptionHandle {
        let cs = Arc::new(ClientSub {
            channel: channel.to_owned(),
            opts,
            callback: Arc::new(callback),
            current: Mutex::new(None),
            error: Mutex::new(None),
            closed: AtomicBool::new(false),
            gate: tokio::sync::Mutex::new(()),
        });
        lock(&self.inner.subs).push(cs.clone());
        resubscribe(self.inner.clone(), cs.clone()).await;
        SubscriptionHandle {
            cs,
            inner: self.inner.clone(),
        }
    }

    /// Closes the connection (`bye normal`), stops reconnecting and waits for the background
    /// task.
    pub async fn close(&self) {
        self.inner.stop.cancel();
        let subs: Vec<_> = lock(&self.inner.subs).drain(..).collect();
        for cs in subs {
            cs.closed.store(true, Ordering::Release);
        }
        let peer = lock(&self.inner.peer).take();
        if let Some(p) = peer {
            let _ = timeout(
                Duration::from_secs(3),
                p.conn.shutdown("normal", "the client closes"),
            )
            .await;
        }
        let task = lock(&self.inner.task).take();
        if let Some(t) = task {
            let _ = t.await;
        }
    }
}

/// Subscribes `cs` on the current connection unless it already is (once per connection).
fn resubscribe(inner: Arc<Inner>, cs: Arc<ClientSub>) -> BoxFuture<()> {
    Box::pin(async move { resubscribe_now(&inner, &cs).await })
}

async fn resubscribe_now(inner: &Arc<Inner>, cs: &Arc<ClientSub>) {
    let Some(peer) = lock(&inner.peer).clone().filter(Peer::is_open) else {
        return;
    };
    if cs.closed.load(Ordering::Acquire) {
        return;
    }
    let _gate = cs.gate.lock().await;
    {
        let cur = lock(&cs.current);
        if let Some((p, s)) = cur.as_ref()
            && *p == peer
            && !s.is_ended()
        {
            return;
        }
    }
    let user_end = cs.opts.on_end.clone();
    let (winner, csw, handle) = (
        Arc::downgrade(inner),
        Arc::downgrade(cs),
        peer.st.node.handle.clone(),
    );
    let on_end: EndFn = Arc::new(move |ch, code, msg| {
        if let Some(f) = &user_end {
            f(ch, code, msg);
        }
        if code == "overloaded" {
            let (winner, csw): (Weak<Inner>, Weak<ClientSub>) = (winner.clone(), csw.clone());
            handle.spawn(async move {
                sleep(Duration::from_secs(1)).await;
                if let (Some(i), Some(c)) = (winner.upgrade(), csw.upgrade()) {
                    resubscribe(i, c).await;
                }
            });
        }
    });
    let mut opts = cs.opts.clone();
    opts.on_end = Some(on_end);
    let cb = cs.callback.clone();
    match peer.subscribe(&cs.channel, opts, cb).await {
        Ok(sub) => {
            *lock(&cs.current) = Some((peer, sub));
            *lock(&cs.error) = None;
        }
        Err(e) => {
            info!("subscribe {} failed: {e}", cs.channel);
            *lock(&cs.error) = Some(e.to_string());
        }
    }
}

fn error_code(e: &Error) -> Option<String> {
    match e {
        Error::Handshake(f) => Some(f.code.as_str().to_owned()),
        Error::PeerBye { code, .. } => Some(code.clone()),
        _ => None,
    }
}

async fn run(node: Node, inner: Arc<Inner>) {
    let core = node.core().clone();
    let mut attempt = 0u32;
    loop {
        if inner.stop.is_cancelled() {
            return;
        }
        let result = match core.key.key() {
            Err(e) => Err(Error::from(e)),
            Ok(key) => {
                let mut cfg =
                    ClientConfig::new(&inner.short, core.info.clone(), core.declarations());
                cfg.limits = core.cfg.limits;
                cfg.features = core.cfg.features.clone();
                let (st, handler) = core.new_handler();
                let attempt_fut =
                    connect(&inner.path, cfg, key, handler, core.connection_options());
                tokio::select! {
                    () = inner.stop.cancelled() => return,
                    r = attempt_fut => r.map(|c| (st, c)),
                }
            }
        };
        let wait = match result {
            Err(e) => {
                *lock(&inner.last_error) = Some(e.to_string());
                let hook = lock(&inner.on_error).clone();
                if let Some(h) = hook {
                    h(&e);
                }
                if !inner.reconnect {
                    return;
                }
                let d = reconnect_delay(error_code(&e).as_deref(), attempt);
                attempt += 1;
                match d {
                    Some(d) => d,
                    None => {
                        error!("{}: reconnecting stopped ({e})", inner.short);
                        return;
                    }
                }
            }
            Ok((st, conn)) => {
                attempt = 0;
                *lock(&inner.last_error) = None;
                let peer = st.attach(&conn);
                *lock(&inner.peer) = Some(peer.clone());
                let _ = inner.up.send(true);
                core.peer_opened(peer);
                let subs: Vec<_> = lock(&inner.subs).clone();
                for cs in &subs {
                    resubscribe(inner.clone(), cs.clone()).await;
                }
                let closed = tokio::select! {
                    () = inner.stop.cancelled() => {
                        conn.close("normal", "the client closes");
                        return;
                    }
                    c = conn.wait_closed() => c,
                };
                *lock(&inner.peer) = None;
                let _ = inner.up.send(false);
                if !inner.reconnect {
                    return;
                }
                let code = (closed.by == crate::connection::ClosedBy::Peer).then_some(closed.code);
                match reconnect_delay(code.as_deref(), 0) {
                    Some(d) => d,
                    None => {
                        error!("{}: reconnecting stopped (bye)", inner.short);
                        return;
                    }
                }
            }
        };
        tokio::select! {
            () = inner.stop.cancelled() => return,
            () = sleep(jitter(wait)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_delay_follows_the_bye_code() {
        assert_eq!(reconnect_delay(Some("version_unsupported"), 0), None);
        assert_eq!(
            reconnect_delay(Some("unauthorized"), 3),
            Some(UNAUTHORIZED_DELAY)
        );
        assert_eq!(reconnect_delay(Some("shutting_down"), 0), Some(BACKOFF[0]));
        assert_eq!(reconnect_delay(Some("shutting_down"), 5), Some(BACKOFF[1]));
        assert_eq!(reconnect_delay(None, 0), Some(BACKOFF[0]));
        assert_eq!(reconnect_delay(Some("timeout"), 9), Some(BACKOFF[3]));
        let j = jitter(Duration::from_secs(1));
        assert!(j >= Duration::from_millis(800) && j <= Duration::from_millis(1200));
    }
}
