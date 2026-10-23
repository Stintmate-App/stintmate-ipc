//! The blocking façade: the same surface as the async API, without `async`.
//!
//! * One process-wide Tokio runtime with **2 worker threads**, created on first use. Nothing
//!   written by the embedder ever runs on it (plan section 5).
//! * Method handlers run on a **handler pool** of standard threads (default 8, per node);
//!   `ordered` methods queue per connection; `inline` ones run on the connection's lane.
//! * Subscription callbacks and `end` callbacks run on **one lane thread per connection**,
//!   started on first use, in order. Connection hooks run on one ordered hooks thread per node.
//! * `call`, `wait_connected` and friends block the calling thread: do not call them from
//!   a runtime thread (they never need to be: callbacks and handlers are not on one).
//!
//! The channels ([`EventChannel`] and friends) and [`Context`] are the async API's types:
//! their producer calls are already plain synchronous calls.

use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, unbounded};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::oneshot;

pub use crate::channels::{
    EventChannel, PublishOptions, StateChannel, StreamChannel, SubscribeHook, SubscribeRequest,
    SubscriptionView,
};
pub use crate::node::{
    Context, Delivery, Event, MethodOpts, Sample, SubscribeOptions, Subscription,
};

use crate::codec::Value;
use crate::connection::{CallHandle, CallOptions};
use crate::decl::Declarations;
use crate::error::{Error, RemoteError};
use crate::handshake::PeerInfo;
use crate::keyfile::KeySource;
use crate::lane::{Job, Lane, LaneFactory, run_job};
use crate::layout::Layout;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// The process-wide runtime (2 workers), created on first use.
pub fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("stintmate-ipc-io")
            .enable_all()
            .build()
            .expect("the runtime starts")
    })
}

fn handle() -> Handle {
    runtime().handle().clone()
}

// --------------------------------------------------------------------------- threads

/// A pool of standard threads running jobs.
struct Pool {
    tx: Mutex<Option<Sender<Job>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Pool {
    fn new(n: usize) -> Arc<Self> {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = unbounded();
        let threads = (0..n.max(1))
            .map(|i| {
                let rx = rx.clone();
                std::thread::Builder::new()
                    .name(format!("stintmate-ipc-handler-{i}"))
                    .spawn(move || {
                        for job in rx {
                            run_job(job);
                        }
                    })
                    .expect("a thread starts")
            })
            .collect();
        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
            threads: Mutex::new(threads),
        })
    }

    /// Queues a job; after `shutdown` it is dropped.
    fn submit(&self, job: Job) {
        if let Some(tx) = lock(&self.tx).as_ref() {
            let _ = tx.send(job);
        }
    }

    /// Lets the threads finish what is queued and joins them.
    fn shutdown(&self) {
        lock(&self.tx).take();
        let me = std::thread::current().id();
        for t in lock(&self.threads).drain(..) {
            if t.thread().id() != me {
                let _ = t.join();
            }
        }
    }
}

/// A lane that is one standard thread.
struct ThreadLane {
    tx: Sender<Job>,
}

impl ThreadLane {
    fn new(name: &str) -> Self {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = unbounded();
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                for job in rx {
                    run_job(job);
                }
            })
            .expect("a thread starts");
        Self { tx }
    }
}

impl Lane for ThreadLane {
    fn submit(&self, job: Job) {
        let _ = self.tx.send(job);
    }
}

fn thread_lanes() -> LaneFactory {
    Arc::new(|| Arc::new(ThreadLane::new("stintmate-ipc-callbacks")) as Arc<dyn Lane>)
}

// --------------------------------------------------------------------------- the node

/// A channel of any kind, for the hooks.
pub trait Channel {
    /// Installs the async subscribe hook of the channel.
    fn set_subscribe_hook(&self, hook: SubscribeHook);
    /// Installs the activity hook of the channel.
    fn set_active_hook(&self, hook: Arc<dyn Fn(bool) + Send + Sync>);
}

macro_rules! impl_channel {
    ($t:ty) => {
        impl Channel for $t {
            fn set_subscribe_hook(&self, hook: SubscribeHook) {
                self.on_subscribe(move |r| hook(r));
            }
            fn set_active_hook(&self, hook: Arc<dyn Fn(bool) + Send + Sync>) {
                self.on_active(move |v| hook(v));
            }
        }
    };
}
impl_channel!(EventChannel);
impl_channel!(StateChannel);
impl_channel!(StreamChannel);

/// Methods, channels and hooks, with handlers running on the pool.
#[derive(Clone)]
pub struct Node {
    node: crate::node::Node,
    pool: Arc<Pool>,
    hooks: Arc<ThreadLane>,
}

impl Node {
    fn new(node: crate::node::Node, pool: Arc<Pool>) -> Self {
        Self {
            node,
            pool,
            hooks: Arc::new(ThreadLane::new("stintmate-ipc-hooks")),
        }
    }

    /// Offers a method; the handler runs on a pool thread (`ordered`: one at a time per
    /// connection, in arrival order; `inline`: on the connection's lane thread). A panic
    /// answers `internal`.
    pub fn method(
        &self,
        name: &str,
        opts: MethodOpts,
        handler: impl Fn(&Context, Value) -> Result<Value, RemoteError> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        let f = Arc::new(handler);
        let pool = self.pool.clone();
        let inline = opts.inline;
        self.node.method_boxed(
            name,
            opts,
            Arc::new(move |ctx: Context, params: Value| {
                let (f, pool) = (f.clone(), pool.clone());
                Box::pin(async move {
                    let (tx, rx) = oneshot::channel();
                    let lane = if inline { ctx.peer().lane() } else { None };
                    let job: Job = Box::new(move || {
                        let r = catch_unwind(AssertUnwindSafe(|| f(&ctx, params)));
                        let _ = tx.send(r);
                    });
                    match lane {
                        Some(l) => l.submit(job),
                        None => pool.submit(job),
                    }
                    match rx.await {
                        Ok(Ok(r)) => r,
                        Ok(Err(_)) => Err(RemoteError::new("internal", "the handler panicked")),
                        Err(_) => Err(RemoteError::new("internal", "the method failed")),
                    }
                })
            }),
        )
    }

    /// Withdraws a method.
    pub fn remove_method(&self, name: &str) {
        self.node.remove_method(name);
    }

    /// Declares an event channel.
    pub fn event_channel(&self, name: &str, versions: &[u32]) -> Result<EventChannel, Error> {
        self.node.event_channel(name, versions)
    }

    /// Declares a state channel.
    pub fn state_channel(
        &self,
        name: &str,
        initial: Value,
        versions: &[u32],
    ) -> Result<StateChannel, Error> {
        self.node.state_channel(name, initial, versions)
    }

    /// Declares a stream channel.
    pub fn stream_channel(
        &self,
        name: &str,
        layout: Layout,
        versions: &[u32],
    ) -> Result<StreamChannel, Error> {
        self.node.stream_channel(name, layout, versions)
    }

    /// Declares a stream channel with a rate and a ring size.
    pub fn stream_channel_with(
        &self,
        name: &str,
        layout: Layout,
        versions: &[u32],
        rate_hz: Option<f64>,
        queue: Option<usize>,
    ) -> Result<StreamChannel, Error> {
        self.node
            .stream_channel_with(name, layout, versions, rate_hz, queue)
    }

    /// What this node offers.
    pub fn declarations(&self) -> Declarations {
        self.node.declarations()
    }

    /// The open connections.
    pub fn peers(&self) -> Vec<Peer> {
        self.node.peers().into_iter().map(Peer::from).collect()
    }

    /// Sets the hook that runs (on a pool thread) before a subscription to `channel` is
    /// accepted; an `Err` refuses it.
    pub fn on_subscribe(
        &self,
        channel: &impl Channel,
        hook: impl Fn(SubscribeRequest) -> Result<(), RemoteError> + Send + Sync + 'static,
    ) {
        let (hook, pool) = (Arc::new(hook), self.pool.clone());
        channel.set_subscribe_hook(Arc::new(move |req| {
            let (hook, (tx, rx)) = (hook.clone(), oneshot::channel());
            pool.submit(Box::new(move || {
                let _ = tx.send(hook(req));
            }));
            Box::pin(async move {
                rx.await
                    .unwrap_or_else(|_| Err(RemoteError::new("internal", "the hook failed")))
            })
        }));
    }

    /// Sets the hook called (on the hooks thread) when the first subscriber of `channel` comes
    /// and the last leaves.
    pub fn on_active(&self, channel: &impl Channel, hook: impl Fn(bool) + Send + Sync + 'static) {
        let (hook, lane) = (Arc::new(hook), self.hooks.clone());
        channel.set_active_hook(Arc::new(move |v| {
            let hook = hook.clone();
            lane.submit(Box::new(move || hook(v)));
        }));
    }

    fn lane_hook(
        &self,
        f: impl Fn(Peer) + Send + Sync + 'static,
    ) -> impl Fn(crate::Peer) + Send + Sync + 'static {
        let (f, lane) = (Arc::new(f), self.hooks.clone());
        move |p| {
            let f = f.clone();
            lane.submit(Box::new(move || f(Peer::from(p))));
        }
    }

    /// Sets the hook called (on the hooks thread) when a connection is established.
    pub fn on_connect(&self, hook: impl Fn(Peer) + Send + Sync + 'static) {
        self.node.on_connect(self.lane_hook(hook));
    }

    /// Sets the hook called (on the hooks thread) when a connection ends.
    pub fn on_disconnect(&self, hook: impl Fn(Peer) + Send + Sync + 'static) {
        self.node.on_disconnect(self.lane_hook(hook));
    }

    /// Sets the hook called (on the hooks thread) when a peer sends `decl`.
    pub fn on_redeclare(&self, hook: impl Fn(Peer) + Send + Sync + 'static) {
        self.node.on_redeclare(self.lane_hook(hook));
    }
}

// --------------------------------------------------------------------------- peer

/// One connection; its calls block the calling thread.
#[derive(Clone, Debug, PartialEq)]
pub struct Peer(crate::Peer);

impl From<crate::Peer> for Peer {
    fn from(p: crate::Peer) -> Self {
        Peer(p)
    }
}

impl Peer {
    /// The async peer.
    pub fn inner(&self) -> &crate::Peer {
        &self.0
    }
    /// Who is on the other end.
    pub fn info(&self) -> &PeerInfo {
        self.0.info()
    }
    /// The other end's process id (server side).
    pub fn pid(&self) -> Option<u32> {
        self.0.pid()
    }
    /// What the other end declares now.
    pub fn decl(&self) -> Declarations {
        self.0.decl()
    }
    /// Whether the connection is up.
    pub fn is_open(&self) -> bool {
        self.0.is_open()
    }
    /// Ends the connection with `bye`.
    pub fn close(&self, code: &str, message: &str) {
        self.0.close(code, message);
    }

    /// Calls a method and waits for the result.
    pub fn call(&self, method: &str, params: Value, options: CallOptions) -> Result<Value, Error> {
        runtime().block_on(self.0.call(method, params, options))
    }

    /// Sends a call and returns it in flight.
    pub fn call_async(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<PendingCall, Error> {
        let pending = runtime().block_on(self.0.call_async(method, params, options))?;
        Ok(PendingCall::new(pending))
    }

    /// Subscribes to a channel of the other end; `callback` runs on the connection's lane.
    pub fn subscribe(
        &self,
        channel: &str,
        opts: SubscribeOptions,
        callback: impl Fn(Delivery) + Send + Sync + 'static,
    ) -> Result<Subscription, Error> {
        runtime().block_on(self.0.subscribe(channel, opts, Arc::new(callback)))
    }
}

/// A call in flight.
pub struct PendingCall {
    handle: CallHandle,
    id: u64,
    rx: Receiver<Result<Value, Error>>,
    done: Mutex<Option<Result<Value, Error>>>,
}

impl PendingCall {
    fn new(pending: crate::connection::PendingCall) -> Self {
        let (handle, id) = (pending.handle(), pending.id());
        let (tx, rx) = crossbeam_channel::bounded(1);
        runtime().spawn(async move {
            let _ = tx.send(pending.await);
        });
        Self {
            handle,
            id,
            rx,
            done: Mutex::new(None),
        }
    }

    /// The id of the request.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Gives up: sends `cancel` and fails the call with [`Error::CallCancelled`]. False when
    /// the answer had already won.
    pub fn cancel(&self) -> bool {
        self.handle.cancel()
    }

    /// Waits for the result, at most `wait` (`None`: as long as it takes). `None` when the
    /// wait ended first; the call is not given up and can be waited for again.
    pub fn wait_timeout(&self, wait: Option<Duration>) -> Option<Result<Value, Error>> {
        let mut done = lock(&self.done);
        if done.is_none() {
            let got = match wait {
                None => self.rx.recv().ok(),
                Some(w) => self.rx.recv_timeout(w).ok(),
            };
            *done = got;
        }
        done.clone_result()
    }

    /// Waits for the result.
    pub fn wait(&self) -> Result<Value, Error> {
        self.wait_timeout(None)
            .unwrap_or(Err(Error::ConnectionLost {
                code: None,
                message: "the call was abandoned".into(),
            }))
    }
}

trait CloneResult {
    fn clone_result(&self) -> Option<Result<Value, Error>>;
}

impl CloneResult for Option<Result<Value, Error>> {
    fn clone_result(&self) -> Option<Result<Value, Error>> {
        self.as_ref().map(|r| match r {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(clone_error(e)),
        })
    }
}

fn clone_error(e: &Error) -> Error {
    match e {
        Error::Remote(r) => Error::Remote(r.clone()),
        Error::CallTimeout => Error::CallTimeout,
        Error::CallCancelled => Error::CallCancelled,
        Error::NotConnected => Error::NotConnected,
        Error::ConnectionLost { code, message } => Error::ConnectionLost {
            code: code.clone(),
            message: message.clone(),
        },
        other => Error::Invalid(other.to_string()),
    }
}

// --------------------------------------------------------------------------- endpoint

/// Builds an [`Endpoint`].
pub struct EndpointBuilder(crate::endpoint::EndpointBuilder);

impl EndpointBuilder {
    /// See [`crate::EndpointBuilder::pipe`].
    pub fn pipe(self, pipe: impl Into<String>) -> Self {
        Self(self.0.pipe(pipe))
    }
    /// See [`crate::EndpointBuilder::registry`].
    pub fn registry(self, dir: impl Into<std::path::PathBuf>) -> Self {
        Self(self.0.registry(dir))
    }
    /// The size of the handler pool (default 8).
    pub fn handler_threads(self, n: usize) -> Self {
        Self(self.0.handler_threads(n))
    }
    /// See [`crate::EndpointBuilder::heartbeat_ms`].
    pub fn heartbeat_ms(self, ms: u32) -> Self {
        Self(self.0.heartbeat_ms(ms))
    }
    /// See [`crate::EndpointBuilder::max_inflight`].
    pub fn max_inflight(self, n: usize) -> Self {
        Self(self.0.max_inflight(n))
    }
    /// See [`crate::EndpointBuilder::event_queue`].
    pub fn event_queue(self, events: usize, bytes: usize) -> Self {
        Self(self.0.event_queue(events, bytes))
    }
    /// Builds the endpoint.
    pub fn build(self) -> Result<Endpoint, Error> {
        let threads = self.0.handler_threads_set();
        let inner = self.0.runtime(handle()).lanes(thread_lanes()).build()?;
        let node = Node::new(inner.deref().clone(), Pool::new(threads));
        Ok(Endpoint { node, inner })
    }
}

/// The server of one endpoint, blocking.
#[derive(Clone)]
pub struct Endpoint {
    node: Node,
    inner: crate::Endpoint,
}

impl Deref for Endpoint {
    type Target = Node;
    fn deref(&self) -> &Node {
        &self.node
    }
}

impl Endpoint {
    /// Starts a builder.
    pub fn builder(info: PeerInfo, key: KeySource) -> EndpointBuilder {
        EndpointBuilder(crate::Endpoint::builder(info, key))
    }
    /// The pipe path.
    pub fn path(&self) -> &str {
        self.inner.path()
    }
    /// Creates the pipe and serves.
    pub fn start(&self) -> Result<(), Error> {
        runtime().block_on(self.inner.start())
    }
    /// Stops serving, says `bye` to every peer, and ends the handler pool.
    pub fn stop(&self) {
        runtime().block_on(self.inner.stop());
        self.node.pool.shutdown();
    }
}

// --------------------------------------------------------------------------- client

/// A client that stays connected, blocking.
#[derive(Clone)]
pub struct Client {
    node: Node,
    inner: crate::Client,
}

impl Deref for Client {
    type Target = Node;
    fn deref(&self) -> &Node {
        &self.node
    }
}

impl Client {
    /// A reconnecting client of the endpoint at `pipe`; call [`start`](Self::start).
    pub fn new(pipe: &str, info: PeerInfo, key: KeySource) -> Result<Client, Error> {
        Self::with(pipe, info, key, |b| b)
    }

    /// As [`new`](Self::new), configuring the async builder (`reconnect(false)`, ...).
    pub fn with(
        pipe: &str,
        info: PeerInfo,
        key: KeySource,
        configure: impl FnOnce(crate::client::ClientBuilder) -> crate::client::ClientBuilder,
    ) -> Result<Client, Error> {
        let b = crate::Client::builder(pipe, info, key)
            .runtime(handle())
            .lanes(thread_lanes());
        let inner = configure(b).build()?;
        let node = Node::new(inner.deref().clone(), Pool::new(8));
        Ok(Client { node, inner })
    }

    /// Connects in the background.
    pub fn start(&self) {
        self.inner.start();
    }

    /// The connection, when it is up.
    pub fn peer(&self) -> Option<Peer> {
        self.inner.peer().map(Peer::from)
    }

    /// Whether the connection is up.
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// The error of the last failed attempt.
    pub fn last_error(&self) -> Option<String> {
        self.inner.last_error()
    }

    /// Waits until connected, at most `wait`.
    pub fn wait_connected(&self, wait: Duration) -> Result<Peer, Error> {
        runtime()
            .block_on(self.inner.wait_connected(wait))
            .map(Peer::from)
    }

    /// Calls a method with the defaults (version 1, 5 s).
    pub fn call(&self, method: &str, params: Value) -> Result<Value, Error> {
        runtime().block_on(self.inner.call(method, params))
    }

    /// Calls a method with options.
    pub fn call_with(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<Value, Error> {
        runtime().block_on(self.inner.call_with(method, params, options))
    }

    /// Sends a call and returns it in flight.
    pub fn call_async(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<PendingCall, Error> {
        let pending = runtime().block_on(self.inner.call_async(method, params, options))?;
        Ok(PendingCall::new(pending))
    }

    /// Subscribes now and after every reconnect; `callback` runs on the connection's lane.
    pub fn subscribe(
        &self,
        channel: &str,
        opts: SubscribeOptions,
        callback: impl Fn(Delivery) + Send + Sync + 'static,
    ) -> crate::SubscriptionHandle {
        runtime().block_on(self.inner.subscribe(channel, opts, callback))
    }

    /// Closes the connection, stops reconnecting and ends the handler pool.
    pub fn close(&self) {
        runtime().block_on(self.inner.close());
        self.node.pool.shutdown();
    }
}
