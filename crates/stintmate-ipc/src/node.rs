//! What a process offers and how it talks to a peer (SPEC 9, 10): the [`Node`] common to
//! servers and clients (methods, channels, hooks), one [`Peer`] per connection, subscriptions
//! in both directions and the per-connection **pump** that alone writes subscription traffic
//! (SPEC 12).
//!
//! No handler or callback of a foreign embedder runs on a runtime thread: method handlers are
//! futures (the blocking façade wraps its closures into jobs for a pool of threads) and
//! subscription callbacks run on the connection's [`Lane`].

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::channels::{
    ChannelCore, EventChannel, OutItem, OutSub, StateChannel, StreamChannel, SubscribeRequest,
};
use crate::codec::{self, Value};
use crate::connection::{
    BoxFuture, CallOptions, CloseInfo, Connection, ConnectionOptions, Handler, PendingCall,
    RequestContext,
};
use crate::decl::{ChannelDecl, ChannelKind, Declarations, MethodDecl, valid_name};
use crate::error::{Error, RemoteError};
use crate::handshake::{Limits, PeerInfo};
use crate::idem::IdempotencyCache;
use crate::keyfile::KeySource;
use crate::lane::{Lane, LaneFactory, tokio_lanes};
use crate::layout::{Layout, Scalar};
use crate::wire::{self, StreamSample};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn uint(m: &Value, key: &str) -> Option<u64> {
    codec::get(m, key).and_then(Value::as_u64)
}

fn text<'a>(m: &'a Value, key: &str) -> Option<&'a str> {
    codec::get(m, key).and_then(Value::as_str)
}

// --------------------------------------------------------------------------- deliveries

/// An event or a state value delivered to a subscription.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// The channel.
    pub channel: String,
    /// The subscription's `seq` (a gap on a state channel means values were coalesced).
    pub seq: u64,
    /// `t_ns` at the owner, when it sent one.
    pub ts: Option<i64>,
    /// The event, or the whole new state.
    pub data: Value,
    /// True for the state snapshot of the `sub` response.
    pub snapshot: bool,
}

/// A stream sample delivered to a subscription.
#[derive(Debug, Clone)]
pub struct Sample {
    /// The channel.
    pub channel: String,
    /// The sample's `seq` (u32, wraps).
    pub seq: u32,
    /// When it was taken at the source.
    pub t_ns: i64,
    /// Samples were dropped before this one (the owner's ring or ours).
    pub dropped: bool,
    /// The packed body.
    pub body: Bytes,
    layout: Arc<Layout>,
}

impl Sample {
    /// The layout of the body.
    pub fn layout(&self) -> &Layout {
        &self.layout
    }

    /// The body as a map by field name.
    pub fn values(&self) -> Result<Value, crate::error::LayoutError> {
        self.layout.unpack_value(&self.body)
    }

    /// The body as flat scalars.
    pub fn scalars(&self) -> Result<Vec<Scalar>, crate::error::LayoutError> {
        let mut out = Vec::new();
        self.layout.unpack(&self.body, &mut out)?;
        Ok(out)
    }
}

/// What a subscription callback gets.
#[derive(Debug, Clone)]
pub enum Delivery {
    /// From an event or state channel.
    Event(Event),
    /// From a stream channel.
    Sample(Sample),
}

/// A subscription callback; runs on the connection's lane, in order.
pub type DeliveryFn = Arc<dyn Fn(Delivery) + Send + Sync>;
/// Called (on the lane) with `(channel, code, message)` when a subscription ends.
pub type EndFn = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;
/// A hook on connections.
pub type PeerHook = Arc<dyn Fn(Peer) + Send + Sync>;

// --------------------------------------------------------------------------- methods

/// A method handler: an async function of the request's [`Context`] and its params.
pub type MethodFn =
    Arc<dyn Fn(Context, Value) -> BoxFuture<Result<Value, RemoteError>> + Send + Sync>;

/// Options of a method (SPEC 9.2).
#[derive(Debug, Clone)]
pub struct MethodOpts {
    /// Versions offered.
    pub versions: Vec<u32>,
    /// One at a time per connection, in arrival order (SPEC 13.3).
    pub ordered: bool,
    /// Declares the method idempotent (SPEC 10.4).
    pub idempotent: bool,
    /// Keep the answers by `ik` (the default for an idempotent method); false declares the
    /// method idempotent but leaves the deduplication to the handler.
    pub idempotency_cache: bool,
    /// Forget answers after this long.
    pub idempotency_ttl: Option<Duration>,
    /// One line for people.
    pub doc: Option<String>,
    /// JSON Schema of the params (informative).
    pub params: Option<Value>,
    /// JSON Schema of the result (informative).
    pub result: Option<Value>,
    /// For the blocking façade: run the handler on the connection's callback lane instead of
    /// the pool (microsecond handlers that answer from memory).
    pub inline: bool,
}

impl Default for MethodOpts {
    fn default() -> Self {
        Self {
            versions: vec![1],
            ordered: false,
            idempotent: false,
            idempotency_cache: true,
            idempotency_ttl: None,
            doc: None,
            params: None,
            result: None,
            inline: false,
        }
    }
}

impl MethodOpts {
    /// Sets the versions offered.
    pub fn versions(mut self, v: &[u32]) -> Self {
        self.versions = v.to_vec();
        self
    }
    /// Sets `ordered`.
    pub fn ordered(mut self, on: bool) -> Self {
        self.ordered = on;
        self
    }
    /// Declares the method idempotent.
    pub fn idempotent(mut self, on: bool) -> Self {
        self.idempotent = on;
        self
    }
    /// Keeps (or not) the answers by `ik`.
    pub fn idempotency_cache(mut self, on: bool) -> Self {
        self.idempotency_cache = on;
        self
    }
    /// Forgets answers after `ttl`.
    pub fn idempotency_ttl(mut self, ttl: Duration) -> Self {
        self.idempotency_ttl = Some(ttl);
        self
    }
    /// Sets the one-line description.
    pub fn doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }
    /// Sets `inline`.
    pub fn inline(mut self, on: bool) -> Self {
        self.inline = on;
        self
    }
}

pub(crate) struct MethodEntry {
    decl: MethodDecl,
    f: MethodFn,
    cache: Option<IdempotencyCache>,
}

struct CtxInner {
    peer: Peer,
    rc: RequestContext,
    handle: Handle,
    deadline: Mutex<Option<Instant>>,
}

/// A request being handled: who calls, what for, and the means to report progress and learn
/// that the caller gave up.
#[derive(Clone)]
pub struct Context(Arc<CtxInner>);

impl Context {
    /// The connection the request arrived on.
    pub fn peer(&self) -> &Peer {
        &self.0.peer
    }
    /// The caller's id for the request.
    pub fn id(&self) -> u64 {
        self.0.rc.id
    }
    /// The method name.
    pub fn method(&self) -> &str {
        &self.0.rc.method
    }
    /// The method version asked for.
    pub fn version(&self) -> u64 {
        self.0.rc.version
    }
    /// The idempotency key, when the caller gave one.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.0.rc.idempotency_key.as_deref()
    }
    fn deadline_at(&self) -> Option<Instant> {
        *lock(&self.0.deadline)
    }
    /// Time left before the caller's deadline.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline_at()
            .map(|d| d.saturating_duration_since(Instant::now()))
    }
    /// Whether the caller's deadline has passed.
    pub fn expired(&self) -> bool {
        self.deadline_at().is_some_and(|d| Instant::now() >= d)
    }
    /// Whether the caller sent `cancel` or went away: stop and return anything.
    pub fn is_cancelled(&self) -> bool {
        self.0.rc.is_cancelled()
    }
    /// Completes when the request is cancelled.
    pub async fn cancelled(&self) {
        self.0.rc.cancelled().await;
    }
    /// Blocks the calling thread until the request is cancelled (true) or `timeout` passes
    /// (false). Only for threads that are not runtime threads (the blocking façade's pool).
    pub fn wait_cancelled(&self, timeout: Duration) -> bool {
        self.0.handle.block_on(async {
            tokio::time::timeout(timeout, self.0.rc.cancelled())
                .await
                .is_ok()
        })
    }
    /// Sends `prog`: the request is alive; `d` is a report, `extend` gives the caller a new
    /// deadline that far from now.
    pub fn progress(&self, d: Option<Value>, extend: Option<Duration>) {
        let ms = extend.map(|e| e.as_millis().max(1) as u64);
        if let Some(ms) = ms {
            *lock(&self.0.deadline) = Some(Instant::now() + Duration::from_millis(ms));
        }
        self.0.rc.progress(d, ms);
    }
}

// --------------------------------------------------------------------------- node

/// Settings of a node (SPEC 8.2, 12, 15).
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// The heartbeat a server announces.
    pub heartbeat_ms: u32,
    /// `max_frame` and `max_message`.
    pub limits: Limits,
    /// Features offered.
    pub features: Vec<String>,
    /// Events one subscription may have queued.
    pub event_queue: usize,
    /// Bytes of events one subscription may have queued.
    pub event_queue_bytes: usize,
    /// Requests in flight per connection before `busy`.
    pub max_inflight: usize,
    /// Handshake timeout.
    pub handshake_timeout: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: crate::handshake::DEFAULT_HEARTBEAT_MS,
            limits: Limits::default(),
            features: Vec::new(),
            event_queue: 1024,
            event_queue_bytes: 8 * 1024 * 1024,
            max_inflight: 1024,
            handshake_timeout: crate::handshake::HANDSHAKE_TIMEOUT,
        }
    }
}

#[derive(Default)]
struct Hooks {
    on_connect: Option<PeerHook>,
    on_disconnect: Option<PeerHook>,
    on_redeclare: Option<PeerHook>,
}

/// The state shared by a node's connections.
pub struct NodeCore {
    pub(crate) info: PeerInfo,
    pub(crate) key: KeySource,
    pub(crate) handle: Handle,
    pub(crate) cfg: NodeConfig,
    methods: RwLock<HashMap<String, Arc<MethodEntry>>>,
    channels: RwLock<HashMap<String, Arc<ChannelCore>>>,
    peers: Mutex<Vec<Peer>>,
    hooks: Mutex<Hooks>,
    lanes: LaneFactory,
    weak: Weak<NodeCore>,
}

/// Methods, channels and hooks: what [`Endpoint`](crate::Endpoint) and
/// [`Client`](crate::Client) have in common (both dereference to it).
#[derive(Clone)]
pub struct Node(pub(crate) Arc<NodeCore>);

impl Node {
    pub(crate) fn new(
        info: PeerInfo,
        key: KeySource,
        handle: Handle,
        cfg: NodeConfig,
        lanes: Option<LaneFactory>,
    ) -> Self {
        let lanes = lanes.unwrap_or_else(|| tokio_lanes(handle.clone()));
        Node(Arc::new_cyclic(|weak| NodeCore {
            info,
            key,
            handle,
            cfg,
            methods: RwLock::default(),
            channels: RwLock::default(),
            peers: Mutex::default(),
            hooks: Mutex::default(),
            lanes,
            weak: weak.clone(),
        }))
    }

    /// This node's identity.
    pub fn info(&self) -> &PeerInfo {
        &self.0.info
    }

    /// Offers a method. The handler is an async function; `Err(RemoteError)` answers an
    /// error. Replaces a method of the same name; peers are told (`decl`).
    pub fn method<F, Fut>(&self, name: &str, opts: MethodOpts, handler: F) -> Result<(), Error>
    where
        F: Fn(Context, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, RemoteError>> + Send + 'static,
    {
        self.method_boxed(name, opts, Arc::new(move |c, p| Box::pin(handler(c, p))))
    }

    /// [`method`](Self::method) with an already boxed handler.
    pub fn method_boxed(&self, name: &str, opts: MethodOpts, f: MethodFn) -> Result<(), Error> {
        check_name(name)?;
        let mut versions = opts.versions.clone();
        versions.sort_unstable();
        versions.dedup();
        if versions.is_empty() {
            return Err(Error::Invalid("a method needs a version".into()));
        }
        let decl = MethodDecl {
            versions,
            ordered: opts.ordered,
            idempotent: opts.idempotent,
            doc: opts.doc,
            params: opts.params,
            result: opts.result,
        };
        let cache = (decl.idempotent && opts.idempotency_cache)
            .then(|| IdempotencyCache::new(opts.idempotency_ttl));
        let entry = Arc::new(MethodEntry { decl, f, cache });
        write(&self.0.methods).insert(name.to_owned(), entry);
        self.0.redeclare();
        Ok(())
    }

    /// Withdraws a method.
    pub fn remove_method(&self, name: &str) {
        write(&self.0.methods).remove(name);
        self.0.redeclare();
    }

    fn add_channel(&self, name: &str, ch: Arc<ChannelCore>) -> Result<(), Error> {
        check_name(name)?;
        {
            let mut g = write(&self.0.channels);
            if g.contains_key(name) {
                return Err(Error::Invalid(format!(
                    "channel {name} is already declared"
                )));
            }
            g.insert(name.to_owned(), ch);
        }
        self.0.redeclare();
        Ok(())
    }

    fn versions(v: &[u32]) -> Result<Vec<u32>, Error> {
        let mut v = v.to_vec();
        v.sort_unstable();
        v.dedup();
        if v.is_empty() {
            return Err(Error::Invalid("a channel needs a version".into()));
        }
        Ok(v)
    }

    /// Declares an event channel.
    pub fn event_channel(&self, name: &str, versions: &[u32]) -> Result<EventChannel, Error> {
        let mut decl = ChannelDecl::new(ChannelKind::Event);
        decl.versions = Self::versions(versions)?;
        let ch = ChannelCore::new(name, decl, self.0.weak.clone(), Value::Nil, None);
        self.add_channel(name, ch.clone())?;
        Ok(EventChannel(ch))
    }

    /// Declares a state channel with its first value.
    pub fn state_channel(
        &self,
        name: &str,
        initial: Value,
        versions: &[u32],
    ) -> Result<StateChannel, Error> {
        let mut decl = ChannelDecl::new(ChannelKind::State);
        decl.versions = Self::versions(versions)?;
        let ch = ChannelCore::new(name, decl, self.0.weak.clone(), initial, None);
        self.add_channel(name, ch.clone())?;
        Ok(StateChannel(ch))
    }

    /// Declares a stream channel with `layout`.
    pub fn stream_channel(
        &self,
        name: &str,
        layout: Layout,
        versions: &[u32],
    ) -> Result<StreamChannel, Error> {
        self.stream_channel_with(name, layout, versions, None, None)
    }

    /// [`stream_channel`](Self::stream_channel) with the nominal rate and the ring size of each
    /// subscription (default: one second at `rate_hz`, at least 64).
    pub fn stream_channel_with(
        &self,
        name: &str,
        layout: Layout,
        versions: &[u32],
        rate_hz: Option<f64>,
        queue: Option<usize>,
    ) -> Result<StreamChannel, Error> {
        let mut decl = ChannelDecl::new(ChannelKind::Stream);
        decl.versions = Self::versions(versions)?;
        decl.layout = Some(layout);
        decl.rate_hz = rate_hz;
        let ch = ChannelCore::new(name, decl, self.0.weak.clone(), Value::Nil, queue);
        self.add_channel(name, ch.clone())?;
        Ok(StreamChannel(ch))
    }

    /// What this node offers now.
    pub fn declarations(&self) -> Declarations {
        self.0.declarations()
    }

    /// The open connections.
    pub fn peers(&self) -> Vec<Peer> {
        lock(&self.0.peers)
            .iter()
            .filter(|p| p.is_open())
            .cloned()
            .collect()
    }

    /// Sets the hook called when a connection is established (handshake done). It runs on the
    /// task that accepted or opened the connection: keep it quick.
    pub fn on_connect(&self, hook: impl Fn(Peer) + Send + Sync + 'static) {
        lock(&self.0.hooks).on_connect = Some(Arc::new(hook));
    }

    /// Sets the hook called when a connection ends.
    pub fn on_disconnect(&self, hook: impl Fn(Peer) + Send + Sync + 'static) {
        lock(&self.0.hooks).on_disconnect = Some(Arc::new(hook));
    }

    /// Sets the hook called when a peer sends `decl` (its declarations changed).
    pub fn on_redeclare(&self, hook: impl Fn(Peer) + Send + Sync + 'static) {
        lock(&self.0.hooks).on_redeclare = Some(Arc::new(hook));
    }

    /// Replaces the factory of callback lanes (the blocking façade installs threads).
    pub(crate) fn core(&self) -> &Arc<NodeCore> {
        &self.0
    }
}

fn write<T>(l: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|p| p.into_inner())
}

fn read<T>(l: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|p| p.into_inner())
}

fn check_name(name: &str) -> Result<(), Error> {
    if valid_name(name) && !name.starts_with("ipc.") {
        Ok(())
    } else {
        Err(Error::Invalid(format!("invalid name {name:?}")))
    }
}

impl NodeCore {
    pub(crate) fn declarations(&self) -> Declarations {
        Declarations {
            methods: read(&self.methods)
                .iter()
                .map(|(n, m)| (n.clone(), m.decl.clone()))
                .collect(),
            channels: read(&self.channels)
                .iter()
                .map(|(n, c)| (n.clone(), c.decl.clone()))
                .collect(),
        }
    }

    pub(crate) fn remove_channel(&self, name: &str) {
        write(&self.channels).remove(name);
        self.redeclare();
    }

    /// Tells every connected peer what changed (SPEC 9.4); a full queue only delays the
    /// next change.
    fn redeclare(&self) {
        let peers: Vec<Peer> = lock(&self.peers).clone();
        if peers.is_empty() {
            return;
        }
        let msg = codec::map([
            ("t", Value::from("decl")),
            ("decl", self.declarations().to_value()),
        ]);
        for p in peers {
            if let Err(e) = p.conn.try_send_message(&msg) {
                debug!("decl not sent: {e}");
            }
        }
    }

    pub(crate) fn peer_opened(&self, peer: Peer) {
        lock(&self.peers).push(peer.clone());
        let hook = lock(&self.hooks).on_connect.clone();
        if let Some(h) = hook {
            h(peer);
        }
    }

    fn peer_closed(&self, peer: Peer) {
        lock(&self.peers).retain(|p| !Arc::ptr_eq(&p.st, &peer.st));
        let hook = lock(&self.hooks).on_disconnect.clone();
        if let Some(h) = hook {
            h(peer);
        }
    }

    pub(crate) fn connection_options(&self) -> ConnectionOptions {
        ConnectionOptions {
            handshake_timeout: self.cfg.handshake_timeout,
            max_inflight: self.cfg.max_inflight,
            ..ConnectionOptions::default()
        }
    }

    pub(crate) fn new_handler(&self) -> (Arc<PeerState>, Arc<dyn Handler>) {
        let st = PeerState::new(self.weak.upgrade().expect("the node is alive"));
        let handler: Arc<dyn Handler> = Arc::new(PeerHandler(st.clone()));
        (st, handler)
    }
}

// --------------------------------------------------------------------------- peer

#[derive(Default)]
struct PumpQ {
    ctrl: VecDeque<Vec<u8>>,
    ready: VecDeque<Arc<OutSub>>,
}

#[derive(Default)]
struct Pump {
    q: Mutex<PumpQ>,
    notify: Notify,
}

/// The state of one connection of a node.
pub struct PeerState {
    pub(crate) node: Arc<NodeCore>,
    conn: Mutex<Option<Connection>>,
    decl: RwLock<Declarations>,
    outsubs: Mutex<HashMap<u32, Arc<OutSub>>>,
    next_sid: AtomicU32,
    insubs: Mutex<HashMap<u32, Arc<InSub>>>,
    pump: Pump,
    started: AtomicBool,
    closed: AtomicBool,
    ordered: Mutex<Option<mpsc::UnboundedSender<BoxFuture<()>>>>,
    lane: Mutex<Option<Arc<dyn Lane>>>,
    stop: CancellationToken,
    weak: Weak<PeerState>,
}

/// One connection of a node: who is on the other end, calls to it, subscriptions to its
/// channels. Cheap to clone; the connection ends when closed, not when handles drop.
#[derive(Clone)]
pub struct Peer {
    pub(crate) st: Arc<PeerState>,
    pub(crate) conn: Connection,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.st, &other.st)
    }
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("app", &self.info().app)
            .finish()
    }
}

/// Options of a subscription.
#[derive(Clone)]
pub struct SubscribeOptions {
    /// The channel version.
    pub version: u32,
    /// The `o` map sent with the `sub`.
    pub options: Option<Value>,
    /// Called (on the lane) when the subscription ends.
    pub on_end: Option<EndFn>,
    /// How long to wait for the response.
    pub timeout: Duration,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            version: 1,
            options: None,
            on_end: None,
            timeout: Duration::from_secs(5),
        }
    }
}

impl Peer {
    /// Who is on the other end.
    pub fn info(&self) -> &PeerInfo {
        &self.conn.info().peer
    }

    /// The other end's process id from the operating system (server side only).
    pub fn pid(&self) -> Option<u32> {
        self.conn.info().client_pid
    }

    /// The raw connection.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// What the other end declares now.
    pub fn decl(&self) -> Declarations {
        read(&self.st.decl).clone()
    }

    /// Whether the connection is up.
    pub fn is_open(&self) -> bool {
        !self.conn.is_closed()
    }

    /// Ends the connection with `bye`.
    pub fn close(&self, code: &str, message: &str) {
        self.conn.close(code, message);
    }

    /// The lane this connection's callbacks run on (`None` once it has ended).
    pub fn lane(&self) -> Option<Arc<dyn Lane>> {
        self.st.lane()
    }

    /// Calls a method of the other end.
    pub async fn call(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<Value, Error> {
        self.conn.call(method, params, options).await
    }

    /// Sends a call and returns it in flight.
    pub async fn call_async(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<PendingCall, Error> {
        self.conn.call_async(method, params, options).await
    }

    /// Subscribes to a channel of the other end. A state channel's snapshot is delivered to
    /// `callback` first (`snapshot: true`).
    pub async fn subscribe(
        &self,
        channel: &str,
        opts: SubscribeOptions,
        callback: DeliveryFn,
    ) -> Result<Subscription, Error> {
        type Slot = Mutex<Option<Result<Arc<InSub>, String>>>;
        let slot: Arc<Slot> = Arc::new(Mutex::new(None));
        let hook = {
            let (st, slot, channel) = (self.st.clone(), slot.clone(), channel.to_owned());
            let (version, on_end) = (opts.version, opts.on_end.clone());
            Arc::new(move |res: &Result<Value, Error>| {
                if let Ok(r) = res {
                    let out = st.register_insub(&channel, version, r, &callback, on_end.clone());
                    *lock(&slot) = Some(out);
                }
            })
        };
        let (ch, version, options) = (channel.to_owned(), opts.version, opts.options.clone());
        let call = CallOptions {
            timeout: Some(opts.timeout),
            send_cancel: false,
            on_result: Some(hook),
            ..CallOptions::default()
        };
        let pending = self
            .conn
            .start_call(
                move |id| {
                    let mut p = vec![
                        ("t", Value::from("sub")),
                        ("id", Value::from(id)),
                        ("ch", Value::from(ch)),
                        ("v", Value::from(version)),
                    ];
                    if let Some(o) = options.filter(|o| o.as_map().is_some_and(|m| !m.is_empty())) {
                        p.push(("o", o));
                    }
                    codec::map(p)
                },
                call,
            )
            .await?;
        pending.await?;
        let out = lock(&slot).take();
        match out {
            Some(Ok(sub)) => Ok(Subscription(sub)),
            Some(Err(m)) => {
                self.conn.close("protocol_error", &m);
                Err(crate::error::ProtocolError::InvalidDeclaration(m).into())
            }
            None => Err(Error::NotConnected),
        }
    }
}

impl PeerState {
    fn new(node: Arc<NodeCore>) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            node,
            conn: Mutex::new(None),
            decl: RwLock::new(Declarations::default()),
            outsubs: Mutex::default(),
            next_sid: AtomicU32::new(1),
            insubs: Mutex::default(),
            pump: Pump::default(),
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            ordered: Mutex::new(None),
            lane: Mutex::new(None),
            stop: CancellationToken::new(),
            weak: weak.clone(),
        })
    }

    /// Binds the established connection (the first call wins) and starts the pump.
    pub(crate) fn attach(self: &Arc<Self>, conn: &Connection) -> Peer {
        if !self.closed.load(Ordering::Acquire) && !self.started.swap(true, Ordering::AcqRel) {
            *write(&self.decl) = conn.info().decl.clone();
            *lock(&self.conn) = Some(conn.clone());
            let (st, c) = (self.clone(), conn.clone());
            self.node.handle.spawn(pump_task(st, c));
        }
        Peer {
            st: self.clone(),
            conn: conn.clone(),
        }
    }

    pub(crate) fn peer(self: &Arc<Self>) -> Option<Peer> {
        let conn = lock(&self.conn).clone()?;
        Some(Peer {
            st: self.clone(),
            conn,
        })
    }

    fn conn(&self) -> Option<Connection> {
        lock(&self.conn).clone()
    }

    pub(crate) fn lane(&self) -> Option<Arc<dyn Lane>> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let mut g = lock(&self.lane);
        Some(g.get_or_insert_with(|| (self.node.lanes)()).clone())
    }

    fn submit(&self, job: crate::lane::Job) {
        match self.lane() {
            Some(l) => l.submit(job),
            None => crate::lane::run_job(job),
        }
    }

    // ---- pump

    pub(crate) fn pump_schedule(&self, sub: Arc<OutSub>) {
        lock(&self.pump.q).ready.push_back(sub);
        self.pump.notify.notify_one();
    }

    fn push_ctrl(&self, encoded: Vec<u8>) {
        lock(&self.pump.q).ctrl.push_back(encoded);
        self.pump.notify.notify_one();
    }

    fn push_msg(&self, msg: &Value) {
        match codec::encode(msg) {
            Ok(b) => self.push_ctrl(b),
            Err(e) => warn!("message not encodable: {e}"),
        }
    }

    fn reply_ok(&self, id: u64, r: Value) {
        self.push_msg(&codec::map([
            ("t", Value::from("res")),
            ("id", Value::from(id)),
            ("ok", Value::Boolean(true)),
            ("r", r),
        ]));
    }

    fn reply_err(&self, id: u64, e: &RemoteError) {
        self.push_msg(&codec::map([
            ("t", Value::from("res")),
            ("id", Value::from(id)),
            ("ok", Value::Boolean(false)),
            ("e", e.to_value()),
        ]));
    }

    /// The owner ends a subscription (`end`, SPEC 10.7). Idempotent.
    pub(crate) fn end_sub(&self, sub: &Arc<OutSub>, code: &str, message: &str) {
        if !sub.active.swap(false, Ordering::AcqRel) {
            return;
        }
        sub.deactivate();
        lock(&self.outsubs).remove(&sub.sid);
        sub.channel.remove(sub);
        self.push_msg(&codec::map([
            ("t", Value::from("end")),
            ("sid", Value::from(sub.sid)),
            ("code", Value::from(code)),
            ("msg", Value::from(message)),
        ]));
    }

    // ---- owner side of subscriptions

    fn on_sub(self: &Arc<Self>, msg: &Value) {
        let Some(id) = uint(msg, "id") else { return };
        let name = text(msg, "ch").unwrap_or("");
        let version = uint(msg, "v").unwrap_or(1);
        let options = codec::get(msg, "o")
            .filter(|o| o.is_map())
            .cloned()
            .unwrap_or(Value::Nil);
        let chan = read(&self.node.channels).get(name).cloned();
        let Some(chan) = chan else {
            self.reply_err(
                id,
                &RemoteError::new("unknown_channel", format!("unknown channel {name}")),
            );
            return;
        };
        let version = u32::try_from(version).unwrap_or(u32::MAX);
        if !chan.decl.versions.contains(&version) {
            let mut e = RemoteError::new(
                "version_unsupported",
                format!("{name} v{version} is not offered"),
            );
            e.details = Some(codec::map([(
                "versions",
                Value::Array(chan.decl.versions.iter().map(|v| Value::from(*v)).collect()),
            )]));
            self.reply_err(id, &e);
            return;
        }
        let existing = lock(&self.outsubs)
            .values()
            .find(|o| Arc::ptr_eq(&o.channel, &chan) && o.version == version && o.is_active())
            .cloned();
        if let Some(sub) = existing {
            // SPEC 10.6: the same subscription again.
            let g = lock(&chan.state);
            let r = sub_result(&chan, &g, sub.sid, sub.seq());
            drop(g);
            self.reply_ok(id, r);
            return;
        }
        let hook = lock(&chan.on_subscribe).clone();
        match (hook, self.peer()) {
            (Some(hook), Some(peer)) => {
                let st = self.clone();
                self.node.handle.spawn(async move {
                    let req = SubscribeRequest {
                        peer,
                        version,
                        options: options.clone(),
                    };
                    match hook(req).await {
                        Ok(()) => st.finish_sub(id, chan, version, options),
                        Err(e) => st.reply_err(id, &e),
                    }
                });
            }
            _ => self.finish_sub(id, chan, version, options),
        }
    }

    fn finish_sub(self: &Arc<Self>, id: u64, chan: Arc<ChannelCore>, version: u32, options: Value) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let sid = self.next_sid.fetch_add(1, Ordering::Relaxed);
        let sub = Arc::new(OutSub::new(
            sid,
            chan.clone(),
            self.weak.clone(),
            version,
            options,
        ));
        let first = {
            // Snapshot and registration under the state lock: the snapshot precedes the first
            // change, and the response precedes both (SPEC 13.2).
            let g = lock(&chan.state);
            let r = sub_result(&chan, &g, sid, 0);
            self.reply_ok(id, r);
            lock(&self.outsubs).insert(sid, sub.clone());
            chan.add(sub)
        };
        if first {
            chan.notify_active(true);
        }
    }

    fn on_unsub(&self, msg: &Value) {
        let Some(sid) = uint(msg, "sid") else { return };
        let sub = lock(&self.outsubs).remove(&(sid as u32));
        if let Some(sub) = sub {
            sub.active.store(false, Ordering::Release);
            sub.deactivate();
            sub.channel.remove(&sub);
        }
    }

    // ---- subscriber side

    fn register_insub(
        self: &Arc<Self>,
        channel: &str,
        version: u32,
        r: &Value,
        callback: &DeliveryFn,
        on_end: Option<EndFn>,
    ) -> Result<Arc<InSub>, String> {
        let sid = uint(r, "sid")
            .and_then(|s| u32::try_from(s).ok())
            .ok_or("sub response without a sid")?;
        let kind = text(r, "kind")
            .and_then(ChannelKind::parse)
            .ok_or("sub response without a kind")?;
        let layout = match kind {
            ChannelKind::Stream => Some(Arc::new(
                Layout::from_value(codec::get(r, "layout").unwrap_or(&Value::Nil))
                    .map_err(|e| format!("bad stream layout: {e}"))?,
            )),
            _ => None,
        };
        let limit = match kind {
            ChannelKind::Stream => read(&self.decl)
                .channels
                .get(channel)
                .and_then(|c| c.rate_hz)
                .map_or(256, |hz| (hz as usize).max(64)),
            ChannelKind::State => 1,
            ChannelKind::Event => self.node.cfg.event_queue,
        };
        let sub = Arc::new(InSub {
            sid,
            channel: channel.to_owned(),
            version,
            kind,
            layout,
            peer: self.weak.clone(),
            callback: callback.clone(),
            on_end,
            ended: AtomicBool::new(false),
            end_info: Mutex::new(None),
            pending: Mutex::new(VecDeque::new()),
            limit,
            last_seq: AtomicU64::new(uint(r, "seq").unwrap_or(0)),
            snapshot: Mutex::new(None),
        });
        lock(&self.insubs).insert(sid, sub.clone());
        if kind == ChannelKind::State {
            let data = codec::get(r, "snap").cloned().unwrap_or(Value::Nil);
            *lock(&sub.snapshot) = Some(data.clone());
            sub.deliver(
                self,
                Delivery::Event(Event {
                    channel: channel.to_owned(),
                    seq: uint(r, "seq").unwrap_or(0),
                    ts: codec::get(r, "ts").and_then(Value::as_i64),
                    data,
                    snapshot: true,
                }),
            );
        }
        Ok(sub)
    }

    fn on_evt(self: &Arc<Self>, conn: &Connection, msg: &Value) {
        let Some(sid) = uint(msg, "sid") else { return };
        let sub = lock(&self.insubs).get(&(sid as u32)).cloned();
        let Some(sub) = sub else { return };
        if sub.kind == ChannelKind::Stream {
            conn.close("protocol_error", "evt for a stream subscription");
            return;
        }
        let data = codec::get(msg, "d").cloned().unwrap_or(Value::Nil);
        let seq = uint(msg, "seq").unwrap_or(0);
        sub.last_seq.store(seq, Ordering::Relaxed);
        if sub.kind == ChannelKind::State {
            *lock(&sub.snapshot) = Some(data.clone());
        }
        sub.deliver(
            self,
            Delivery::Event(Event {
                channel: sub.channel.clone(),
                seq,
                ts: codec::get(msg, "ts").and_then(Value::as_i64),
                data,
                snapshot: false,
            }),
        );
    }

    fn on_end(self: &Arc<Self>, msg: &Value) {
        let Some(sid) = uint(msg, "sid") else { return };
        let sub = lock(&self.insubs).remove(&(sid as u32));
        if let Some(sub) = sub {
            self.end_insub(
                &sub,
                text(msg, "code").unwrap_or("channel_closed"),
                text(msg, "msg").unwrap_or(""),
            );
        }
    }

    fn end_insub(&self, sub: &Arc<InSub>, code: &str, message: &str) {
        if sub.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        *lock(&sub.end_info) = Some((code.to_owned(), message.to_owned()));
        if let Some(f) = sub.on_end.clone() {
            let (ch, c, m) = (sub.channel.clone(), code.to_owned(), message.to_owned());
            self.submit_unclosed(Box::new(move || f(&ch, &c, &m)));
        }
    }

    /// Like `submit` but also during the close (the lane is dropped right after).
    fn submit_unclosed(&self, job: crate::lane::Job) {
        let mut g = lock(&self.lane);
        let lane = g.get_or_insert_with(|| (self.node.lanes)()).clone();
        drop(g);
        lane.submit(job);
    }

    fn unsub_local(self: &Arc<Self>, sub: &Arc<InSub>, code: &str, message: &str) {
        let known = lock(&self.insubs).remove(&sub.sid).is_some();
        if known {
            if let Some(c) = self.conn() {
                let _ = c.try_send_message(&codec::map([
                    ("t", Value::from("unsub")),
                    ("sid", Value::from(sub.sid)),
                ]));
            }
            self.end_insub(sub, code, message);
        }
    }

    fn on_stream(self: &Arc<Self>, conn: &Connection, s: StreamSample) {
        let sub = lock(&self.insubs).get(&s.sid).cloned();
        let Some(sub) = sub else { return };
        let Some(layout) = sub.layout.clone() else {
            conn.close(
                "protocol_error",
                "stream frame for a non-stream subscription",
            );
            return;
        };
        if s.body.len() != layout.size() {
            conn.close(
                "protocol_error",
                &format!(
                    "stream {}: body of {} bytes, layout says {}",
                    sub.channel,
                    s.body.len(),
                    layout.size()
                ),
            );
            return;
        }
        sub.last_seq.store(u64::from(s.seq), Ordering::Relaxed);
        sub.deliver(
            self,
            Delivery::Sample(Sample {
                channel: sub.channel.clone(),
                seq: s.seq,
                t_ns: s.t_ns,
                dropped: s.flags & wire::STREAM_FLAG_DROPPED != 0,
                body: s.body,
                layout,
            }),
        );
    }

    fn on_decl(&self, conn: &Connection, msg: &Value) {
        let parsed = codec::get(msg, "decl")
            .map(Declarations::from_value)
            .unwrap_or_else(|| Ok(Declarations::default()));
        match parsed {
            Ok(d) => {
                *write(&self.decl) = d;
                let hook = lock(&self.node.hooks).on_redeclare.clone();
                if let (Some(h), Some(p)) = (hook, self.weak.upgrade().and_then(|s| s.peer())) {
                    h(p);
                }
            }
            Err(e) => conn.close("protocol_error", &format!("bad decl: {e}")),
        }
    }

    // ---- ordered methods

    fn run_ordered(
        self: &Arc<Self>,
        run: BoxFuture<Result<Value, RemoteError>>,
    ) -> BoxFuture<Result<Value, RemoteError>> {
        let (tx, rx) = oneshot::channel();
        let job: BoxFuture<()> = Box::pin(async move {
            let _ = tx.send(run.await);
        });
        let queued = {
            let mut g = lock(&self.ordered);
            if self.closed.load(Ordering::Acquire) {
                false
            } else {
                let sender = g.get_or_insert_with(|| {
                    let (jtx, mut jrx) = mpsc::unbounded_channel::<BoxFuture<()>>();
                    let (stop, handle) = (self.stop.clone(), self.node.handle.clone());
                    self.node.handle.spawn(async move {
                        loop {
                            let job = tokio::select! {
                                () = stop.cancelled() => break,
                                j = jrx.recv() => match j { Some(j) => j, None => break },
                            };
                            // Spawned so that a panic fails one request, not the lane.
                            let mut task = handle.spawn(job);
                            tokio::select! {
                                () = stop.cancelled() => { task.abort(); break }
                                _ = &mut task => {}
                            }
                        }
                    });
                    jtx
                });
                sender.send(job).is_ok()
            }
        };
        Box::pin(async move {
            if !queued {
                return Err(RemoteError::new(
                    "shutting_down",
                    "the connection is closing",
                ));
            }
            rx.await
                .unwrap_or_else(|_| Err(RemoteError::new("internal", "the method failed")))
        })
    }

    // ---- close

    fn on_closed(self: &Arc<Self>, info: &CloseInfo) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.stop.cancel();
        let conn = lock(&self.conn).take();
        let outs: Vec<_> = lock(&self.outsubs).drain().map(|(_, s)| s).collect();
        for sub in outs {
            sub.active.store(false, Ordering::Release);
            sub.deactivate();
            sub.channel.remove(&sub);
        }
        let ins: Vec<_> = lock(&self.insubs).drain().map(|(_, s)| s).collect();
        for sub in ins {
            self.end_insub(&sub, &info.code, &info.message);
        }
        lock(&self.ordered).take();
        if let Some(conn) = conn {
            self.node.peer_closed(Peer {
                st: self.clone(),
                conn,
            });
        }
        lock(&self.lane).take();
    }
}

/// The `r` of a `sub` response.
fn sub_result(chan: &ChannelCore, state: &crate::channels::StateData, sid: u32, seq: u64) -> Value {
    let mut r = vec![
        ("sid", Value::from(sid)),
        ("kind", Value::from(chan.kind().name())),
        ("seq", Value::from(seq)),
    ];
    match chan.kind() {
        ChannelKind::State => {
            r.push(("snap", state.value.clone()));
            r.push(("ts", Value::from(state.ts)));
        }
        ChannelKind::Stream => {
            if let Some(l) = &chan.decl.layout {
                r.push(("layout", l.to_value()));
            }
        }
        ChannelKind::Event => {}
    }
    codec::map(r)
}

async fn pump_task(st: Arc<PeerState>, conn: Connection) {
    enum Work {
        Ctrl(Vec<u8>),
        Sub(Arc<OutSub>),
    }
    loop {
        let work = {
            let mut q = lock(&st.pump.q);
            q.ctrl
                .pop_front()
                .map(Work::Ctrl)
                .or_else(|| q.ready.pop_front().map(Work::Sub))
        };
        match work {
            None => {
                tokio::select! {
                    () = st.stop.cancelled() => return,
                    () = st.pump.notify.notified() => {}
                }
            }
            Some(Work::Ctrl(b)) => {
                if conn.send_encoded(&b).await.is_err() {
                    return;
                }
            }
            Some(Work::Sub(sub)) => {
                if !sub.is_active() {
                    continue;
                }
                let (items, more) = sub.take(64);
                for it in items {
                    if !sub.is_active() {
                        break;
                    }
                    let sent = match it {
                        OutItem::Msg(b) => conn.send_encoded(&b).await,
                        OutItem::Sample {
                            seq,
                            t_ns,
                            flags,
                            body,
                        } => {
                            conn.send_stream_sample(sub.sid, seq as u32, t_ns, flags, &body)
                                .await
                        }
                    };
                    if sent.is_err() {
                        return;
                    }
                }
                if more {
                    lock(&st.pump.q).ready.push_back(sub);
                }
            }
        }
    }
}

// --------------------------------------------------------------------------- handler

pub(crate) struct PeerHandler(pub(crate) Arc<PeerState>);

fn ready(r: Result<Value, RemoteError>) -> BoxFuture<Result<Value, RemoteError>> {
    Box::pin(async move { r })
}

async fn exec(m: Arc<MethodEntry>, ctx: Context, params: Value) -> Result<Value, RemoteError> {
    if ctx.expired() {
        return Err(RemoteError::new(
            "timeout",
            "the deadline passed before the request ran",
        ));
    }
    let f = m.f.clone();
    let c = ctx.clone();
    let call = move || f(c, params);
    match (&m.cache, ctx.idempotency_key()) {
        (Some(cache), Some(ik)) => {
            let key = (
                ctx.peer().info().app.clone(),
                ctx.method().to_owned(),
                ik.to_owned(),
            );
            cache.run(key, ctx.deadline_at(), call).await
        }
        _ => call().await,
    }
}

impl Handler for PeerHandler {
    fn request(&self, rc: RequestContext) -> BoxFuture<Result<Value, RemoteError>> {
        let st = &self.0;
        let peer = st.attach(&rc.conn);
        let m = read(&st.node.methods).get(&rc.method).cloned();
        let Some(m) = m else {
            return ready(Err(RemoteError::new(
                "unknown_method",
                format!("unknown method {}", rc.method),
            )));
        };
        let wanted = u32::try_from(rc.version).unwrap_or(u32::MAX);
        if !m.decl.versions.contains(&wanted) {
            let mut e = RemoteError::new(
                "version_unsupported",
                format!("{} v{} is not offered", rc.method, rc.version),
            );
            e.details = Some(codec::map([(
                "versions",
                Value::Array(m.decl.versions.iter().map(|v| Value::from(*v)).collect()),
            )]));
            return ready(Err(e));
        }
        let params = rc.params.clone();
        let deadline = rc.deadline.map(|d| Instant::now() + d);
        let ctx = Context(Arc::new(CtxInner {
            peer,
            rc,
            handle: st.node.handle.clone(),
            deadline: Mutex::new(deadline),
        }));
        let ordered = m.decl.ordered;
        let run: BoxFuture<Result<Value, RemoteError>> = Box::pin(exec(m, ctx, params));
        if ordered { st.run_ordered(run) } else { run }
    }

    fn message(&self, conn: &Connection, message: Value) {
        let st = &self.0;
        st.attach(conn);
        match text(&message, "t").unwrap_or("") {
            "sub" => st.on_sub(&message),
            "unsub" => st.on_unsub(&message),
            "end" => st.on_end(&message),
            "evt" => st.on_evt(conn, &message),
            "decl" => st.on_decl(conn, &message),
            "res" => {
                // A response nobody waits for: a subscription the subscriber gave up on is
                // ended (SPEC 15.1).
                if codec::get(&message, "ok") == Some(&Value::Boolean(true))
                    && let Some(sid) = codec::get(&message, "r").and_then(|r| uint(r, "sid"))
                {
                    let _ = conn.try_send_message(&codec::map([
                        ("t", Value::from("unsub")),
                        ("sid", Value::from(sid)),
                    ]));
                }
            }
            _ => {}
        }
    }

    fn stream(&self, conn: &Connection, sample: StreamSample) {
        self.0.attach(conn);
        self.0.on_stream(conn, sample);
    }

    fn closed(&self, info: &CloseInfo) {
        self.0.on_closed(info);
    }
}

// --------------------------------------------------------------------------- insubs

pub(crate) struct InSub {
    sid: u32,
    channel: String,
    version: u32,
    kind: ChannelKind,
    layout: Option<Arc<Layout>>,
    peer: Weak<PeerState>,
    callback: DeliveryFn,
    on_end: Option<EndFn>,
    ended: AtomicBool,
    end_info: Mutex<Option<(String, String)>>,
    pending: Mutex<VecDeque<Delivery>>,
    limit: usize,
    last_seq: AtomicU64,
    snapshot: Mutex<Option<Value>>,
}

impl InSub {
    /// Hands `item` to the callback on the lane, with the owner's bounded-queue rules between
    /// the reader and the application (SPEC 12.4): streams drop the oldest (the next sample
    /// says `dropped`), state keeps the latest, events end the subscription `overloaded`.
    fn deliver(self: &Arc<Self>, st: &Arc<PeerState>, mut item: Delivery) {
        let mut pending = lock(&self.pending);
        if self.kind == ChannelKind::State
            && let Some(last) = pending.back_mut()
        {
            *last = item;
            return;
        }
        if pending.len() >= self.limit {
            if self.kind == ChannelKind::Stream {
                pending.pop_front();
                if let Delivery::Sample(s) = &mut item {
                    s.dropped = true;
                }
                pending.push_back(item);
                return;
            }
            pending.clear();
            drop(pending);
            st.unsub_local(self, "overloaded", "callback too slow: local queue full");
            return;
        }
        pending.push_back(item);
        drop(pending);
        let sub = self.clone();
        st.submit(Box::new(move || {
            let item = lock(&sub.pending).pop_front();
            if let Some(item) = item
                && !sub.ended.load(Ordering::Acquire)
            {
                (sub.callback)(item);
            }
        }));
    }
}

/// A subscription to a channel of the other end, on one connection.
#[derive(Clone)]
pub struct Subscription(pub(crate) Arc<InSub>);

impl Subscription {
    /// The channel.
    pub fn channel(&self) -> &str {
        &self.0.channel
    }
    /// The subscription id the owner chose.
    pub fn sid(&self) -> u32 {
        self.0.sid
    }
    /// The channel version.
    pub fn version(&self) -> u32 {
        self.0.version
    }
    /// The channel's kind.
    pub fn kind(&self) -> ChannelKind {
        self.0.kind
    }
    /// A stream's layout.
    pub fn layout(&self) -> Option<&Layout> {
        self.0.layout.as_deref()
    }
    /// The last `seq` seen.
    pub fn last_seq(&self) -> u64 {
        self.0.last_seq.load(Ordering::Relaxed)
    }
    /// A state channel's latest value seen.
    pub fn snapshot(&self) -> Option<Value> {
        lock(&self.0.snapshot).clone()
    }
    /// Whether it has ended.
    pub fn is_ended(&self) -> bool {
        self.0.ended.load(Ordering::Acquire)
    }
    /// `(code, message)` it ended with.
    pub fn end_info(&self) -> Option<(String, String)> {
        lock(&self.0.end_info).clone()
    }
    /// Unsubscribes (`unsub`); no callback runs after the lane's next turn.
    pub fn close(&self) {
        if let Some(st) = self.0.peer.upgrade() {
            st.unsub_local(&self.0, "normal", "unsubscribed");
        }
    }
}
