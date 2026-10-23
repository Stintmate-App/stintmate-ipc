//! The channels a node owns (SPEC 9.3, 10.6-10.9, 12): events, state and streams, and the
//! owner's side of each subscription, with the bounded queue of its kind.
//!
//! Producers (`publish`, `set`, `send`) are plain synchronous calls that never block and never
//! write to the pipe: they put the item in the subscription's queue and wake the connection's
//! pump task, which alone writes (SPEC 12).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use bytes::Bytes;

use crate::clock::now_ns;
use crate::codec::{self, Value};
use crate::connection::BoxFuture;
use crate::decl::{ChannelDecl, ChannelKind};
use crate::error::{Error, LayoutError, RemoteError};
use crate::handshake::PeerInfo;
use crate::layout::{Layout, Scalar};
use crate::node::{NodeCore, Peer, PeerState};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// What an `on_subscribe` hook sees.
pub struct SubscribeRequest {
    /// Who subscribes.
    pub peer: Peer,
    /// The channel version asked for.
    pub version: u32,
    /// The `o` map of the `sub` (nil when it sent none).
    pub options: Value,
}

/// Runs before a subscription is accepted; an `Err` refuses it with that error.
pub type SubscribeHook =
    Arc<dyn Fn(SubscribeRequest) -> BoxFuture<Result<(), RemoteError>> + Send + Sync>;

/// Called with `true` when the first subscriber comes and `false` when the last leaves.
pub type ActiveHook = Arc<dyn Fn(bool) + Send + Sync>;

// --------------------------------------------------------------------------- out subs

#[derive(Default)]
pub(crate) struct OutQ {
    seq: u64,
    scheduled: bool,
    events: VecDeque<Bytes>,
    bytes: usize,
    state: Option<Bytes>,
    ring: VecDeque<(u64, i64, Bytes)>,
    dropped: bool,
}

/// One thing the pump writes for a subscription.
pub(crate) enum OutItem {
    Msg(Bytes),
    Sample {
        seq: u64,
        t_ns: i64,
        flags: u32,
        body: Bytes,
    },
}

/// The owner's side of one subscription.
pub(crate) struct OutSub {
    pub(crate) sid: u32,
    pub(crate) channel: Arc<ChannelCore>,
    pub(crate) peer: Weak<PeerState>,
    pub(crate) version: u32,
    pub(crate) options: Value,
    pub(crate) active: AtomicBool,
    ring_max: usize,
    q: Mutex<OutQ>,
}

/// The encoded `evt` message with `packed` (an already encoded value) as its `d`.
fn evt_message(sid: u32, seq: u64, ts: i64, packed: &[u8]) -> Bytes {
    let mut v = Vec::with_capacity(packed.len() + 40);
    v.push(0x85);
    for part in [
        Value::from("t"),
        Value::from("evt"),
        Value::from("sid"),
        Value::from(sid),
        Value::from("seq"),
        Value::from(seq),
        Value::from("ts"),
        Value::from(ts),
        Value::from("d"),
    ] {
        codec::encode_into(&mut v, &part).expect("small values encode");
    }
    v.extend_from_slice(packed);
    v.into()
}

impl OutSub {
    pub(crate) fn new(
        sid: u32,
        channel: Arc<ChannelCore>,
        peer: Weak<PeerState>,
        version: u32,
        options: Value,
    ) -> Self {
        let ring_max = channel.ring_max();
        Self {
            sid,
            channel,
            peer,
            version,
            options,
            active: AtomicBool::new(true),
            ring_max,
            q: Mutex::new(OutQ::default()),
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// The last `seq` offered.
    pub(crate) fn seq(&self) -> u64 {
        lock(&self.q).seq
    }

    /// Stops the subscription and forgets what it queued.
    pub(crate) fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        let mut q = lock(&self.q);
        q.events.clear();
        q.bytes = 0;
        q.state = None;
        q.ring.clear();
    }

    fn schedule(self: &Arc<Self>, st: &Arc<PeerState>, was_scheduled: bool) {
        if !was_scheduled {
            st.pump_schedule(self.clone());
        }
    }

    /// Queues an event; ends the subscription `overloaded` when it does not fit (SPEC 12.2).
    pub(crate) fn offer_event(self: &Arc<Self>, st: &Arc<PeerState>, ts: i64, packed: &[u8]) {
        let (max_n, max_bytes) = (st.node.cfg.event_queue, st.node.cfg.event_queue_bytes);
        let (overloaded, was) = {
            let mut q = lock(&self.q);
            if !self.is_active() {
                return;
            }
            q.seq += 1;
            let msg = evt_message(self.sid, q.seq, ts, packed);
            if q.events.len() + 1 > max_n || q.bytes + msg.len() > max_bytes {
                (true, true)
            } else {
                q.bytes += msg.len();
                q.events.push_back(msg);
                let was = q.scheduled;
                q.scheduled = true;
                (false, was)
            }
        };
        if overloaded {
            st.end_sub(self, "overloaded", "subscriber too slow: queue full");
        } else {
            self.schedule(st, was);
        }
    }

    /// Queues a state value, replacing a pending one (SPEC 12.3).
    pub(crate) fn offer_state(self: &Arc<Self>, st: &Arc<PeerState>, ts: i64, packed: &[u8]) {
        let was = {
            let mut q = lock(&self.q);
            if !self.is_active() {
                return;
            }
            q.seq += 1;
            q.state = Some(evt_message(self.sid, q.seq, ts, packed));
            let was = q.scheduled;
            q.scheduled = true;
            was
        };
        self.schedule(st, was);
    }

    /// Queues a sample; the oldest goes when the ring is full (SPEC 12.1).
    pub(crate) fn offer_sample(self: &Arc<Self>, st: &Arc<PeerState>, t_ns: i64, body: &Bytes) {
        let was = {
            let mut q = lock(&self.q);
            if !self.is_active() {
                return;
            }
            q.seq += 1;
            if q.ring.len() >= self.ring_max {
                q.ring.pop_front();
                q.dropped = true;
            }
            let seq = q.seq;
            q.ring.push_back((seq, t_ns, body.clone()));
            let was = q.scheduled;
            q.scheduled = true;
            was
        };
        self.schedule(st, was);
    }

    /// What the pump writes next, and whether more is queued (then the subscription stays
    /// scheduled; otherwise the next offer schedules it again).
    pub(crate) fn take(&self, max: usize) -> (Vec<OutItem>, bool) {
        let mut q = lock(&self.q);
        let mut items = Vec::new();
        if let Some(s) = q.state.take() {
            items.push(OutItem::Msg(s));
        }
        while items.len() < max {
            let Some(m) = q.events.pop_front() else {
                break;
            };
            q.bytes -= m.len();
            items.push(OutItem::Msg(m));
        }
        while items.len() < max {
            let Some((seq, t_ns, body)) = q.ring.pop_front() else {
                break;
            };
            let flags = u32::from(std::mem::take(&mut q.dropped));
            items.push(OutItem::Sample {
                seq,
                t_ns,
                flags,
                body,
            });
        }
        let more = q.state.is_some() || !q.events.is_empty() || !q.ring.is_empty();
        q.scheduled = more;
        (items, more)
    }
}

// --------------------------------------------------------------------------- channel core

pub(crate) struct StateData {
    pub(crate) value: Value,
    pub(crate) ts: i64,
}

/// A channel this node owns.
pub(crate) struct ChannelCore {
    pub(crate) name: String,
    pub(crate) decl: ChannelDecl,
    pub(crate) node: Weak<NodeCore>,
    pub(crate) subs: Mutex<Arc<Vec<Arc<OutSub>>>>,
    pub(crate) state: Mutex<StateData>,
    pub(crate) on_subscribe: Mutex<Option<SubscribeHook>>,
    on_active: Mutex<Option<ActiveHook>>,
    queue: Option<usize>,
}

impl ChannelCore {
    pub(crate) fn new(
        name: &str,
        decl: ChannelDecl,
        node: Weak<NodeCore>,
        initial: Value,
        queue: Option<usize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_owned(),
            decl,
            node,
            subs: Mutex::new(Arc::new(Vec::new())),
            state: Mutex::new(StateData {
                value: initial,
                ts: now_ns(),
            }),
            on_subscribe: Mutex::new(None),
            on_active: Mutex::new(None),
            queue,
        })
    }

    pub(crate) fn kind(&self) -> ChannelKind {
        self.decl.kind
    }

    /// A stream subscription's ring: one second at `rate_hz`, at least 64 (SPEC 12.1).
    fn ring_max(&self) -> usize {
        self.queue
            .unwrap_or_else(|| (self.decl.rate_hz.unwrap_or(0.0) as usize).max(64))
            .max(1)
    }

    pub(crate) fn snapshot_subs(&self) -> Arc<Vec<Arc<OutSub>>> {
        lock(&self.subs).clone()
    }

    pub(crate) fn notify_active(&self, v: bool) {
        let cb = lock(&self.on_active).clone();
        if let Some(cb) = cb {
            cb(v);
        }
    }

    /// Adds `sub`; the caller holds whatever lock orders it against the producers. Returns
    /// whether it is the first one: the caller then calls `notify_active(true)` after releasing
    /// its locks.
    pub(crate) fn add(&self, sub: Arc<OutSub>) -> bool {
        let mut g = lock(&self.subs);
        let first = g.is_empty();
        let mut v: Vec<_> = g.iter().cloned().collect();
        v.push(sub);
        *g = Arc::new(v);
        first
    }

    pub(crate) fn remove(&self, sub: &Arc<OutSub>) {
        let last = {
            let mut g = lock(&self.subs);
            if !g.iter().any(|s| Arc::ptr_eq(s, sub)) {
                return;
            }
            let v: Vec<_> = g.iter().filter(|s| !Arc::ptr_eq(s, sub)).cloned().collect();
            let last = v.is_empty();
            *g = Arc::new(v);
            last
        };
        if last {
            self.notify_active(false);
        }
    }

    fn close(self: &Arc<Self>, code: &str, message: &str) {
        for sub in self.snapshot_subs().iter() {
            if let Some(st) = sub.peer.upgrade() {
                st.end_sub(sub, code, message);
            }
        }
        if let Some(node) = self.node.upgrade() {
            node.remove_channel(&self.name);
        }
    }

    fn peer_state_of(sub: &OutSub) -> Option<Arc<PeerState>> {
        sub.peer.upgrade()
    }

    fn event_to(
        self: &Arc<Self>,
        data: &Value,
        ts: Option<i64>,
        pick: &dyn Fn(&OutSub) -> bool,
    ) -> Result<usize, Error> {
        let subs = self.snapshot_subs();
        if subs.is_empty() {
            return Ok(0);
        }
        let ts = ts.unwrap_or_else(now_ns);
        let mut packed: Option<Vec<u8>> = None;
        let mut n = 0;
        for sub in subs.iter() {
            if !pick(sub) {
                continue;
            }
            let Some(st) = Self::peer_state_of(sub) else {
                continue;
            };
            if packed.is_none() {
                packed = Some(codec::encode(data)?);
            }
            if let Some(p) = &packed {
                match self.kind() {
                    ChannelKind::State => sub.offer_state(&st, ts, p),
                    _ => sub.offer_event(&st, ts, p),
                }
            }
            n += 1;
        }
        Ok(n)
    }
}

/// A read-only view of one subscription to a channel this node owns, for
/// `publish_filtered`.
pub struct SubscriptionView<'a> {
    sub: &'a OutSub,
    peer: &'a Peer,
}

impl SubscriptionView<'_> {
    /// Who subscribed.
    pub fn peer_info(&self) -> &PeerInfo {
        self.peer.info()
    }

    /// The subscriber's process id from the operating system (server side only).
    pub fn pid(&self) -> Option<u32> {
        self.peer.pid()
    }

    /// The channel version it subscribed to.
    pub fn version(&self) -> u32 {
        self.sub.version
    }

    /// Its `options` map (nil when it sent none).
    pub fn options(&self) -> &Value {
        &self.sub.options
    }

    /// One option by key.
    pub fn option(&self, key: &str) -> Option<&Value> {
        codec::get(&self.sub.options, key)
    }

    /// The connection it arrived on: compare with the `peer` of a request to deliver only
    /// to the connection that made it.
    pub fn connection(&self) -> &Peer {
        self.peer
    }
}

/// Options of an event publish.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishOptions {
    /// `t_ns` of the event; default now.
    pub ts: Option<i64>,
    /// Only the subscriptions of this channel version.
    pub version: Option<u32>,
}

macro_rules! channel_common {
    ($t:ty) => {
        impl $t {
            /// The channel's name.
            pub fn name(&self) -> &str {
                &self.0.name
            }

            /// How many subscriptions it has.
            pub fn subscriber_count(&self) -> usize {
                self.0.snapshot_subs().len()
            }

            /// Whether anybody is subscribed: a producer may stop while nobody listens.
            pub fn is_active(&self) -> bool {
                self.subscriber_count() > 0
            }

            /// Sets the hook that runs before a subscription is accepted; it may refuse.
            pub fn on_subscribe(
                &self,
                hook: impl Fn(SubscribeRequest) -> BoxFuture<Result<(), RemoteError>>
                + Send
                + Sync
                + 'static,
            ) {
                *lock(&self.0.on_subscribe) = Some(Arc::new(hook));
            }

            /// Sets the hook called when the first subscriber comes (`true`) and the last one
            /// leaves (`false`). Runs on whichever thread caused it; it must be quick.
            pub fn on_active(&self, hook: impl Fn(bool) + Send + Sync + 'static) {
                *lock(&self.0.on_active) = Some(Arc::new(hook));
            }

            /// Ends every subscription (`end` with `code`) and withdraws the channel.
            pub fn close(&self, code: &str, message: &str) {
                self.0.close(code, message);
            }
        }
    };
}

/// Discrete events; a subscriber too slow for its bounded queue is ended `overloaded`
/// (SPEC 12.2).
#[derive(Clone)]
pub struct EventChannel(pub(crate) Arc<ChannelCore>);
/// A current value: a new subscriber gets it in the `sub` response, then every change; a slow
/// subscriber gets only the latest (SPEC 12.3).
#[derive(Clone)]
pub struct StateChannel(pub(crate) Arc<ChannelCore>);
/// High-rate fixed-layout samples; a full ring drops the oldest (SPEC 12.1).
#[derive(Clone)]
pub struct StreamChannel(pub(crate) Arc<ChannelCore>);

channel_common!(EventChannel);
channel_common!(StateChannel);
channel_common!(StreamChannel);

impl EventChannel {
    /// Sends `data` to every subscriber. Never blocks. Returns how many it was queued for.
    pub fn publish(&self, data: &Value) -> Result<usize, Error> {
        self.publish_with(data, PublishOptions::default())
    }

    /// [`publish`](Self::publish) with a timestamp and/or a version filter.
    pub fn publish_with(&self, data: &Value, opts: PublishOptions) -> Result<usize, Error> {
        self.0.event_to(data, opts.ts, &|s| {
            opts.version.is_none_or(|v| s.version == v)
        })
    }

    /// Sends `data` only to the subscriptions `accept` returns true for. The payload is packed
    /// once, and only if some subscription takes it. `accept` runs on the calling thread; it
    /// must be quick and must not block.
    pub fn publish_filtered(
        &self,
        data: &Value,
        accept: impl Fn(&SubscriptionView<'_>) -> bool,
    ) -> Result<usize, Error> {
        self.0.event_to(data, None, &|s| {
            let Some(st) = s.peer.upgrade() else {
                return false;
            };
            let Some(peer) = st.peer() else {
                return false;
            };
            accept(&SubscriptionView {
                sub: s,
                peer: &peer,
            })
        })
    }

    /// Sends `data` only to `peer`'s subscriptions to this channel: a reply to one subscriber.
    pub fn send_to(&self, peer: &Peer, data: &Value) -> Result<usize, Error> {
        self.0.event_to(data, None, &|s| {
            s.peer
                .upgrade()
                .is_some_and(|st| Arc::ptr_eq(&st, &peer.st))
        })
    }
}

impl StateChannel {
    /// The current value.
    pub fn value(&self) -> Value {
        lock(&self.0.state).value.clone()
    }

    /// Replaces the value and pushes it (always the whole value, never a delta).
    pub fn set(&self, value: &Value) -> Result<(), Error> {
        let packed = codec::encode(value)?;
        let ts = now_ns();
        let mut g = lock(&self.0.state);
        g.value = value.clone();
        g.ts = ts;
        for sub in self.0.snapshot_subs().iter() {
            if let Some(st) = sub.peer.upgrade() {
                sub.offer_state(&st, ts, &packed);
            }
        }
        Ok(())
    }

    /// Pushes `value` to `peer`'s subscriptions only (a snapshot again after a resync); the
    /// channel's value and the other subscribers are untouched.
    pub fn send_to(&self, peer: &Peer, value: &Value) -> Result<usize, Error> {
        let packed = codec::encode(value)?;
        let _g = lock(&self.0.state);
        let mut n = 0;
        for sub in self.0.snapshot_subs().iter() {
            if let Some(st) = sub.peer.upgrade()
                && Arc::ptr_eq(&st, &peer.st)
            {
                sub.offer_state(&st, now_ns(), &packed);
                n += 1;
            }
        }
        Ok(n)
    }
}

impl StreamChannel {
    /// The layout of its samples.
    pub fn layout(&self) -> &Layout {
        self.0.decl.layout.as_ref().expect("a stream has a layout")
    }

    /// Sends one sample (`body`: exactly `layout().size()` bytes). `t_ns`: when it was taken
    /// (default now). Returns how many subscriptions it was queued for.
    pub fn send(&self, body: &[u8], t_ns: Option<i64>) -> Result<usize, LayoutError> {
        let subs = self.0.snapshot_subs();
        if subs.is_empty() {
            return Ok(0);
        }
        if body.len() != self.layout().size() {
            return Err(LayoutError::Mismatch(format!(
                "{}: sample of {} bytes, layout is {}",
                self.0.name,
                body.len(),
                self.layout().size()
            )));
        }
        let t = t_ns.unwrap_or_else(now_ns);
        let body = Bytes::copy_from_slice(body);
        let mut n = 0;
        for sub in subs.iter() {
            if let Some(st) = sub.peer.upgrade() {
                sub.offer_sample(&st, t, &body);
                n += 1;
            }
        }
        Ok(n)
    }

    /// Packs flat `values` with the layout and sends them.
    pub fn send_values(&self, values: &[Scalar], t_ns: Option<i64>) -> Result<usize, LayoutError> {
        if !self.is_active() {
            return Ok(0);
        }
        let mut body = Vec::with_capacity(self.layout().size());
        self.layout().pack(values, &mut body)?;
        self.send(&body, t_ns)
    }
}
