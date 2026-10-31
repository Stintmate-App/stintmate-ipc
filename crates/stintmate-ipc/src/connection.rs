//! A connection after the handshake (SPEC 5, 10, 15): reader and writer tasks, the bounded
//! writer queue, the heartbeat, `bye`, and the table of calls with one completion path
//! ([`crate::calls`]).
//!
//! Sending: when nothing is queued or being written, the sending thread writes its frames to
//! the transport itself (a non-blocking write: no hop to the writer task, no wake-up). When
//! the transport cannot take all of it, or something is already queued, the rest goes through
//! the bounded queue to the writer task, which writes everything queued per wake-up as one
//! write. A counter of queued items, raised under the write lock, keeps one total order per
//! connection (SPEC 13.1): a sender writes directly only while that counter is zero.
//!
//! Everything here is generic over the byte stream, so the same code runs over a named pipe
//! ([`connect`], [`accept`]) and over an in-memory duplex in unit tests. A [`Connection`] is a
//! cheap handle; its tasks keep running until [`Connection::close`] (or the peer, or an error)
//! ends it: call [`Connection::wait_closed`] to wait until every task of it has finished.
//!
//! The nodes ([`node`](crate::node)) build on [`Handler`]: it answers the peer's `req`s and sees the messages
//! the connection does not handle itself (`sub`, `unsub`, `end`, `evt`, `decl`, a `res` nobody
//! waits for) and the stream samples.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, warn};

use crate::calls::{CallEntry, CallResult, CallTable, ProgressFn, ResultHook};
use crate::codec::{self, Value};
use crate::decl::Declarations;
use crate::error::{Error, ProtocolError, RemoteError};
use crate::handshake::{
    ClientConfig, ClientHandshake, HANDSHAKE_TIMEOUT, HandshakeFailure, Limits, PeerInfo,
    ServerConfig, ServerHandshake, bye_message, to_frame,
};
use crate::keyfile::Key;
use crate::transport::open_pipe;
use crate::wire::{self, Frame, FrameReader, StreamSample};

/// A boxed future, as handlers return it.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Timings and bounds of a connection; the defaults are the ones of the SPEC.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// How long a handshake may take (SPEC 15.1).
    pub handshake_timeout: Duration,
    /// Client: how long [`connect`] waits for a free pipe instance (SPEC 3.3).
    pub open_deadline: Duration,
    /// Frames the writer queue holds before senders wait.
    pub queue_capacity: usize,
    /// Close when no write makes progress for this long (SPEC 12.4).
    pub write_timeout: Duration,
    /// How long to wait for the peer to close after our `bye` (SPEC 10.12).
    pub bye_wait: Duration,
    /// Requests of the peer in flight before the connection answers `busy` (SPEC 12.4).
    pub max_inflight: usize,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            handshake_timeout: HANDSHAKE_TIMEOUT,
            open_deadline: Duration::from_secs(5),
            queue_capacity: 1024,
            write_timeout: Duration::from_secs(30),
            bye_wait: Duration::from_secs(1),
            max_inflight: 1024,
        }
    }
}

/// Which side opened the pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Created the pipe and accepted the connection.
    Server,
    /// Opened the pipe.
    Client,
}

/// What the handshake established about a connection.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Which side this is.
    pub role: Role,
    /// The negotiated wire version.
    pub version: u32,
    /// The peer's identity.
    pub peer: PeerInfo,
    /// The peer's declarations.
    pub decl: Declarations,
    /// The features in effect.
    pub features: Vec<String>,
    /// The server's run id (`welcome.run`); on the server side, its own.
    pub run: String,
    /// What this side accepts.
    pub limits: Limits,
    /// What the peer accepts: what this side may send.
    pub peer_limits: Limits,
    /// The heartbeat period of both peers (SPEC 15.2).
    pub heartbeat_ms: u32,
    /// The client's process id from the operating system (server side only, SPEC 3.4).
    pub client_pid: Option<u32>,
    /// The server's process id from the operating system (client side, over a named pipe).
    pub server_pid: Option<u32>,
}

/// Who ended a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClosedBy {
    /// This side (`close`, a protocol error, a heartbeat loss).
    Local,
    /// The peer sent `bye`.
    Peer,
    /// The pipe broke or a write failed.
    Transport,
}

/// Why a connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseInfo {
    /// Who ended it.
    pub by: ClosedBy,
    /// The `bye` code (sent or received), or a local condition (`transport`).
    pub code: String,
    /// The text.
    pub message: String,
}

/// A request of the peer, handed to [`Handler::request`].
pub struct RequestContext {
    /// The connection it arrived on.
    pub conn: Connection,
    /// The caller's id for it.
    pub id: u64,
    /// The method name.
    pub method: String,
    /// The method version (1 when absent).
    pub version: u64,
    /// The params (an empty map when absent).
    pub params: Value,
    /// The caller's deadline from when it sent the request, when it gave one. The connection
    /// does not enforce it: the handler may abandon work that outlives it.
    pub deadline: Option<Duration>,
    /// The idempotency key, when given.
    pub idempotency_key: Option<String>,
    cancel: CancellationToken,
}

impl RequestContext {
    /// Completes when the caller sent `cancel` or the connection is closing. The handler's
    /// result is still sent (unless the connection is closing).
    pub fn cancelled(&self) -> impl Future<Output = ()> + '_ {
        self.cancel.cancelled()
    }

    /// Whether [`cancelled`](Self::cancelled) would complete now.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Sends a `prog` for this request: `d` is a progress report, `dl_ms` a new deadline for
    /// the caller. Advisory: returns false when it could not be queued.
    pub fn progress(&self, d: Option<Value>, dl_ms: Option<u64>) -> bool {
        let mut pairs = vec![("t", Value::from("prog")), ("id", Value::from(self.id))];
        if let Some(d) = d {
            pairs.push(("d", d));
        }
        if let Some(ms) = dl_ms {
            pairs.push(("dl", Value::from(ms)));
        }
        self.conn.try_send_message(&codec::map(pairs)).is_ok()
    }
}

/// What a connection hands its owner: the peer's requests and the messages it does not handle.
pub trait Handler: Send + Sync + 'static {
    /// Answers a `req`. Exactly one `res` is sent with the result (SPEC 10.2); a panic becomes
    /// an `internal` error.
    fn request(&self, ctx: RequestContext) -> BoxFuture<Result<Value, RemoteError>>;

    /// A `sub`, `unsub`, `end`, `evt` or `decl` message, or a `res` for a call nobody waits
    /// for (shape-checked). Runs on the reader task: it must not block.
    fn message(&self, conn: &Connection, message: Value) {
        let _ = (conn, message);
    }

    /// A stream sample (kind 2). Runs on the reader task: it must not block.
    fn stream(&self, conn: &Connection, sample: StreamSample) {
        let _ = (conn, sample);
    }

    /// The connection ended; called once, before pending calls are failed.
    fn closed(&self, info: &CloseInfo) {
        let _ = info;
    }
}

/// A handler that refuses every method with `unknown_method`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoHandler;

impl Handler for NoHandler {
    fn request(&self, ctx: RequestContext) -> BoxFuture<Result<Value, RemoteError>> {
        Box::pin(async move {
            Err(RemoteError::new(
                "unknown_method",
                format!("no method {}", ctx.method),
            ))
        })
    }
}

/// A [`Handler`] made of an async closure over [`RequestContext`].
pub struct FnHandler<F>(pub F);

impl<F, Fut> Handler for FnHandler<F>
where
    F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, RemoteError>> + Send + 'static,
{
    fn request(&self, ctx: RequestContext) -> BoxFuture<Result<Value, RemoteError>> {
        Box::pin((self.0)(ctx))
    }
}

/// Options of one call.
#[derive(Clone)]
pub struct CallOptions {
    /// The method version (`v`).
    pub version: u32,
    /// The deadline, sent as `dl`; `None` waits as long as the connection lives.
    pub timeout: Option<Duration>,
    /// The idempotency key (`ik`, SPEC 10.4).
    pub idempotency_key: Option<String>,
    /// Called with the `d` of each `prog`, on the reader task: it must not block.
    pub on_progress: Option<ProgressFn>,
    /// Whether giving up (deadline, cancel) sends `cancel` to the owner (SPEC 10.3). A `sub`
    /// wait is not cancelled (SPEC 15.1).
    pub send_cancel: bool,
    /// Called on the reader task with the result, before the caller is woken and before the
    /// next frame is handled: a `sub` response registers its subscription here, so the first
    /// `evt` cannot overtake it. Must not block.
    pub on_result: Option<ResultHook>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            version: 1,
            timeout: Some(Duration::from_secs(5)),
            idempotency_key: None,
            on_progress: None,
            send_cancel: true,
            on_result: None,
        }
    }
}

impl CallOptions {
    /// The defaults: version 1, 5 s.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the deadline.
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    /// Waits without a deadline.
    pub fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Sets the method version.
    pub fn version(mut self, v: u32) -> Self {
        self.version = v;
        self
    }

    /// Sets the idempotency key.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Sets the progress callback.
    pub fn on_progress(mut self, f: impl Fn(Value) + Send + Sync + 'static) -> Self {
        self.on_progress = Some(Arc::new(f));
        self
    }
}

/// A call in flight; await it for the result. Dropping it does not cancel the call (its
/// deadline still applies); use [`cancel`](Self::cancel) or a [`CallHandle`].
pub struct PendingCall {
    handle: CallHandle,
    rx: oneshot::Receiver<CallResult>,
}

impl PendingCall {
    /// The id of the request.
    pub fn id(&self) -> u64 {
        self.handle.id
    }

    /// A handle that can cancel the call while this is being awaited.
    pub fn handle(&self) -> CallHandle {
        self.handle.clone()
    }

    /// Gives up: sends `cancel`, fails the call with [`Error::CallCancelled`], ignores the
    /// late `res`. False when the call had already completed (its `res` won the race).
    pub fn cancel(&self) -> bool {
        self.handle.cancel()
    }
}

impl Future for PendingCall {
    type Output = Result<Value, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::ConnectionLost {
                code: None,
                message: "the call was abandoned".into(),
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Cancels one call from anywhere.
#[derive(Clone)]
pub struct CallHandle {
    id: u64,
    conn: Connection,
    send_cancel: bool,
}

impl CallHandle {
    /// See [`PendingCall::cancel`].
    pub fn cancel(&self) -> bool {
        self.conn
            .0
            .give_up(self.id, Error::CallCancelled, self.send_cancel)
    }
}

enum Out {
    /// Whole frames of one message (or one stream sample), written together.
    Frames(Vec<u8>),
    /// The `bye` frame: the last thing written.
    Bye(Vec<u8>),
}

struct Inner {
    info: ConnectionInfo,
    handler: Arc<dyn Handler>,
    opts: ConnectionOptions,
    calls: CallTable,
    next_id: AtomicU64,
    out: mpsc::Sender<Out>,
    /// The write half of the transport, shared by direct writers and the writer task; only
    /// ever held for one non-blocking poll. Dropped when the writer task ends (the pipe
    /// closes with it).
    wr: Mutex<Option<BoxWrite>>,
    /// Items in the writer queue or being written by the writer task (raised under `wr`): a
    /// direct write is allowed only while it is zero.
    queued: AtomicUsize,
    /// Stops the reader, writer and heartbeat; always after `reason` is set.
    abort: CancellationToken,
    /// Set as soon as a close reason is: nothing is sent after it, calls are failed.
    closing: CancellationToken,
    reason: Mutex<Option<CloseInfo>>,
    tracker: TaskTracker,
    inbound: Mutex<HashMap<u64, CancellationToken>>,
    pings: Mutex<HashMap<i64, oneshot::Sender<()>>>,
    ping_seq: AtomicI64,
    start: Instant,
    last_sent: AtomicU64,
    last_recv: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

type BoxWrite = Box<dyn AsyncWrite + Send + Unpin>;

/// What a direct write did.
enum Direct {
    /// Everything is on the transport (or what was left is queued, in order).
    Done,
    /// The writer is busy: nothing was written; the caller queues it.
    Busy,
}

/// An established connection; clone it freely.
#[derive(Clone)]
pub struct Connection(Arc<Inner>);

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("peer", &self.0.info.peer.app)
            .field("closing", &self.0.closing.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Records why the connection ends (the first reason wins), stops new sends, tells the
    /// handler and fails every pending call. False when a reason was already there.
    fn begin_close(&self, info: CloseInfo) -> bool {
        {
            let mut r = lock(&self.reason);
            if r.is_some() {
                return false;
            }
            *r = Some(info.clone());
        }
        self.closing.cancel();
        self.handler.closed(&info);
        let (code, text) = (
            (info.by == ClosedBy::Peer).then(|| info.code.clone()),
            info.message.clone(),
        );
        self.calls.fail_all(|| Error::ConnectionLost {
            code: code.clone(),
            message: text.clone(),
        });
        true
    }

    fn abort_with(&self, by: ClosedBy, code: &str, message: impl Into<String>) {
        self.begin_close(CloseInfo {
            by,
            code: code.to_owned(),
            message: message.into(),
        });
        self.abort.cancel();
    }

    fn lost(&self) -> Error {
        match lock(&self.reason).clone() {
            Some(i) => Error::ConnectionLost {
                code: (i.by == ClosedBy::Peer).then_some(i.code),
                message: i.message,
            },
            None => Error::NotConnected,
        }
    }

    /// Frames for one message, chunked when it exceeds the peer's `max_frame`.
    fn frames_for(&self, message: &Value) -> Result<Vec<u8>, Error> {
        self.frames_for_encoded(&codec::encode(message)?)
    }

    /// As [`frames_for`](Self::frames_for) for a message that is already encoded.
    fn frames_for_encoded(&self, encoded: &[u8]) -> Result<Vec<u8>, Error> {
        let lim = self.info.peer_limits;
        if encoded.len() as u64 > u64::from(lim.max_message) {
            return Err(Error::TooLarge {
                size: encoded.len(),
                max: lim.max_message,
            });
        }
        let mut out = Vec::with_capacity(encoded.len() + 5);
        if encoded.len() < lim.max_frame as usize {
            wire::write_message_frame(&mut out, encoded)?;
        } else {
            for f in wire::chunk_frames(encoded, wire::chunk_data_for(lim.max_frame))? {
                out.extend_from_slice(&f);
            }
        }
        Ok(out)
    }

    /// With the write lock held: writes `frames` directly when nothing is queued, handing
    /// what the transport does not take now to the writer task (there is room: the queue is
    /// empty). Never blocks.
    fn write_direct(&self, w: &mut BoxWrite, frames: &[u8]) -> Result<Direct, Error> {
        if self.closing.is_cancelled() {
            return Err(self.lost());
        }
        if self.queued.load(Ordering::Acquire) != 0 {
            return Ok(Direct::Busy);
        }
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut off = 0;
        while off < frames.len() {
            match Pin::new(&mut *w).poll_write(&mut cx, &frames[off..]) {
                Poll::Ready(Ok(n)) if n > 0 => off += n,
                Poll::Ready(Ok(_)) => return Err(self.write_failed("wrote zero bytes")),
                Poll::Ready(Err(e)) => return Err(self.write_failed(&e.to_string())),
                Poll::Pending => break,
            }
        }
        let rest = if off < frames.len() {
            frames[off..].to_vec()
        } else {
            match Pin::new(&mut *w).poll_flush(&mut cx) {
                Poll::Ready(Ok(())) => {
                    self.last_sent.store(self.now_ms(), Ordering::Relaxed);
                    return Ok(Direct::Done);
                }
                Poll::Ready(Err(e)) => return Err(self.write_failed(&e.to_string())),
                // An empty item: the writer task finishes the flush.
                Poll::Pending => Vec::new(),
            }
        };
        self.queued.fetch_add(1, Ordering::AcqRel);
        if self.out.try_send(Out::Frames(rest)).is_err() {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            return Err(self.lost());
        }
        Ok(Direct::Done)
    }

    fn write_failed(&self, what: &str) -> Error {
        self.abort_with(
            ClosedBy::Transport,
            "transport",
            format!("write failed: {what}"),
        );
        self.lost()
    }

    /// Writes directly, or reserves a place in the queue (then the caller enqueues, or
    /// releases the place when that fails). True when the frames are already written.
    fn direct_or_reserve(&self, frames: &[u8]) -> Result<bool, Error> {
        let mut g = lock(&self.wr);
        let Some(w) = g.as_mut() else {
            return Err(self.lost());
        };
        match self.write_direct(w, frames)? {
            Direct::Done => Ok(true),
            Direct::Busy => {
                self.queued.fetch_add(1, Ordering::AcqRel);
                Ok(false)
            }
        }
    }

    async fn send_frames(&self, frames: Vec<u8>) -> Result<(), Error> {
        if self.direct_or_reserve(&frames)? {
            return Ok(());
        }
        let r = tokio::select! {
            r = self.out.send(Out::Frames(frames)) => r.map_err(|_| self.lost()),
            () = self.closing.cancelled() => Err(self.lost()),
        };
        if r.is_err() {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
        r
    }

    fn try_send_frames(&self, frames: Vec<u8>) -> Result<(), Error> {
        if self.direct_or_reserve(&frames)? {
            return Ok(());
        }
        self.out.try_send(Out::Frames(frames)).map_err(|_| {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            Error::ConnectionLost {
                code: None,
                message: "the writer queue is full".into(),
            }
        })
    }

    /// Runs `f` with direct writes while nothing is queued (under the write lock). `None`
    /// when the writer is busy or the connection is closing.
    fn with_direct<R>(&self, f: impl FnOnce(&mut DirectWriter<'_>) -> R) -> Option<R> {
        let mut g = lock(&self.wr);
        let w = g.as_mut()?;
        if self.closing.is_cancelled() || self.queued.load(Ordering::Acquire) != 0 {
            return None;
        }
        Some(f(&mut DirectWriter { inner: self, w }))
    }

    /// The writer task's writes: everything in `buf`, then a flush, holding the write lock
    /// for one poll at a time.
    async fn write_shared(&self, buf: &[u8]) -> std::io::Result<()> {
        let mut off = 0;
        std::future::poll_fn(|cx| {
            let mut g = lock(&self.wr);
            let Some(w) = g.as_mut() else {
                return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
            };
            while off < buf.len() {
                match Pin::new(&mut *w).poll_write(cx, &buf[off..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into()));
                    }
                    Poll::Ready(Ok(n)) => off += n,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            Pin::new(&mut *w).poll_flush(cx)
        })
        .await
    }

    async fn send_message(&self, message: &Value) -> Result<(), Error> {
        self.send_frames(self.frames_for(message)?).await
    }

    fn stream_frame(
        &self,
        sid: u32,
        seq: u32,
        t_ns: i64,
        flags: u32,
        body: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let mut f = Vec::with_capacity(body.len() + 24);
        wire::write_stream_frame(&mut f, sid, seq, t_ns, flags, body)?;
        if f.len() - 4 > self.info.peer_limits.max_frame as usize {
            return Err(Error::TooLarge {
                size: f.len() - 4,
                max: self.info.peer_limits.max_frame,
            });
        }
        Ok(f)
    }

    fn try_send_message(&self, message: &Value) -> Result<(), Error> {
        self.try_send_frames(self.frames_for(message)?)
    }

    /// Ends the connection with a `bye` (SPEC 10.12): nothing is sent after it; the peer has
    /// `bye_wait` to close, then the pipe is dropped.
    fn close(self: &Arc<Self>, code: &str, message: &str) {
        let began = self.begin_close(CloseInfo {
            by: ClosedBy::Local,
            code: code.to_owned(),
            message: message.to_owned(),
        });
        if !began {
            return;
        }
        let sent = to_frame(&bye_message(code, message)).ok().is_some_and(|f| {
            let _w = lock(&self.wr);
            self.queued.fetch_add(1, Ordering::AcqRel);
            self.out.try_send(Out::Bye(f)).is_ok()
        });
        if !sent {
            self.abort.cancel();
            return;
        }
        let me = self.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                () = me.abort.cancelled() => {}
                () = sleep(me.opts.bye_wait) => me.abort.cancel(),
            }
        });
    }

    fn protocol_error(self: &Arc<Self>, error: &ProtocolError) {
        warn!(peer = %self.info.peer.app, "protocol error: {error}");
        self.close("protocol_error", &error.to_string());
    }

    /// Removes call `id` (if still waiting), completes it with `error`, and tells the owner.
    fn give_up(self: &Arc<Self>, id: u64, error: Error, send_cancel: bool) -> bool {
        if !self.calls.complete(id, Err(error)) {
            return false;
        }
        if send_cancel && !self.closing.is_cancelled() {
            let m = codec::map([("t", Value::from("cancel")), ("id", Value::from(id))]);
            let _ = self.try_send_message(&m);
        }
        true
    }

    fn reply(&self, id: u64, result: Result<Value, RemoteError>) -> Value {
        match result {
            Ok(r) => codec::map([
                ("t", Value::from("res")),
                ("id", Value::from(id)),
                ("ok", Value::Boolean(true)),
                ("r", r),
            ]),
            Err(e) => codec::map([
                ("t", Value::from("res")),
                ("id", Value::from(id)),
                ("ok", Value::Boolean(false)),
                ("e", e.to_value()),
            ]),
        }
    }
}

/// Direct writes from the sending thread, under the connection's write lock
/// ([`Connection::write_direct_with`]).
pub(crate) struct DirectWriter<'a> {
    inner: &'a Inner,
    w: &'a mut BoxWrite,
}

impl DirectWriter<'_> {
    /// Writes one message's (or sample's) frames. False when they did not all go out
    /// directly (the rest is queued, in order, or the connection failed): stop writing here.
    pub(crate) fn write(&mut self, frames: &[u8]) -> bool {
        matches!(self.inner.write_direct(self.w, frames), Ok(Direct::Done))
            && self.inner.queued.load(Ordering::Acquire) == 0
    }

    /// The frames of a message that is already encoded.
    pub(crate) fn frames_for_encoded(&self, encoded: &[u8]) -> Result<Vec<u8>, Error> {
        self.inner.frames_for_encoded(encoded)
    }

    /// The frame of a stream sample.
    pub(crate) fn stream_frame(
        &self,
        sid: u32,
        seq: u32,
        t_ns: i64,
        flags: u32,
        body: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.inner.stream_frame(sid, seq, t_ns, flags, body)
    }
}

impl Connection {
    /// Marks request id `id` as in flight for a `sub` waiting for its `res` (SPEC 10.2). A `sub`
    /// or `req` using an id that is still in flight is a protocol error: it closes the
    /// connection and this returns false.
    pub(crate) fn claim_id(&self, id: u64) -> bool {
        let inner = &self.0;
        let mut inb = lock(&inner.inbound);
        if inb.contains_key(&id) {
            drop(inb);
            inner.protocol_error(&ProtocolError::InvalidDeclaration(format!(
                "id {id} reused while in flight"
            )));
            return false;
        }
        inb.insert(id, inner.closing.child_token());
        true
    }

    /// Frees an id taken with [`claim_id`](Self::claim_id), before its `res` is written.
    pub(crate) fn release_id(&self, id: u64) {
        lock(&self.0.inbound).remove(&id);
    }

    /// Starts the tasks of a connection whose handshake is done.
    fn start<IO>(
        io: IO,
        reader: FrameReader,
        info: ConnectionInfo,
        handler: Arc<dyn Handler>,
        opts: ConnectionOptions,
    ) -> Connection
    where
        IO: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (rd, wr) = tokio::io::split(io);
        let (tx, rx) = mpsc::channel(opts.queue_capacity.max(1));
        let wr: BoxWrite = Box::new(wr);
        let inner = Arc::new(Inner {
            info,
            handler,
            opts,
            calls: CallTable::default(),
            next_id: AtomicU64::new(1),
            out: tx,
            wr: Mutex::new(Some(wr)),
            queued: AtomicUsize::new(0),
            abort: CancellationToken::new(),
            closing: CancellationToken::new(),
            reason: Mutex::new(None),
            tracker: TaskTracker::new(),
            inbound: Mutex::new(HashMap::new()),
            pings: Mutex::new(HashMap::new()),
            ping_seq: AtomicI64::new(1),
            start: Instant::now(),
            last_sent: AtomicU64::new(0),
            last_recv: AtomicU64::new(0),
        });
        inner.tracker.spawn(reader_task(inner.clone(), rd, reader));
        inner.tracker.spawn(writer_task(inner.clone(), rx));
        inner.tracker.spawn(heartbeat_task(inner.clone()));
        Connection(inner)
    }

    /// What the handshake established.
    pub fn info(&self) -> &ConnectionInfo {
        &self.0.info
    }

    /// Whether the connection is closing or closed.
    pub fn is_closed(&self) -> bool {
        self.0.closing.is_cancelled()
    }

    /// Completes once the connection is closing or closed (when [`is_closed`](Self::is_closed)
    /// turns true); does not wait for its tasks, see [`wait_closed`](Self::wait_closed).
    pub async fn wait_closing(&self) {
        self.0.closing.cancelled().await;
    }

    /// Why the connection ended, once it has begun to.
    pub fn close_info(&self) -> Option<CloseInfo> {
        lock(&self.0.reason).clone()
    }

    /// Ends the connection: sends `bye` with `code` and `message`, fails the pending calls
    /// and lets the peer close for up to the configured wait (1 s). Does not block; see
    /// [`wait_closed`](Self::wait_closed). No-op when it is already ending.
    pub fn close(&self, code: &str, message: &str) {
        self.0.close(code, message);
    }

    /// [`close`](Self::close) and wait until every task of the connection has finished.
    pub async fn shutdown(&self, code: &str, message: &str) {
        self.close(code, message);
        self.wait_closed().await;
    }

    /// Waits until the connection has ended and all its tasks (reader, writer, heartbeat,
    /// timers, handlers) have finished and released the pipe; returns why it ended.
    pub async fn wait_closed(&self) -> CloseInfo {
        self.0.abort.cancelled().await;
        self.0.tracker.close();
        self.0.tracker.wait().await;
        self.close_info().expect("a reason is set before abort")
    }

    /// Calls in flight (for tests and diagnostics).
    pub fn pending_calls(&self) -> usize {
        self.0.calls.len()
    }

    /// Sends one message (chunked when the peer's `max_frame` requires it). Waits for room
    /// in the writer queue.
    pub async fn send_message(&self, message: &Value) -> Result<(), Error> {
        self.0.send_message(message).await
    }

    /// Sends one message that is already encoded (a valid SPEC 6.1 message), waiting for room
    /// in the writer queue. The nodes use it to pack a payload once for many subscribers.
    pub async fn send_encoded(&self, encoded: &[u8]) -> Result<(), Error> {
        self.0
            .send_frames(self.0.frames_for_encoded(encoded)?)
            .await
    }

    /// As [`send_message`](Self::send_message) without waiting: fails when the queue is full.
    pub fn try_send_message(&self, message: &Value) -> Result<(), Error> {
        self.0.try_send_message(message)
    }

    /// Sends a stream sample (kind 2), waiting for room in the queue.
    pub async fn send_stream_sample(
        &self,
        sid: u32,
        seq: u32,
        t_ns: i64,
        flags: u32,
        body: &[u8],
    ) -> Result<(), Error> {
        let f = self.0.stream_frame(sid, seq, t_ns, flags, body)?;
        self.0.send_frames(f).await
    }

    /// Writes from this thread while the writer is idle (see the module docs); `None` when it
    /// is busy or the connection is closing. The node's pump uses it to skip its own task.
    pub(crate) fn write_direct_with<R>(
        &self,
        f: impl FnOnce(&mut DirectWriter<'_>) -> R,
    ) -> Option<R> {
        self.0.with_direct(f)
    }

    /// Calls `method` with `params` and waits for the result.
    pub async fn call(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<Value, Error> {
        self.call_async(method, params, options).await?.await
    }

    /// Sends the request and returns the call in flight.
    pub async fn call_async(
        &self,
        method: &str,
        params: Value,
        options: CallOptions,
    ) -> Result<PendingCall, Error> {
        let (method, ik, v, dl) = (
            method.to_owned(),
            options.idempotency_key.clone(),
            options.version,
            options.timeout.map(|t| t.as_millis().max(1) as u64),
        );
        self.start_call(
            move |id| {
                let mut pairs = vec![
                    ("t", Value::from("req")),
                    ("id", Value::from(id)),
                    ("m", Value::from(method)),
                    ("v", Value::from(v)),
                    ("p", params),
                ];
                if let Some(dl) = dl {
                    pairs.push(("dl", Value::from(dl)));
                }
                if let Some(ik) = ik {
                    pairs.push(("ik", Value::from(ik)));
                }
                codec::map(pairs)
            },
            options,
        )
        .await
    }

    /// The general form: registers a call, builds its message from the id (`req`, `sub`, ...)
    /// and sends it. The `res` with that id completes it with `r`, or [`Error::Remote`].
    pub async fn start_call(
        &self,
        build: impl FnOnce(u64) -> Value,
        options: CallOptions,
    ) -> Result<PendingCall, Error> {
        let inner = &self.0;
        if inner.closing.is_cancelled() {
            return Err(inner.lost());
        }
        let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let done = CancellationToken::new();
        let (dtx, drx) = match options.timeout {
            Some(t) => {
                let (a, b) = watch::channel(tokio::time::Instant::now() + t);
                (Some(a), Some(b))
            }
            None => (None, None),
        };
        let entry = CallEntry::new(tx, options.on_progress.clone(), dtx, done.clone())
            .with_hook(options.on_result.clone());
        if inner.calls.insert(id, entry).is_err() {
            return Err(inner.lost());
        }
        // The request goes out first: the deadline's task starts off the critical path (it
        // sees `done` if the response already came).
        if let Err(e) = inner.send_message(&build(id)).await {
            inner.calls.complete(id, Err(Error::NotConnected));
            return Err(e);
        }
        if let Some(mut drx) = drx {
            let me = inner.clone();
            let send_cancel = options.send_cancel;
            inner.tracker.spawn(async move {
                loop {
                    let at = *drx.borrow_and_update();
                    tokio::select! {
                        () = done.cancelled() => return,
                        () = sleep_until(at) => {
                            me.give_up(id, Error::CallTimeout, send_cancel);
                            return;
                        }
                        changed = drx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
        Ok(PendingCall {
            handle: CallHandle {
                id,
                conn: self.clone(),
                send_cancel: options.send_cancel,
            },
            rx,
        })
    }

    /// Sends `ping` and waits for the `pong`; returns the round trip.
    pub async fn ping(&self, wait: Duration) -> Result<Duration, Error> {
        let inner = &self.0;
        let ts = inner.ping_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        lock(&inner.pings).insert(ts, tx);
        let start = Instant::now();
        let m = codec::map([("t", Value::from("ping")), ("ts", Value::from(ts))]);
        let result = async {
            inner.send_message(&m).await?;
            match timeout(wait, rx).await {
                Ok(Ok(())) => Ok(start.elapsed()),
                Ok(Err(_)) => Err(inner.lost()),
                Err(_) => Err(Error::CallTimeout),
            }
        }
        .await;
        lock(&inner.pings).remove(&ts);
        result
    }
}

// ----------------------------------------------------------------------------- tasks

async fn reader_task<IO: AsyncRead + Send + 'static>(
    inner: Arc<Inner>,
    mut rd: ReadHalf<IO>,
    mut frames: FrameReader,
) {
    frames.set_limits(inner.info.limits.max_frame, inner.info.limits.max_message);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if !inner.closing.is_cancelled() {
            loop {
                match frames.next_frame() {
                    Ok(Some(f)) => handle_frame(&inner, f),
                    Ok(None) => break,
                    Err(e) => {
                        inner.protocol_error(&e);
                        break;
                    }
                }
                if inner.closing.is_cancelled() {
                    break;
                }
            }
        }
        let n = tokio::select! {
            () = inner.abort.cancelled() => return,
            r = rd.read(&mut buf) => r,
        };
        match n {
            Ok(0) => {
                inner.abort_with(ClosedBy::Transport, "transport", "the peer closed the pipe");
                return;
            }
            Ok(n) => {
                inner.last_recv.store(inner.now_ms(), Ordering::Relaxed);
                if !inner.closing.is_cancelled() {
                    frames.push(&buf[..n]);
                }
            }
            Err(e) => {
                inner.abort_with(
                    ClosedBy::Transport,
                    "transport",
                    format!("read failed: {e}"),
                );
                return;
            }
        }
    }
}

/// Writes what the direct path could not: everything queued per wake-up, as one write.
async fn writer_task(inner: Arc<Inner>, mut rx: mpsc::Receiver<Out>) {
    const BATCH: usize = 64 * 1024;
    /// Drops the write half when the task ends, however it ends.
    struct Release<'a>(&'a Inner);
    impl Drop for Release<'_> {
        fn drop(&mut self) {
            lock(&self.0.wr).take();
        }
    }
    let _release = Release(&inner);
    let mut buf = Vec::new();
    loop {
        let item = tokio::select! {
            biased;
            () = inner.abort.cancelled() => return,
            i = rx.recv() => i,
        };
        let mut bye = None;
        let mut n = 0;
        buf.clear();
        match item {
            None => return,
            Some(Out::Frames(b)) => {
                buf.extend_from_slice(&b);
                n += 1;
                while buf.len() < BATCH {
                    match rx.try_recv() {
                        Ok(Out::Frames(b)) => {
                            buf.extend_from_slice(&b);
                            n += 1;
                        }
                        Ok(Out::Bye(b)) => {
                            bye = Some(b);
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            Some(Out::Bye(b)) => bye = Some(b),
        }
        if n > 0 {
            match timeout(inner.opts.write_timeout, inner.write_shared(&buf)).await {
                Ok(Ok(())) => {
                    inner.last_sent.store(inner.now_ms(), Ordering::Relaxed);
                    inner.queued.fetch_sub(n, Ordering::AcqRel);
                }
                Ok(Err(e)) => {
                    inner.abort_with(
                        ClosedBy::Transport,
                        "transport",
                        format!("write failed: {e}"),
                    );
                    return;
                }
                Err(_) => {
                    inner.abort_with(
                        ClosedBy::Transport,
                        "overloaded",
                        format!(
                            "no write progress for {} s",
                            inner.opts.write_timeout.as_secs_f32()
                        ),
                    );
                    return;
                }
            }
        }
        if let Some(b) = bye {
            let _ = timeout(inner.opts.bye_wait, inner.write_shared(&b)).await;
            // Nothing else is sent; the pipe is dropped when the peer closes or the wait of
            // `close` ends.
            inner.abort.cancelled().await;
            return;
        }
    }
}

async fn heartbeat_task(inner: Arc<Inner>) {
    let h = Duration::from_millis(u64::from(inner.info.heartbeat_ms).max(1));
    let tick = (h / 4).clamp(Duration::from_millis(5), Duration::from_secs(1));
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let ping = to_frame(&codec::map([("t", Value::from("ping"))])).expect("ping encodes");
    loop {
        tokio::select! {
            () = inner.closing.cancelled() => return,
            _ = interval.tick() => {}
        }
        let now = inner.now_ms();
        let quiet_recv = now.saturating_sub(inner.last_recv.load(Ordering::Relaxed));
        if quiet_recv >= (h * 3).as_millis() as u64 {
            warn!(peer = %inner.info.peer.app, "nothing received for {quiet_recv} ms: closing");
            inner.close("timeout", "heartbeat lost");
            return;
        }
        let quiet_sent = now.saturating_sub(inner.last_sent.load(Ordering::Relaxed));
        if quiet_sent >= h.as_millis() as u64 {
            // A full queue means the writer is busy anyway.
            let _ = inner.try_send_frames(ping.clone());
        }
    }
}

// ----------------------------------------------------------------------------- incoming

fn handle_frame(inner: &Arc<Inner>, frame: Frame) {
    let conn = || Connection(inner.clone());
    let bytes = match frame {
        Frame::Stream(s) => {
            inner.handler.stream(&conn(), s);
            return;
        }
        Frame::Message(b) => b,
    };
    let msg = match codec::decode_message(&bytes).and_then(|m| {
        codec::validate_message(&m)?;
        Ok(m)
    }) {
        Ok(m) => m,
        Err(e) => {
            inner.protocol_error(&e);
            return;
        }
    };
    let t = codec::message_type(&msg).unwrap_or("").to_owned();
    let uint = |k: &str| codec::get(&msg, k).and_then(Value::as_u64);
    match t.as_str() {
        "req" => handle_req(inner, &msg),
        "res" => {
            let id = uint("id").unwrap_or(0);
            let result = if codec::get(&msg, "ok").and_then(Value::as_bool) == Some(true) {
                Ok(codec::get(&msg, "r").cloned().unwrap_or(Value::Nil))
            } else {
                Err(Error::Remote(codec::get(&msg, "e").map_or_else(
                    || RemoteError::new("internal", ""),
                    RemoteError::from_value,
                )))
            };
            if !inner.calls.complete(id, result) {
                // Late or unknown: ignored (SPEC 10.2); the owner may want to see it.
                inner.handler.message(&conn(), msg);
            }
        }
        "prog" => {
            let id = uint("id").unwrap_or(0);
            let dl = uint("dl").filter(|&d| d > 0);
            if let Some(Some(cb)) = inner.calls.on_progress(id, dl) {
                cb(codec::get(&msg, "d").cloned().unwrap_or(Value::Nil));
            }
        }
        "cancel" => {
            if let Some(tok) = lock(&inner.inbound).get(&uint("id").unwrap_or(0)) {
                tok.cancel();
            }
        }
        "ping" => {
            let mut pairs = vec![("t", Value::from("pong"))];
            if let Some(ts) = codec::get(&msg, "ts").filter(|v| !v.is_nil()) {
                pairs.push(("ts", ts.clone()));
            }
            let _ = inner.try_send_message(&codec::map(pairs));
        }
        "pong" => {
            if let Some(ts) = codec::get(&msg, "ts").and_then(Value::as_i64)
                && let Some(tx) = lock(&inner.pings).remove(&ts)
            {
                let _ = tx.send(());
            }
        }
        "bye" => {
            let text = |k| {
                codec::get(&msg, k)
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned()
            };
            let code = if text("code").is_empty() {
                "normal".to_owned()
            } else {
                text("code")
            };
            debug!(peer = %inner.info.peer.app, %code, "bye received");
            inner.abort_with(ClosedBy::Peer, &code, text("msg"));
        }
        "sub" | "unsub" | "end" | "evt" | "decl" => inner.handler.message(&conn(), msg),
        _ => {} // unknown types are ignored (SPEC 6.3)
    }
}

fn handle_req(inner: &Arc<Inner>, msg: &Value) {
    let id = codec::get(msg, "id").and_then(Value::as_u64).unwrap_or(0);
    let cancel = inner.closing.child_token();
    {
        let mut inb = lock(&inner.inbound);
        if inb.contains_key(&id) {
            drop(inb);
            inner.protocol_error(&ProtocolError::InvalidDeclaration(format!(
                "request id {id} reused while in flight"
            )));
            return;
        }
        if inb.len() >= inner.opts.max_inflight {
            drop(inb);
            let mut e = RemoteError::new("busy", "too many requests in flight");
            e.retry_ms = Some(100);
            let _ = inner.try_send_message(&inner.reply(id, Err(e)));
            return;
        }
        inb.insert(id, cancel.clone());
    }
    let get = |k: &str| codec::get(msg, k).filter(|v| !v.is_nil());
    let ctx = RequestContext {
        conn: Connection(inner.clone()),
        id,
        method: get("m").and_then(Value::as_str).unwrap_or("").to_owned(),
        version: get("v").and_then(Value::as_u64).unwrap_or(1),
        params: get("p").cloned().unwrap_or_else(|| Value::Map(Vec::new())),
        deadline: get("dl").and_then(Value::as_u64).map(Duration::from_millis),
        idempotency_key: get("ik").and_then(Value::as_str).map(str::to_owned),
        cancel,
    };
    // Called here, in arrival order, so an `ordered` method can queue itself in order.
    let request = inner.handler.request(ctx);
    let me = inner.clone();
    // One task per request: the handler's future is polled here, a panic caught per poll
    // (a second spawned task per request cost about 2 us of every round trip).
    inner.tracker.spawn(async move {
        let outcome = tokio::select! {
            r = CatchUnwind(request) => r,
            () = me.abort.cancelled() => {
                lock(&me.inbound).remove(&id);
                return;
            }
        };
        let result = outcome.unwrap_or_else(|()| {
            warn!("handler of request {id} panicked");
            Err(RemoteError::new("internal", "the method failed"))
        });
        lock(&me.inbound).remove(&id);
        if let Err(e) = me.send_message(&me.reply(id, result)).await {
            debug!("res {id} not sent: {e}");
        }
    });
}

/// Polls a future, turning a panic in a poll into `Err(())`.
struct CatchUnwind<F>(F);

impl<F: Future + Unpin> Future for CatchUnwind<F> {
    type Output = Result<F::Output, ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = &mut self.0;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Pin::new(f).poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

// ----------------------------------------------------------------------------- handshake

/// Reads the next message of the handshake; a `bye` is [`Error::PeerBye`].
async fn read_message<IO: AsyncRead + Unpin>(
    io: &mut IO,
    reader: &mut FrameReader,
) -> Result<Value, Error> {
    let mut buf = [0u8; 4096];
    loop {
        match reader.next_frame()? {
            Some(Frame::Message(b)) => {
                let m = codec::decode_message(&b)?;
                if codec::message_type(&m)? == "bye" {
                    let text = |k| {
                        codec::get(&m, k)
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_owned()
                    };
                    return Err(Error::PeerBye {
                        code: text("code"),
                        message: text("msg"),
                    });
                }
                return Ok(m);
            }
            Some(Frame::Stream(_)) => {
                return Err(ProtocolError::InvalidKind(wire::KIND_STREAM).into());
            }
            None => {}
        }
        let n = io.read(&mut buf).await?;
        if n == 0 {
            return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
        }
        reader.push(&buf[..n]);
    }
}

async fn write_message<IO: AsyncWrite + Unpin>(io: &mut IO, m: &Value) -> Result<(), Error> {
    io.write_all(&to_frame(m)?).await?;
    io.flush().await?;
    Ok(())
}

/// Ends a failed handshake: the `bye` goes out (best effort, briefly) unless the peer's own
/// `bye` was the cause.
async fn fail<IO: AsyncWrite + Unpin>(io: &mut IO, error: Error) -> Error {
    let failure = match error {
        Error::Protocol(p) => HandshakeFailure::from(p),
        Error::Handshake(f) => f,
        other => return other,
    };
    let _ = timeout(
        Duration::from_millis(500),
        write_message(io, &failure.bye()),
    )
    .await;
    Error::Handshake(failure)
}

/// Runs the server's side of the handshake on `io` (the pipe of an accepted client) and starts
/// the connection. `client_pid` is [`Accepted::client_pid`](crate::transport::Accepted).
pub async fn accept_io<IO>(
    mut io: IO,
    client_pid: Option<u32>,
    config: ServerConfig,
    key: Key,
    handler: Arc<dyn Handler>,
    opts: ConnectionOptions,
) -> Result<Connection, Error>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let limits = config.limits;
    let (heartbeat_ms, run) = (config.heartbeat_ms, config.run.clone());
    let hs = ServerHandshake::new(config, key, None);
    let mut reader = FrameReader::for_handshake();
    let step = timeout(opts.handshake_timeout, async {
        write_message(&mut io, &hs.hello()).await?;
        let auth = read_message(&mut io, &mut reader).await?;
        hs.on_auth(&auth).map_err(Error::Handshake)
    })
    .await;
    let accepted = match step {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => return Err(fail(&mut io, e).await),
        Err(_) => {
            let _ = timeout(
                Duration::from_millis(500),
                write_message(&mut io, &bye_message("timeout", "no auth in time")),
            )
            .await;
            return Err(Error::HandshakeTimeout);
        }
    };
    write_message(&mut io, &accepted.welcome).await?;
    let info = ConnectionInfo {
        role: Role::Server,
        version: accepted.version,
        peer: accepted.peer,
        decl: accepted.decl,
        features: accepted.features,
        run,
        limits,
        peer_limits: accepted.peer_limits,
        heartbeat_ms,
        client_pid,
        server_pid: None,
    };
    Ok(Connection::start(io, reader, info, handler, opts))
}

/// Runs the client's side of the handshake on `io` and starts the connection.
pub async fn connect_io<IO>(
    io: IO,
    config: ClientConfig,
    key: Key,
    handler: Arc<dyn Handler>,
    opts: ConnectionOptions,
) -> Result<Connection, Error>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    connect_io_from(io, None, config, key, handler, opts).await
}

/// [`connect_io`] knowing the server's process id.
async fn connect_io_from<IO>(
    mut io: IO,
    server_pid: Option<u32>,
    config: ClientConfig,
    key: Key,
    handler: Arc<dyn Handler>,
    opts: ConnectionOptions,
) -> Result<Connection, Error>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let limits = config.limits;
    let mut hs = ClientHandshake::new(config, key);
    let mut reader = FrameReader::for_handshake();
    let step = timeout(opts.handshake_timeout, async {
        let hello = read_message(&mut io, &mut reader).await?;
        let auth = hs.on_hello(&hello).map_err(Error::Handshake)?;
        write_message(&mut io, &auth).await?;
        let welcome = read_message(&mut io, &mut reader).await?;
        hs.on_welcome(&welcome).map_err(Error::Handshake)
    })
    .await;
    let est = match step {
        Ok(Ok(e)) => e,
        Ok(Err(e)) => return Err(fail(&mut io, e).await),
        Err(_) => return Err(Error::HandshakeTimeout),
    };
    let info = ConnectionInfo {
        role: Role::Client,
        version: est.version,
        peer: est.peer,
        decl: est.decl,
        features: est.features,
        run: est.run,
        limits,
        peer_limits: est.peer_limits,
        heartbeat_ms: est.heartbeat_ms,
        client_pid: None,
        server_pid,
    };
    Ok(Connection::start(io, reader, info, handler, opts))
}

/// Opens the pipe `path` (SPEC 3.3) and connects to the endpoint behind it.
pub async fn connect(
    path: &str,
    config: ClientConfig,
    key: Key,
    handler: Arc<dyn Handler>,
    opts: ConnectionOptions,
) -> Result<Connection, Error> {
    let pipe = open_pipe(path, opts.open_deadline).await?;
    let pid = crate::transport::server_pid(&pipe);
    connect_io_from(pipe, pid, config, key, handler, opts).await
}

/// Serves an accepted client: [`accept_io`] on its pipe.
pub async fn accept(
    accepted: crate::transport::Accepted,
    config: ServerConfig,
    key: Key,
    handler: Arc<dyn Handler>,
    opts: ConnectionOptions,
) -> Result<Connection, Error> {
    accept_io(
        accepted.pipe,
        accepted.client_pid,
        config,
        key,
        handler,
        opts,
    )
    .await
}
