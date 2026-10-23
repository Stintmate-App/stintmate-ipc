//! The server of one endpoint (SPEC 4, 8): creates the pipe, authenticates every client and
//! serves the methods and channels declared on it.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::connection::accept;
use crate::error::Error;
use crate::handshake::{HandshakeFailure, Limits, PeerInfo, ServerConfig, to_frame};
use crate::keyfile::KeySource;
use crate::lane::LaneFactory;
use crate::naming::{self, RegistryEntry};
use crate::node::{Node, NodeConfig};
use crate::transport::{Accepted, PipeListener};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Called with the client's process id and the error when a handshake fails.
pub type HandshakeFailedHook = Arc<dyn Fn(Option<u32>, &Error) + Send + Sync>;

/// Builds an [`Endpoint`].
pub struct EndpointBuilder {
    info: PeerInfo,
    key: KeySource,
    pipe: Option<String>,
    registry: Option<PathBuf>,
    cfg: NodeConfig,
    handle: Option<Handle>,
    lanes: Option<LaneFactory>,
    handler_threads: usize,
}

impl EndpointBuilder {
    /// The pipe: a full `\\.\pipe\...` path or a short name (default
    /// `stintmate-<app>[-<process>]-<user>`).
    pub fn pipe(mut self, pipe: impl Into<String>) -> Self {
        self.pipe = Some(pipe.into());
        self
    }
    /// The folder of the endpoint registry (`<workspace>\ipc\endpoints`).
    pub fn registry(mut self, dir: impl Into<PathBuf>) -> Self {
        self.registry = Some(dir.into());
        self
    }
    /// Size of the handler pool of the blocking façade (default 8; unused by the async API).
    pub fn handler_threads(mut self, n: usize) -> Self {
        self.handler_threads = n.max(1);
        self
    }
    /// The heartbeat announced to clients.
    pub fn heartbeat_ms(mut self, ms: u32) -> Self {
        self.cfg.heartbeat_ms = ms;
        self
    }
    /// Requests in flight per connection before `busy`.
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.cfg.max_inflight = n;
        self
    }
    /// The bounds of an event subscription's queue.
    pub fn event_queue(mut self, events: usize, bytes: usize) -> Self {
        self.cfg.event_queue = events;
        self.cfg.event_queue_bytes = bytes;
        self
    }
    /// `max_frame` and `max_message`.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.cfg.limits = limits;
        self
    }
    /// Features offered (SPEC 8.8).
    pub fn features(mut self, features: Vec<String>) -> Self {
        self.cfg.features = features;
        self
    }
    /// How long a client has to authenticate.
    pub fn handshake_timeout(mut self, t: Duration) -> Self {
        self.cfg.handshake_timeout = t;
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

    /// The configured size of the handler pool.
    pub fn handler_threads_set(&self) -> usize {
        self.handler_threads
    }

    /// Builds the endpoint (it does not listen until [`Endpoint::start`]).
    pub fn build(self) -> Result<Endpoint, Error> {
        let handle = match self.handle {
            Some(h) => h,
            None => Handle::try_current()
                .map_err(|_| Error::Invalid("no Tokio runtime: pass one with runtime()".into()))?,
        };
        let bad = |e: crate::error::NameError| Error::Invalid(e.to_string());
        let (path, short) = match &self.pipe {
            Some(p) => naming::split_pipe(p).map_err(bad)?,
            None => (
                naming::pipe_path(&self.info.app, self.info.process.as_deref()).map_err(bad)?,
                naming::short_name(&self.info.app, self.info.process.as_deref()).map_err(bad)?,
            ),
        };
        let short = naming::ascii_lower(&short);
        let rest = short.strip_prefix("stintmate-").unwrap_or(&short);
        let endpoint_id = match naming::pipe_user() {
            Ok(u) => rest
                .strip_suffix(&format!("-{u}"))
                .unwrap_or(rest)
                .to_owned(),
            Err(_) => rest.to_owned(),
        };
        let node = Node::new(self.info, self.key, handle, self.cfg, self.lanes);
        Ok(Endpoint {
            node,
            inner: Arc::new(Inner {
                path,
                short,
                endpoint_id,
                registry: self.registry,
                run: naming::new_run_id(),
                running: Mutex::new(None),
                on_failed: Mutex::new(None),
            }),
        })
    }
}

struct Running {
    stop: CancellationToken,
    task: JoinHandle<()>,
}

struct Inner {
    path: String,
    short: String,
    endpoint_id: String,
    registry: Option<PathBuf>,
    run: String,
    running: Mutex<Option<Running>>,
    on_failed: Mutex<Option<HandshakeFailedHook>>,
}

/// The server of one endpoint. Cheap to clone; declare methods and channels (through
/// [`Node`]) before or after [`start`](Self::start).
#[derive(Clone)]
pub struct Endpoint {
    node: Node,
    inner: Arc<Inner>,
}

impl Deref for Endpoint {
    type Target = Node;
    fn deref(&self) -> &Node {
        &self.node
    }
}

/// `YYYY-MM-DDTHH:MM:SS+00:00` for now (UTC).
fn iso_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs()) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil from days (H. Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}+00:00",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

impl Endpoint {
    /// Starts a builder.
    pub fn builder(info: PeerInfo, key: KeySource) -> EndpointBuilder {
        EndpointBuilder {
            info,
            key,
            pipe: None,
            registry: None,
            cfg: NodeConfig::default(),
            handle: None,
            lanes: None,
            handler_threads: 8,
        }
    }

    /// The full pipe path.
    pub fn path(&self) -> &str {
        &self.inner.path
    }

    /// The short name bound into the handshake.
    pub fn short_name(&self) -> &str {
        &self.inner.short
    }

    /// The id of this start (`welcome.run`).
    pub fn run_id(&self) -> &str {
        &self.inner.run
    }

    /// Sets the hook called when a client fails its handshake.
    pub fn on_handshake_failed(&self, hook: impl Fn(Option<u32>, &Error) + Send + Sync + 'static) {
        *lock(&self.inner.on_failed) = Some(Arc::new(hook));
    }

    /// Creates the pipe and serves. [`Error::EndpointInUse`] when another process serves this
    /// name.
    pub async fn start(&self) -> Result<(), Error> {
        let mut running = lock(&self.inner.running);
        if running.is_some() {
            return Err(Error::Invalid("the endpoint is already started".into()));
        }
        let listener = PipeListener::bind(&self.inner.path)?;
        if let Some(dir) = &self.inner.registry {
            let n = &self.node.core().info;
            let entry = RegistryEntry {
                endpoint: self.inner.endpoint_id.clone(),
                pipe: self.inner.path.clone(),
                app: n.app.clone(),
                process: n.process.clone(),
                pid: std::process::id(),
                run: self.inner.run.clone(),
                versions: vec![2],
                started_at: iso_now(),
                name: n.name.clone(),
                version: n.version.clone(),
                extra: Default::default(),
            };
            if let Err(e) = naming::register(dir, &entry) {
                warn!("cannot write the registry entry: {e}");
            }
        }
        let stop = CancellationToken::new();
        let task = self
            .node
            .core()
            .handle
            .spawn(accept_loop(self.clone(), listener, stop.clone()));
        *running = Some(Running { stop, task });
        Ok(())
    }

    /// Stops listening, says `bye shutting_down` to every peer and waits briefly for them to
    /// close; withdraws the registry entry.
    pub async fn stop(&self) {
        let running = lock(&self.inner.running).take();
        if let Some(r) = running {
            r.stop.cancel();
            let _ = r.task.await;
        }
        let peers = self.node.peers();
        for p in &peers {
            p.close("shutting_down", "the endpoint stops");
        }
        for p in peers {
            let _ = timeout(Duration::from_secs(2), p.conn.wait_closed()).await;
        }
        if let Some(dir) = &self.inner.registry {
            naming::unregister(dir, &self.inner.endpoint_id, &self.inner.run);
        }
    }
}

async fn accept_loop(ep: Endpoint, mut listener: PipeListener, stop: CancellationToken) {
    loop {
        let accepted = tokio::select! {
            () = stop.cancelled() => break,
            a = listener.accept() => a,
        };
        match accepted {
            Ok(a) => {
                let (ep, stop) = (ep.clone(), stop.clone());
                tokio::spawn(serve(ep, a, stop));
            }
            Err(e) => {
                warn!("accept failed: {e}");
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = sleep(Duration::from_millis(100)) => {}
                }
            }
        }
    }
}

async fn serve(ep: Endpoint, accepted: Accepted, stop: CancellationToken) {
    let core = ep.node.core().clone();
    let pid = accepted.client_pid;
    let key = match core.key.key() {
        Ok(k) => k,
        Err(e) => {
            warn!("no key: {e}");
            let f = HandshakeFailure::unavailable("the server has no key");
            if let Ok(frame) = to_frame(&f.bye()) {
                let mut pipe = accepted.pipe;
                let _ = timeout(Duration::from_millis(500), async {
                    pipe.write_all(&frame).await?;
                    pipe.flush().await
                })
                .await;
            }
            return;
        }
    };
    let (st, handler) = core.new_handler();
    let mut cfg = ServerConfig::new(
        &ep.inner.short,
        core.info.clone(),
        core.handshake_declarations(&st),
    );
    cfg.limits = core.cfg.limits;
    cfg.features = core.cfg.features.clone();
    cfg.heartbeat_ms = core.cfg.heartbeat_ms;
    cfg.run = ep.inner.run.clone();
    match accept(accepted, cfg, key, handler, core.connection_options()).await {
        Ok(conn) => {
            if stop.is_cancelled() {
                conn.close("shutting_down", "the endpoint stops");
                return;
            }
            let peer = st.attach(&conn);
            core.peer_opened(peer);
        }
        Err(e) => {
            info!("handshake with pid {pid:?} failed: {e}");
            let hook = lock(&ep.inner.on_failed).clone();
            if let Some(h) = hook {
                h(pid, &e);
            }
        }
    }
}
