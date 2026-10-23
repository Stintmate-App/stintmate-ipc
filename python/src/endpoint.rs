//! The endpoint side: `Endpoint`, the node API shared with `Client` (methods, channels,
//! hooks), `Context` and the channels.
//!
//! Handlers run on the core's handler pool (or the connection's lane for `inline`) and attach
//! to the interpreter only while Python code runs; hooks run on the node's hooks thread;
//! nothing here runs Python on a Tokio thread.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyList, PyTuple};
use stintmate_ipc::blocking::{self, PublishOptions, SubscribeRequest as RSubscribeRequest};
use stintmate_ipc::{Context as RContext, Error, MethodOpts, RemoteError, Value};

use crate::client::Peer;
use crate::convert::{from_value, to_value};
use crate::errors::{ErrSpec, to_pyerr};
use crate::keys::{Layout, PeerInfo, key_source};
use crate::support::{attach, enqueue, log_callback_error};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// A wait of "as long as it takes" that no timer overflows on.
const FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);

type Slot = Mutex<Option<Py<PyAny>>>;

fn slot_get(py: Python<'_>, slot: &Slot) -> Option<Py<PyAny>> {
    lock(slot).as_ref().map(|c| c.clone_ref(py))
}

fn slot_set(slot: &Slot, cb: Option<Py<PyAny>>) {
    *lock(slot) = cb.filter(|c| Python::attach(|py| !c.is_none(py)));
}

// --------------------------------------------------------------------------- errors

/// What a handler's exception answers: a `RemoteError` its code, anything else `internal`
/// (logged with its traceback).
fn remote_of(py: Python<'_>, err: PyErr, what: &str) -> RemoteError {
    let value = err.value(py).clone().into_any();
    let is_remote = py
        .import("stintmate_ipc._errors")
        .and_then(|m| m.getattr("RemoteError"))
        .and_then(|c| value.is_instance(&c))
        .unwrap_or(false);
    if is_remote {
        let code = value
            .getattr("code")
            .and_then(|c| c.extract::<String>())
            .unwrap_or_else(|_| "internal".into());
        let message = value
            .getattr("message")
            .and_then(|c| c.extract::<String>())
            .unwrap_or_default();
        let mut e = RemoteError::new(code, message);
        e.details = value
            .getattr("data")
            .ok()
            .filter(|d| !d.is_none())
            .and_then(|d| to_value(&d).ok());
        e.retry_ms = value
            .getattr("retry_ms")
            .ok()
            .and_then(|r| r.extract::<u64>().ok());
        return e;
    }
    let kind = err
        .get_type(py)
        .name()
        .map(|n| n.to_string())
        .unwrap_or_default();
    let text = value.str().map(|s| s.to_string()).unwrap_or_default();
    log_callback_error(py, what, err);
    RemoteError::new("internal", format!("{kind}: {text}"))
}

// --------------------------------------------------------------------------- Context

/// What a method handler gets with its params.
#[pyclass(module = "stintmate_ipc")]
pub struct Context {
    inner: RContext,
    params: Py<PyAny>,
    peer: OnceLock<Py<Peer>>,
}

#[pymethods]
impl Context {
    /// The connection the request arrived on (the same object while this context lives).
    #[getter]
    fn peer(&self, py: Python<'_>) -> PyResult<Py<Peer>> {
        if let Some(p) = self.peer.get() {
            return Ok(p.clone_ref(py));
        }
        let p = Peer::obj(py, blocking::Peer::from(self.inner.peer().clone()), None)?;
        Ok(self.peer.get_or_init(|| p).clone_ref(py))
    }

    /// The caller's id for the request.
    #[getter]
    fn id(&self) -> u64 {
        self.inner.id()
    }

    /// The method name.
    #[getter]
    fn method(&self) -> String {
        self.inner.method().to_owned()
    }

    /// The version asked for.
    #[getter]
    fn version(&self) -> u64 {
        self.inner.version()
    }

    /// The decoded params (also the handler's second argument).
    #[getter]
    fn params(&self, py: Python<'_>) -> Py<PyAny> {
        self.params.clone_ref(py)
    }

    /// The idempotency key, when the caller gave one.
    #[getter]
    fn idempotency_key(&self) -> Option<String> {
        self.inner.idempotency_key().map(str::to_owned)
    }

    /// The caller sent `cancel` or went away: stop and return anything.
    #[getter]
    fn cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Blocks until the request is cancelled (`True`) or `timeout` seconds pass (`False`).
    #[pyo3(signature = (timeout=None))]
    fn wait_cancelled(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let wait = match timeout {
            None => FOREVER,
            Some(t) => Duration::try_from_secs_f64(t)
                .map_err(|_| PyValueError::new_err("timeout must be a non-negative number"))?
                .min(FOREVER),
        };
        let ctx = self.inner.clone();
        Ok(py.detach(move || ctx.wait_cancelled(wait)))
    }

    /// Seconds left before the caller's deadline, or `None` when it set none.
    #[getter]
    fn remaining(&self) -> Option<f64> {
        self.inner.remaining().map(|d| d.as_secs_f64())
    }

    /// Whether the caller's deadline has passed.
    fn expired(&self) -> bool {
        self.inner.expired()
    }

    /// Sends `prog` (a long request is alive); `extend_s` gives the caller a new deadline that
    /// many seconds from now.
    #[pyo3(signature = (data=None, extend_s=None))]
    fn progress(&self, data: Option<&Bound<'_, PyAny>>, extend_s: Option<f64>) -> PyResult<()> {
        let d = match data {
            Some(d) if !d.is_none() => Some(to_value(d)?),
            _ => None,
        };
        let extend = extend_s
            .map(Duration::try_from_secs_f64)
            .transpose()
            .map_err(|_| PyValueError::new_err("extend_s must be a non-negative number"))?;
        self.inner.progress(d, extend);
        Ok(())
    }
}

/// Runs the Python handler for one request, on the calling (pool or lane) thread.
fn run_handler(handler: &Py<PyAny>, ctx: &RContext, params: Value) -> Result<Value, RemoteError> {
    let mut out = None;
    attach(|py| {
        let r = (|| -> Result<Value, RemoteError> {
            let p = from_value(py, &params)
                .map_err(|e| remote_of(py, e, "decoding the params failed"))?;
            let obj = Py::new(
                py,
                Context {
                    inner: ctx.clone(),
                    params: p.clone().unbind(),
                    peer: OnceLock::new(),
                },
            )
            .map_err(|e| remote_of(py, e, "creating the context failed"))?;
            let result = handler
                .call1(py, (obj, p))
                .map_err(|e| remote_of(py, e, "method handler failed"))?;
            to_value(result.bind(py)).map_err(|e| remote_of(py, e, "the handler's result"))
        })();
        out = Some(r);
    });
    out.unwrap_or_else(|| {
        Err(RemoteError::new(
            "shutting_down",
            "the interpreter is closing",
        ))
    })
}

// --------------------------------------------------------------------------- views

/// What an `on_subscribe` hook gets.
#[pyclass(module = "stintmate_ipc", frozen, get_all)]
pub struct SubscribeRequest {
    peer: Py<Peer>,
    version: u32,
    options: Py<PyAny>,
}

/// A read-only view of one subscription to a channel this process owns, for
/// `EventChannel.publish(accept=...)`.
#[pyclass(module = "stintmate_ipc", frozen, get_all)]
pub struct SubscriptionView {
    /// Who subscribed.
    peer: Py<PeerInfo>,
    /// Its process id from the operating system.
    pid: Option<u32>,
    /// The channel version it subscribed to.
    version: u32,
    /// Its `options` map (empty when it sent none; read-only).
    options: Py<PyAny>,
    /// The connection it arrived on: equal (`==`) to the `peer` of a request that came on it.
    connection: Py<Peer>,
}

fn options_obj(py: Python<'_>, v: &Value) -> PyResult<Py<PyAny>> {
    if v.is_nil() {
        return Ok(py.None());
    }
    Ok(from_value(py, v)?.unbind())
}

// --------------------------------------------------------------------------- channels

#[derive(Default)]
struct ChannelHooks {
    on_subscribe: Slot,
    on_active: Slot,
}

macro_rules! channel_class {
    ($name:ident, $rust:ty, $pyname:literal, $kind:literal, { $($extra:tt)* }) => {
        #[doc = concat!("A ", $kind, " channel this process owns.")]
        #[pyclass(module = "stintmate_ipc", name = $pyname)]
        pub struct $name {
            inner: $rust,
            hooks: Arc<ChannelHooks>,
        }

        impl $name {
            fn wrap(node: &blocking::Node, inner: $rust) -> Self {
                let hooks = Arc::new(ChannelHooks::default());
                let h = hooks.clone();
                node.on_subscribe(&inner, move |req: RSubscribeRequest| {
                    let mut out = Ok(());
                    attach(|py| {
                        let Some(cb) = slot_get(py, &h.on_subscribe) else {
                            return;
                        };
                        let r = (|| -> PyResult<()> {
                            let peer = Peer::obj(py, blocking::Peer::from(req.peer), None)?;
                            let obj = SubscribeRequest {
                                peer,
                                version: req.version,
                                options: options_obj(py, &req.options)?,
                            };
                            cb.call1(py, (obj,)).map(|_| ())
                        })();
                        if let Err(e) = r {
                            out = Err(remote_of(py, e, "on_subscribe hook failed"));
                        }
                    });
                    out
                });
                let h = hooks.clone();
                node.on_active(&inner, move |active: bool| {
                    attach(|py| {
                        if let Some(cb) = slot_get(py, &h.on_active)
                            && let Err(e) = cb.call1(py, (active,))
                        {
                            log_callback_error(py, "on_active hook failed", e);
                        }
                    });
                });
                Self { inner, hooks }
            }
        }

        #[pymethods]
        impl $name {
            /// The channel's name.
            #[getter]
            fn name(&self) -> String {
                self.inner.name().to_owned()
            }

            /// `"event"`, `"state"` or `"stream"`.
            #[getter]
            fn kind(&self) -> &'static str {
                $kind
            }

            /// How many subscriptions it has.
            #[getter]
            fn subscriber_count(&self) -> usize {
                self.inner.subscriber_count()
            }

            /// Whether anybody is subscribed.
            #[getter]
            fn active(&self) -> bool {
                self.inner.is_active()
            }

            /// Ends every subscription (`end` with `code`) and withdraws the channel.
            #[pyo3(signature = (code="channel_closed", message=""))]
            fn close(&self, py: Python<'_>, code: &str, message: &str) {
                let ch = self.inner.clone();
                let (code, message) = (code.to_owned(), message.to_owned());
                py.detach(move || ch.close(&code, &message));
            }

            /// `hook(request)` runs before a subscription is accepted (on a pool thread);
            /// raise `RemoteError` to refuse it.
            #[getter]
            fn get_on_subscribe(&self, py: Python<'_>) -> Option<Py<PyAny>> {
                slot_get(py, &self.hooks.on_subscribe)
            }

            #[setter]
            fn set_on_subscribe(&self, cb: Option<Py<PyAny>>) {
                slot_set(&self.hooks.on_subscribe, cb);
            }

            /// `hook(bool)`: the first subscriber came (`True`) or the last one left.
            #[getter]
            fn get_on_active(&self, py: Python<'_>) -> Option<Py<PyAny>> {
                slot_get(py, &self.hooks.on_active)
            }

            #[setter]
            fn set_on_active(&self, cb: Option<Py<PyAny>>) {
                slot_set(&self.hooks.on_active, cb);
            }

            $($extra)*
        }
    };
}

channel_class!(
    EventChannel,
    blocking::EventChannel,
    "EventChannel",
    "event",
    {
        /// Sends `data` to every subscriber (never blocks); `accept(view)` picks subscriptions.
        /// Returns how many it was queued for.
        #[pyo3(signature = (data, *, ts=None, version=None, accept=None))]
        fn publish(
            &self,
            py: Python<'_>,
            data: &Bound<'_, PyAny>,
            ts: Option<i64>,
            version: Option<u32>,
            accept: Option<Py<PyAny>>,
        ) -> PyResult<usize> {
            let v = to_value(data)?;
            let r = match accept {
                None => self.inner.publish_with(&v, PublishOptions { ts, version }),
                Some(cb) => {
                    if ts.is_some() {
                        return Err(PyValueError::new_err("ts cannot be combined with accept"));
                    }
                    self.inner.publish_filtered(&v, |view| {
                        if version.is_some_and(|x| view.version() != x) {
                            return false;
                        }
                        let r = (|| -> PyResult<bool> {
                            let obj = SubscriptionView {
                                peer: Py::new(py, PeerInfo::from_rust(py, view.peer_info())?)?,
                                pid: view.pid(),
                                version: view.version(),
                                options: options_obj(py, view.options())
                                    .and_then(|o| mapping_proxy(py, o))?,
                                connection: Peer::obj(
                                    py,
                                    blocking::Peer::from(view.connection().clone()),
                                    None,
                                )?,
                            };
                            cb.call1(py, (obj,))?.is_truthy(py)
                        })();
                        r.unwrap_or_else(|e| {
                            log_callback_error(py, "accept failed", e);
                            false
                        })
                    })
                }
            };
            r.map_err(|e| to_pyerr(py, &e))
        }
    }
);

fn mapping_proxy(py: Python<'_>, options: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let d = if options.is_none(py) {
        PyDict::new(py).into_any().unbind()
    } else {
        options
    };
    Ok(py
        .import("types")?
        .getattr("MappingProxyType")?
        .call1((d,))?
        .unbind())
}

channel_class!(
    StateChannel,
    blocking::StateChannel,
    "StateChannel",
    "state",
    {
        /// The current value.
        #[getter]
        fn value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            from_value(py, &self.inner.value())
        }

        /// Replaces the value and pushes it (always the whole value, never a delta).
        fn set(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
            self.inner
                .set(&to_value(value)?)
                .map_err(|e| to_pyerr(py, &e))
        }
    }
);

channel_class!(
    StreamChannel,
    blocking::StreamChannel,
    "StreamChannel",
    "stream",
    {
        /// The layout of the samples.
        #[getter]
        fn layout(&self) -> Layout {
            Layout::from_rust(self.inner.layout().clone())
        }

        /// Sends one sample (`body`: the packed layout); returns how many subscribers got it.
        #[pyo3(signature = (body, *, t_ns=None))]
        fn send(&self, body: &[u8], t_ns: Option<i64>) -> PyResult<usize> {
            self.inner
                .send(body, t_ns)
                .map_err(|e| PyValueError::new_err(e.to_string()))
        }
    }
);

// --------------------------------------------------------------------------- Node

/// Hooks a node calls; the attributes of `Endpoint` and `Client`.
#[derive(Default)]
pub struct NodeShared {
    on_connect: Slot,
    on_disconnect: Slot,
    on_redeclare: Slot,
}

fn peer_hook(
    shared: &Arc<NodeShared>,
    pick: fn(&NodeShared) -> &Slot,
) -> impl Fn(blocking::Peer) + Send + Sync + 'static {
    let shared = shared.clone();
    move |p| {
        attach(|py| {
            let Some(cb) = slot_get(py, pick(&shared)) else {
                return;
            };
            let r = Peer::obj(py, p, None).and_then(|peer| cb.call1(py, (peer,)));
            if let Err(e) = r {
                log_callback_error(py, "connection hook failed", e);
            }
        });
    }
}

/// The base of `Endpoint` and `Client`: methods, channels and connection hooks.
#[pyclass(name = "_Node", module = "stintmate_ipc", subclass)]
pub struct Node {
    node: blocking::Node,
    shared: Arc<NodeShared>,
}

impl Node {
    /// Wraps `node` and installs the connection hooks.
    pub fn new(node: &blocking::Node) -> Self {
        let shared = Arc::new(NodeShared::default());
        node.on_connect(peer_hook(&shared, |s| &s.on_connect));
        node.on_disconnect(peer_hook(&shared, |s| &s.on_disconnect));
        node.on_redeclare(peer_hook(&shared, |s| &s.on_redeclare));
        Self {
            node: node.clone(),
            shared,
        }
    }
}

fn versions_of(v: Option<Vec<u32>>) -> Vec<u32> {
    let mut v = v.unwrap_or_else(|| vec![1]);
    v.sort_unstable();
    v.dedup();
    v
}

fn register(
    node: &blocking::Node,
    name: &str,
    opts: MethodOpts,
    handler: Py<PyAny>,
) -> Result<(), Error> {
    node.method(name, opts, move |ctx, params| {
        run_handler(&handler, ctx, params)
    })
}

#[pymethods]
impl Node {
    /// Offers a method; `handler(ctx, params) -> result` runs on a pool thread (`ordered`:
    /// one at a time per connection, in arrival order; `inline`: on the connection's lane,
    /// for handlers that answer from memory and never block). Usable as a decorator. Raise
    /// `RemoteError` from the handler to answer an error.
    #[pyo3(signature = (name, handler=None, *, versions=None, ordered=false, idempotent=false,
        idempotency_cache=true, idempotency_ttl=None, doc=None, params=None, result=None,
        inline=false))]
    fn method(
        &self,
        py: Python<'_>,
        name: &str,
        handler: Option<Py<PyAny>>,
        versions: Option<Vec<u32>>,
        ordered: bool,
        idempotent: bool,
        idempotency_cache: bool,
        idempotency_ttl: Option<f64>,
        doc: Option<String>,
        params: Option<&Bound<'_, PyAny>>,
        result: Option<&Bound<'_, PyAny>>,
        inline: bool,
    ) -> PyResult<Py<PyAny>> {
        let ttl = match idempotency_ttl {
            Some(t) if t > 0.0 => Some(
                Duration::try_from_secs_f64(t)
                    .map_err(|_| PyValueError::new_err("idempotency_ttl is out of range"))?,
            ),
            Some(_) => return Err(PyValueError::new_err("idempotency_ttl must be positive")),
            None => None,
        };
        let schema = |v: Option<&Bound<'_, PyAny>>| -> PyResult<Option<Value>> {
            v.filter(|v| !v.is_none()).map(to_value).transpose()
        };
        let opts = MethodOpts {
            versions: versions_of(versions),
            ordered,
            idempotent,
            idempotency_cache,
            idempotency_ttl: ttl,
            doc,
            params: schema(params)?,
            result: schema(result)?,
            inline,
        };
        let node = self.node.clone();
        let name = name.to_owned();
        match handler {
            Some(h) => {
                let out = h.clone_ref(py);
                let r = py.detach(|| register(&node, &name, opts, h));
                r.map_err(|e| to_pyerr(py, &e))?;
                Ok(out)
            }
            None => {
                let f = PyCFunction::new_closure(
                    py,
                    None,
                    None,
                    move |args: &Bound<'_, PyTuple>, _kw: Option<&Bound<'_, PyDict>>| {
                        let py = args.py();
                        let f = args.get_item(0)?;
                        let (node, name, opts) = (node.clone(), name.clone(), opts.clone());
                        let h = f.clone().unbind();
                        py.detach(|| register(&node, &name, opts, h))
                            .map_err(|e| to_pyerr(py, &e))?;
                        Ok::<_, PyErr>(f.unbind())
                    },
                )?;
                Ok(f.into_any().unbind())
            }
        }
    }

    /// Withdraws a method.
    fn remove_method(&self, py: Python<'_>, name: &str) {
        let node = self.node.clone();
        let name = name.to_owned();
        py.detach(move || node.remove_method(&name));
    }

    /// Declares an event channel.
    #[pyo3(signature = (name, *, versions=None))]
    fn event_channel(
        &self,
        py: Python<'_>,
        name: &str,
        versions: Option<Vec<u32>>,
    ) -> PyResult<EventChannel> {
        let ch = self
            .node
            .event_channel(name, &versions_of(versions))
            .map_err(|e| to_pyerr(py, &e))?;
        Ok(EventChannel::wrap(&self.node, ch))
    }

    /// Declares a state channel with an initial value.
    #[pyo3(signature = (name, initial=None, *, versions=None))]
    fn state_channel(
        &self,
        py: Python<'_>,
        name: &str,
        initial: Option<&Bound<'_, PyAny>>,
        versions: Option<Vec<u32>>,
    ) -> PyResult<StateChannel> {
        let init = initial.map(to_value).transpose()?.unwrap_or(Value::Nil);
        let ch = self
            .node
            .state_channel(name, init, &versions_of(versions))
            .map_err(|e| to_pyerr(py, &e))?;
        Ok(StateChannel::wrap(&self.node, ch))
    }

    /// Declares a stream channel with a layout.
    #[pyo3(signature = (name, layout, *, versions=None, rate_hz=None, queue=None))]
    fn stream_channel(
        &self,
        py: Python<'_>,
        name: &str,
        layout: &Bound<'_, Layout>,
        versions: Option<Vec<u32>>,
        rate_hz: Option<f64>,
        queue: Option<usize>,
    ) -> PyResult<StreamChannel> {
        let ch = self
            .node
            .stream_channel_with(
                name,
                layout.borrow().inner.clone(),
                &versions_of(versions),
                rate_hz,
                queue,
            )
            .map_err(|e| to_pyerr(py, &e))?;
        Ok(StreamChannel::wrap(&self.node, ch))
    }

    /// What this node offers, as `stintmate_ipc.decl.Declarations`.
    fn declarations<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        declarations_obj(py, &self.node.declarations())
    }

    /// The open connections.
    #[getter]
    fn peers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items = self
            .node
            .peers()
            .into_iter()
            .map(|p| Peer::obj(py, p, None))
            .collect::<PyResult<Vec<_>>>()?;
        PyList::new(py, items)
    }

    /// `hook(peer)`: a connection was established.
    #[getter]
    fn get_on_connect(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        slot_get(py, &self.shared.on_connect)
    }

    #[setter]
    fn set_on_connect(&self, cb: Option<Py<PyAny>>) {
        slot_set(&self.shared.on_connect, cb);
    }

    /// `hook(peer)`: a connection ended.
    #[getter]
    fn get_on_disconnect(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        slot_get(py, &self.shared.on_disconnect)
    }

    #[setter]
    fn set_on_disconnect(&self, cb: Option<Py<PyAny>>) {
        slot_set(&self.shared.on_disconnect, cb);
    }

    /// `hook(peer)`: a peer sent `decl` again.
    #[getter]
    fn get_on_redeclare(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        slot_get(py, &self.shared.on_redeclare)
    }

    #[setter]
    fn set_on_redeclare(&self, cb: Option<Py<PyAny>>) {
        slot_set(&self.shared.on_redeclare, cb);
    }
}

/// Declarations as `stintmate_ipc.decl.Declarations`.
pub fn declarations_obj<'py>(
    py: Python<'py>,
    d: &stintmate_ipc::decl::Declarations,
) -> PyResult<Bound<'py, PyAny>> {
    let wire = from_value(py, &d.to_value())?;
    py.import("stintmate_ipc.decl")?
        .getattr("Declarations")?
        .call_method1("from_wire", (wire,))
}

// --------------------------------------------------------------------------- Endpoint

/// The server of one endpoint: creates the pipe, authenticates every client and serves the
/// methods and channels declared on it.
#[pyclass(extends = Node, module = "stintmate_ipc", subclass)]
pub struct Endpoint {
    inner: blocking::Endpoint,
    info: Py<PeerInfo>,
    registry: Option<PathBuf>,
    on_handshake_failed: Arc<Slot>,
}

#[pymethods]
impl Endpoint {
    /// `pipe`: a full path or a short name (default `stintmate-<app>[-<process>]-<user>`);
    /// `registry`: a folder for the endpoint registry entry; `key`: a `KeyFile` or 32 bytes.
    #[new]
    #[pyo3(signature = (info, key, *, pipe=None, registry=None, handshake_timeout=None,
        handler_threads=8, heartbeat_ms=None, event_queue=1024, event_queue_bytes=8_388_608,
        max_inflight=None, features=None))]
    fn new(
        py: Python<'_>,
        info: &Bound<'_, PeerInfo>,
        key: &Bound<'_, PyAny>,
        pipe: Option<String>,
        registry: Option<PathBuf>,
        handshake_timeout: Option<f64>,
        handler_threads: usize,
        heartbeat_ms: Option<u32>,
        event_queue: usize,
        event_queue_bytes: usize,
        max_inflight: Option<usize>,
        features: Option<Vec<String>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let rinfo = info.borrow().to_rust(py)?;
        let mut b = blocking::Endpoint::builder(rinfo, key_source(key)?)
            .handler_threads(handler_threads.max(1))
            .event_queue(event_queue, event_queue_bytes);
        if let Some(p) = pipe {
            b = b.pipe(p);
        }
        if let Some(r) = &registry {
            b = b.registry(r.clone());
        }
        if let Some(t) = handshake_timeout {
            b = b.handshake_timeout(
                Duration::try_from_secs_f64(t)
                    .map_err(|_| PyValueError::new_err("handshake_timeout is out of range"))?,
            );
        }
        if let Some(ms) = heartbeat_ms {
            b = b.heartbeat_ms(ms);
        }
        if let Some(n) = max_inflight {
            b = b.max_inflight(n);
        }
        if let Some(f) = features {
            b = b.features(f);
        }
        let inner = py.detach(move || b.build()).map_err(|e| to_pyerr(py, &e))?;
        let failed: Arc<Slot> = Arc::default();
        {
            let failed = failed.clone();
            inner.on_handshake_failed(move |pid: Option<u32>, err: &Error| {
                let (failed, spec) = (failed.clone(), ErrSpec::of(err));
                enqueue(move |py| {
                    if let Some(cb) = slot_get(py, &failed)
                        && let Err(e) = cb.call1(py, (pid, spec.to_pyerr(py).into_value(py)))
                    {
                        log_callback_error(py, "on_handshake_failed failed", e);
                    }
                });
            });
        }
        let node = Node::new(&inner);
        Ok(PyClassInitializer::from(node).add_subclass(Self {
            inner,
            info: info.clone().unbind(),
            registry,
            on_handshake_failed: failed,
        }))
    }

    /// The pipe path.
    #[getter]
    fn path(&self) -> String {
        self.inner.path().to_owned()
    }

    /// The short name of the pipe.
    #[getter]
    fn short_name(&self) -> String {
        self.inner.short_name().to_owned()
    }

    /// The run id announced in `welcome`.
    #[getter]
    fn run(&self) -> String {
        self.inner.run_id().to_owned()
    }

    /// Who this endpoint is.
    #[getter]
    fn info(&self, py: Python<'_>) -> Py<PeerInfo> {
        self.info.clone_ref(py)
    }

    /// The registry folder, or `None`.
    #[getter]
    fn registry(&self) -> Option<PathBuf> {
        self.registry.clone()
    }

    /// `hook(pid, error)`: a client failed its handshake.
    #[getter]
    fn get_on_handshake_failed(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        slot_get(py, &self.on_handshake_failed)
    }

    #[setter]
    fn set_on_handshake_failed(&self, cb: Option<Py<PyAny>>) {
        slot_set(&self.on_handshake_failed, cb);
    }

    /// Creates the pipe and serves; `EndpointInUse` when another process serves this name.
    fn start(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<Self>> {
        let ep = slf.borrow(py).inner.clone();
        py.detach(move || ep.start())
            .map_err(|e| to_pyerr(py, &e))?;
        Ok(slf)
    }

    /// Stops serving and says `bye` to every peer.
    #[pyo3(signature = (code="shutting_down", message="the endpoint stops"))]
    fn stop(&self, py: Python<'_>, code: &str, message: &str) {
        let ep = self.inner.clone();
        py.detach(move || ep.stop_with(code, message));
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exc))]
    fn __exit__(&self, py: Python<'_>, _exc: &Bound<'_, PyTuple>) {
        self.stop(py, "shutting_down", "");
    }
}
