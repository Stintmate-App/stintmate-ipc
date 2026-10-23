//! `Client`, `connect`, `Peer`, `PendingCall`, subscriptions and the objects they deliver.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyWeakrefMethods, PyWeakrefReference};
use stintmate_ipc::blocking;
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::{Delivery, Error, SubscribeOptions, Value};

use crate::convert::{from_value, to_value};
use crate::errors::{ErrSpec, exc_object, make, to_pyerr};
use crate::keys::{Layout, PeerInfo, key_source};
use crate::support::{attach, enqueue, log_callback_error};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// A wait of "as long as it takes" that no timer overflows on.
const FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);

fn secs(name: &str, v: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(v).map_err(|_| {
        PyValueError::new_err(format!("{name} must be a non-negative number of seconds"))
    })
}

// --------------------------------------------------------------------------- delivered objects

/// One event or state value delivered to a subscriber.
#[pyclass(module = "stintmate_ipc", get_all)]
pub struct Event {
    channel: String,
    seq: u64,
    ts: Option<i64>,
    data: Py<PyAny>,
    snapshot: bool,
}

#[pymethods]
impl Event {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "Event({:?}, seq={}, snapshot={}, data={})",
            self.channel,
            self.seq,
            self.snapshot,
            self.data
                .bind(py)
                .repr()
                .map(|r| r.to_string())
                .unwrap_or_default()
        )
    }
}

/// One stream sample.
#[pyclass(module = "stintmate_ipc", get_all)]
pub struct Sample {
    channel: String,
    seq: u32,
    t_ns: i64,
    dropped: bool,
    body: Py<PyBytes>,
    layout: Py<Layout>,
}

#[pymethods]
impl Sample {
    /// The flat values of the body.
    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyTuple>> {
        self.layout
            .get()
            .unpack_tuple(py, self.body.bind(py).as_bytes())
    }

    /// The values by field name.
    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.layout
            .get()
            .unpack_dict_obj(py, self.body.bind(py).as_bytes())
    }
}

/// The callback the core runs on the connection's lane for each delivery.
fn delivery_callback(cb: Py<PyAny>) -> impl Fn(Delivery) + Send + Sync + 'static {
    let layout: Mutex<Option<Py<Layout>>> = Mutex::new(None);
    move |d: Delivery| {
        attach(|py| {
            let obj = match d {
                Delivery::Event(e) => from_value(py, &e.data).and_then(|data| {
                    Py::new(
                        py,
                        Event {
                            channel: e.channel,
                            seq: e.seq,
                            ts: e.ts,
                            data: data.unbind(),
                            snapshot: e.snapshot,
                        },
                    )
                    .map(Py::into_any)
                }),
                Delivery::Sample(s) => {
                    let cached = lock(&layout).as_ref().map(|l| l.clone_ref(py));
                    let lay = match cached {
                        Some(l) => Ok(l),
                        None => Py::new(py, Layout::from_rust(s.layout().clone())).inspect(|l| {
                            *lock(&layout) = Some(l.clone_ref(py));
                        }),
                    };
                    lay.and_then(|layout| {
                        Py::new(
                            py,
                            Sample {
                                channel: s.channel.clone(),
                                seq: s.seq,
                                t_ns: s.t_ns,
                                dropped: s.dropped,
                                body: PyBytes::new(py, &s.body).unbind(),
                                layout,
                            },
                        )
                        .map(Py::into_any)
                    })
                }
            };
            if let Err(e) = obj.and_then(|o| cb.call1(py, (o,))) {
                log_callback_error(py, "subscription callback failed", e);
            }
        });
    }
}

// --------------------------------------------------------------------------- call options

#[allow(clippy::too_many_arguments)]
fn call_options(
    py: Python<'_>,
    version: u32,
    timeout: Option<f64>,
    idempotency_key: Option<String>,
    on_progress: Option<Py<PyAny>>,
) -> PyResult<CallOptions> {
    let _ = py;
    let mut o = CallOptions::new().version(version);
    o = match timeout {
        Some(t) => o.timeout(secs("timeout", t)?),
        None => o.no_timeout(),
    };
    if let Some(k) = idempotency_key {
        o = o.idempotency_key(k);
    }
    if let Some(cb) = on_progress {
        o = o.on_progress(move |v: Value| {
            let cb = Python::attach(|py| cb.clone_ref(py));
            enqueue(move |py| {
                if let Err(e) = from_value(py, &v).and_then(|a| cb.call1(py, (a,))) {
                    log_callback_error(py, "progress callback failed", e);
                }
            });
        });
    }
    Ok(o)
}

fn params_value(params: Option<&Bound<'_, PyAny>>) -> PyResult<Value> {
    match params {
        None => Ok(Value::Map(Vec::new())),
        Some(p) if p.is_none() => Ok(Value::Map(Vec::new())),
        Some(p) => to_value(p),
    }
}

// --------------------------------------------------------------------------- PendingCall

struct DoneState {
    done: bool,
    callbacks: Vec<Py<PyAny>>,
}

struct Hooks {
    state: Mutex<DoneState>,
    slot: OnceLock<Py<PendingCall>>,
}

struct Shared {
    pc: blocking::PendingCall,
    method: String,
    hooks: Arc<Hooks>,
    outcome: Mutex<Option<Result<Py<PyAny>, Py<PyAny>>>>,
}

/// A request in flight (future-like). `result()` waits and returns or raises.
#[pyclass(module = "stintmate_ipc")]
pub struct PendingCall {
    shared: Arc<Shared>,
}

/// A hook for `CallOptions::on_result` that runs the done callbacks on the callback thread.
type ResultHook = Arc<dyn Fn(&Result<Value, Error>) + Send + Sync>;

fn done_hook(hooks: Arc<Hooks>) -> ResultHook {
    Arc::new(move |_| {
        let callbacks = {
            let mut st = lock(&hooks.state);
            st.done = true;
            std::mem::take(&mut st.callbacks)
        };
        for cb in callbacks {
            let hooks = hooks.clone();
            enqueue(move |py| run_done_callback(py, &hooks, &cb));
        }
    })
}

fn run_done_callback(py: Python<'_>, hooks: &Hooks, cb: &Py<PyAny>) {
    if let Some(obj) = hooks.slot.get()
        && let Err(e) = cb.call1(py, (obj.clone_ref(py),))
    {
        log_callback_error(py, "done callback failed", e);
    }
}

impl PendingCall {
    fn start(
        py: Python<'_>,
        method: &str,
        options: CallOptions,
        send: impl FnOnce(CallOptions) -> Result<blocking::PendingCall, Error> + Send,
    ) -> PyResult<Py<PendingCall>> {
        let hooks = Arc::new(Hooks {
            state: Mutex::new(DoneState {
                done: false,
                callbacks: Vec::new(),
            }),
            slot: OnceLock::new(),
        });
        let mut options = options;
        options.on_result = Some(done_hook(hooks.clone()));
        let pc = py.detach(|| send(options)).map_err(|e| to_pyerr(py, &e))?;
        let obj = Py::new(
            py,
            PendingCall {
                shared: Arc::new(Shared {
                    pc,
                    method: method.to_owned(),
                    hooks: hooks.clone(),
                    outcome: Mutex::new(None),
                }),
            },
        )?;
        let _ = hooks.slot.set(obj.clone_ref(py));
        Ok(obj)
    }

    /// Waits (the interpreter detached) and caches the outcome as Python objects.
    fn outcome(
        &self,
        py: Python<'_>,
        wait: Option<Duration>,
    ) -> Option<Result<Py<PyAny>, Py<PyAny>>> {
        if let Some(o) = lock(&self.shared.outcome).as_ref() {
            return Some(clone_outcome(py, o));
        }
        let sh = self.shared.clone();
        let got = py.detach(move || sh.pc.wait_timeout(wait))?;
        let mut cache = lock(&self.shared.outcome);
        if cache.is_none() {
            *cache = Some(match got {
                Ok(v) => match from_value(py, &v) {
                    Ok(o) => Ok(o.unbind()),
                    Err(e) => Err(e.into_value(py).into_any()),
                },
                Err(e) => Err(exc_object(py, &e)),
            });
        }
        cache.as_ref().map(|o| clone_outcome(py, o))
    }
}

fn clone_outcome(py: Python<'_>, o: &Result<Py<PyAny>, Py<PyAny>>) -> Result<Py<PyAny>, Py<PyAny>> {
    match o {
        Ok(v) => Ok(v.clone_ref(py)),
        Err(e) => Err(e.clone_ref(py)),
    }
}

#[pymethods]
impl PendingCall {
    /// The id of the request.
    #[getter]
    fn id(&self) -> u64 {
        self.shared.pc.id()
    }

    /// The method called.
    #[getter]
    fn method(&self) -> String {
        self.shared.method.clone()
    }

    /// Whether the call has finished (a result, an error, a timeout or a cancel).
    fn done(&self) -> bool {
        lock(&self.shared.hooks.state).done
    }

    /// `cb(call)` once done, on a library thread (or now, on this one, if already done).
    fn add_done_callback(slf: &Bound<'_, Self>, cb: Py<PyAny>) {
        let this = slf.borrow();
        let hooks = this.shared.hooks.clone();
        let mut st = lock(&hooks.state);
        if st.done {
            drop(st);
            run_done_callback(slf.py(), &hooks, &cb);
        } else {
            st.callbacks.push(cb);
        }
    }

    /// The error of the call, or `None` (also while it is pending).
    fn exception(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        if !self.done() {
            return None;
        }
        self.outcome(py, None)?.err()
    }

    /// Gives up: sends `cancel` and fails the call with `CallCancelled`. False when the call
    /// was already done or its response won the race.
    #[pyo3(signature = (reason=""))]
    fn cancel(&self, reason: &str) -> bool {
        let _ = reason;
        self.shared.pc.cancel()
    }

    /// Waits for the response. `timeout` only limits this wait (the call stays pending).
    #[pyo3(signature = (timeout=None))]
    fn result(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let wait = timeout.map(|t| secs("timeout", t)).transpose()?;
        match self.outcome(py, wait) {
            Some(Ok(v)) => Ok(v),
            Some(Err(e)) => Err(PyErr::from_value(e.into_bound(py))),
            None => Err(make(
                py,
                "CallTimeout",
                (format!("{}: no response yet", self.shared.method),),
            )),
        }
    }
}

// --------------------------------------------------------------------------- subscriptions

/// A `threading.Event`-like view of a flag that the library owns: `is_set()` and
/// `wait(timeout)` (the flag is read live; it cannot be set or cleared from Python).
#[pyclass(module = "stintmate_ipc", frozen)]
pub struct FlagEvent {
    get: Box<dyn Fn() -> bool + Send + Sync>,
    /// Blocks until the flag is set or the time given passes (`None`: as long as it takes);
    /// returns whether it is set. Woken by the library, it does not poll.
    wait: Box<dyn Fn(Option<Duration>) -> bool + Send + Sync>,
}

/// Longest sleep of a `FlagEvent.wait` before it looks at the signals (Ctrl+C).
const SIGNAL_SLICE: Duration = Duration::from_millis(500);

#[pymethods]
impl FlagEvent {
    /// Whether the flag is set.
    fn is_set(&self) -> bool {
        (self.get)()
    }

    fn __bool__(&self) -> bool {
        (self.get)()
    }

    /// Waits until the flag is set (at most `timeout` seconds; `None`: as long as it takes);
    /// returns whether it is set.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let end = timeout
            .map(|t| secs("timeout", t))
            .transpose()?
            .map(|d| std::time::Instant::now() + d);
        loop {
            let left = end.map(|e| e.saturating_duration_since(std::time::Instant::now()));
            let slice = left.map_or(SIGNAL_SLICE, |l| l.min(SIGNAL_SLICE));
            if py.detach(|| (self.wait)(Some(slice))) {
                return Ok(true);
            }
            if left.is_some_and(|l| l <= SIGNAL_SLICE) {
                return Ok((self.get)());
            }
            py.check_signals()?;
        }
    }
}

/// A subscription on one connection.
#[pyclass(module = "stintmate_ipc")]
pub struct Subscription {
    inner: stintmate_ipc::Subscription,
    peer: Py<Peer>,
    ended: OnceLock<Py<FlagEvent>>,
}

#[pymethods]
impl Subscription {
    /// The connection it is on.
    #[getter]
    fn peer(&self, py: Python<'_>) -> Py<Peer> {
        self.peer.clone_ref(py)
    }

    /// Set once the subscription has ended (`is_set()`, `wait(timeout)`).
    #[getter]
    fn ended(&self, py: Python<'_>) -> PyResult<Py<FlagEvent>> {
        if let Some(e) = self.ended.get() {
            return Ok(e.clone_ref(py));
        }
        let inner = self.inner.clone();
        let e = Py::new(
            py,
            FlagEvent {
                get: Box::new({
                    let i = inner.clone();
                    move || i.is_ended()
                }),
                wait: Box::new(move |d| blocking::wait_for(inner.wait_ended(), d)),
            },
        )?;
        Ok(self.ended.get_or_init(|| e).clone_ref(py))
    }

    /// The channel.
    #[getter]
    fn channel(&self) -> String {
        self.inner.channel().to_owned()
    }

    /// The stream's `Layout` (`None` for an event or state channel).
    #[getter]
    fn layout(&self) -> Option<Layout> {
        self.inner.layout().map(|l| Layout::from_rust(l.clone()))
    }

    /// Unsubscribes.
    fn close(&self) {
        self.inner.close();
    }
}

/// A subscription that a `Client` keeps across reconnects.
#[pyclass(module = "stintmate_ipc", frozen)]
pub struct ClientSubscription {
    handle: Arc<stintmate_ipc::SubscriptionHandle>,
    channel: String,
    closed: std::sync::atomic::AtomicBool,
    subscribed: OnceLock<Py<FlagEvent>>,
}

#[pymethods]
impl ClientSubscription {
    /// The channel.
    #[getter]
    fn channel(&self) -> String {
        self.channel.clone()
    }

    /// The stream's `Layout` once it is live (`None` for other channels, or until then).
    #[getter]
    fn layout(&self) -> Option<Layout> {
        let sub = self.handle.current()?;
        sub.layout().map(|l| Layout::from_rust(l.clone()))
    }

    /// The error of the last attempt to subscribe, or `None`.
    #[getter]
    fn error(&self, py: Python<'_>) -> Option<PyErr> {
        self.handle
            .error()
            .map(|m| make(py, "RemoteError", (m.clone(), m)))
    }

    /// Whether it is live on the current connection.
    #[getter]
    fn subscribed(&self, py: Python<'_>) -> PyResult<Py<FlagEvent>> {
        if let Some(e) = self.subscribed.get() {
            return Ok(e.clone_ref(py));
        }
        let h = self.handle.clone();
        let e = Py::new(
            py,
            FlagEvent {
                get: Box::new({
                    let h = h.clone();
                    move || h.is_subscribed()
                }),
                wait: Box::new(move |d| blocking::wait_for(h.wait_subscribed(), d)),
            },
        )?;
        Ok(self.subscribed.get_or_init(|| e).clone_ref(py))
    }

    /// Unsubscribes and stops re-subscribing.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.handle.close();
    }
}

#[allow(clippy::too_many_arguments)]
fn subscribe_options(
    version: u32,
    options: Option<&Bound<'_, PyAny>>,
    timeout: f64,
    on_end: Option<Py<PyAny>>,
    sub_slot: Arc<OnceLock<Py<PyAny>>>,
) -> PyResult<SubscribeOptions> {
    let mut o = SubscribeOptions {
        version,
        options: match options {
            Some(x) if !x.is_none() => Some(to_value(x)?),
            _ => None,
        },
        timeout: secs("timeout", timeout)?,
        ..SubscribeOptions::default()
    };
    if let Some(cb) = on_end {
        o.on_end = Some(Arc::new(
            move |_channel: &str, code: &str, message: &str| {
                attach(|py| {
                    let sub = sub_slot
                        .get()
                        .map_or_else(|| py.None(), |s| s.clone_ref(py));
                    if let Err(e) = cb.call1(py, (sub, code, message)) {
                        log_callback_error(py, "on_end callback failed", e);
                    }
                });
            },
        ));
    }
    Ok(o)
}

// --------------------------------------------------------------------------- Peer

/// One connection.
#[pyclass(module = "stintmate_ipc", weakref)]
pub struct Peer {
    inner: blocking::Peer,
    _owner: Option<blocking::Client>,
    closed: OnceLock<Py<FlagEvent>>,
}

/// The live Python `Peer` objects by connection (weak), so that one connection is always the
/// same Python object (`a is b`).
static PEERS: Mutex<Vec<(usize, Py<PyWeakrefReference>)>> = Mutex::new(Vec::new());

impl Peer {
    /// The one Python object of a connection; `owner` keeps the `Client` of `connect()` alive.
    pub(crate) fn obj(
        py: Python<'_>,
        inner: blocking::Peer,
        owner: Option<blocking::Client>,
    ) -> PyResult<Py<Peer>> {
        let key = std::ptr::from_ref(inner.inner().connection().info()) as usize;
        let mut cache = lock(&PEERS);
        if let Some((_, w)) = cache.iter().find(|(k, _)| *k == key)
            && let Some(o) = w.bind(py).upgrade()
            && let Ok(p) = o.cast_into::<Peer>()
        {
            if owner.is_some() {
                let mut b = p.borrow_mut();
                if b._owner.is_none() {
                    b._owner = owner;
                }
            }
            return Ok(p.unbind());
        }
        cache.retain(|(k, w)| *k != key && w.bind(py).upgrade().is_some());
        let obj = Py::new(
            py,
            Peer {
                inner,
                _owner: owner,
                closed: OnceLock::new(),
            },
        )?;
        let weak = PyWeakrefReference::new(obj.bind(py))?;
        cache.push((key, weak.unbind()));
        Ok(obj)
    }
}

#[pymethods]
impl Peer {
    /// Who is on the other end.
    #[getter]
    fn info(&self, py: Python<'_>) -> PyResult<PeerInfo> {
        PeerInfo::from_rust(py, self.inner.info())
    }

    /// The other end's process id, from the operating system (either side).
    #[getter]
    fn pid(&self) -> Option<u32> {
        self.inner.pid()
    }

    /// The protocol version negotiated (2).
    #[getter]
    fn version(&self) -> u32 {
        2
    }

    /// The server's `run` id.
    #[getter]
    fn run(&self) -> String {
        self.inner.inner().connection().info().run.clone()
    }

    /// Whether the connection is up.
    #[getter]
    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    /// Set once the connection is closing or closed (`is_set()`, `wait(timeout)`).
    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<Py<FlagEvent>> {
        if let Some(e) = self.closed.get() {
            return Ok(e.clone_ref(py));
        }
        let conn = self.inner.inner().connection().clone();
        let e = Py::new(
            py,
            FlagEvent {
                get: Box::new({
                    let c = conn.clone();
                    move || c.is_closed()
                }),
                wait: Box::new(move |d| blocking::wait_for(conn.wait_closing(), d)),
            },
        )?;
        Ok(self.closed.get_or_init(|| e).clone_ref(py))
    }

    /// The `bye` code the connection ended with, once it has ended.
    #[getter]
    fn close_code(&self) -> Option<String> {
        self.inner.inner().connection().close_info().map(|c| c.code)
    }

    /// The `bye` text the connection ended with, once it has ended.
    #[getter]
    fn close_message(&self) -> Option<String> {
        self.inner
            .inner()
            .connection()
            .close_info()
            .map(|c| c.message)
    }

    /// What the other end declares now (`stintmate_ipc.decl.Declarations`).
    #[getter]
    fn decl<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        crate::endpoint::declarations_obj(py, &self.inner.decl())
    }

    /// Two `Peer` objects are equal when they are the same connection.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .cast::<Peer>()
            .is_ok_and(|o| o.borrow().inner.inner() == self.inner.inner())
    }

    fn __hash__(&self) -> isize {
        std::ptr::from_ref(self.inner.inner().connection().info()) as isize
    }

    /// Ends the connection with `bye`.
    #[pyo3(signature = (code="normal", message=""))]
    fn close(&self, py: Python<'_>, code: &str, message: &str) {
        let p = self.inner.clone();
        py.detach(|| p.close(code, message));
    }

    /// Calls a method and waits for the result.
    #[pyo3(signature = (method, params=None, *, version=1, timeout=Some(5.0), idempotency_key=None, on_progress=None))]
    fn call(
        &self,
        py: Python<'_>,
        method: &str,
        params: Option<&Bound<'_, PyAny>>,
        version: u32,
        timeout: Option<f64>,
        idempotency_key: Option<String>,
        on_progress: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let value = params_value(params)?;
        let has_progress = on_progress.is_some();
        let opts = call_options(py, version, timeout, idempotency_key, on_progress)?;
        let peer = self.inner.clone();
        let m = method.to_owned();
        let r = py.detach(move || peer.call(&m, value, opts));
        if has_progress {
            crate::support::flush(py);
        }
        match r {
            Ok(v) => Ok(from_value(py, &v)?.unbind()),
            Err(e) => Err(to_pyerr(py, &e)),
        }
    }

    /// Sends a call and returns it in flight.
    #[pyo3(signature = (method, params=None, *, version=1, timeout=Some(5.0), idempotency_key=None, on_progress=None))]
    fn call_async(
        &self,
        py: Python<'_>,
        method: &str,
        params: Option<&Bound<'_, PyAny>>,
        version: u32,
        timeout: Option<f64>,
        idempotency_key: Option<String>,
        on_progress: Option<Py<PyAny>>,
    ) -> PyResult<Py<PendingCall>> {
        let value = params_value(params)?;
        let opts = call_options(py, version, timeout, idempotency_key, on_progress)?;
        let peer = self.inner.clone();
        let m = method.to_owned();
        PendingCall::start(py, method, opts, move |o| peer.call_async(&m, value, o))
    }

    /// Subscribes to a channel of the other end; `callback(Event | Sample)` runs on the
    /// connection's callback lane, in order.
    #[pyo3(signature = (channel, callback, *, version=1, options=None, on_end=None, timeout=5.0))]
    fn subscribe(
        &self,
        py: Python<'_>,
        channel: &str,
        callback: Py<PyAny>,
        version: u32,
        options: Option<&Bound<'_, PyAny>>,
        on_end: Option<Py<PyAny>>,
        timeout: f64,
    ) -> PyResult<Py<Subscription>> {
        let slot = Arc::new(OnceLock::new());
        let opts = subscribe_options(version, options, timeout, on_end, slot.clone())?;
        let peer = self.inner.clone();
        let peer_obj = Peer::obj(py, peer.clone(), None)?;
        let ch = channel.to_owned();
        let cb = delivery_callback(callback);
        let sub = py
            .detach(move || peer.subscribe(&ch, opts, cb))
            .map_err(|e| to_pyerr(py, &e))?;
        let obj = Py::new(
            py,
            Subscription {
                inner: sub,
                peer: peer_obj,
                ended: OnceLock::new(),
            },
        )?;
        let _ = slot.set(obj.clone_ref(py).into_any());
        Ok(obj)
    }

    fn __repr__(&self) -> String {
        format!(
            "Peer({:?}, open={})",
            self.inner.info().app,
            self.inner.is_open()
        )
    }
}

// --------------------------------------------------------------------------- Client

/// Asks the client object's (overridden) `reconnect_delay` for the next delay; on a failure
/// or when the object is gone, the built-in policy decides.
fn policy_call(
    me: &OnceLock<Py<PyWeakrefReference>>,
    code: Option<&str>,
    attempt: u32,
) -> Option<Duration> {
    let builtin = || stintmate_ipc::client::reconnect_delay(code, attempt);
    if me.get().is_none() {
        return builtin();
    }
    Python::attach(|py| {
        let Some(obj) = me.get().and_then(|w| w.bind(py).upgrade()) else {
            return builtin();
        };
        let r = obj
            .call_method1("reconnect_delay", (code, attempt))
            .and_then(|v| {
                if v.is_none() {
                    Ok(None)
                } else {
                    v.extract::<f64>().and_then(|s| {
                        Duration::try_from_secs_f64(s.max(0.0))
                            .map(Some)
                            .map_err(|_| PyValueError::new_err("reconnect_delay: bad seconds"))
                    })
                }
            });
        match r {
            Ok(d) => d,
            Err(e) => {
                log_callback_error(py, "reconnect_delay failed", e);
                builtin()
            }
        }
    })
}

/// A client that stays connected: connects in the background, retries with backoff and
/// re-subscribes after every reconnect.
#[pyclass(extends = crate::endpoint::Node, module = "stintmate_ipc", subclass, weakref)]
pub struct Client {
    /// Where the object is found by the reconnect loop when a subclass overrides
    /// `reconnect_delay` (a weak reference, set when the client starts).
    me: Arc<OnceLock<Py<PyWeakrefReference>>>,
    inner: blocking::Client,
    last_error: Arc<Mutex<Option<ErrSpec>>>,
    on_connect_error: Arc<Mutex<Option<Py<PyAny>>>>,
    /// Live subscriptions: one lives until `close()`, whether or not the caller keeps it.
    subs: Mutex<Vec<Py<ClientSubscription>>>,
}

impl Client {
    fn build(
        py: Python<'_>,
        pipe: &str,
        info: &Bound<'_, PeerInfo>,
        key: &Bound<'_, PyAny>,
        reconnect: bool,
        event_queue: usize,
    ) -> PyResult<Self> {
        let info = info.borrow().to_rust(py)?;
        let key = key_source(key)?;
        let me: Arc<OnceLock<Py<PyWeakrefReference>>> = Arc::default();
        let inner = blocking::Client::with(pipe, info, key, |b| {
            let b = b.reconnect(reconnect).event_queue(event_queue.max(1));
            let me = me.clone();
            b.reconnect_policy(Arc::new(move |code, attempt| {
                policy_call(&me, code, attempt)
            }))
        })
        .map_err(|e| to_pyerr(py, &e))?;
        let last_error: Arc<Mutex<Option<ErrSpec>>> = Arc::default();
        let user_cb: Arc<Mutex<Option<Py<PyAny>>>> = Arc::default();
        {
            let (last, user) = (last_error.clone(), user_cb.clone());
            inner.on_connect_error(move |e: &Error| {
                let spec = ErrSpec::of(e);
                *lock(&last) = Some(spec.clone());
                let cb = lock(&user)
                    .as_ref()
                    .map(|c| Python::attach(|py| c.clone_ref(py)));
                if let Some(cb) = cb {
                    enqueue(move |py| {
                        let exc = spec.to_pyerr(py).into_value(py);
                        if let Err(e) = cb.call1(py, (exc,)) {
                            log_callback_error(py, "on_connect_error callback failed", e);
                        }
                    });
                }
            });
        }
        Ok(Self {
            me,
            inner,
            last_error,
            on_connect_error: user_cb,
            subs: Mutex::default(),
        })
    }

    /// Lets the reconnect loop find this object (only when a subclass overrides
    /// `reconnect_delay`).
    fn arm(slf: &Bound<'_, Self>) -> PyResult<()> {
        let me = slf.borrow().me.clone();
        if me.get().is_none() {
            // A subclass that overrides `reconnect_delay` decides the delays (as in 0.1.4).
            let py = slf.py();
            let base = py.get_type::<Self>().getattr("reconnect_delay")?;
            if !slf.get_type().getattr("reconnect_delay")?.is(&base) {
                let w = PyWeakrefReference::new(slf.as_any())?;
                let _ = me.set(w.unbind());
            }
        }
        Ok(())
    }

    fn wait(&self, py: Python<'_>, wait: Duration, raw: bool) -> PyResult<Py<Peer>> {
        let c = self.inner.clone();
        match py.detach(move || c.wait_connected(wait)) {
            Ok(p) => Peer::obj(py, p, None),
            Err(Error::NotConnected) => {
                // As in 0.1.4: `NotConnected`, with the last connect error as its cause.
                let spec = lock(&self.last_error).clone();
                Err(match spec {
                    Some(s) if raw => s.to_pyerr(py),
                    Some(s) => {
                        let cause = s.to_pyerr(py);
                        let err = make(
                            py,
                            "NotConnected",
                            (format!("not connected: {}", cause.value(py)),),
                        );
                        err.set_cause(py, Some(cause));
                        err
                    }
                    None => make(py, "NotConnected", ("not connected",)),
                })
            }
            Err(e) => Err(to_pyerr(py, &e)),
        }
    }
}

#[pymethods]
impl Client {
    /// `event_queue`: how many events of one subscription may wait for a slow callback before
    /// the subscription ends `overloaded` (and is made again 1 s later); state values coalesce
    /// to the latest and stream samples drop the oldest instead (SPEC 12.4).
    #[new]
    #[pyo3(signature = (pipe, info, key, *, reconnect=true, event_queue=1024))]
    fn new(
        py: Python<'_>,
        pipe: &str,
        info: &Bound<'_, PeerInfo>,
        key: &Bound<'_, PyAny>,
        reconnect: bool,
        event_queue: usize,
    ) -> PyResult<PyClassInitializer<Self>> {
        let client = Self::build(py, pipe, info, key, reconnect, event_queue)?;
        let node = crate::endpoint::Node::new(&client.inner);
        Ok(PyClassInitializer::from(node).add_subclass(client))
    }

    /// Seconds to wait (before jitter) before the next attempt, chosen by the `bye` code that
    /// ended the last connection or attempt (SPEC 15.3); `None` stops reconnecting.
    #[pyo3(signature = (code=None, attempt=0))]
    fn reconnect_delay(&self, code: Option<&str>, attempt: u32) -> Option<f64> {
        stintmate_ipc::client::reconnect_delay(code, attempt).map(|d| d.as_secs_f64())
    }

    /// Connects in the background (once); returns the client.
    fn start(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<Self>> {
        Self::arm(slf.bind(py))?;
        slf.borrow(py).inner.start();
        Ok(slf)
    }

    /// Starts and waits for the first connection.
    #[pyo3(signature = (timeout=5.0))]
    fn connect(slf: &Bound<'_, Self>, py: Python<'_>, timeout: f64) -> PyResult<Py<Peer>> {
        Self::arm(slf)?;
        let this = slf.borrow();
        this.inner.start();
        this.wait(py, secs("timeout", timeout)?, false)
    }

    /// Waits until connected (at most `timeout` seconds; `None`: as long as it takes).
    #[pyo3(signature = (timeout=None))]
    fn wait_connected(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<Peer>> {
        let wait = timeout
            .map(|t| secs("timeout", t))
            .transpose()?
            .unwrap_or(FOREVER);
        self.wait(py, wait, false)
    }

    /// The connection, when it is up.
    #[getter]
    fn peer(&self, py: Python<'_>) -> PyResult<Option<Py<Peer>>> {
        self.inner
            .peer()
            .map(|p| Peer::obj(py, p, None))
            .transpose()
    }

    /// Whether the connection is up.
    #[getter]
    fn connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// The error of the last failed attempt, or `None`.
    #[getter]
    fn last_error(&self, py: Python<'_>) -> Option<PyErr> {
        lock(&self.last_error).as_ref().map(|s| s.to_pyerr(py))
    }

    /// `callback(exception)`, called on a library thread after each failed attempt.
    #[getter]
    fn get_on_connect_error(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        lock(&self.on_connect_error)
            .as_ref()
            .map(|c| c.clone_ref(py))
    }

    #[setter]
    fn set_on_connect_error(&self, cb: Option<Py<PyAny>>) {
        *lock(&self.on_connect_error) = cb;
    }

    /// Calls a method and waits for the result.
    #[pyo3(signature = (method, params=None, *, version=1, timeout=Some(5.0), idempotency_key=None, on_progress=None))]
    fn call(
        &self,
        py: Python<'_>,
        method: &str,
        params: Option<&Bound<'_, PyAny>>,
        version: u32,
        timeout: Option<f64>,
        idempotency_key: Option<String>,
        on_progress: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let value = params_value(params)?;
        let has_progress = on_progress.is_some();
        let opts = call_options(py, version, timeout, idempotency_key, on_progress)?;
        let c = self.inner.clone();
        let m = method.to_owned();
        let r = py.detach(move || c.call_with(&m, value, opts));
        if has_progress {
            crate::support::flush(py);
        }
        match r {
            Ok(v) => Ok(from_value(py, &v)?.unbind()),
            Err(e) => Err(to_pyerr(py, &e)),
        }
    }

    /// Sends a call and returns it in flight.
    #[pyo3(signature = (method, params=None, *, version=1, timeout=Some(5.0), idempotency_key=None, on_progress=None))]
    fn call_async(
        &self,
        py: Python<'_>,
        method: &str,
        params: Option<&Bound<'_, PyAny>>,
        version: u32,
        timeout: Option<f64>,
        idempotency_key: Option<String>,
        on_progress: Option<Py<PyAny>>,
    ) -> PyResult<Py<PendingCall>> {
        let value = params_value(params)?;
        let opts = call_options(py, version, timeout, idempotency_key, on_progress)?;
        let c = self.inner.clone();
        let m = method.to_owned();
        PendingCall::start(py, method, opts, move |o| c.call_async(&m, value, o))
    }

    /// Subscribes now and after every reconnect; `callback(Event | Sample)` runs on the
    /// connection's callback lane, in order.
    #[pyo3(signature = (channel, callback, *, version=1, options=None, on_end=None, timeout=5.0))]
    fn subscribe(
        &self,
        py: Python<'_>,
        channel: &str,
        callback: Py<PyAny>,
        version: u32,
        options: Option<&Bound<'_, PyAny>>,
        on_end: Option<Py<PyAny>>,
        timeout: f64,
    ) -> PyResult<Py<ClientSubscription>> {
        let slot = Arc::new(OnceLock::new());
        let opts = subscribe_options(version, options, timeout, on_end, slot.clone())?;
        let c = self.inner.clone();
        let ch = channel.to_owned();
        let cb = delivery_callback(callback);
        let handle = py.detach(move || c.subscribe(&ch, opts, cb));
        let obj = Py::new(
            py,
            ClientSubscription {
                handle: Arc::new(handle),
                channel: channel.to_owned(),
                closed: false.into(),
                subscribed: OnceLock::new(),
            },
        )?;
        let _ = slot.set(obj.clone_ref(py).into_any());
        let mut subs = lock(&self.subs);
        subs.retain(|s| !s.get().closed.load(Ordering::Acquire));
        subs.push(obj.clone_ref(py));
        drop(subs);
        Ok(obj)
    }

    /// Closes the connection and stops reconnecting.
    fn close(&self, py: Python<'_>) {
        let c = self.inner.clone();
        let subs = std::mem::take(&mut *lock(&self.subs));
        py.detach(move || {
            c.close();
            drop(subs);
        });
    }

    /// As `close()`.
    fn stop(&self, py: Python<'_>) {
        self.close(py);
    }
}

/// Opens one connection (no reconnect): handshake and return the `Peer`.
///
/// `node`: a `Client` made with `reconnect=False` whose methods and channels this connection
/// offers; it becomes that client's connection (its `call`, `connected` and `peer` use it) and
/// the caller owns the reconnection (call `connect` again after the peer closes). `pipe` must
/// then be the client's own pipe; `info` and `key` are the client's.
#[pyfunction]
#[pyo3(signature = (pipe, info, key, *, timeout=5.0, node=None, event_queue=1024))]
#[allow(clippy::too_many_arguments)]
pub fn connect(
    py: Python<'_>,
    pipe: &str,
    info: &Bound<'_, PeerInfo>,
    key: &Bound<'_, PyAny>,
    timeout: f64,
    node: Option<&Bound<'_, PyAny>>,
    event_queue: usize,
) -> PyResult<Py<Peer>> {
    if let Some(n) = node.filter(|n| !n.is_none()) {
        let Ok(c) = n.cast::<Client>() else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "node must be a Client made with reconnect=False",
            ));
        };
        let c = c.borrow();
        c.inner.start();
        return c.wait(py, secs("timeout", timeout)?, true);
    }
    let client = Client::build(py, pipe, info, key, false, event_queue)?;
    client.inner.start();
    match client.wait(py, secs("timeout", timeout)?, true) {
        Ok(p) => {
            let inner = p.borrow(py).inner.clone();
            Peer::obj(py, inner, Some(client.inner.clone()))
        }
        Err(e) => {
            let c = client.inner.clone();
            py.detach(move || c.close());
            Err(e)
        }
    }
}

/// Whether the interpreter may be finalizing: nothing attaches after this is set.
#[pyfunction]
pub fn _shutdown() {
    crate::support::SHUTDOWN.store(true, Ordering::Release);
}
