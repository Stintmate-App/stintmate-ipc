//! A tiny Rust endpoint for the binding's tests (`stintmate_ipc._testing`). Not API.

use std::time::Duration;

use pyo3::prelude::*;
use stintmate_ipc::blocking::{Endpoint, EventChannel, MethodOpts, StateChannel, StreamChannel};
use stintmate_ipc::codec;
use stintmate_ipc::handshake::PeerInfo;
use stintmate_ipc::layout::{FieldType, Layout};
use stintmate_ipc::{KeySource, RemoteError, Value};

use crate::convert::to_value;
use crate::errors::to_pyerr;

/// A running endpoint with a few methods and one channel of each kind.
///
/// Methods: `t.echo` (returns the params), `t.sleep` (`{"ms": n, "progress": bool}`: sleeps
/// in 10 ms steps, reporting `i` per step when asked, fails `cancelled` when cancelled),
/// `t.fail` (a `RemoteError` with data and `retry_ms`), `t.count` (a counter, to test
/// idempotency). Channels: event `t.events`, state `t.state`, stream `t.stream`
/// (`a: u32`, `b: f32 x 2`).
#[pyclass(module = "stintmate_ipc._testing")]
pub struct TestEndpoint {
    ep: Endpoint,
    events: EventChannel,
    state: StateChannel,
    stream: StreamChannel,
}

#[pymethods]
impl TestEndpoint {
    /// The pipe path.
    #[getter]
    fn path(&self) -> String {
        self.ep.path().to_owned()
    }

    /// Publishes an event on `t.events`; returns how many subscribers it was queued for.
    fn publish(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<usize> {
        self.events
            .publish(&to_value(data)?)
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Sets the value of `t.state`.
    fn set_state(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        self.state
            .set(&to_value(data)?)
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Sends one sample on `t.stream`.
    fn send_sample(&self, a: u32, b0: f32, b1: f32) -> PyResult<usize> {
        let mut body = a.to_le_bytes().to_vec();
        body.extend_from_slice(&b0.to_le_bytes());
        body.extend_from_slice(&b1.to_le_bytes());
        self.stream
            .send(&body, None)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Subscribers of `t.events`.
    fn subscriber_count(&self) -> usize {
        self.events.subscriber_count()
    }

    /// Stops serving.
    fn stop(&self, py: Python<'_>) {
        let ep = self.ep.clone();
        py.detach(move || ep.stop());
    }
}

fn ms_of(params: &Value, key: &str, default: u64) -> u64 {
    codec::get(params, key)
        .and_then(Value::as_u64)
        .unwrap_or(default)
}

/// Starts a test endpoint on `pipe` with a 32-byte key.
#[pyfunction]
pub fn start(py: Python<'_>, pipe: &str, key: &[u8]) -> PyResult<TestEndpoint> {
    let k: [u8; 32] = key
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("a key is 32 bytes"))?;
    let pipe = pipe.to_owned();
    let built = py.detach(move || -> Result<TestEndpoint, stintmate_ipc::Error> {
        let ep = Endpoint::builder(PeerInfo::new("t6test"), KeySource::Bytes(k))
            .pipe(pipe)
            .build()?;
        ep.method("t.echo", MethodOpts::default(), |_c, p| Ok(p))?;
        ep.method("t.fail", MethodOpts::default(), |_c, _p| {
            let mut e = RemoteError::new("game_not_running", "AMS2 is not running");
            e.details = Some(Value::Map(vec![(Value::from("sim"), Value::from("ams2"))]));
            e.retry_ms = Some(250);
            Err(e)
        })?;
        ep.method("t.sleep", MethodOpts::default(), |ctx, p| {
            let ms = ms_of(&p, "ms", 1000);
            let progress = codec::get(&p, "progress")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            for i in 0..(ms / 10).max(1) {
                if ctx.wait_cancelled(Duration::from_millis(10)) {
                    return Err(RemoteError::new("cancelled", "cancelled"));
                }
                if progress {
                    ctx.progress(Some(Value::from(i)), None);
                }
            }
            Ok(Value::Map(vec![(Value::from("slept"), Value::from(ms))]))
        })?;
        let counter = std::sync::atomic::AtomicU64::new(0);
        ep.method(
            "t.count",
            MethodOpts {
                idempotent: true,
                ..MethodOpts::default()
            },
            move |_c, _p| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                Ok(Value::from(n))
            },
        )?;
        let events = ep.event_channel("t.events", &[1])?;
        let state = ep.state_channel(
            "t.state",
            Value::Map(vec![(Value::from("n"), Value::from(0))]),
            &[1],
        )?;
        let layout = Layout::new(
            [
                ("a".to_owned(), FieldType::U32, 1),
                ("b".to_owned(), FieldType::F32, 2),
            ],
            Some("t"),
        )
        .map_err(|e| stintmate_ipc::Error::Invalid(e.to_string()))?;
        let stream = ep.stream_channel("t.stream", layout, &[1])?;
        ep.start()?;
        Ok(TestEndpoint {
            ep,
            events,
            state,
            stream,
        })
    });
    built.map_err(|e| to_pyerr(py, &e))
}
