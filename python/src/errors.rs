//! Rust errors -> the exception classes of `stintmate_ipc._errors`.

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use stintmate_ipc::Error;

use crate::convert::from_value;

/// Builds an exception of the named class (or the error raised while building it).
pub fn make<'py>(
    py: Python<'py>,
    name: &str,
    args: impl IntoPyObject<'py, Target = PyTuple, Output = Bound<'py, PyTuple>>,
) -> PyErr {
    let built = py
        .import("stintmate_ipc._errors")
        .and_then(|m| m.getattr(name))
        .and_then(|c| {
            args.into_pyobject(py)
                .map_err(Into::into)
                .and_then(|a| c.call1(a))
        });
    match built {
        Ok(inst) => PyErr::from_value(inst),
        Err(e) => e,
    }
}

/// The exception for an error.
pub fn to_pyerr(py: Python<'_>, e: &Error) -> PyErr {
    let msg = e.to_string();
    match e {
        Error::PipeNotFound(_) => make(py, "PipeNotFound", (2, msg)),
        Error::PipeBusy(_) => make(py, "PipeBusy", (231, msg)),
        Error::EndpointInUse(_) => make(py, "EndpointInUse", (5, msg)),
        Error::Handshake(f) => make(py, "HandshakeError", (f.code.as_str(), f.message.clone())),
        Error::HandshakeTimeout => make(py, "HandshakeError", ("timeout", msg)),
        Error::PeerBye { code, message } => {
            make(py, "HandshakeError", (code.clone(), message.clone()))
        }
        Error::KeyFile(k) => make(py, "KeyFileError", (k.to_string(),)),
        Error::NotConnected => make(py, "NotConnected", (msg,)),
        Error::ConnectionLost { .. } => make(py, "ConnectionLost", (msg,)),
        Error::CallTimeout => make(py, "CallTimeout", (msg,)),
        Error::CallCancelled => make(py, "CallCancelled", (msg,)),
        Error::Remote(r) => {
            let data = match &r.details {
                Some(d) => from_value(py, d)
                    .map(Bound::unbind)
                    .unwrap_or_else(|_| py.None()),
                None => py.None(),
            };
            make(
                py,
                "RemoteError",
                (r.code.clone(), r.message.clone(), data, r.retry_ms),
            )
        }
        Error::TooLarge { .. } | Error::Protocol(_) => make(py, "ProtocolError", (msg,)),
        Error::Encode(_) => pyo3::exceptions::PyTypeError::new_err(msg),
        Error::Invalid(_) => pyo3::exceptions::PyValueError::new_err(msg),
        Error::Io(io) => make(py, "PipeClosed", (io.raw_os_error().unwrap_or(0), msg)),
    }
}

/// An exception as a Python object (for the outcome of a call, for callbacks).
pub fn exc_object(py: Python<'_>, e: &Error) -> Py<PyAny> {
    to_pyerr(py, e).into_value(py).into_any()
}

/// An owned, GIL-free description of a connection error, turned into an exception later.
#[derive(Clone, Debug)]
pub struct ErrSpec {
    class: &'static str,
    errno: Option<i32>,
    code: Option<String>,
    message: String,
}

impl ErrSpec {
    /// Describes `e`.
    pub fn of(e: &Error) -> Self {
        let message = e.to_string();
        let (class, errno, code) = match e {
            Error::PipeNotFound(_) => ("PipeNotFound", Some(2), None),
            Error::PipeBusy(_) => ("PipeBusy", Some(231), None),
            Error::EndpointInUse(_) => ("EndpointInUse", Some(5), None),
            Error::Handshake(f) => ("HandshakeError", None, Some(f.code.as_str().to_owned())),
            Error::HandshakeTimeout => ("HandshakeError", None, Some("timeout".to_owned())),
            Error::PeerBye { code, message } => {
                return Self {
                    class: "HandshakeError",
                    errno: None,
                    code: Some(code.clone()),
                    message: message.clone(),
                };
            }
            Error::KeyFile(_) => ("KeyFileError", None, None),
            Error::NotConnected => ("NotConnected", None, None),
            Error::Io(io) => ("PipeClosed", Some(io.raw_os_error().unwrap_or(0)), None),
            _ => ("IpcError", None, None),
        };
        let message = match e {
            Error::Handshake(f) => f.message.clone(),
            _ => message,
        };
        Self {
            class,
            errno,
            code,
            message,
        }
    }

    /// The exception.
    pub fn to_pyerr(&self, py: Python<'_>) -> PyErr {
        match (&self.code, self.errno) {
            (Some(c), _) => make(py, self.class, (c.clone(), self.message.clone())),
            (None, Some(n)) => make(py, self.class, (n, self.message.clone())),
            (None, None) => make(py, self.class, (self.message.clone(),)),
        }
    }
}
