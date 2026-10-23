//! The native module `stintmate_ipc._native`: the Python binding of the Rust implementation
//! (PRY-085 T6: values, errors, keys, naming, layouts, the client side; T7 adds the endpoint).
//!
#![allow(clippy::too_many_arguments)] // PyO3 methods take their Python keyword arguments

//! Rules: foreign code never runs on a Tokio thread; blocking calls release the interpreter
//! (`Python::detach`); library threads attach per callback and never while holding a lock.

mod client;
mod convert;
mod direct;
mod endpoint;
mod errors;
mod keys;
mod support;
mod testing;

use pyo3::prelude::*;

/// The library's version.
#[pyfunction]
fn version() -> &'static str {
    stintmate_ipc::VERSION
}

/// The revision of SPEC.md the library follows.
#[pyfunction]
fn spec_revision() -> &'static str {
    stintmate_ipc::SPEC_REVISION
}

/// Runs the conformance tool (`stintmate-ipc-check`) with `argv`; returns its exit status. The
/// interpreter is released while the checks run.
#[pyfunction]
fn check_main(py: Python<'_>, argv: Vec<String>) -> i32 {
    py.detach(|| stintmate_ipc_check::cli::main(&argv))
}

/// Re-reads the level of the `stintmate_ipc` logger (call after changing it).
#[pyfunction]
fn refresh_log_level(py: Python<'_>) {
    support::refresh_log_level(py);
}

/// Encodes a value as MessagePack (direct from Python objects).
#[pyfunction]
fn _encode<'py>(
    py: Python<'py>,
    obj: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
    Ok(pyo3::types::PyBytes::new(py, &direct::encode(obj)?))
}

/// Decodes MessagePack (direct to Python objects, validating the SPEC 6.1 subset).
#[pyfunction]
fn _decode<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    direct::decode(py, data)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    support::init_logging(py);
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(spec_revision, m)?)?;
    m.add_function(wrap_pyfunction!(refresh_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(check_main, m)?)?;
    m.add_function(wrap_pyfunction!(_encode, m)?)?;
    m.add_function(wrap_pyfunction!(_decode, m)?)?;
    m.add("PROTOCOL_VERSION", 2)?;

    m.add_class::<keys::PeerInfo>()?;
    m.add_class::<keys::KeyFile>()?;
    m.add_class::<keys::Layout>()?;
    m.add_function(wrap_pyfunction!(keys::default_key_path, m)?)?;
    m.add_function(wrap_pyfunction!(keys::ensure_key, m)?)?;
    m.add_function(wrap_pyfunction!(keys::read_key, m)?)?;
    m.add_function(wrap_pyfunction!(keys::parse_key, m)?)?;
    m.add_function(wrap_pyfunction!(keys::format_key, m)?)?;
    m.add_function(wrap_pyfunction!(keys::pipe_user, m)?)?;
    m.add_function(wrap_pyfunction!(keys::short_name, m)?)?;
    m.add_function(wrap_pyfunction!(keys::pipe_path, m)?)?;
    m.add_function(wrap_pyfunction!(keys::find_workspace, m)?)?;
    m.add_function(wrap_pyfunction!(keys::registry_dir, m)?)?;
    m.add_function(wrap_pyfunction!(keys::_list_endpoints, m)?)?;
    m.add_function(wrap_pyfunction!(keys::_pipe_exists, m)?)?;

    m.add_class::<client::Client>()?;
    m.add_class::<client::Peer>()?;
    m.add_class::<client::PendingCall>()?;
    m.add_class::<client::Subscription>()?;
    m.add_class::<client::ClientSubscription>()?;
    m.add_class::<client::FlagEvent>()?;
    m.add_class::<client::Event>()?;
    m.add_class::<client::Sample>()?;
    m.add_function(wrap_pyfunction!(client::connect, m)?)?;
    m.add_function(wrap_pyfunction!(client::_shutdown, m)?)?;

    m.add_class::<endpoint::Node>()?;
    m.add_class::<endpoint::Endpoint>()?;
    m.add_class::<endpoint::Context>()?;
    m.add_class::<endpoint::SubscribeRequest>()?;
    m.add_class::<endpoint::SubscriptionView>()?;
    m.add_class::<endpoint::EventChannel>()?;
    m.add_class::<endpoint::StateChannel>()?;
    m.add_class::<endpoint::StreamChannel>()?;

    m.add_class::<testing::TestEndpoint>()?;
    m.add_function(wrap_pyfunction!(testing::start, m)?)?;
    Ok(())
}
