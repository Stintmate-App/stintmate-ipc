//! Conformance checks for Stintmate IPC v2 (PRY-085 T5): the library behind the
//! `stintmate-ipc-check` tool, so the Python binding and the tests run the same checks.
//!
//! * [`server`]: S00-S24, against an endpoint (`server <pipe>`).
//! * [`client`]: C01-C11, serving a test endpoint to a client (`client <pipe>`).
//! * [`raw`]: the raw-wire peer both use, on the codec and the transport of `stintmate-ipc`.
//! * [`report`]: results and their text and JSON forms. [`cli::main`] is the command line.

pub mod cli;
pub mod client;
pub mod raw;
pub mod report;
pub mod server;
