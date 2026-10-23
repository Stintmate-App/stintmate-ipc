//! Threads and logging shared by the binding.
//!
//! Foreign code never runs on a Tokio thread: callbacks that the core would run on the reader
//! (progress, done, connect errors) are queued here and run on one library thread that
//! attaches to the interpreter per job. Log records are forwarded by another thread, so that
//! a `tracing` event emitted while the core holds a lock never waits for the interpreter.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::{Sender, unbounded};
use pyo3::prelude::*;

/// Set when the interpreter is finalizing: nothing attaches after that.
pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

type PyJob = Box<dyn FnOnce(Python<'_>) + Send>;

fn callback_queue() -> &'static Sender<PyJob> {
    static Q: OnceLock<Sender<PyJob>> = OnceLock::new();
    Q.get_or_init(|| {
        let (tx, rx) = unbounded::<PyJob>();
        std::thread::Builder::new()
            .name("stintmate-ipc-py-callbacks".into())
            .spawn(move || {
                ON_CALLBACK_THREAD.with(|c| c.set(true));
                for job in rx {
                    if SHUTDOWN.load(Ordering::Acquire) {
                        continue;
                    }
                    Python::attach(job);
                }
            })
            .expect("a thread starts");
        tx
    })
}

thread_local! {
    static ON_CALLBACK_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Waits until the callbacks queued so far have run (so a call that reports progress returns
/// after its last `on_progress`). Does nothing on the callback thread itself.
pub fn flush(py: Python<'_>) {
    if ON_CALLBACK_THREAD.with(std::cell::Cell::get) {
        return;
    }
    let (tx, rx) = crossbeam_channel::bounded::<()>(1);
    enqueue(move |_| {
        let _ = tx.send(());
    });
    py.detach(|| {
        let _ = rx.recv_timeout(std::time::Duration::from_secs(5));
    });
}

/// Runs `job` on the library's callback thread, in order with the other queued jobs.
pub fn enqueue(job: impl FnOnce(Python<'_>) + Send + 'static) {
    let _ = callback_queue().send(Box::new(job));
}

/// Attaches the calling (library) thread and runs `job`, unless the interpreter is going down.
pub fn attach(job: impl FnOnce(Python<'_>)) {
    if !SHUTDOWN.load(Ordering::Acquire) {
        Python::attach(job);
    }
}

/// Logs the error of a callback with its traceback, through `logging`.
pub fn log_callback_error(py: Python<'_>, what: &str, err: PyErr) {
    let logged = py
        .import("logging")
        .and_then(|m| m.getattr("getLogger"))
        .and_then(|g| g.call1(("stintmate_ipc",)))
        .and_then(|l| {
            let kw = pyo3::types::PyDict::new(py);
            kw.set_item("exc_info", err.value(py))?;
            l.call_method("error", (what,), Some(&kw))
        });
    if logged.is_err() {
        err.print(py);
    }
}

// --------------------------------------------------------------------------- logging

struct Record {
    level: log::Level,
    target: String,
    message: String,
}

struct Bridge {
    tx: Sender<Record>,
}

impl log::Log for Bridge {
    fn enabled(&self, m: &log::Metadata<'_>) -> bool {
        m.target().starts_with("stintmate_ipc")
    }

    fn log(&self, r: &log::Record<'_>) {
        if self.enabled(r.metadata()) {
            let _ = self.tx.send(Record {
                level: r.level(),
                target: r.target().to_owned(),
                message: r.args().to_string(),
            });
        }
    }

    fn flush(&self) {}
}

fn py_level(l: log::Level) -> u8 {
    match l {
        log::Level::Error => 40,
        log::Level::Warn => 30,
        log::Level::Info => 20,
        log::Level::Debug | log::Level::Trace => 10,
    }
}

/// Installs the `tracing` -> `logging` bridge once (the `log` feature of `tracing` turns
/// events into `log` records while no `tracing` subscriber is set).
pub fn init_logging(py: Python<'_>) {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let (tx, rx) = unbounded::<Record>();
        std::thread::Builder::new()
            .name("stintmate-ipc-py-log".into())
            .spawn(move || {
                for r in rx {
                    if SHUTDOWN.load(Ordering::Acquire) {
                        continue;
                    }
                    Python::attach(|py| {
                        let _ = py
                            .import("logging")
                            .and_then(|m| m.getattr("getLogger"))
                            .and_then(|g| g.call1((r.target.replace("::", "."),)))
                            .and_then(|l| l.call_method1("log", (py_level(r.level), r.message)));
                    });
                }
            })
            .expect("a thread starts");
        let _ = log::set_boxed_logger(Box::new(Bridge { tx }));
    });
    refresh_log_level(py);
}

/// Sets how verbose the core is from the effective level of the `stintmate_ipc` logger.
pub fn refresh_log_level(py: Python<'_>) {
    let level = py
        .import("logging")
        .and_then(|m| m.getattr("getLogger"))
        .and_then(|g| g.call1(("stintmate_ipc",)))
        .and_then(|l| l.call_method0("getEffectiveLevel"))
        .and_then(|l| l.extract::<u32>())
        .unwrap_or(30);
    log::set_max_level(match level {
        0..=9 => log::LevelFilter::Trace,
        10..=19 => log::LevelFilter::Debug,
        20..=29 => log::LevelFilter::Info,
        30..=39 => log::LevelFilter::Warn,
        _ => log::LevelFilter::Error,
    });
}
