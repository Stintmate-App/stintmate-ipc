//! The library's runtime (spec 2.2): the hooks thread and the log thread, the log callback,
//! the thread hooks, `user_data` release bookkeeping, handle counting and shutdown.
//!
//! Every callback the library makes into the caller goes through [`enter`]: it fails once
//! [`stintmate_ipc_shutdown`] has started, and shutdown waits for the callbacks that run.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::{CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicIsize, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, Once};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, unbounded};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};

use crate::{
    STINTMATE_IPC_E_INTERNAL, STINTMATE_IPC_E_INVALID_ARG, STINTMATE_IPC_E_SHUTDOWN,
    STINTMATE_IPC_E_WAIT_TIMEOUT, STINTMATE_IPC_OK, UserData, guard, invalid, record_error,
};

/// Releases a `user_data` (rule 5): runs exactly once, after the last callback that can
/// receive it.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_release_cb = Option<unsafe extern "C-unwind" fn(user_data: *mut c_void)>;

/// A log record of the core: `level` is 10 (debug) to 40 (error).
#[allow(non_camel_case_types)]
pub type stintmate_ipc_log_cb = Option<
    unsafe extern "C-unwind" fn(
        user_data: *mut c_void,
        level: i32,
        target: *const c_char,
        message: *const c_char,
    ),
>;

/// A thread hook: called on a library thread that runs callbacks.
#[allow(non_camel_case_types)]
pub type stintmate_ipc_thread_cb = Option<unsafe extern "C-unwind" fn(user_data: *mut c_void)>;

type Job = Box<dyn FnOnce() + Send>;

enum Msg {
    Run(Job),
    Stop,
}

/// A thread that runs jobs in order (the hooks thread, the log thread).
struct Worker {
    tx: Sender<Msg>,
    join: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy)]
struct ThreadHooks {
    on_start: stintmate_ipc_thread_cb,
    on_stop: stintmate_ipc_thread_cb,
    user: UserData,
}

/// The state of a registered `user_data` that has a `release`.
struct ReleaseSlot {
    user: UserData,
    release: Mutex<stintmate_ipc_release_cb>,
}

struct State {
    shutdown: AtomicBool,
    /// The number of callbacks running (and the condvar shutdown waits on).
    active: Mutex<usize>,
    idle: Condvar,
    /// Set by the first handle, hooks thread or log thread: thread hooks can no longer change.
    locked: AtomicBool,
    hooks: Mutex<Option<ThreadHooks>>,
    regs: Mutex<HashMap<u64, Arc<ReleaseSlot>>>,
    closers: Mutex<HashMap<u64, Box<dyn Fn() + Send>>>,
    hooks_worker: Mutex<Option<Worker>>,
    log_worker: Mutex<Option<Worker>>,
    log: Mutex<Option<(stintmate_ipc_log_cb, Arc<Registration>)>>,
    log_level: AtomicI32,
    next_id: AtomicU64,
    live_handles: AtomicIsize,
}

static STATE: LazyLock<State> = LazyLock::new(|| State {
    shutdown: AtomicBool::new(false),
    active: Mutex::new(0),
    idle: Condvar::new(),
    locked: AtomicBool::new(false),
    hooks: Mutex::new(None),
    regs: Mutex::new(HashMap::new()),
    closers: Mutex::new(HashMap::new()),
    hooks_worker: Mutex::new(None),
    log_worker: Mutex::new(None),
    log: Mutex::new(None),
    log_level: AtomicI32::new(0),
    next_id: AtomicU64::new(1),
    live_handles: AtomicIsize::new(0),
});

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

pub(crate) fn is_shutdown() -> bool {
    STATE.shutdown.load(Ordering::Acquire)
}

// ------------------------------------------------------------------------ callbacks

thread_local! {
    /// How many callbacks this thread is inside of.
    static DEPTH: Cell<usize> = const { Cell::new(0) };
    static CHECKED: Cell<bool> = const { Cell::new(false) };
    static THREAD: RefCell<Option<ThreadHooks>> = const { RefCell::new(None) };
}

/// Calls `on_start` once on this thread (when hooks are set) and arranges `on_stop`.
fn ensure_thread() {
    if CHECKED.try_with(Cell::get).unwrap_or(true) {
        return;
    }
    let _ = CHECKED.try_with(|c| c.set(true));
    let hooks = *lock(&STATE.hooks);
    if let Some(h) = hooks {
        if let Some(cb) = h.on_start {
            // SAFETY: the caller's hook and `user_data`.
            let _ = catch_unwind(AssertUnwindSafe(|| unsafe { cb(h.user.get()) }));
        }
        let _ = THREAD.try_with(|t| *t.borrow_mut() = Some(h));
    }
}

/// Runs `on_stop` of this thread now.
fn stop_thread() {
    let h = THREAD.try_with(|t| t.borrow_mut().take()).ok().flatten();
    if let Some(h) = h
        && let Some(cb) = h.on_stop
    {
        // SAFETY: the caller's hook and `user_data`.
        let _ = catch_unwind(AssertUnwindSafe(|| unsafe { cb(h.user.get()) }));
    }
}

/// Runs `on_stop` when a thread that ran callbacks ends (not after shutdown: shutdown joins
/// its own threads and runs their hooks itself).
struct ThreadExit;

impl Drop for ThreadExit {
    fn drop(&mut self) {
        if !is_shutdown() {
            stop_thread();
        }
    }
}

thread_local! {
    static EXIT: ThreadExit = const { ThreadExit };
}

/// Proof that a callback may run (shutdown has not started); shutdown waits for it.
pub(crate) struct Active(());

/// Asks to run a callback: `None` once shutdown started. The first call on a thread runs its
/// `on_start` hook.
pub(crate) fn enter() -> Option<Active> {
    {
        let mut n = lock(&STATE.active);
        if is_shutdown() {
            return None;
        }
        *n += 1;
    }
    let _ = DEPTH.try_with(|d| d.set(d.get() + 1));
    let _ = EXIT.try_with(|_| ());
    ensure_thread();
    Some(Active(()))
}

impl Drop for Active {
    fn drop(&mut self) {
        let _ = DEPTH.try_with(|d| d.set(d.get().saturating_sub(1)));
        let mut n = lock(&STATE.active);
        *n -= 1;
        if *n == 0 {
            STATE.idle.notify_all();
        }
    }
}

/// Whether the calling thread is inside a callback of the library.
pub(crate) fn in_callback() -> bool {
    DEPTH.try_with(Cell::get).unwrap_or(0) > 0
}

// ------------------------------------------------------------------------ workers

fn spawn_worker(name: &str) -> Worker {
    STATE.locked.store(true, Ordering::Release);
    let (tx, rx) = unbounded::<Msg>();
    let join = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            // The thread's own hook calls (not tied to a callback).
            ensure_thread();
            for msg in rx {
                match msg {
                    Msg::Run(job) => {
                        let _ = catch_unwind(AssertUnwindSafe(job));
                    }
                    Msg::Stop => break,
                }
            }
            stop_thread();
        })
        .ok();
    Worker { tx, join }
}

fn post_to(slot: &Mutex<Option<Worker>>, name: &str, job: Job) {
    if is_shutdown() {
        return;
    }
    let mut w = lock(slot);
    let w = w.get_or_insert_with(|| spawn_worker(name));
    let _ = w.tx.send(Msg::Run(job));
}

/// Runs `job` on the hooks thread, in order with the other hooks (never on a runtime thread).
/// The job runs only while shutdown has not started.
#[allow(dead_code)] // used by the node and peer hooks of the later tasks
pub(crate) fn post_hook(job: impl FnOnce() + Send + 'static) {
    post_to(
        &STATE.hooks_worker,
        "stintmate-ipc-hooks",
        Box::new(move || {
            if let Some(_g) = enter() {
                job();
            }
        }),
    );
}

// ------------------------------------------------------------------------ user_data

/// A `user_data` the caller registered, with its optional `release`. Callbacks hold an `Arc`
/// to it; when the last one goes, the release runs on the hooks thread (or in shutdown).
pub(crate) struct Registration {
    id: u64,
    slot: Arc<ReleaseSlot>,
}

impl Registration {
    /// Registers `user_data` with its `release` (null: nothing to release).
    pub(crate) fn new(user_data: *mut c_void, release: stintmate_ipc_release_cb) -> Arc<Self> {
        let id = STATE.next_id.fetch_add(1, Ordering::Relaxed);
        let slot = Arc::new(ReleaseSlot {
            user: UserData(user_data),
            release: Mutex::new(release),
        });
        if release.is_some() {
            lock(&STATE.regs).insert(id, slot.clone());
        }
        Arc::new(Self { id, slot })
    }

    /// The caller's pointer.
    pub(crate) fn user_data(&self) -> *mut c_void {
        self.slot.user.get()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if lock(&self.slot.release).is_none() {
            return;
        }
        let (id, slot) = (self.id, self.slot.clone());
        post_hook_ungated(move || {
            // Shutdown runs the releases that are left itself.
            if let Some(_g) = enter()
                && run_release(&slot)
            {
                lock(&STATE.regs).remove(&id);
            }
        });
    }
}

/// Like [`post_hook`] but the job decides about the gate (releases do).
fn post_hook_ungated(job: impl FnOnce() + Send + 'static) {
    post_to(&STATE.hooks_worker, "stintmate-ipc-hooks", Box::new(job));
}

fn run_release(slot: &ReleaseSlot) -> bool {
    let cb = lock(&slot.release).take();
    match cb {
        Some(cb) => {
            // SAFETY: the caller's `release` and `user_data`; taken out, so it runs once.
            let _ = catch_unwind(AssertUnwindSafe(|| unsafe { cb(slot.user.get()) }));
            true
        }
        None => false,
    }
}

// ------------------------------------------------------------------------ handles

/// Counts a live handle (`stintmate_ipc_live_handles`) and, optionally, how to close it at
/// shutdown. Dropping it uncounts.
pub(crate) struct Handle {
    id: u64,
}

impl Handle {
    /// A new handle; this also freezes the thread hooks.
    pub(crate) fn new() -> Self {
        STATE.locked.store(true, Ordering::Release);
        STATE.live_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            id: STATE.next_id.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// A new handle that `close` stops when the library shuts down (`close` holds its own
    /// clone of the object).
    pub(crate) fn with_closer(close: impl Fn() + Send + 'static) -> Self {
        let h = Self::new();
        lock(&STATE.closers).insert(h.id, Box::new(close));
        h
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        lock(&STATE.closers).remove(&self.id);
        STATE.live_handles.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Counts a handle that has no wrapper struct to hold a [`Handle`] (a channel).
pub(crate) fn count_handle() {
    STATE.locked.store(true, Ordering::Release);
    STATE.live_handles.fetch_add(1, Ordering::Relaxed);
}

/// Uncounts a handle counted with [`count_handle`].
pub(crate) fn uncount_handle() {
    STATE.live_handles.fetch_sub(1, Ordering::Relaxed);
}

/// The number of handles handed out and not yet released (a leak check for tests).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_live_handles() -> isize {
    STATE.live_handles.load(Ordering::Relaxed)
}

// ------------------------------------------------------------------------ thread hooks

/// Sets the hooks called once on each library thread that runs callbacks, before its first
/// callback (`on_start`) and when it ends (`on_stop`). Both may be null. Must be set before
/// the first handle is created (and before the log callback sends its first record); else
/// `STINTMATE_IPC_E_INVALID_ARG`. `user_data` is never released.
///
/// # Safety
/// The hooks and `user_data` must be usable from library threads for the life of the process.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_set_thread_hooks(
    on_start: stintmate_ipc_thread_cb,
    on_stop: stintmate_ipc_thread_cb,
    user_data: *mut c_void,
) -> i32 {
    guard(|| {
        if STATE.locked.load(Ordering::Acquire) {
            return invalid("thread hooks must be set before the first handle is created");
        }
        *lock(&STATE.hooks) = Some(ThreadHooks {
            on_start,
            on_stop,
            user: UserData(user_data),
        });
        STINTMATE_IPC_OK
    })
}

// ------------------------------------------------------------------------ logging

fn level_number(l: &Level) -> i32 {
    if *l == Level::ERROR {
        40
    } else if *l == Level::WARN {
        30
    } else if *l == Level::INFO {
        20
    } else if *l == Level::DEBUG {
        10
    } else {
        5
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }
}

/// Hands the core's `tracing` events to the log thread.
struct LogSubscriber;

impl Subscriber for LogSubscriber {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        let level = STATE.log_level.load(Ordering::Relaxed);
        level != 0 && level_number(metadata.level()) >= level && !is_shutdown()
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(STATE.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let md = event.metadata();
        let level = level_number(md.level());
        let mut v = MessageVisitor::default();
        event.record(&mut v);
        let message = format!("{}{}", v.message, v.fields);
        let target = md.target().to_owned();
        post_to(
            &STATE.log_worker,
            "stintmate-ipc-log",
            Box::new(move || {
                let Some(_g) = enter() else { return };
                let current = lock(&STATE.log).clone();
                let Some((Some(cb), reg)) = current else {
                    return;
                };
                if level < STATE.log_level.load(Ordering::Relaxed) {
                    return;
                }
                let c = |s: &str| CString::new(s.replace('\0', "")).unwrap_or_default();
                let (t, m) = (c(&target), c(&message));
                // SAFETY: the caller's callback; the registration keeps `user_data` alive.
                let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
                    cb(reg.user_data(), level, t.as_ptr(), m.as_ptr());
                }));
            }),
        );
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// Sends the core's log records to `cb`, from the log thread, when their level is at least
/// `level` (10 debug, 20 info, 30 warn, 40 error; 0: off). Replacing the callback (or passing
/// a null `cb`) releases the previous `user_data`.
///
/// # Safety
/// `cb` and `user_data` must be usable from library threads until released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_set_log_callback(
    cb: stintmate_ipc_log_cb,
    user_data: *mut c_void,
    release: stintmate_ipc_release_cb,
    level: i32,
) -> i32 {
    guard(|| {
        if !(0..=40).contains(&level) {
            return invalid("level must be 0 (off) or 10 to 40");
        }
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            // Another global subscriber (a host that has its own) keeps its place.
            let _ = tracing::subscriber::set_global_default(LogSubscriber);
        });
        let reg = Registration::new(user_data, release);
        STATE.log_level.store(level, Ordering::Relaxed);
        let old = lock(&STATE.log).replace((cb, reg));
        drop(old);
        STINTMATE_IPC_OK
    })
}

/// Sets the level filter of the log callback (0: off).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_set_log_level(level: i32) -> i32 {
    guard(|| {
        if !(0..=40).contains(&level) {
            return invalid("level must be 0 (off) or 10 to 40");
        }
        STATE.log_level.store(level, Ordering::Relaxed);
        STINTMATE_IPC_OK
    })
}

// ------------------------------------------------------------------------ shutdown

/// Ends the library: no callback starts after this, running ones are waited for (up to
/// `timeout_ms`; `STINTMATE_IPC_FOREVER` waits as long as it takes), handles are closed, the
/// library's threads end and every pending `release` runs before it returns. Later calls fail
/// with `STINTMATE_IPC_E_SHUTDOWN`. Idempotent; not callable from a callback.
/// `STINTMATE_IPC_E_WAIT_TIMEOUT` when callbacks were still running (call again to finish).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_shutdown(timeout_ms: i64) -> i32 {
    match catch_unwind(shutdown_inner(timeout_ms)) {
        Ok(status) => status,
        Err(_) => {
            record_error(STINTMATE_IPC_E_INTERNAL, "panic in shutdown");
            STINTMATE_IPC_E_INTERNAL
        }
    }
}

fn shutdown_inner(timeout_ms: i64) -> impl FnOnce() -> i32 + std::panic::UnwindSafe {
    move || {
        if in_callback() {
            record_error(
                STINTMATE_IPC_E_INVALID_ARG,
                "shutdown cannot be called from a callback",
            );
            return STINTMATE_IPC_E_INVALID_ARG;
        }
        let deadline = u64::try_from(timeout_ms)
            .ok()
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        {
            let _n = lock(&STATE.active);
            STATE.shutdown.store(true, Ordering::Release);
        }
        let closers: Vec<_> = lock(&STATE.closers).drain().map(|(_, f)| f).collect();
        for close in closers {
            let _ = catch_unwind(AssertUnwindSafe(close));
        }
        let mut n = lock(&STATE.active);
        while *n > 0 {
            match deadline {
                None => n = STATE.idle.wait(n).unwrap_or_else(|p| p.into_inner()),
                Some(d) => {
                    let left = d.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        drop(n);
                        record_error(STINTMATE_IPC_E_WAIT_TIMEOUT, "callbacks are still running");
                        return STINTMATE_IPC_E_WAIT_TIMEOUT;
                    }
                    n = STATE
                        .idle
                        .wait_timeout(n, left)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
            }
        }
        drop(n);
        for slot in [&STATE.hooks_worker, &STATE.log_worker] {
            let w = lock(slot).take();
            if let Some(mut w) = w {
                let _ = w.tx.send(Msg::Stop);
                if let Some(j) = w.join.take() {
                    let _ = j.join();
                }
            }
        }
        drop(lock(&STATE.log).take());
        let slots: Vec<_> = lock(&STATE.regs).drain().map(|(_, s)| s).collect();
        for s in slots {
            run_release(&s);
        }
        STINTMATE_IPC_OK
    }
}

/// The status every entry point answers after shutdown.
pub(crate) fn shutdown_status() -> i32 {
    record_error(STINTMATE_IPC_E_SHUTDOWN, "the library was shut down");
    STINTMATE_IPC_E_SHUTDOWN
}
