//! Where callbacks run. A [`Lane`] runs jobs one at a time, in the order submitted, never on
//! the thread that submits. The async API's default lane is a Tokio task; the blocking façade
//! installs a standard thread per connection (plan section 5).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::error;

/// A unit of work for a lane or a pool.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// An ordered executor of callbacks.
pub trait Lane: Send + Sync + 'static {
    /// Queues `job`; it runs after the jobs submitted before it. Never blocks.
    fn submit(&self, job: Job);
}

/// Makes the lane of a connection, once, when it is first needed.
pub type LaneFactory = Arc<dyn Fn() -> Arc<dyn Lane> + Send + Sync>;

/// Runs a job, logging a panic instead of unwinding into the lane.
pub fn run_job(job: Job) {
    if catch_unwind(AssertUnwindSafe(job)).is_err() {
        error!("a callback panicked");
    }
}

/// A lane that is a Tokio task: the jobs run on the runtime, so they must be quick.
pub struct TokioLane {
    tx: mpsc::UnboundedSender<Job>,
}

impl TokioLane {
    /// Starts the task on `handle`; it ends when the lane is dropped and drained.
    pub fn new(handle: &Handle) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
        handle.spawn(async move {
            while let Some(job) = rx.recv().await {
                run_job(job);
            }
        });
        Self { tx }
    }
}

impl Lane for TokioLane {
    fn submit(&self, job: Job) {
        let _ = self.tx.send(job);
    }
}

/// The default factory: a [`TokioLane`] per connection.
pub fn tokio_lanes(handle: Handle) -> LaneFactory {
    Arc::new(move || Arc::new(TokioLane::new(&handle)) as Arc<dyn Lane>)
}
