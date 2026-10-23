//! The table of calls waiting for a `res`.
//!
//! **One completion path.** The table owns the `oneshot` sender of every call. The `res`, the
//! deadline timer, `cancel()` and the loss of the connection all race to *remove* the entry
//! under the table's lock; only the one that removed it completes the call, so a call is
//! completed exactly once and a late `res` finds nothing (SPEC 10.2, 15.1).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::codec::Value;
use crate::error::Error;

/// Called on the connection's reader with the `d` of each `prog` of a call.
pub type ProgressFn = Arc<dyn Fn(Value) + Send + Sync>;

/// Called with the result of a call just before its waiter is woken.
pub type ResultHook = Arc<dyn Fn(&Result<Value, Error>) + Send + Sync>;

/// What a call waits for.
pub(crate) type CallResult = Result<Value, Error>;

/// One call in flight.
pub(crate) struct CallEntry {
    tx: oneshot::Sender<CallResult>,
    progress: Option<ProgressFn>,
    deadline: Option<watch::Sender<Instant>>,
    done: CancellationToken,
    hook: Option<ResultHook>,
}

impl CallEntry {
    pub(crate) fn new(
        tx: oneshot::Sender<CallResult>,
        progress: Option<ProgressFn>,
        deadline: Option<watch::Sender<Instant>>,
        done: CancellationToken,
    ) -> Self {
        Self {
            tx,
            progress,
            deadline,
            done,
            hook: None,
        }
    }

    pub(crate) fn with_hook(mut self, hook: Option<ResultHook>) -> Self {
        self.hook = hook;
        self
    }

    fn finish(self, result: CallResult) {
        if let Some(h) = &self.hook {
            h(&result);
        }
        self.done.cancel();
        // The receiver may be gone (a caller that stopped waiting): nothing to tell.
        let _ = self.tx.send(result);
    }
}

#[derive(Default)]
struct Table {
    map: HashMap<u64, CallEntry>,
    closed: bool,
}

/// The calls of one connection.
#[derive(Default)]
pub(crate) struct CallTable {
    inner: Mutex<Table>,
}

impl CallTable {
    fn lock(&self) -> MutexGuard<'_, Table> {
        // The guarded sections never panic; a poisoned lock still holds a valid table.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Registers a call. `Err` with the entry when the table is closed (the connection is
    /// gone): the caller must not send the request.
    pub(crate) fn insert(&self, id: u64, entry: CallEntry) -> Result<(), CallEntry> {
        let mut t = self.lock();
        if t.closed {
            return Err(entry);
        }
        t.map.insert(id, entry);
        Ok(())
    }

    /// Removes call `id` and completes it with `result`. Returns whether this caller won, i.e.
    /// removed the entry; every other racer gets `false` and must do nothing.
    pub(crate) fn complete(&self, id: u64, result: CallResult) -> bool {
        let entry = self.lock().map.remove(&id);
        match entry {
            Some(e) => {
                e.finish(result);
                true
            }
            None => false,
        }
    }

    /// Closes the table and completes every call with `error()`; later `insert`s fail.
    /// Returns how many calls this completed.
    pub(crate) fn fail_all(&self, error: impl Fn() -> Error) -> usize {
        let drained: Vec<CallEntry> = {
            let mut t = self.lock();
            t.closed = true;
            t.map.drain().map(|(_, e)| e).collect()
        };
        let n = drained.len();
        for e in drained {
            e.finish(Err(error()));
        }
        n
    }

    /// A `prog` for call `id`: moves its deadline when `dl_ms` is given and returns the progress
    /// callback. `None` when the call is not waiting any more.
    pub(crate) fn on_progress(&self, id: u64, dl_ms: Option<u64>) -> Option<Option<ProgressFn>> {
        let t = self.lock();
        let e = t.map.get(&id)?;
        if let (Some(ms), Some(tx)) = (dl_ms, &e.deadline) {
            let _ = tx.send(Instant::now() + std::time::Duration::from_millis(ms));
        }
        Some(e.progress.clone())
    }

    /// Calls in flight.
    pub(crate) fn len(&self) -> usize {
        self.lock().map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Barrier;

    fn entry() -> (CallEntry, oneshot::Receiver<CallResult>) {
        let (tx, rx) = oneshot::channel();
        (CallEntry::new(tx, None, None, CancellationToken::new()), rx)
    }

    #[test]
    fn a_closed_table_refuses_calls() {
        let t = CallTable::default();
        assert_eq!(t.fail_all(|| Error::NotConnected), 0);
        assert!(t.insert(1, entry().0).is_err());
    }

    #[tokio::test]
    async fn complete_is_once() {
        let t = CallTable::default();
        let (e, rx) = entry();
        t.insert(7, e).ok().unwrap();
        assert_eq!(t.len(), 1);
        assert!(t.complete(7, Ok(Value::from(1))));
        assert!(!t.complete(7, Err(Error::CallTimeout)));
        assert_eq!(rx.await.unwrap().unwrap(), Value::from(1));
        assert_eq!(t.fail_all(|| Error::NotConnected), 0);
    }

    /// Response, deadline, cancel and loss race for the same call: exactly one removes the
    /// entry, and the call completes once with that racer's result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_completes_exactly_once() {
        let mut wins = [0usize; 4];
        for i in 0..10_000u64 {
            let table = Arc::new(CallTable::default());
            let (e, rx) = entry();
            table.insert(i, e).ok().unwrap();
            let barrier = Arc::new(Barrier::new(4));
            let mut tasks = Vec::new();
            for who in 0..4usize {
                let (table, barrier) = (table.clone(), barrier.clone());
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    match who {
                        0 => table.complete(i, Ok(Value::from(i))) as usize,
                        1 => table.complete(i, Err(Error::CallTimeout)) as usize,
                        2 => table.complete(i, Err(Error::CallCancelled)) as usize,
                        _ => table.fail_all(|| Error::ConnectionLost {
                            code: None,
                            message: "lost".into(),
                        }),
                    }
                }));
            }
            let mut won = [0usize; 4];
            for (who, t) in tasks.into_iter().enumerate() {
                won[who] = t.await.unwrap();
            }
            assert_eq!(won.iter().sum::<usize>(), 1, "iteration {i}: {won:?}");
            let winner = won.iter().position(|&w| w == 1).unwrap();
            wins[winner] += 1;
            match (winner, rx.await.expect("completed exactly once")) {
                (0, Ok(v)) => assert_eq!(v, Value::from(i)),
                (1, Err(Error::CallTimeout)) | (2, Err(Error::CallCancelled)) => {}
                (3, Err(Error::ConnectionLost { .. })) => {}
                (w, r) => panic!("iteration {i}: racer {w} won but the call got {r:?}"),
            }
            assert_eq!(table.len(), 0);
        }
        eprintln!("race winners (response, timeout, cancel, loss): {wins:?}");
    }
}
