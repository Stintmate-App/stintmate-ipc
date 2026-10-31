//! The owner's memory of idempotent requests (SPEC 10.4).

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

use crate::codec::Value;
use crate::error::RemoteError;

/// `(peer app, method, ik)`.
pub(crate) type Key = (String, String, String);

/// Codes that mean "not done, try later": they never consume a key (SPEC 10.4).
/// `writer_busy` is an application code with the same meaning that a method may use.
const NOT_CACHED: &[&str] = &[
    "busy",
    "unavailable",
    "timeout",
    "cancelled",
    "overloaded",
    "shutting_down",
    "writer_busy",
];

type Answer = Result<Value, RemoteError>;

struct Done {
    answer: Answer,
    at: Instant,
    tick: u64,
}

#[derive(Default)]
struct State {
    done: HashMap<Key, Done>,
    order: VecDeque<(Key, u64)>,
    tick: u64,
    running: HashMap<Key, watch::Receiver<bool>>,
}

/// Answers by key for the life of the process (or `ttl`): a repeat gets the first answer
/// instead of a second execution. Durable deduplication across restarts is the application's.
pub(crate) struct IdempotencyCache {
    max_entries: usize,
    ttl: Option<Duration>,
    wait: Duration,
    state: Mutex<State>,
}

struct Running<'a> {
    cache: &'a IdempotencyCache,
    key: Key,
    tx: watch::Sender<bool>,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.cache.lock().running.remove(&self.key);
        let _ = self.tx.send(true);
    }
}

impl IdempotencyCache {
    pub(crate) fn new(ttl: Option<Duration>) -> Self {
        Self {
            max_entries: 4096,
            ttl,
            wait: Duration::from_secs(30),
            state: Mutex::new(State::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Runs `f` once per key. A repeat that arrives while the first still runs waits for it, at
    /// most until `deadline` (default 30 s from now), then answers the retryable `busy`.
    pub(crate) async fn run<F, Fut>(&self, key: Key, deadline: Option<Instant>, f: F) -> Answer
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Answer>,
    {
        let limit = deadline.unwrap_or_else(|| Instant::now() + self.wait);
        let guard = loop {
            let mut rx = {
                let mut st = self.lock();
                if let Some(d) = st.done.get(&key) {
                    if self.ttl.is_some_and(|t| d.at.elapsed() >= t) {
                        st.done.remove(&key);
                    } else {
                        return d.answer.clone();
                    }
                }
                match st.running.get(&key) {
                    Some(rx) => rx.clone(),
                    None => {
                        let (tx, rx) = watch::channel(false);
                        st.running.insert(key.clone(), rx);
                        break Running {
                            cache: self,
                            key: key.clone(),
                            tx,
                        };
                    }
                }
            };
            if timeout_at(limit, rx.wait_for(|done| *done)).await.is_err() {
                let mut e = RemoteError::new(
                    "busy",
                    "the first request with this idempotency key is still running",
                );
                e.retry_ms = Some(500);
                return Err(e);
            }
            // Finished (or abandoned): look again.
        };
        let answer = f().await;
        let cacheable = match &answer {
            Ok(_) => true,
            Err(e) => !NOT_CACHED.contains(&e.code.as_str()),
        };
        if cacheable {
            let mut st = self.lock();
            st.tick += 1;
            let tick = st.tick;
            st.order.push_back((key.clone(), tick));
            st.done.insert(
                key,
                Done {
                    answer: answer.clone(),
                    at: Instant::now(),
                    tick,
                },
            );
            while st.done.len() > self.max_entries {
                let Some((k, t)) = st.order.pop_front() else {
                    break;
                };
                if st.done.get(&k).is_some_and(|d| d.tick == t) {
                    st.done.remove(&k);
                }
            }
        }
        drop(guard);
        answer
    }
}
