"""Thread helpers: a cache of worker threads, serial lanes and a concurrency gate.

The reader of a connection runs work it finds (a request handler, a subscription callback)
**on its own thread** after handing the reading over to another worker ("leader/follower"):
the work starts at once instead of after a thread wake-up, and the next read starts in
parallel. Lanes keep the order where the protocol or the API promises it (subscription
callbacks of one connection; ``ordered`` methods).
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from collections.abc import Callable

log = logging.getLogger(__name__)
Job = Callable[[], None]
IDLE_EXIT_S = 30.0


class _Worker:
    __slots__ = ("wake", "job")

    def __init__(self) -> None:
        self.wake = threading.Lock()
        self.wake.acquire()
        self.job: Job | None = None


class Workers:
    """Unbounded cache of daemon threads: :meth:`run` hands a job to an idle thread (one
    wake-up) or starts a new one; a thread idle for 30 s exits."""

    def __init__(self, name: str = "ipc-worker") -> None:
        self.name = name
        self._lock = threading.Lock()
        self._idle: list[_Worker] = []
        self._count = 0
        self._closed = False

    def run(self, job: Job) -> None:
        with self._lock:
            if self._idle:
                w = self._idle.pop()
                w.job = job
                w.wake.release()
                return
            self._count += 1
            n = self._count
        threading.Thread(target=self._loop, args=(job,), daemon=True,
                         name=f"{self.name}-{n}").start()

    def _loop(self, job: Job | None) -> None:
        me = _Worker()
        while job is not None:
            try:
                job()
            except Exception:  # noqa: BLE001
                log.exception("worker job failed")
            job = None
            with self._lock:
                if self._closed:
                    return
                self._idle.append(me)
            if me.wake.acquire(timeout=IDLE_EXIT_S):
                job, me.job = me.job, None
            else:
                with self._lock:
                    if me in self._idle:
                        self._idle.remove(me)
                        return
                # Taken just as the wait timed out: the job is coming.
                me.wake.acquire()
                job, me.job = me.job, None

    def close(self) -> None:
        with self._lock:
            self._closed = True
            idle, self._idle = self._idle, []
        for w in idle:
            w.job = None
            w.wake.release()


def _safe(job: Job) -> None:
    try:
        job()
    except Exception:  # noqa: BLE001
        log.exception("job failed")


class Lane:
    """Runs jobs one at a time, in the order offered. :meth:`offer` returns a runner when
    the lane was idle (the caller runs it, or schedules it on a worker), else ``None`` (the
    running runner will take the job)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._q: deque[Job] = deque()
        self._busy = False

    def offer(self, job: Job) -> Job | None:
        with self._lock:
            if self._busy:
                self._q.append(job)
                return None
            self._busy = True

        def runner() -> None:
            nxt: Job | None = job
            while nxt is not None:
                _safe(nxt)
                with self._lock:
                    if self._q:
                        nxt = self._q.popleft()
                    else:
                        self._busy = False
                        nxt = None

        return runner


class Gate:
    """At most ``limit`` jobs at once; more wait in a queue (in order)."""

    def __init__(self, limit: int) -> None:
        self.limit = max(1, limit)
        self._lock = threading.Lock()
        self._q: deque[Job] = deque()
        self._running = 0

    def offer(self, job: Job) -> Job | None:
        with self._lock:
            if self._running >= self.limit:
                self._q.append(job)
                return None
            self._running += 1

        def runner() -> None:
            nxt: Job | None = job
            while nxt is not None:
                _safe(nxt)
                with self._lock:
                    if self._q:
                        nxt = self._q.popleft()
                    else:
                        self._running -= 1
                        nxt = None

        return runner
