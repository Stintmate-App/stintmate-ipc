# Benchmarks

## Rust codec (PRY-085 T1, 2026-10-23)

`cargo bench -p stintmate-ipc --bench codec` (criterion 0.8, release profile with thin LTO),
Windows 11 on the development PC, median of the criterion estimate. The `req` is the one of
the `frames.json` vector (102 bytes, nested params with a `bin`), as a `rmpv::Value` tree.

| Benchmark | Time | Plan target (section 6) | Python 0.1.4 |
|---|---|---|---|
| `req` encode (validate + write) | 0.20 us | <= 1.0 us | 1.9 us |
| `req` decode (validating decoder, to `Value`) | 1.48 us | <= 1.0 us | 1.2 us |
| `req` decode + message shape check | 1.55 us | | |
| Stream frame write (48-byte body) | 8 ns | | 0.37 us |
| Stream frame read (push + `next_frame`) | 28 ns | | |
| Layout 12 x f32 pack | 20 ns | | 0.38 us |
| Layout 12 x f32 unpack | 39 ns | | 0.24 us |

Notes:

- The decode target (<= 1.0 us) is stated for the codec "from/to Python objects"; the number
  here is to the neutral `Value` tree, which allocates a `String` per key and a `Vec` per
  container (about 25 allocations for this `req`). The Python binding converts directly to
  Python objects without that tree (plan section 5), so the target is judged there (T6).
  Encode is far under the target.

## Python binding, Python to Python (PRY-085 T7, 2026-10-23, this PC)

`python/bench/bench.py --repeat 3 --scale 0.5` (medians of 3 runs, every role a separate
process, release wheel) and `python/bench/codec.py` (best of 5, 200 000 iterations).

| Metric | Result | Target (plan 6) | Baseline 0.1.4 |
|---|---|---|---|
| rr p50 / p99 | 65 us / 100 us | <= 45 / <= 100 us | 67 / 93-120 us |
| event 200 Hz, Python subscriber p50 (p99) | 79 us (174 us) | <= 50 us | 60 us |
| stream 200 Hz, 48 B, p99 | 135 us (p50 60 us) | <= 100 us | 85 us (p50 38 us) |
| stream CPU, publisher / subscriber | 1.9 % / 0.3 % | < 0.5 % per side | 0.4-0.6 % |
| codec `req` (89 B) encode / decode | 0.49 us / 0.85 us | <= 1.0 / <= 1.0 us | 1.9 / 1.2 us |

Missed: rr p50, event p50, stream p99 and publisher CPU. The codec targets are met by direct
paths between Python objects and bytes (`_encode`/`_decode`: no `Value` tree, SPEC 6.1 checked
while decoding); the live path still goes through the core's `Value` because the core hands
callbacks a decoded tree (a decode-to-Python hook in the core would be the next step). The
publisher's CPU includes the benchmark's own Python pacing loop (`time.sleep` plus
`Layout.pack` per sample). Deliveries hop from the connection lane to the interpreter once;
profiling that hop (and the handler-pool hop of rr) is the lead for the misses.

## Python binding after T7b (PRY-085 T7b, 2026-10-23, this PC)

Same command as T7 (`bench.py --repeat 3 --scale 0.5`, medians of 3 runs, release build),
"before" re-measured on the T7 tree the same day, right before the changes.

| Metric | T7 (before) | T7b (after) | Target (plan 6) | Baseline 0.1.4 |
|---|---|---|---|---|
| rr p50 / p99 | 66 / 132 us | 56 / 109 us | <= 45 / <= 100 us | 67 / 93-120 us |
| event 200 Hz p50 (p99) | 80 (154) us | 69 (176) us | <= 50 us | 60 us |
| stream 200 Hz 48 B p50 / p99 | 63 / 129 us | 61 / 147 us | p99 <= 100 us | 38 / 85 us |
| stream CPU publisher / subscriber | 1.0 / 0.8 % | 2.1 / 1.2 % | < 0.5 % per side | 0.4-0.6 % |

The CPU column moves by a factor of two between runs with no code change (Windows accounts
process time in 15.6 ms ticks; T7's own run read 1.9 / 0.3 %): it is over the target either
way and is not changed by T7b.

What was measured (stage floors, `cargo test --release -p stintmate-ipc --test latency --
--ignored --nocapture`, one process, `Instant` = QueryPerformanceCounter):

| Stage | p50 |
|---|---|
| 64-byte ping-pong over a Tokio named pipe, caller in `block_on` (the transport floor) | 13 us |
| rr Rust to Rust, blocking façade, pool handler / inline handler | 40 / 38 us |
| the same without a call deadline (no timer task per call) | 37-38 us |
| rr Python to Python in one process, pool / inline handler (before → after) | 57 → 54 / 52 → 47 us |

So about 25 us of a round trip is the core between the pipe and the caller (thread hops: the
caller to the writer task, the reader to the handler pool and back, the reader to the waiting
caller; a timer task per call), and 10-15 us the binding. Changes:

- **Thread states kept per library thread** (binding): each attach from a handler-pool,
  callback-lane or callback thread created and destroyed a Python thread state; now each
  thread keeps one until it ends. About 3-5 us per handler call or delivery.
- **One task per request** (core): the handler's future is polled by the request's task, a
  panic caught per poll, instead of a second spawned task. About 1-2 us.
- **The request goes out before the deadline's timer task starts** (core). About 1 us.

Left (the misses, for the next step): the event and stream rows and the publisher CPU are
dominated by thread wake-ups, not by encoding (codec rows above: under 1 us). A sample or
event costs a wake of a runtime worker (the subscription pump, then the writer task and the
overlapped write's completion) on the sending side and a wake of the callback lane on the
receiving side; 0.1.4 wrote from the publishing thread and read on a thread that ran the
callback. The lead is a direct write from the caller's thread when the writer queue is
empty (the 0.1.4 shape), which the core's writer task does not allow today. The live path
still converts through `Value` (the core hands decoded trees); `direct.rs` serves `_encode`
/ `_decode`: at 1-2 us per message that is not where the time goes.

## Direct writes and one I/O worker (PRY-085 T7c, 2026-10-23, this PC)

Method: `python/bench/bench.py --high --repeat 5 --scale 0.5 --only rr,event,stream` (every
process in the HIGH priority class, 200 warm-up calls for rr and a 0.5 s settle for the
channels, 5 runs, the median of the per-run medians; the per-run values are printed, which is
the noise figure below). 0.1.4 was re-measured the same day with its own `bench/bench.py`
and the same priority (a copy with one `SetPriorityClass` line, outside the repository).
Rust to Rust: `cargo test --release -p stintmate-ipc --test latency -- --ignored --nocapture
--test-threads 1` (one process, 3000 calls, 1000 events / samples at 200 Hz). The PC was in
use: single runs still jump (one run in five of event or stream had p50 x2-x7 and a p99 of
milliseconds in the "before" set); the medians below were stable to +-3 us (p50) and
+-15 us (p99) across repeated sets. CPU is the least stable column (Windows counts process
time in 15.6 ms ticks; the same build read 0.8 to 2.3 % on the publisher between sets).

| Metric (us; Python to Python unless noted) | T7b (before) | T7c (after) | Target (plan 6) | 0.1.4, same day |
|---|---|---|---|---|
| rr p50 / p99 | 56 / 111 | 45 / 91 | <= 45 / <= 100 | 68 / 104 |
| event 200 Hz p50 (p99) | 84 (205) | 59 (166) | <= 50 | 76 (188) |
| stream 200 Hz 48 B p50 / p99 | 53 / 126 | 38 / 109 | p99 <= 100 | 57 / 135 |
| stream CPU publisher / subscriber | 1.35 / 0.63 % | 0.94 / 0.63 % | < 0.5 % per side | 1.04 / 0.83 % |
| Rust to Rust rr p50 (pool handler) | 40 | 29-34 | | |
| Rust to Rust event 200 Hz p50 (p99) | 49 (150) | 40 (109) | | |
| Rust to Rust stream 200 Hz p50 (p99) | 45 (133) | 37-39 (110) | | |

A 30 s stream run with per-thread CPU (`BENCH_THREADS=1`, same build) splits the subscriber's
0.8 % into the I/O worker (0.4 %) and the callback lane (0.4 %), and the publisher's into its
own Python loop (sleep, `Layout.pack`, `send`: most of it) and the I/O worker (0.2 %, the
write completions). A 3 x 30 s set read 2.3 / 1.3 % for T7c and 0.73 / 1.41 % for 0.1.4.

What changed:

- **Direct writes** (core, `connection.rs`): a sender writes its frames to the pipe from its
  own thread (a non-blocking write) when nothing is queued or being written; otherwise, or
  for what the pipe does not take at once, the bounded queue and the writer task as before.
  A counter of queued items, raised under the write lock, keeps one order per connection.
  The writer task now writes everything queued per wake-up as one write.
- **Offers written by the publishing thread** (node, `pump_direct`): an event, state value
  or sample offered while the connection's pump has nothing queued or in hand and the writer
  is idle is written by the offering thread: no wake-up of the pump task nor of the writer
  per message. The pump takes its turn before its work, so a direct write never overtakes
  work the pump holds. `NodeConfig::direct_writes` (default on) turns it off (a test of the
  owner's queue rules uses that).
- **One I/O worker** for the blocking façade's runtime (was 2): handlers and callbacks run
  on their own threads, so the runtime only moves bytes; one worker drops the work-stealing
  wake-ups between them (rr -2 us, event -4 us p50 in A/B sets).

Still missed: event p50 (59 against 50: the receive side still costs two wake-ups, the I/O
worker on the pipe's completion and the callback lane; 0.1.4 ran the callback on its reader
thread) and the stream p99 by about 10 us, which on this PC is scheduling noise (the bare
pipe's own p99 is 25-50 us). Both are better than 0.1.4 on the same day. The CPU target
(< 0.5 % per side) is not met by either implementation on this PC; T7c is at 0.1.4's level.

## Final (PRY-085, before the cutover, 2026-10-23, this PC)

Tree: `rust` head `6ce0ee7` (T7c's fast path plus T7d and T7e), release build of the binding
into a venv in the worktree (`maturin develop --release`). Method: exactly that of the T7c
section: `python/bench/bench.py --high --repeat 5 --scale 0.5 --only rr,event,stream` (every
process in the HIGH priority class, 5 runs, the median of the per-run medians) and, Rust to
Rust, `cargo test --release -p stintmate-ipc --test latency -- --ignored --nocapture
--test-threads 1`. 0.1.4 (tag `v0.1.4`, extracted with `git archive` into its own venv) ran its
own `bench/bench.py` with one `SetPriorityClass(HIGH)` line added at the start of `main`, the
same parameters. The sets were alternated in one session (Rust, 0.1.4, 0.1.4, Rust, 0.1.4,
Rust, 0.1.4: 3 Rust sets, 4 0.1.4 sets) and Rust to Rust ran 3 times; the ranges below are the
spread across sets. The codec is `_native._encode/_decode` against 0.1.4's `wire.pack/unpack`
on the 89 B `req` of `python/bench/codec.py` (best of 5 x 200 000, 3 repetitions).

| Metric (us; Python to Python) | Final Rust (3 sets) | 0.1.4 (4 sets) | T7c as published |
|---|---|---|---|
| rr p50 / p99 | 43-45 / 56-74 | 67-70 / 95-121 | 45 / 91 |
| event 200 Hz p50 (p99) | 46-56 (117-133) | 61-78 (153-165) | 59 (166) |
| stream 200 Hz 48 B p50 / p99 | 33-39 / 93-99 | 40-41 / 101-106 | 38 / 109 |
| stream CPU publisher / subscriber (%) | 0.62-1.04 / 0.73-0.83 | 0.73-1.04 / 0.73-0.83 | 0.94 / 0.63 |
| codec `req` (89 B) encode / decode | 0.49 / 0.84-0.89 | 1.30 / 1.00 | 0.49 / 0.85 |

Per-set medians (p50 / p99): Rust rr 45/74, 43/60, 44/56; event 56/133, 48/124, 46/117; stream
39/99, 35/98, 33/93. 0.1.4 rr 70/121, 68/102, 69/98, 67/95; event 78/165, 64/167, 61/156,
61/153; stream 41/101, 40/104, 40/106, 40/104. Within a set, the Rust per-run p50 stays within
+-4 us (one run of 50 against 43 in a set), the 0.1.4 per-run p50 within +-3 us.

| Rust to Rust (us, p50 (p99), 3 runs) | Final | T7c as published |
|---|---|---|
| rr, pool handler | 28.4 (45.8), 30.4 (45.4), 28.7 (64.6) | 29-34 |
| rr, inline handler | 25.7, 29.3, 25.4 (p99 48-50) | |
| event 200 Hz | 32.2 (108.6), 31.2 (92.3), 32.4 (83.5) | 40 (109) |
| stream 200 Hz | 29.0 (90.7), 27.5 (78.4), 28.8 (91.0) | 37-39 (110) |
| transport floor (64 B ping-pong) | 12.3-12.9 (p99 33-41) | 13 |

Verdict. No row is worse than 0.1.4 beyond the noise. The Python binding is ahead of 0.1.4 on
rr (about 25 us at p50 and 30-40 us at p99), on event (10-30 us at p50) and on the codec
(2.7x faster to encode, 1.2x to decode); stream p50 is 5-7 us ahead and stream p99 is equal to
slightly ahead (93-99 against 101-106 us, the gap is within the +-8 us spread of p99 across
sets). CPU is level with 0.1.4 (0.6-1.0 % per side on both; the Windows 15.6 ms tick makes this
column unreliable and the < 0.5 % target is met by neither). Against T7c's published numbers
the final tree is equal or slightly better (event p50 46-56 against 59, stream p50 33-39
against 38; Rust to Rust event and stream are 8-10 us lower than in T7c, which T7c's own
caveat on a busier PC may explain), so T7d and T7e (0.1.4 semantics, `Peer.closed`, layout)
cost nothing measurable. Still missed against plan 6: event p50 <= 50 us in the first Rust set
(56; 46 and 48 in the other two) and the CPU target.

## 0.3.0: Python through the DLL (2026-10-26, this PC)

The ctypes package over `stintmate_ipc.dll`, alternated with v0.2.0 in one session, Python to
Python, microseconds p50 / p99.

| Metric | 0.3.0 (DLL) | v0.2.0 |
|---|---|---|
| rr | 73.3 / 116.9 | 45.6 / 75.5 |
| event 200 Hz | 102.0 / 202.6 | 57.8 / 135.4 |
| stream 200 Hz, 48 B | 54.8 / 116.9 | 38.6 / 106.8 |

The wire, the I/O thread, the pool hand-off and the wake are equal within 0.5 us. The
difference is Python/ctypes marshalling: msgpack in Python, decode and re-encode in the DLL,
four ctypes crossings. In-process Rust through the C ABI: rr 36 / 69.

Stream CPU at 200 Hz (desk, 6 alternated rounds, median):

| Role | 0.3.0 | v0.2.0 |
|---|---|---|
| publisher | 1.56 % | 1.22 % |
| subscriber | 1.30 % | 0.86 % |
