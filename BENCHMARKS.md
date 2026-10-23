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
