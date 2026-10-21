# Benchmarks

The reference implementation on the reference PC, measured 2026-10-20.

- PC: Intel Core i7-10700K (8 cores / 16 threads), 32 GB, Windows 11 Pro 26200,
  CPython 3.12.15, msgpack 1.2.3. The driver's Stintmate apps idle in the background.
- Run: `uv run python bench/bench.py --repeat 3 --json bench/results/<name>.json`
  (`--quick` for a short run, `--only rr,event,stream,stream3`, `--deliver`, and
  `BENCH_INLINE=1` for an inline request handler).
- Every role is a separate process. Latency is the receiver's `perf_counter_ns()` minus the
  sender's stamp (one QueryPerformanceCounter clock for every process). CPU % is
  (user + system time) / wall time of the process, 100 % = one core; Windows accounts CPU
  in 15.6 ms ticks.
- Scenarios, as in the PRY-056 research of 2026-10-19: **rr** 5000 request/response round
  trips (~60-byte params, the handler echoes them), after 200 warm-up calls; **event** 1000
  events (~120 bytes) at 200 Hz to one subscriber; **stream** 200 Hz samples of a 48-byte
  layout (12 x f32; 73-byte frames) for 30 s to one subscriber; **stream x3** the same for
  15 s to three subscribers. The publisher is paced with `time.sleep` on a thread (it
  wakes 0.3 ms late at p50, as in the research).
- Each figure is the **median of 3 runs** (max: the largest of the 3).

## Results (defaults)

In microseconds, p50 / p95 / p99 / max:

| Scenario | n | p50 | p95 | p99 | max | CPU publisher | CPU subscriber | Target | |
|---|---|---|---|---|---|---|---|---|---|
| rr | 15000 | **67** | 80 | 93 | 323 | - | - | p50 <= 60 | missed by 7 us (inline handler: 58, below) |
| event | 3000 | **60** | 99 | 126 | 332 | 0.31 % | 0.94 % | p50 <= 80 | met |
| stream 200 Hz | 18000 | 38 | 66 | **85** | 179 | **0.62 %** | **0.36 %** | p99 <= 200, CPU < 1 % | met |
| stream 200 Hz x3 | 27000 | 57 | 86 | **110** | 367 | 0.52 % | 0.42 / 0.52 / 0.63 % | | met |

Every message and sample was delivered in every run (no drops at 200 Hz). Defaults: event
callbacks run in the connection's callback lane (`deliver="thread"`), stream callbacks on
the reader (`deliver="auto"` = inline for streams), handlers on worker threads.

## Variants

| Variant | p50 | p95 | p99 | CPU sub | |
|---|---|---|---|---|---|
| rr, handler `inline=True` | **58** | 75 | 90 | | The handler runs on the server's reader: no worker hand-off. For handlers that answer from memory. |
| stream, `deliver="thread"` | 47 | 76 | 97 | 1.04 % | The callback lane's thread hand-off per sample costs ~0.5 % of a core at 200 Hz; this is why streams default to inline. |
| stream, `deliver="inline"` (explicit) | 41 | 66 | 86 | 0.47 % | Same as the default. |

## Deadline sweep (0.1.2)

The heartbeat thread (every 0.25 s) fails the calls whose deadline passed, so a call nobody
waits on still times out (SPEC.md 15). Measured 2026-10-22 on the same PC with
`uv run python bench/bench.py --only sweep`: one sweep over **1000 in-flight calls**, none
expired (the worst case for the scan), p50 **26 us**, p99 30 us, max 31 us (200 sweeps).
Normally 0-3 calls are in flight: the sweep is a few microseconds every 0.25 s.

## Baselines on the same PC, same night

| Transport | rr p50 | event p50 | stream p50 | |
|---|---|---|---|---|
| This package's raw pipe layer (`_win32.PipeHandle`, 50-byte ping-pong on one thread each side, no protocol) | 16-21 | | | the floor of ctypes overlapped I/O |
| Research `mpconn` (stdlib `multiprocessing.connection`, blocking `recv` on the main thread; `--quick`) | 22.6 | 44.3 | 31.3 | no multiplexing: one blocking reader is also the caller |
| Research `mpconn`, 2026-10-19 | 23 | 31 | 28 | |

The paced one-way latency of the raw pipe layer (a 200 Hz sender and a blocked reader)
measured 39-62 us p50 on different runs this night: a thread that sleeps between samples
pays the operating system's wake-up, and that dominates the event and stream figures.

## Where a round trip goes (default rr, p50)

Measured by stamping each step in an instrumented copy of the benchmark (not part of the package):

| Step | us |
|---|---|
| caller builds and writes the `req`; the server's reader wakes | 24 |
| server parses it, hands the reading on, runs the handler | 16 |
| handler's `res` is written; the client's reader wakes | 18 |
| client parses the `res` | 5 |
| the waiting caller wakes | 8 |

A blocking single-threaded client (the research's `mpconn`) has two thread wake-ups per
round trip; a multiplexed peer, which must read responses, events and streams while the
caller waits, has four (two readers, the handler, the caller). The leader/follower reader
(the thread that read a request runs its handler and hands the reading to another worker)
removed the handler's wake-up from the critical path (it was ~35 us with a thread pool);
`inline=True` removes the rest of it. Reaching 60 us with a worker-thread handler would need
fewer Python steps than CPython allows per wake-up, or a C extension; neither is worth it
for the apps' calls (milliseconds of work each).

## CPU

The 200 Hz stream costs 0.4-0.6 % of a core on each side with the defaults: one `struct`
pack, one lock and one `WriteFile` per sample at the publisher (nothing at all while nobody
subscribes); one wake-up, one parse and the callback at the subscriber. Requests and events
at a few per second cost nothing measurable. An idle connection costs a `ping` per 5 s.
