# Benchmarks

These figures were measured during development, on Windows 11 on a desktop PC in use at the
time (no isolated benchmark machine), and have not been re-measured on the v0.3.0 tag.
Treat them as orders of magnitude for this implementation, not as guarantees. Latencies are in
microseconds unless stated.

## Codec

Command: `cargo bench -p stintmate-ipc --bench codec` (criterion 0.8, release profile with
thin LTO). Value: the median of criterion's estimate. The `req` is the one of the
`vectors/frames.json` vector (102 bytes, nested params with a `bin`), as an `rmpv::Value` tree.

| Benchmark (name in `benches/codec.rs`) | Time |
|---|---|
| `req encode` (validate + write) | 0.20 us |
| `req decode` (validating decoder, to `Value`) | 1.48 us |
| `req decode + validate` (decode + message shape check) | 1.55 us |
| `stream frame write` (48-byte body) | 8 ns |
| `stream frame read` (push + `next_frame`) | 28 ns |
| `layout 12xf32 pack` | 20 ns |
| `layout 12xf32 unpack` | 39 ns |

Decoding allocates a `String` per map key and a `Vec` per container (about 25 allocations for
this `req`).

## Latency, Rust to Rust

Command: `cargo test --release -p stintmate-ipc --test latency -- --ignored --nocapture
--test-threads 1`. One process, the blocking facade over real named pipes with random names;
3000 request/response calls, then 1000 events and 1000 stream samples (48 bytes) at 200 Hz,
timed from the publishing call to the subscriber's callback; times from `Instant` (the
performance counter). Three runs, p50 (p99):

| Metric | Result |
|---|---|
| Transport floor: 64-byte ping-pong over a Tokio named pipe | 12.3-12.9 (p99 33-41) |
| Request/response, pool handler | 28.4 (45.8), 30.4 (45.4), 28.7 (64.6) |
| Request/response, inline handler | 25.7, 29.3, 25.4 (p99 48-50) |
| Event, 200 Hz | 32.2 (108.6), 31.2 (92.3), 32.4 (83.5) |
| Stream sample, 200 Hz | 29.0 (90.7), 27.5 (78.4), 28.8 (91.0) |

Notes on the method: the machine was in use, so single runs can jump (a p99 of milliseconds
appeared occasionally and the p50 of one run in five could double). The blocking facade runs
the Tokio runtime with one worker thread. A sender writes to the pipe from its own thread when
nothing is queued (`direct_writes`, on by default; `EndpointBuilder::direct_writes(false)`
turns it off).

CPU usage was measured only through a Python binding that is not part of this repository and
is not reported here.
