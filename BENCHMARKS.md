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
