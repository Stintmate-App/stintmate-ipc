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
