# Test vectors

Language-neutral vectors for Stintmate IPC v2 ([SPEC.md](../SPEC.md)). Every
implementation should load them in its own tests: decode each `hex`, compare with the
expected content, compute the MACs, and feed the error cases to its receiver.

| File | What |
|---|---|
| `frames.json` | Frames after the handshake: requests, responses, errors, subscriptions, events, a stream sample with its layout, a chunked message with a stream sample between its chunks, a reserved kind, unknown types and keys. |
| `handshake.json` | A full handshake with a fixed key and nonces: the key file bytes, the exact MAC inputs and outputs, the three frames (`hello`, `auth`, `welcome`), a wrong key, and the MAC for another pipe name. |
| `errors.json` | 20 stateless cases: bytes received after a handshake that must end the connection with `bye protocol_error` (receiver `max_frame` 1048576). It is not the full list of protocol errors: those that need state (a reused request id, an invalid declaration, an `evt` or kind-2 frame against the subscription's kind, a stream body of the wrong size for a known `sid`) and trailing data after a MessagePack value have no vector. |
| `keyfile.json` | Valid and invalid key file contents. |
| `layouts.json` | Stream layouts, values and the packed bytes (every field type, arrays). |

## Conventions

- `hex`: lowercase hexadecimal of the bytes on the wire (frames included: length, kind,
  payload). Several frames may follow each other in one `hex`.
- Decoded MessagePack values are JSON, with two escapes: `{"$bin": "<hex>"}` is a
  MessagePack `bin`, `{"$int": "<decimal>"}` an integer outside ±2^53 (so JavaScript and
  JSON parsers keep it exact).
- Floats are float64 unless a layout says `f32`.
- **Decoding is what must match.** MessagePack allows several encodings of one value (an
  int in a wider form, map keys in another order); the bytes here are what the reference
  encoder writes (shortest forms, keys in the order listed). An implementation that
  re-encodes the listed message in that order with shortest forms gets the same bytes.
- `frames.json` cases with an `action` say, in free text, what the receiver does with that frame; the Rust tests decode and re-encode the frames (reserved kinds excepted) but do not execute the `action` text.

The Rust tests (`crates/stintmate-ipc/tests/vectors.rs` and `handshake.rs`) load these files
and fail if the reference code disagrees with them. They feed each `hex` to the frame reader whole and one byte at a time. For `errors.json` they check that the frame reader, the MessagePack decoder or the message validator reports an error; whether the connection then sends `bye` `protocol_error` is exercised by the connection tests, not by these files. This repository holds no generator
script: the files are the source.
