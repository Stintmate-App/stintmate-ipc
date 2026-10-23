# Stintmate IPC

Stintmate IPC v2 is the protocol the Stintmate apps and connectors talk over: Windows named
pipes, MessagePack frames, an authenticated handshake with a per-user key file, calls,
events, state and stream channels. The protocol is defined in [`SPEC.md`](SPEC.md); the
byte-exact test vectors are in [`vectors/`](vectors/).

This repository holds the reference implementation, written in Rust:

| Path | What |
|---|---|
| `crates/stintmate-ipc/` | the core library (Rust API, async on Tokio, plus a blocking façade) |
| `crates/stintmate-ipc-check/` | `stintmate-ipc-check`, the conformance tool |
| `crates/stintmate-ipc-ffi/` | the C ABI (`stintmate_ipc.dll`), for C, C++, C# and other languages |
| `python/` | the Python binding (`stintmate-ipc` package, PyO3, abi3 wheel for Python 3.12+) |

**Status:** being built (version 0.2.0 in progress). Until it is released, the skeleton only
reports its version.

## Building

Requirements on Windows: Rust (the version in `rust-toolchain.toml`, installed by rustup) with
the MSVC target and the Visual Studio Build Tools C++ workload; [uv](https://docs.astral.sh/uv/)
for the Python binding.

```
cargo build --workspace
cargo test --workspace
uvx maturin build --release --manifest-path python/Cargo.toml   # the Python wheel
```

A Python project can depend on the binding by path; uv rebuilds it when the Rust sources
change (`[tool.uv] cache-keys` in `python/pyproject.toml`):

```toml
[tool.uv.sources]
stintmate-ipc = { path = "<this repository>/python", editable = true }
```

## Licence

Apache License 2.0 ([`LICENSE`](LICENSE), [`NOTICE`](NOTICE)). Every dependency compiled into
the library is permissively licensed (checked by `cargo deny`, [`deny.toml`](deny.toml)).
