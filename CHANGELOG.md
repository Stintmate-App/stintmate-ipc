# Changelog

## 0.3.1+docs.1 (2026-11-01)

- Releases are published by the `release` workflow (`.github/workflows/release.yml`, run by hand):
  it checks the tag, runs the CI checks, builds the DLL and the conformance tool, and uploads
  them with the header and `SHA256SUMS`. README, "Releases". The code is unchanged.
- The Python wheel is no longer part of this repository's releases; it is published by the
  `stintmate-ipc-python` repository.

## 0.3.1 (2026-10-31)

- The wire format is unchanged (SPEC revision 2.0), and so are the C ABI (1.0) and the public
  Rust API. A connection now refuses, with `bye protocol_error`, messages that were accepted
  before:
  - an `evt` without `d`, a successful `res` without `r`, and an error `res` whose `e` has no
    `msg` (a nil `d` or `r` still counts as given);
  - a `sub` whose `id` is still in flight as a `req` or another `sub`, and a `req` whose `id` is
    still in flight as a `sub` (the id is free again once its `res` is written).
  Every sender in this repository already wrote those fields and used a fresh id each time.
- A panic in the filter of `EventChannel::publish_filtered` is caught and logged: that
  subscription skips the event and the others receive it. Before, the panic reached the
  publisher and the later subscriptions got nothing. SPEC 19.6 states the guarantee.
- `vectors/errors.json` gains the three missing-field cases. The SPEC status is now Stable.

## 0.3.0+docs.1 (2026-10-31)

- Documentation corrected against the code. The behaviour and the protocol are unchanged.

## 0.3.0 (2026-10-26)

- ABI 1.0: a complete C ABI for `stintmate_ipc.dll` (nodes, peers, calls, subscriptions,
  endpoints, clients, thread hooks, shutdown, reconnect policy); every language uses the DLL.
- The Python binding (`python/`) and the C and C# examples are removed.
- The core public API only gains items: `Peer::id`, `PendingCall::on_done`, the reconnect
  policy raced against stop.
- The protocol is unchanged (SPEC revision 2.0).

Renamed / changed in the C ABI since 0.2.0:

- `stintmate_ipc_endpoint_new(info, key, opts, out)` (was `pipe, info, key_path,
  handler_threads, out`).
- `stintmate_ipc_client_new` gains `key` and `opts`.
- `stintmate_ipc_endpoint_method` is `stintmate_ipc_method` on a node, with `method_opts` and
  a `release`; the subscription entry points (`client_subscribe`, `peer_subscribe`,
  `on_subscribe`) take a `release` as well.
- `stintmate_ipc_pipe_path` gains `user`.
- `stintmate_ipc_ensure_key` gains `key_out`.
- `E_REMOTE` reads the error record (`stintmate_ipc_last_error`).
- `peer_info` is `stintmate_ipc_peer_get_info`.
- The `stintmate_ipc_call_cancel` reason is ignored by the protocol (for the caller's logs).
- A timeout of 0 means "do not wait" (`STINTMATE_IPC_FOREVER`, -1, means none).
- Handshake errors carry their `code`.

## 0.2.0 (2026-10-23)

- The reference implementation in Rust: core crate, conformance tool, C ABI and the Python
  binding (PyO3, abi3). Replaces the Python implementation of 0.1.x entirely; the protocol
  (`SPEC.md`, revision 2.0) and its test vectors are unchanged.

Earlier 0.1.x tags exist but are not listed here.
