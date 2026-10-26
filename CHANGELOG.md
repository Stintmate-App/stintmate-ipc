# Changelog

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
