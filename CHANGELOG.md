# Changelog

The specification (SPEC.md) and the Python package are versioned separately: the
specification by wire version and revision (2.0), the package by semantic versioning.

### Package 0.1.3, 2026-10-22

- Public filtered publish: `EventChannel.publish(data, *, ts=None, version=None, accept=None)`.
  `accept(view)` gets a read-only `SubscriptionView` (`peer`: `PeerInfo`, `pid`, `version`,
  `options`: read-only map, empty when none; `connection`: the `Peer` it arrived on, as `Context.peer`) and returns whether that subscription takes the
  event; a raising `accept` skips its subscription (logged). The payload is packed once, and
  not at all when nobody takes it. Returns the number delivered; never blocks. Without
  `accept` nothing changes. Replaces walking the private `_subs` / `Peer._offer_event`.
- `SubscriptionView` is exported. SPEC.md 19.6 (guide) on subscription options. No wire change.

### Package 0.1.2, 2026-10-22

- Public cancel: `PendingCall.cancel(reason="") -> bool` sends `cancel`, fails the call with
  the new `CallCancelled` (not a `CallTimeout`), runs its done callbacks on the calling
  thread and ignores the late `res`. False when the call was already done or its response
  won the race. Callable from any thread, a callback or an inline handler. No wire change.
- A call's deadline is now enforced without a waiter: the heartbeat tick fails expired calls
  with `CallTimeout` (SPEC.md 15), so a call followed only by done callbacks ends on time.
  One scan of the in-flight calls per 0.25 s tick (26 us for 1000 calls).
- asyncio: cancelling the awaiting task, or an `asyncio.wait_for` timeout, sends `cancel`.
- Vectors `cancel_unknown_id` and `res_after_cancel`; conformance check `S24` (a `cancel`
  for an unknown id is ignored).
- Licensed under the Apache License 2.0 (LICENSE, NOTICE).
- `Peer._call_timed_out` is replaced by the private `Peer._give_up`.

### Specification revision 2.0 (wire version 2), 2026-10-20

First revision of Stintmate IPC v2, the successor of the 2026 Pitwall connector protocol
(v1, JSON Lines). Named pipes with a user-only DACL; endpoint names per app and process
and an optional registry; frames of three kinds (messages, stream samples, chunks);
a MessagePack subset; mutual HMAC-SHA256 handshake with the workspace key; declarations
of versioned methods and channels; request/response with deadlines, progress, cancel and
idempotency keys; event, state and stream channels with snapshots and per-kind
slow-subscriber rules; ordering guarantees; QPC timestamps; heartbeat; reconnection;
conformance; security considerations; implementer's guide with C# and Rust sketches.

### Package 0.1.1

- The idempotency cache never stores answers with retryable codes (`shutting_down`,
  `writer_busy`, `busy`, `unavailable`, ...): a retry with the same `ik` runs the handler
  again (SPEC.md 10.4).
- `Endpoint.method(..., idempotency_cache=True, idempotency_ttl=None)`: opt out of the
  cache or forget answers after a TTL.
- A repeat waiting on an in-flight request with the same `ik` has a deadline (the
  request's, or 30 s) and answers `busy` with `retry_ms` instead of hanging.
- `EventChannel.send_to(peer, data, ...)` and `StateChannel.send_to(peer, value, ...)`:
  public per-subscriber sends.
- `Client.reconnect_delay(code, attempt)`: reconnect delay by `bye` code (SPEC.md 15.3).
- `stintmate-ipc-check` leaves the process at once after its run.

### Package 0.1.0 (not tagged yet)

- `Endpoint`, `Client`, `connect`, `Peer`: symmetric connections, handler threads,
  ordered and inline methods, idempotency cache, bounded queues, chunked messages,
  heartbeat, reconnect with re-subscription, endpoint registry.
- Key file helpers (atomic creation with a user-only DACL), names, layouts, declarations.
- `stintmate_ipc.aio`: asyncio bridge.
- `stintmate-ipc-check`: server and client conformance checks, `list`, `keygen`.
- Test vectors and their generator; benchmarks.
