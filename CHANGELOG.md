# Changelog

The specification (SPEC.md) and the Python package are versioned separately: the
specification by wire version and revision (2.0), the package by semantic versioning.

## Unreleased

### Specification revision 2.0 (wire version 2), 2026-10-20

First revision of Stintmate IPC v2, the successor of the 2026 Pitwall connector protocol
(v1, JSON Lines). Named pipes with a user-only DACL; endpoint names per app and process
and an optional registry; frames of three kinds (messages, stream samples, chunks);
a MessagePack subset; mutual HMAC-SHA256 handshake with the workspace key; declarations
of versioned methods and channels; request/response with deadlines, progress, cancel and
idempotency keys; event, state and stream channels with snapshots and per-kind
slow-subscriber rules; ordering guarantees; QPC timestamps; heartbeat; reconnection;
conformance; security considerations; implementer's guide with C# and Rust sketches.

### Package 0.1.0 (not tagged yet)

- `Endpoint`, `Client`, `connect`, `Peer`: symmetric connections, handler threads,
  ordered and inline methods, idempotency cache, bounded queues, chunked messages,
  heartbeat, reconnect with re-subscription, endpoint registry.
- Key file helpers (atomic creation with a user-only DACL), names, layouts, declarations.
- `stintmate_ipc.aio`: asyncio bridge.
- `stintmate-ipc-check`: server and client conformance checks, `list`, `keygen`.
- Test vectors and their generator; benchmarks.
