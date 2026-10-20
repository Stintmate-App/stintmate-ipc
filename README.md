# stintmate-ipc

**Stintmate IPC v2**: the open protocol the programs of a Stintmate installation use to
talk to each other on one Windows PC, and its Python reference implementation.

- **[SPEC.md](SPEC.md)**: the protocol. Windows named pipes, current user only; a mutual
  HMAC-SHA256 handshake with a key in the user's workspace; length-prefixed frames carrying
  MessagePack messages and fixed-layout binary stream samples. Three primitives:
  request/response, event and state channels with subscription (snapshot on subscribe),
  high-rate streams. Brokerless: each program serves its own endpoint.
- **[vectors/](vectors/README.md)**: language-neutral test vectors (frames, a full
  handshake with fixed key and nonces, error cases, key files, layouts).
- **`stintmate_ipc`**: the Python package (Python >= 3.12, Windows; depends on `msgpack`
  only; pipe I/O through `ctypes`).
- **`stintmate-ipc-check`**: a conformance checker for any implementation, server or client.
- **[BENCHMARKS.md](BENCHMARKS.md)**: latency and CPU on the reference PC.

Status: 0.1.0, draft protocol revision 2.0. Not published yet; the licence is to be
decided before it is.

## Install

From another uv project (the Stintmate apps use a path dependency):

```toml
[project]
dependencies = ["stintmate-ipc"]

[tool.uv.sources]
stintmate-ipc = { path = "../../packages/stintmate-ipc", editable = true }
```

For development of the package itself: `uv sync`, then `uv run pytest -q`.

## Quick start

A server (an **endpoint**) declares methods and channels and starts:

```python
from stintmate_ipc import Endpoint, KeyFile, Layout, PeerInfo, RemoteError, default_key_path

key = KeyFile(default_key_path(), create=True)          # <workspace>\ipc\key
ep = Endpoint(PeerInfo("pitwall", name="Pitwall", version="0.9.0"), key,
              pipe=r"\\.\pipe\stintmate-pitwall-leona")  # default: from app/process + user

@ep.method("pitwall.write", ordered=True, idempotent=True)
def write(ctx, params):                                  # runs on a handler thread
    if params.get("type") not in ("lap", "session_start"):
        raise RemoteError("bad_request", "type: unknown message type")
    return {"stored": True, "caller_pid": ctx.peer.pid}

laps = ep.event_channel("pitwall.lap_completed")
status = ep.state_channel("pitwall.connectors", initial={"connected": []})
ep.start()

laps.publish({"lap": 12, "time_s": 91.234})              # never blocks
status.set({"connected": ["connector-ams2"]})            # subscribers get it (and new ones the snapshot)
```

A client (a connector) connects, keeps the connection, offers a stream and calls:

```python
from stintmate_ipc import Client, KeyFile, Layout, PeerInfo, default_key_path, pipe_path

TELEMETRY = Layout([("speed_kmh", "f32"), ("throttle", "f32"), ("brake", "f32"),
                    ("gear", "i8"), ("flags", "u8"), ("rpm", "u16")], name="telemetry")

c = Client(pipe_path("pitwall"),
           PeerInfo("connector-examplesim", name="ExampleSim connector", version="1.2.0",
                    process="recorder", meta={"sim": "examplesim"}),
           KeyFile(default_key_path()))
c.method("connector.live_frame", lambda ctx, p: {"speed_kmh": 212.5})  # Pitwall calls it
tel = c.stream_channel("connector.telemetry", TELEMETRY, rate_hz=200)
c.connect(timeout=5)                                     # then reconnects by itself

c.call("pitwall.write", {"type": "lap", "lap": 12}, timeout=30,
       idempotency_key="examplesim/9f3c2a71/lap/12")
c.subscribe("pitwall.lap_completed", lambda ev: print(ev.seq, ev.data))

# in the 200 Hz loop: costs nothing while nobody subscribes, never blocks
tel.send_values(212.5, 1.0, 0.0, 5, 0, 7400)
```

## API summary

| Name | What |
|---|---|
| `Endpoint(info, key, *, pipe=None, registry=None, handshake_timeout=5.0, **node)` | Server. `.start()` (raises `EndpointInUse`), `.stop()`, `.peers`, `.run`, `.path`; `on_connect`, `on_disconnect`, `on_handshake_failed` hooks. |
| `Client(pipe, info, key, *, reconnect=True, **node)` | Persistent client. `.start()`, `.connect(timeout)`, `.wait_connected(timeout)`, `.peer`, `.connected`, `.call(...)`, `.call_async(...)`, `.subscribe(...)` (kept across reconnects), `.close()`; `on_connect`, `on_disconnect`, `on_connect_error`. `.reconnect_delay(code, attempt=0) -> float | None` is the reconnect policy by the `bye` code that ended the last connection or attempt: `shutting_down` 0.25 then 0.5 s, `unauthorized` 10 s (`UNAUTHORIZED_DELAY_S`), `version_unsupported` stop (`None`; see `last_error`), else the `BACKOFF` ladder; override it to change the policy. |
| `connect(pipe, info, key, *, timeout=5.0)` | One connection, no reconnect; returns a `Peer`. |
| node options (both) | `handler_threads=8`, `heartbeat_ms=5000`, `max_frame=1 MiB`, `max_message=256 MiB`, `event_queue=1024`, `event_queue_bytes=8 MiB`, `max_inflight=1024`, `features=()`. |
| `.method(name, handler=None, *, versions=(1,), ordered=False, idempotent=False, idempotency_cache=True, idempotency_ttl=None, inline=False, doc=None)` | Offer a method (decorator or direct). `handler(ctx, params) -> result`; raise `RemoteError(code, msg, details, retry_ms)` to answer an error. Idempotent methods remember answers by `ik` (process lifetime by default; retryable error answers are never remembered): `idempotency_cache=False` declares the method but keeps no cache (the handler deduplicates), `idempotency_ttl=seconds` forgets answers after that long. |
| `.event_channel(name, *, versions=(1,))` -> `EventChannel` | `.publish(data, ts=None, version=None)`; `.send_to(peer, data, ts=None, version=None)` -> int queues it for one peer's subscriptions only (a reply to a single subscriber; `peer` from `SubscribeRequest.peer` or `Context.peer`). |
| `.state_channel(name, initial=None, *, versions=(1,))` -> `StateChannel` | `.set(value)`, `.value`; `.send_to(peer, value, ts=None)` -> int pushes a value to one peer's subscriptions only (the channel value is unchanged). |
| `.stream_channel(name, layout, *, versions=(1,), rate_hz=None, queue=None)` -> `StreamChannel` | `.send(body, t_ns=None)`, `.send_values(*values, t_ns=None)`. |
| `Channel` | `.subscriber_count`, `.active`, `.on_active(bool)` (first subscriber came / last left), `.on_subscribe(req)` (may refuse), `.close()`. |
| `Peer` | One connection: `.info` (`PeerInfo`), `.pid` (from the OS), `.decl`, `.run`, `.call(method, params=None, *, version=1, timeout=5.0, idempotency_key=None, on_progress=None)`, `.call_async(...)` -> `PendingCall`, `.subscribe(channel, callback, *, version=1, options=None, on_end=None, deliver="auto")` -> `Subscription`, `.ping()`, `.close(code, message)`. |
| `Context` (handlers) | `.peer`, `.method`, `.version`, `.params`, `.idempotency_key`, `.deadline`, `.remaining`, `.cancelled`, `.wait_cancelled(timeout)`, `.progress(data, extend_s=None)`. |
| `Event` / `Sample` (callbacks) | `Event`: `.channel`, `.seq`, `.ts`, `.data`, `.snapshot`. `Sample`: `.seq`, `.t_ns`, `.dropped`, `.body`, `.values()`, `.as_dict()`. |
| `Layout(fields, name=None)` | `[(name, type[, count])]`, types `u8 i8 u16 i16 u32 i32 u64 i64 f32 f64`; `.size`, `.pack(*v)`, `.pack_dict(d)`, `.unpack(b)`, `.unpack_dict(b)`. |
| `PeerInfo(app, name="", version="", process=None, meta={})` | Who this program is. |
| `KeyFile(path, create=False)`, `default_key_path(workspace=None)`, `ensure_key(path)`, `read_key(path)` | The key file (`<workspace>\ipc\key`), created atomically if missing. |
| `pipe_path(app, process=None, user=None)`, `short_name(...)`, `find_workspace()`, `list_endpoints(dir)`, `registry_dir(ws)` | Names and discovery. |
| Errors | `RemoteError` (`.code`, `.message`, `.details`, `.retry_ms`), `CallTimeout`, `ConnectionLost`, `NotConnected`, `HandshakeError` (`.code`), `EndpointInUse`, `PipeNotFound`, `KeyFileError`, `ProtocolError`. |
| `stintmate_ipc.aio` | `AsyncClient(client)`: `await .call(...)`, `async with .subscribe(channel) as events: async for ev in events`, `wrap_call(pending)`. |

## Threads, and embedding in an app

The package is thread-based (the pattern of Pitwall's core: overlapped Win32 I/O, the GIL
released while a thread waits). Per connection: a sender thread (the only writer, holding
the bounded queues) and a reader that hands work to a cache of worker threads.

- **Handlers** run on worker threads (at most `handler_threads` at once per node);
  `ordered=True` methods one at a time per connection; `inline=True` methods on the reader
  (only for handlers that answer from memory and never block).
- **Subscription callbacks** of event and state channels run, by default, one at a time
  per connection in arrival order on a worker (`deliver="thread"`): a callback may block or
  call the peer. A slow callback gets the protocol's rules locally: state keeps the latest,
  events end the subscription `overloaded` (the `Client` subscribes again after 1 s).
  **Stream** callbacks run, by default, on the connection's reader (`deliver="inline"`):
  they must be quick (store the sample, hand it on) and cannot wait on the peer
  (`call()` raises there; `call_async()` works). That halves the subscriber's CPU per
  stream. `deliver="thread"` or `"inline"` overrides the default (`"auto"`) for any kind;
  a slow `"thread"` stream callback drops the oldest samples.
- **Producers never block**: `publish`, `send_to`, `set`, `send` and `call` only queue when the pipe is
  busy; the 200 Hz loop of a recorder costs one `struct.pack` and a lock.

**Plain threaded apps**: use it as above.

**asyncio apps** (an MCP server): keep the threads, await results with the bridge:

```python
from stintmate_ipc.aio import AsyncClient
client = Client(pipe_path("pitwall"), info, key).start()
ac = AsyncClient(client)
await ac.wait_connected(5)
state = await ac.call("pitwall.live_state")
async with ac.subscribe("pitwall.lap_completed") as events:
    async for ev in events:
        ...
```

Handlers of an asyncio app run on worker threads; run a coroutine there with
`asyncio.run_coroutine_threadsafe(coro, loop).result()` (or `anyio.from_thread.run`). Do not
time anything with asyncio on Windows: its timers have 15.6 ms resolution.

**Qt apps** (the control apps): callbacks arrive on worker threads; never touch widgets
from them. Emit a signal of a `QObject` living in the GUI thread; Qt queues it across
threads:

```python
from PySide6.QtCore import QObject, Signal

class Bridge(QObject):
    lap = Signal(object)          # connected to a slot in the GUI thread

bridge = Bridge()                 # created in the GUI thread
bridge.lap.connect(window.on_lap)
client.subscribe("pitwall.lap_completed", bridge.lap.emit)   # emit is thread-safe
```

Calls from the GUI thread should be `call_async(...)` with `add_done_callback` emitting a
signal, so the GUI never waits on the pipe.

## The conformance checker

`stintmate-ipc-check` tests an implementation of the protocol, in any language.

```text
stintmate-ipc-check server <pipe> [--key-file F] [--listen 2] [--channel C ...]
                                  [--call METHOD=JSON ...] [--fast] [--json]
stintmate-ipc-check client <pipe> [--key-file F] [--timeout 60] [--serve 5]
                                  [--reconnect 10] [--json]
stintmate-ipc-check list [--workspace W] [--all]
stintmate-ipc-check keygen [PATH]
```

- **server**: connects to your endpoint (`\\.\pipe\stintmate-...` or the short name) and
  checks the hello, both MACs, the refusal of a wrong MAC, an unsupported version, a
  malformed auth and a silent client, ping, ignoring unknown types/keys/kinds, the error
  codes, chunk reassembly, protocol errors, every declared channel (fields, snapshots,
  layouts, sequence numbers, timestamps, rate), duplicate subscriptions, the heartbeat and
  `bye`. `--call` also calls methods you name (they may have side effects).
- **client**: serves a test endpoint on `<pipe>` (declaring `check.echo`, `check.fail`,
  `check.events`, `check.state`, `check.stream`); start your client against that name. It
  checks the auth, ping, unknown types, unknown methods, chunks, your declared channels,
  the well-formedness of what your client sends, its reconnection after
  `bye shutting_down` and that it refuses a wrong server MAC.
- The key defaults to `<workspace>\ipc\key`; `--key-file` names another. Each line of the
  report names the SPEC section behind it. Exit status: 0 conformant, 1 a check failed,
  2 usage error. Python: `uv run stintmate-ipc-check ...` in this project.

## Repository

```
SPEC.md  CHANGELOG.md  BENCHMARKS.md  README.md
src/stintmate_ipc/   _win32 (pipes), wire (frames, MessagePack), handshake, keyfile,
                     naming (names, workspace, registry), layout, decl, peer (the
                     connection engine), node (Endpoint, Client), aio, check/ (the checker)
tests/               pytest over real pipes (unique pipe names; never the apps' pipes)
vectors/             test vectors; scripts/make_vectors.py regenerates them
bench/bench.py       benchmarks (BENCHMARKS.md)
```

Known limits of the Python implementation: MessagePack's timestamp extension type is
refused at the top level of a message but decoded (not refused) inside containers, because
the `msgpack` library decodes it before the package sees it; senders never produce it.
