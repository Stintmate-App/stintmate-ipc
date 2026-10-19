# Stintmate IPC, version 2

| | |
|---|---|
| Document | Stintmate IPC v2 specification, revision 2.0 |
| Status | Draft for review (2026-10-20) |
| Wire version | `2` |
| Reference implementation | the Python package `stintmate-ipc` in this repository |
| Test vectors | [`vectors/`](vectors/README.md) |
| Conformance tool | `stintmate-ipc-check` ([README](README.md#the-conformance-checker)) |

Stintmate IPC is how the programs of a Stintmate installation talk to each other on one
Windows PC: the telemetry database (Pitwall), the simulator connectors, the voice radio,
the hub, and programs written by third parties. It is **brokerless** (each program serves
its own endpoint and the others connect to it directly), **local** (Windows named pipes,
current user only) and **open**: this document, with the test vectors, is enough to
implement it in any language that can open a named pipe, compute HMAC-SHA256 and read and
write MessagePack.

It offers three primitives on one framing:

- **request/response**: call a named, versioned method and get one answer;
- **channels** with subscription: **event** channels (discrete facts, pushed),
  **state** channels (a current value: sent on subscribe, then on every change);
- **streams**: high-rate binary samples of a declared fixed layout.

## Contents

1. [Conventions](#1-conventions)
2. [Overview](#2-overview)
3. [Transport: named pipes](#3-transport-named-pipes)
4. [Endpoints and discovery](#4-endpoints-and-discovery)
5. [Framing](#5-framing)
6. [Message encoding](#6-message-encoding)
7. [Names and versions](#7-names-and-versions)
8. [Handshake](#8-handshake)
9. [Declarations](#9-declarations)
10. [Messages](#10-messages)
11. [Errors](#11-errors)
12. [Flow control and slow subscribers](#12-flow-control-and-slow-subscribers)
13. [Ordering](#13-ordering)
14. [Time](#14-time)
15. [Timeouts, heartbeat and reconnection](#15-timeouts-heartbeat-and-reconnection)
16. [Versioning and compatibility](#16-versioning-and-compatibility)
17. [Conformance](#17-conformance)
18. [Security considerations](#18-security-considerations)
19. [Implementer's guide](#19-implementers-guide)
20. [History](#20-history)
- [Appendix A: constants](#appendix-a-constants)
- [Appendix B: the Stintmate endpoints (informative)](#appendix-b-the-stintmate-endpoints-informative)

## 1. Conventions

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**,
**SHOULD NOT**, **RECOMMENDED**, **MAY** and **OPTIONAL** are to be interpreted as described
in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and
[RFC 8174](https://www.rfc-editor.org/rfc/rfc8174) when, and only when, they appear in all
capitals.

Terms:

| Term | Meaning |
|---|---|
| **endpoint** | A named pipe served by one process, identified by its endpoint id (section 4). |
| **server** | The process that creates the pipe of an endpoint and accepts connections. |
| **client** | A process that opens an endpoint's pipe. |
| **peer** | Either end of a connection. After the handshake both ends are peers with the same rights (section 2.2). |
| **owner** | The peer that declares a method or channel; the other peer calls it or subscribes to it. |
| **frame** | One unit on the wire (section 5). |
| **message** | One MessagePack map in a kind-1 frame (or reassembled from kind-3 chunks). |
| **sample** | One stream item in a kind-2 frame. |
| `u8`, `u16le`, `u32le`, `i64le`, ... | Unsigned or signed integers of that many bits, little-endian. |
| `uint` | A MessagePack integer >= 0 and < 2^64. |
| `bin` | A MessagePack `bin` value (bytes). |
| `t_ns` | A timestamp in nanoseconds of the system's performance counter (section 14). |

Examples show messages in a JSON-like notation; `b"..."` or `bin(32)` marks a `bin`.

## 2. Overview

### 2.1 One connection

```
client                                                  server (endpoint)
  | open \\.\pipe\stintmate-<app>[-<process>]-<user>        |
  |<------------------- hello (nonce, versions) -------------|
  |------- auth (version, nonce, MAC, identity, decl) ------>|   verifies the MAC
  |<------------- welcome (MAC, identity, decl) -------------|   client verifies the MAC
  |                                                          |
  |<==================== symmetric session ===============>|
  |  req / res / prog / cancel      sub / res / evt / unsub / end
  |  stream samples (kind 2)        ping / pong   decl   bye
```

### 2.2 Symmetric sessions

After the handshake the protocol does not distinguish client and server: either peer MAY
call the methods the other declared and subscribe to the channels the other declared. A
third-party connector therefore only ever needs to be a client: it opens Pitwall's
endpoint, declares its own methods (`live_frame`, say) and streams (telemetry) in the
handshake, and Pitwall calls and subscribes to them over the same connection.

### 2.3 Brokerless

No process routes messages for others. A program that needs another program's data opens
that program's endpoint. A process MAY serve one endpoint and be a client of several.

## 3. Transport: named pipes

### 3.1 The pipe

An endpoint is a Windows named pipe `\\.\pipe\<name>` where `<name>` is the endpoint's
short name (section 4.1).

- The pipe MUST be created in **byte mode** (`PIPE_TYPE_BYTE | PIPE_READMODE_BYTE`).
- The server SHOULD allow unlimited instances (`PIPE_UNLIMITED_INSTANCES`): one instance per
  connection. A server MAY limit them; a client that gets `ERROR_PIPE_BUSY` waits (3.3).
- In and out buffer sizes are not part of the protocol; 64 KiB is RECOMMENDED.
- Both directions are independent: a peer MUST be able to write while a read is pending on
  the same handle. With Win32 that means overlapped I/O (or a runtime that does it for
  you, such as .NET's `PipeOptions.Asynchronous`, Tokio, go-winio or Node's `net`). A
  blocking read on a synchronous handle blocks every write on it.

### 3.2 Server security

When it creates an instance the server:

- MUST pass a security descriptor whose DACL grants access to the **current user only**:
  a protected DACL with one ACE allowing `GENERIC_ALL` to the user's SID, in SDDL
  `D:P(A;;GA;;;<user SID>)`. (The default DACL of a named pipe lets every local user
  read.)
- MUST set `PIPE_REJECT_REMOTE_CLIENTS`.
- MUST create the **first** instance with `FILE_FLAG_FIRST_PIPE_INSTANCE`. If that fails
  (`ERROR_ACCESS_DENIED`), another process already serves the name: the server MUST NOT
  serve this endpoint (it reports "endpoint in use" and exits or retries later). Later
  instances are created without the flag while the first one exists.
- SHOULD keep one instance waiting in `ConnectNamedPipe` at all times while it serves, so
  clients rarely see `ERROR_PIPE_BUSY`, and SHOULD create the next instance **before** it
  hands a connected one over: a pipe name exists only while one of its instances does, and
  a client that leaves at once could otherwise take the name with it (the next client then
  gets `ERROR_FILE_NOT_FOUND` although the server runs).

### 3.3 Client open

A client opens the pipe with `CreateFile(path, GENERIC_READ | GENERIC_WRITE, 0, NULL,
OPEN_EXISTING, flags, NULL)`:

- `flags` SHOULD include `SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION`, so a process
  that squats the name cannot impersonate the client beyond identifying it. (.NET:
  `TokenImpersonationLevel.Identification`; Rust Tokio: `security_qos_flags`.)
- `ERROR_FILE_NOT_FOUND` (2): nobody serves the endpoint (the program is not running).
  Retry later (section 15.3).
- `ERROR_PIPE_BUSY` (231): every instance is taken. Call `WaitNamedPipe(path, timeout)`
  (1 s RECOMMENDED) and open again.
- The client MUST NOT trust the pipe until the handshake proved the server holds the key
  (section 8).

### 3.4 Caller identity

The server learns the client's **process id** from the operating system with
`GetNamedPipeClientProcessId` on the connected instance; a client MAY learn the server's
with `GetNamedPipeServerProcessId`.

- A server that needs the caller's identity (logging, correlating a request with the
  process tree that made it) MUST use the OS-provided PID. Process ids sent inside
  messages are claims and MUST NOT be used for anything but display.
- A PID identifies a process only while it runs; Windows reuses PIDs. A server MAY rely on
  the PID while the connection is open (the client process holds its handle) and SHOULD
  read it once, right after `ConnectNamedPipe` returns.
- Identity claims (`peer` in `auth` and `welcome`, section 8.4) are authenticated only in
  the sense that the sender holds the key; every holder of the key may claim any `app`.

## 4. Endpoints and discovery

### 4.1 Names

```
short name = "stintmate-" endpoint-id "-" user
endpoint-id = app [ "-" process ]
pipe path  = "\\.\pipe\" short name
```

| Part | Rule |
|---|---|
| `app` | `^[a-z0-9][a-z0-9_-]{0,47}$`. The app id: `pitwall`, `voice`, `hub`, and for connectors `connector-<connector id>` (e.g. `connector-ams2`). |
| `process` | `^[a-z0-9][a-z0-9_-]{0,31}$`, OPTIONAL: which process of a multi-process app (`radio`, `ui`, `recorder`). |
| `user` | The Windows user name of the server process (`GetUserNameW`) with ASCII letters `A`-`Z` lowercased and every other character unchanged. (ASCII-only lowercasing is the same in every language; full Unicode lowercasing is not.) |

Examples: `\\.\pipe\stintmate-pitwall-leona`, `\\.\pipe\stintmate-voice-radio-leona`,
`\\.\pipe\stintmate-connector-ams2-recorder-leona`.

- Implementations MUST build names from their parts and MUST NOT parse a name back into
  parts (`a-b` + `c` and `a` + `b-c` give the same name; the registry and the handshake
  carry the parts).
- Names are case-insensitive to Windows; implementations MUST use the lowercase form
  above, because the short name is bound into the handshake MACs byte for byte.
- An app MUST NOT serve an endpoint id of another app. App ids of third parties SHOULD be
  `connector-<id>` with the connector's own stable id, or another prefix unlikely to
  collide.

### 4.2 Finding an endpoint

Discovery is by **well-known names**: a client that knows the app (and process) it wants
builds the name (4.1) and opens it. There is no lookup service.

- The endpoints of the Stintmate apps are listed in appendix B.
- A program that serves endpoints others must find, but whose names others cannot know in
  advance (a third-party connector), publishes them where its users' tools look (for the
  Stintmate hub: the connector descriptor `<workspace>\connectors\<id>\connector.json`)
  and in the registry (4.4).

### 4.3 The workspace

Stintmate keeps per-user data in a **workspace** folder. The IPC uses it for the key file
(8.1) and the registry (4.4). A program finds it, in this order:

1. its own configuration or command line, if it has one;
2. the environment variable `STINTMATE_WORKSPACE`, if set and not blank;
3. the pointer file `%APPDATA%\Stintmate\settings.json` (UTF-8, a BOM allowed): a JSON
   object whose string field `workspace` is the folder.

No workspace means no key file: the program cannot connect (8.1) and SHOULD say so.

### 4.4 The endpoint registry

`<workspace>\ipc\endpoints\<endpoint-id>.json`, one file per running endpoint:

```json
{"schema": 1, "endpoint": "pitwall", "pipe": "\\\\.\\pipe\\stintmate-pitwall-leona",
 "app": "pitwall", "process": null, "pid": 4242, "run": "5f1c2a9be03d4471",
 "versions": [2], "started_at": "2026-10-20T18:00:00+00:00", "name": "Pitwall",
 "version": "0.9.0"}
```

- A server that has a workspace SHOULD write its entry after its pipe exists, atomically
  (a temporary file in the same folder, then a replace), and SHOULD delete it when it
  stops if the entry still carries its own `run`.
- A client MAY read the registry to list running endpoints. An entry is **alive** only if
  its pipe exists (`WaitNamedPipe(pipe, 1)` does not fail with `ERROR_FILE_NOT_FOUND`);
  the PID alone proves nothing (reuse). Entries of dead endpoints are stale: readers
  ignore them and MAY delete those older than 10 s.
- The registry is information, not authority: a client MUST still authenticate the
  endpoint (section 8). Unknown fields MUST be ignored.

## 5. Framing

### 5.1 Frames

Both directions carry a sequence of frames:

```
frame   = length:u32le  kind:u8  payload
length  = 1 + len(payload)      ; the bytes after the length field, at least 1
```

| `kind` | Name | Payload |
|---|---|---|
| `0x01` | message | One MessagePack map (section 6). |
| `0x02` | stream sample | Section 5.3. |
| `0x03` | chunk | Part of one message too large for one frame (section 5.4). |
| `0x80`-`0xFF` | reserved, ignorable | A receiver MUST skip the frame (it knows its length). Future versions may define them; senders MUST NOT send them unless a negotiated feature (8.8) defines them. |
| `0x00`, `0x04`-`0x7F` | invalid | Protocol error (5.5). |

### 5.2 Limits

Each peer announces `max_frame`, the largest `length` it accepts, and `max_message`, the
largest message it accepts after reassembly (8.2, 8.3).

- `max_frame` MUST be >= 65536 (`MIN_MAX_FRAME`); `max_message` MUST be >= `max_frame` and
  <= 2^32 - 1. The reference implementation uses 1 MiB and 256 MiB.
- Until the handshake is complete, every frame MUST be <= 65536 bytes (`length` <= 65536)
  in both directions, whatever was announced.
- A sender MUST NOT send a frame whose `length` exceeds the receiver's `max_frame`, nor a
  message larger than the receiver's `max_message`.
- A receiver MUST check `length` as soon as it has the 4 length bytes and MUST NOT wait
  for (or buffer) the body of a frame it will refuse.

### 5.3 Stream samples (kind 2)

```
payload = sid:u32le  seq:u32le  t_ns:i64le  flags:u32le  body
```

| Field | Meaning |
|---|---|
| `sid` | The subscription the sample belongs to (assigned by the owner, 10.6). |
| `seq` | The sample's sequence number in this subscription: 1 for the first sample the owner offered after subscribing, +1 for every sample offered after it, whether delivered or dropped; it wraps from 2^32 - 1 to 0. |
| `t_ns` | When the sample was taken at the source (section 14), not when it was sent. |
| `flags` | Bit 0 `dropped`: one or more samples of this subscription were dropped since the previous delivered one (12.1). Other bits are reserved: senders MUST send 0, receivers MUST ignore them. |
| `body` | Exactly the stream's layout size in bytes (9.3). |

The header is 20 bytes; a 48-byte sample is a 73-byte frame. A kind-2 frame with a payload
shorter than 20 bytes, or whose body length differs from the subscription's layout size,
is a protocol error. A sample for a `sid` the receiver does not know (it just
unsubscribed) MUST be ignored.

### 5.4 Chunked messages (kind 3)

A message larger than the receiver's `max_frame - 1` MUST be sent as chunks; any message
MAY be:

```
payload = total:u32le  offset:u32le  data      ; data is at least 1 byte
```

- `total` is the length of the whole encoded message and is the same in every chunk of
  it; `offset` is where `data` goes. The first chunk has offset 0, each next one starts
  where the previous ended, and the message is complete when `offset + len(data) ==
  total`. The receiver then decodes the `total` bytes as one message, exactly as if it
  had come in one kind-1 frame.
- A receiver MUST refuse a first chunk whose `total` exceeds its `max_message` (it does
  not need the other chunks to know).
- **One message at a time:** between the first and the last chunk of a message, the sender
  MUST NOT send any kind-1 frame or any chunk of another message. Kind-2 frames (stream
  samples) and reserved kinds MAY be interleaved. This keeps messages in order (13.2)
  while a large transfer does not delay real-time samples.
- Chunk size is the sender's choice within the receiver's `max_frame`; 256 KiB is
  RECOMMENDED (a 1.7 MB lap is 7 chunks; a 48-byte sample waits at most one chunk write).

Violations (a first chunk with an offset other than 0, a gap or overlap, a changed
`total`, an empty chunk, more data than `total`, a kind-1 frame inside a transfer) are
protocol errors.

### 5.5 Protocol errors

A receiver that finds a framing or encoding violation (invalid kind, `length` 0, a frame
over `max_frame`, a bad chunk, a payload that is not a valid message, section 6.2) MUST
stop processing the connection's input, SHOULD send `bye` with code `protocol_error` and a
short reason, and MUST close the connection (section 10.12). It MUST NOT try to
resynchronise: the byte stream cannot be trusted after a violation.

## 6. Message encoding

### 6.1 The MessagePack subset

Messages are [MessagePack](https://github.com/msgpack/msgpack/blob/master/spec.md) values
restricted to:

| Type | Notes |
|---|---|
| nil, boolean | |
| integer | Signed 64-bit or unsigned 64-bit range. Senders SHOULD keep ids and counters < 2^53 (JavaScript); receivers MUST echo ids exactly. |
| float | Senders SHOULD encode float64; receivers MUST accept float32 too. NaN and infinities SHOULD NOT be sent in messages (they have no JSON form). |
| str | UTF-8; a receiver MAY treat invalid UTF-8 as a protocol error. |
| bin | Raw bytes: telemetry channels, images, nonces, MACs. Never base64. |
| array | |
| map | **Keys MUST be strings.** Order is not significant. Keys MUST be unique. |

Extension types (including the timestamp type -1) MUST NOT be sent; a receiver SHOULD
treat them as a protocol error. A receiver SHOULD support nesting at least 32 levels deep
and MAY refuse deeper values as a protocol error.

Encoders SHOULD use the shortest form of every value (as common MessagePack libraries do);
decoders MUST accept every valid form (an integer in a wider form than needed, for
instance). The test vectors' bytes are those of the shortest forms with keys in the order
listed.

### 6.2 Messages

A message is a map with a string field `t`, the message type (section 10). A kind-1 payload
(or reassembled chunks) that is not a valid MessagePack value, is not a map, or has no
string `t` is a protocol error, as is trailing data after the value.

Field values in this document are REQUIRED unless marked OPTIONAL; an OPTIONAL field that
is absent and one that is nil mean the same ("not given"), unless stated otherwise.

### 6.3 Extensibility rules (every receiver)

- **Ignore unknown keys** in every map defined by this document (messages, `e`, `peer`,
  declarations, registry entries). Senders of a newer revision may add OPTIONAL fields.
- **Ignore messages whose `t` is unknown** (after the handshake). New message types that
  a peer must understand are only sent when a feature (8.8) or a newer wire version was
  negotiated.
- **Skip frames of kinds 0x80-0xFF.**
- Application payloads (`p`, `r`, `d`, `snap`) follow the same rule by convention: method
  and channel owners add only OPTIONAL fields within a version (section 7.2).

## 7. Names and versions

### 7.1 Method and channel names

`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`, at most 64 characters, e.g. `pitwall.write`,
`connector.telemetry`, `voice.say`. Names are scoped to the peer that declares them: two
endpoints may both have a `control.state`. Names starting with `ipc.` are reserved for
this protocol; applications MUST NOT declare them.

The first segment SHOULD name the app or the role (`pitwall.`, `connector.`, `voice.`,
`control.`) so logs read well.

### 7.2 Versions of methods and channels

Each method and channel declares the versions it offers (integers 1-65535), and every
`req` and `sub` names one (`v`).

- Within a version, the owner MAY only add OPTIONAL fields to params, results and event
  data, and add error codes; callers and subscribers MUST ignore fields they do not know.
- Anything else (removing or renaming a field, changing its type or meaning, changing a
  stream layout) is a new version. An owner MAY offer old and new versions side by side
  during a migration and MUST answer `version_unsupported` (with `details.versions`) for
  a version it does not offer.
- A stream's layout is fixed per channel version.

## 8. Handshake

### 8.1 The key file

Both ends prove they hold a shared 32-byte secret, the **key**, stored in
`<workspace>\ipc\key` (section 4.3).

- **Format:** exactly 64 hexadecimal characters (the 32 key bytes), RECOMMENDED lowercase,
  OPTIONALLY followed by ASCII whitespace (`\r\n` is RECOMMENDED); a UTF-8 BOM at the start
  SHOULD be accepted. Readers MUST accept upper and lower case and MUST refuse anything
  else (a wrong length, other characters) with an error that names the file.
- **Creation:** whoever needs the key first and finds no file creates it (in Stintmate,
  the hub's onboarding, but any server or client MAY). Creation MUST be atomic and MUST
  NOT replace an existing key: write 32 bytes from a cryptographically secure generator
  (`BCryptGenRandom`, `RandomNumberGenerator`, `getrandom`) as above into a temporary
  file in the same folder, then rename it to `key` with a rename that fails if `key`
  exists (`MoveFileExW` without `MOVEFILE_REPLACE_EXISTING`). If the rename fails because
  the file exists, delete the temporary file and use the existing key. The file SHOULD be
  created with a protected DACL for the current user only (`D:P(A;;FA;;;<user SID>)`).
- **Reading:** implementations SHOULD read the key at every connection attempt (clients)
  and every accepted connection (servers), or cache it and re-read when its modification
  time or size changes, so a replaced key takes effect without restarts.
- The key MUST NOT appear in logs, messages, crash reports or command lines.

### 8.2 `hello` (server, first frame)

As soon as a client connects, the server sends:

```
{"t": "hello", "proto": "stintmate-ipc", "versions": [2], "nonce": bin(32),
 "max_frame": 1048576, "max_message": 268435456, "features": [],
 "clock": [t_ns, unix_ns]}
```

| Field | Type | Meaning |
|---|---|---|
| `proto` | str | Always `stintmate-ipc`. A client MUST close if it differs. |
| `versions` | [uint] | Wire versions the server speaks. This document is version 2. |
| `nonce` | bin(32) | 32 fresh random bytes per connection (`s_nonce`). |
| `max_frame` | uint | The server's frame limit (5.2). OPTIONAL: absent means 65536. |
| `max_message` | uint | OPTIONAL: absent means `max_frame`. |
| `features` | [str] | OPTIONAL: optional extensions the server supports (8.8). |
| `clock` | [int, int] | OPTIONAL: the server's `t_ns` and Unix time in ns read together (14.3). |

`hello` and the stability of these fields are the same for every wire version, so that a
client can always negotiate.

### 8.3 `auth` (client)

```
{"t": "auth", "v": 2, "nonce": bin(32), "mac": bin(32),
 "peer": {"app": "connector-examplesim", "name": "ExampleSim connector",
          "version": "1.2.0", "process": "recorder", "meta": {"sim": "examplesim"}},
 "decl": {...}, "max_frame": 1048576, "max_message": 268435456, "features": []}
```

| Field | Type | Meaning |
|---|---|---|
| `v` | uint | The chosen wire version: the highest version in `hello.versions` the client speaks. None in common: the client sends `bye version_unsupported` and closes. |
| `nonce` | bin(32) | 32 fresh random bytes (`c_nonce`). |
| `mac` | bin(32) | The client MAC (8.5). |
| `peer` | map | The client's identity (8.4). |
| `decl` | map | What the client offers (section 9). OPTIONAL: absent means nothing. |
| `max_frame`, `max_message` | uint | OPTIONAL, as in `hello`. |
| `features` | [str] | OPTIONAL: the features of `hello.features` the client wants. |

### 8.4 `welcome` (server) and identities

After verifying the client MAC (8.5), the server sends:

```
{"t": "welcome", "v": 2, "mac": bin(32), "run": "5f1c2a9be03d4471",
 "peer": {"app": "pitwall", "name": "Pitwall", "version": "0.9.0", "process": "core"},
 "decl": {...}, "heartbeat_ms": 5000, "features": []}
```

| Field | Type | Meaning |
|---|---|---|
| `v` | uint | Echoes `auth.v`. |
| `mac` | bin(32) | The server MAC (8.5). The client MUST verify it before it trusts or sends anything else. |
| `run` | str | A random id the server picks when it starts (at least 64 bits). It changes when the server restarts: a client that sees another `run` after reconnecting knows the server's state (sequence numbers, subscriptions, in-memory caches) was lost. |
| `peer` | map | The server's identity. |
| `decl` | map | What the server offers. |
| `heartbeat_ms` | uint | The heartbeat period both peers use (15.2), >= 100. RECOMMENDED 5000. |
| `features` | [str] | OPTIONAL: the features in effect: the subset of `auth.features` the server accepted. |

`peer` (in `auth` and `welcome`):

| Field | Type | Meaning |
|---|---|---|
| `app` | str | The app id (4.1 syntax). |
| `process` | str | OPTIONAL: which process of the app. |
| `name` | str | OPTIONAL: a display name. |
| `version` | str | OPTIONAL: the program's version, for logs. |
| `meta` | map | OPTIONAL: app-specific facts (a connector's `sim`, say). |

### 8.5 MACs

```
client_mac = HMAC-SHA256(key, "stintmate-ipc/" V " client" 0x00 || s_nonce || c_nonce || name)
server_mac = HMAC-SHA256(key, "stintmate-ipc/" V " server" 0x00 || c_nonce || s_nonce || name)
```

- `V` is the negotiated version in decimal ASCII (`2`): `"stintmate-ipc/2 client"` then one
  zero byte.
- `name` is the endpoint's **short name** as UTF-8 bytes, lowercase, without
  `\\.\pipe\` (`stintmate-pitwall-leona`). Binding it means an `auth` captured on one
  endpoint is useless on another.
- `||` is concatenation of raw bytes; the nonces are the 32 raw bytes.
- MACs MUST be compared in constant time (`CryptographicOperations.FixedTimeEquals`,
  `hmac.compare_digest`, `subtle::ConstantTimeEq`, `crypto.timingSafeEqual`).

The vectors in [`vectors/handshake.json`](vectors/handshake.json) give the exact inputs and
outputs for a fixed key and nonces.

### 8.6 Failures

| Situation | Who | What |
|---|---|---|
| Wrong client MAC | server | Send `bye` `unauthorized` (no detail beyond "authentication failed"), close. No `welcome`. |
| Wrong server MAC | client | Send `bye` `unauthorized`, close, send nothing else. Report "the endpoint failed authentication: another key, or not the real endpoint". |
| `auth.v` not offered | server | `bye` `version_unsupported`, close. |
| A malformed `hello`, `auth` or `welcome` (missing field, wrong type, nonce not 32 bytes) | receiver | `bye` `protocol_error`, close. |
| Any other message before the handshake completes | receiver | `bye` `protocol_error`, close. |
| Server has no key (no file) | server | `bye` `unavailable`, close. |
| `hello`, `auth`, `welcome` after the handshake | receiver | Protocol error. |

Neither side sends anything but `hello`/`auth`/`welcome`/`bye` before the handshake
completes: the client may send other messages only after it has verified `welcome`; the
server, only after it sent `welcome`.

### 8.7 Version negotiation

The server lists every wire version it speaks in `hello.versions`; the client picks the
highest it also speaks and states it in `auth.v`; the server confirms it in `welcome.v`.
From `welcome` on, the connection follows that version's rules. Section 16 says how
versions evolve.

### 8.8 Features

A feature is an optional extension, named `<owner>.<name>` (`^[a-z0-9_.-]{1,64}$`), defined
by a later revision of this document or by a third party for its own peers. The server
lists those it supports in `hello.features`; the client lists in `auth.features` those it
wants; `welcome.features` is the set in effect. A peer MUST NOT use a feature that is not
in effect. Revision 2.0 defines none.

## 9. Declarations

### 9.1 Format

`decl` (in `auth`, `welcome` and `decl` messages):

```
{"methods":  {"pitwall.write":      {"v": [1], "ordered": true, "idempotent": true,
                                     "doc": "Store one write API message."},
              "pitwall.live_state": {"v": [1]}},
 "channels": {"pitwall.lap_completed": {"kind": "event", "v": [1]},
              "pitwall.connectors":    {"kind": "state", "v": [1]},
              "connector.telemetry":   {"kind": "stream", "v": [1], "rate_hz": 200,
                                        "layout": {"size": 48, "fields": [["speed_kmh", "f32"], ...]}}}}
```

`methods` and `channels` are maps from names (7.1) to descriptions; both are OPTIONAL
(absent: none). A receiver MUST treat an invalid declaration (a bad name, an empty `v`, an
unknown `kind`, a stream without a valid layout) as a protocol error.

### 9.2 Methods

| Field | Type | Meaning |
|---|---|---|
| `v` | [uint] | The versions offered, not empty. |
| `ordered` | bool | OPTIONAL (false): the owner handles this method's requests from one connection one at a time, in arrival order (13.3). |
| `idempotent` | bool | OPTIONAL (false): requests with an idempotency key are deduplicated (10.4). |
| `doc` | str | OPTIONAL: one line for people. |
| `params`, `result` | map | OPTIONAL: [JSON Schema](https://json-schema.org/) of `p` and `r`, informative. |

### 9.3 Channels and layouts

| Field | Type | Meaning |
|---|---|---|
| `kind` | str | `event`, `state` or `stream`. |
| `v` | [uint] | The versions offered, not empty. |
| `doc` | str | OPTIONAL. |
| `data` | map | OPTIONAL, event and state: JSON Schema of `d` (and `snap`), informative. |
| `layout` | map | REQUIRED for streams: the layout of the channel's **highest** version. The `sub` response carries the layout of the version subscribed (10.6). |
| `rate_hz` | number | OPTIONAL, streams: the nominal sample rate (subscribers size buffers from it). |

A **layout**:

```
{"name": "telemetry", "size": 48,
 "fields": [["speed_kmh", "f32"], ["gear", "i8"], ["flags", "u8"], ["rpm", "u16"],
            ["tyre_temp_c", "f32", 4], ...]}
```

- `fields`: an array of `[name, type]` or `[name, type, count]`; `name` matches
  `^[a-z][a-z0-9_]{0,63}$` and is unique; `type` is one of `u8 i8 u16 i16 u32 i32 u64 i64
  f32 f64`; `count` (1-4096, default 1) makes an array of that many consecutive values.
- Fields are packed **in order, little-endian, with no padding or alignment**; floats are
  IEEE 754. `size` is the sum of the field sizes and MUST match it; a body is exactly
  `size` bytes; layouts are at most 65536 bytes and 1024 fields.
- `name` is OPTIONAL and informative.
- Owners SHOULD put fields wider than one byte at offsets that are multiples of their size
  (pad with explicit `u8` fields) so readers can map the body onto a struct; the protocol
  does not require it.

### 9.4 Updates

A peer MAY replace its declarations at any time with a `decl` message (10.11): the new map
replaces the old one whole. Subscriptions to channels that disappear end with `end`
`channel_closed` (sent by the owner). Requests already sent to a method that disappears
still get their response.

## 10. Messages

### 10.1 Summary

| `t` | From | Fields | Answered by |
|---|---|---|---|
| `req` | caller | `id`, `m`, `v`, `p`, `dl`?, `ik`? | exactly one `res` |
| `res` | owner | `id`, `ok`, `r` or `e` | - |
| `prog` | owner | `id`, `d`?, `dl`? | - |
| `cancel` | caller | `id` | (the `res` still comes) |
| `sub` | subscriber | `id`, `ch`, `v`, `o`? | exactly one `res` |
| `unsub` | subscriber | `sid` | - |
| `end` | owner | `sid`, `code`, `msg`? | - |
| `evt` | owner | `sid`, `seq`, `ts`, `d` | - |
| `decl` | either | `decl` | - |
| `ping` | either | `ts`? | `pong` |
| `pong` | either | `ts`? (echoed) | - |
| `bye` | either | `code`, `msg`? | (the connection closes) |

Kind-2 frames carry stream samples (5.3). Ids (`id`) are chosen by the sender of the `req`
or `sub` and live in that direction's space: a request from each peer may carry the same
number. `sid` values are chosen by the owner of the subscription and live in its space.

### 10.2 `req` and `res`

```
{"t": "req", "id": 7, "m": "pitwall.write", "v": 1, "p": {...}, "dl": 30000,
 "ik": "examplesim/2b9f1c/lap/4"}
{"t": "res", "id": 7, "ok": true, "r": {"session_id": 28}}
{"t": "res", "id": 8, "ok": false, "e": {"code": "game_not_running",
                                       "msg": "ExampleSim is not running.",
                                       "details": {...}, "retry_ms": 2000}}
```

`req`:

| Field | Type | Meaning |
|---|---|---|
| `id` | uint | Unique among this sender's requests and subscriptions still waiting for a `res` on this connection. Reusing an id still in flight is a protocol error. SHOULD be < 2^53. |
| `m` | str | The method name. |
| `v` | uint | The method version. OPTIONAL: absent means 1. |
| `p` | any | The params; a map by convention. OPTIONAL: absent means an empty map. |
| `dl` | uint | OPTIONAL: the caller's deadline in milliseconds from when it sent the request (15.1). |
| `ik` | str | OPTIONAL: the idempotency key (10.4), at most 256 bytes of UTF-8. |

`res`: `id` echoes the request's exactly; `ok` is `true` with `r` (any value, nil allowed),
or `false` with `e`:

| `e` field | Type | Meaning |
|---|---|---|
| `code` | str | A code of section 11 or an application code (lowercase snake case). |
| `msg` | str | One sentence for people. |
| `details` | map | OPTIONAL: machine-readable details. |
| `retry_ms` | uint | OPTIONAL: when retrying makes sense, after how long. |

Rules:

- The owner MUST answer every `req` exactly once while the connection is open, including
  requests it did not understand: an unknown `m` gets `unknown_method`, a `v` not offered
  gets `version_unsupported` with `details.versions`, invalid params `bad_request`.
- A `res` whose `id` the receiver is not waiting for MUST be ignored.
- Requests MAY be pipelined: a caller may send many before the first `res`; responses may
  come in any order (13.3).
- An application's refusal that a caller should treat as an answer rather than a failure
  (a radio that declines a `say`, say) MAY be a successful `res` whose `r` says so; this
  is the method's definition, not the protocol's.

### 10.3 Long requests: `prog` and `cancel`

- `prog` (owner): the request `id` is still being worked on. `d` OPTIONAL, any value (a
  progress report). `dl` OPTIONAL: a new deadline in milliseconds from now; the caller
  SHOULD then wait until then instead of its original deadline. A `prog` for an id the
  caller is not waiting for is ignored.
- `cancel` (caller): it gave up on request `id` (its deadline passed, or its user went
  away). The owner SHOULD stop the work if it can and MUST still send the `res` (often
  `cancelled`), which the caller ignores. A `cancel` for an unknown id is ignored.

### 10.4 Idempotency keys

Requests that change something and that a caller may need to repeat (because the
connection broke before the answer came, or because the caller queued them on disk while
the owner was down) SHOULD carry an **idempotency key** `ik`.

- **Caller:** the key names one operation, forever: the same key MUST only be sent again
  with the same method and the same params. It SHOULD be built from a random part fixed
  when the operation is first created plus a counter, e.g.
  `<producer id>/<random 64-bit session id in hex>/<kind>/<n>`
  (`ams2/9f3c2a71b0e44d18/lap/12`). The key MUST be assigned before the request is first
  sent, and stored with it if the request is queued on disk.
- **Owner** of a method declared `idempotent: true`: two requests with the same `ik` from
  peers with the same `peer.app` are one operation. The owner MUST NOT apply it twice; it
  SHOULD answer the repeat with the first answer (same `r`, or the same error); if the
  first is still running it MAY make the repeat wait or answer `busy` with `retry_ms`.
  The method's documentation states how long keys are remembered: for durable writes
  (laps stored in a database) that means as long as the data, so the key belongs in the
  same transaction as the write.
- Errors that mean "not done, try later" (`busy`, `unavailable`, `timeout`, `cancelled`,
  `overloaded`, `shutting_down`) do not consume the key: the operation may still be done
  by a later repeat.
- A caller replaying a queue MUST replay requests of an `ordered` method in their original
  order and wait for each answer before sending the next when later ones depend on earlier
  ones.

*Example (Stintmate):* the AMS2 connector writes each finished lap to its outbox on disk
with a key `ams2/<session key>/lap/<n>`, calls `pitwall.write` with it when Pitwall runs,
and deletes the file after an `ok` (or a non-retryable error). A lap written twice because
the first answer was lost is stored once.

### 10.5 Writes that create things

When a later request refers to something an earlier one created (laps refer to their
session), owners SHOULD let the caller name it with its own key in the params (a
`session_key` chosen by the caller), so a queued sequence can be replayed without knowing
the ids the owner will assign.

### 10.6 Subscriptions: `sub`, `res`, `unsub`

```
{"t": "sub", "id": 10, "ch": "pitwall.connectors", "v": 1, "o": {...}}
{"t": "res", "id": 10, "ok": true, "r": {"sid": 1, "kind": "state", "seq": 0,
                                       "snap": {...}, "ts": t_ns}}
{"t": "res", "id": 11, "ok": true, "r": {"sid": 2, "kind": "stream", "seq": 0,
                                       "layout": {...}}}
{"t": "unsub", "sid": 1}
```

`sub` fields: `id` (as for `req`), `ch` (channel name), `v` (OPTIONAL, default 1), `o`
(OPTIONAL map: channel-specific options, such as a rate; the owner MAY refuse them with
`bad_request`).

The owner answers with a `res`:

- errors: `unknown_channel`, `version_unsupported` (with `details.versions`),
  `bad_request`, or an application refusal;
- success, `r`:

| Field | Type | Meaning |
|---|---|---|
| `sid` | uint | The subscription id, chosen by the owner, unique among this connection's subscriptions to the owner's channels for the life of the connection (never reused), >= 1, < 2^32. |
| `kind` | str | The channel's kind. |
| `seq` | uint | The last `seq` sent on this subscription (0 for a new one). |
| `snap` | any | State channels: the current value (nil if it has none). REQUIRED for state, absent otherwise. |
| `ts` | int | OPTIONAL, state: `t_ns` when the value was set. |
| `layout` | map | REQUIRED for streams: the layout of the version subscribed. |

Rules:

- One subscription per channel and version per connection: a second `sub` for a channel and
  version already subscribed on this connection MUST be answered with the same `sid` (and,
  for state, the current value as `snap`); nothing is delivered twice. A subscriber uses
  that to resynchronise.
- `unsub` (subscriber): ends a subscription. Items for that `sid` already in flight may
  still arrive and MUST be ignored. An `unsub` for an unknown `sid` is ignored.
- Subscriptions end when the connection closes. After a reconnection the subscriber
  subscribes again (15.3).
- An owner MAY produce a stream only while it has subscribers (it knows them: subscriptions
  are messages). Connectors running next to a game SHOULD do so.

### 10.7 `end`

```
{"t": "end", "sid": 1, "code": "overloaded", "msg": "subscriber too slow"}
```

The owner ended a subscription: `overloaded` (12.2), `channel_closed` (the channel was
withdrawn), `version_unsupported` (the version was withdrawn), or an application code.
After `end` the owner sends nothing more for that `sid`. The subscriber MAY subscribe again
(a new `sid`).

### 10.8 `evt`

```
{"t": "evt", "sid": 1, "seq": 3, "ts": t_ns, "d": {...}}
```

An event (event channels) or a new value (state channels) on subscription `sid`. `seq`
counts the items offered to this subscription: 1 for the first after subscribing, +1 each
time; for state channels a gap means intermediate values were coalesced (12.3); for event
channels there are no gaps (events are not dropped: 12.2). `ts` is `t_ns` when the event
happened or the value was set at the owner. `d` is the event or the **whole** new value
(state channels never send deltas). An `evt` for an unknown `sid` is ignored.

### 10.9 Stream samples

Kind-2 frames (5.3), only for subscriptions of kind `stream`; `evt` is never used for
streams and kind-2 frames never for other kinds (both are protocol errors).

### 10.10 `ping` and `pong`

`ping` with an OPTIONAL `ts` (any integer, typically `t_ns`); the receiver MUST answer
`pong` with the same `ts` (if present) promptly. Either peer MAY ping at any time; section
15.2 says when they SHOULD.

### 10.11 `decl`

`{"t": "decl", "decl": {...}}` replaces the sender's declarations (9.4).

### 10.12 `bye`

```
{"t": "bye", "code": "shutting_down", "msg": "Pitwall is stopping"}
```

The sender is closing the connection. After `bye` it MUST NOT send anything else; it
SHOULD wait for the peer to close (up to 1 s) and then close. A peer that receives `bye`
MUST stop sending and close. Codes: `normal`, `shutting_down` (the program stops; the
client may reconnect later), `protocol_error`, `unauthorized`, `version_unsupported`,
`timeout` (heartbeat), `unavailable`, `overloaded` (the connection's own output could not
be written), or an application code. Requests in flight fail on the caller's side
(`disconnected`, a local condition, never sent).

## 11. Errors

### 11.1 Codes

Error codes are lowercase snake case. This document defines:

| Code | Where | Meaning | Retry? |
|---|---|---|---|
| `bad_request` | `res` | The params or options are invalid (`msg` names the field). | No |
| `unknown_method` | `res` | No such method on this peer. | No |
| `unknown_channel` | `res` | No such channel on this peer. | No |
| `version_unsupported` | `res`, `end`, `bye` | The method, channel or wire version is not offered; `details.versions` lists those that are. | With another version |
| `unauthorized` | `bye` | Handshake MAC failure. | After the key is fixed |
| `busy` | `res` | The owner cannot take it now (too many requests in flight, the resource is locked). | Yes, after `retry_ms` |
| `unavailable` | `res`, `bye` | What the method needs is not there now (the game is not running, no key). | Yes, later |
| `timeout` | `res`, `bye` | The deadline passed before or while the owner worked; for `bye`, the heartbeat failed. | Yes, with an idempotency key |
| `cancelled` | `res` | The caller cancelled. | - |
| `overloaded` | `end`, `bye` | A subscriber or a connection could not keep up (12). | Subscribe again |
| `too_large` | `res` | The params or result would exceed a limit of the method. | No |
| `channel_closed` | `end` | The channel was withdrawn. | No |
| `internal` | `res` | The owner failed (a bug). `msg` SHOULD NOT leak internals beyond a short description. | Maybe |
| `protocol_error` | `bye` | The peer broke this specification. | No (fix the peer) |
| `shutting_down` | `res`, `bye` | The owner is stopping. | Yes, after reconnecting |
| `normal` | `bye`, `end` | Nothing wrong. | - |

`disconnected` is the name implementations SHOULD give the local failure of a call whose
connection closed before its `res` (it is never sent).

Applications MAY define further codes for their methods (`game_not_running`,
`not_on_track`, `writer_busy`, ...); they SHOULD document them with the method and SHOULD
NOT redefine the codes above.

### 11.2 Protocol errors

Section 5.5 for framing. Also protocol errors (send `bye protocol_error`, close): a
message type of this document with a missing REQUIRED field or a field of the wrong type
(`id` not an unsigned integer, `m` not a string), a reused request id, an invalid
declaration, `evt` or kind-2 frames contradicting the subscription's kind, a stream body of
the wrong size, handshake messages after the handshake. Implementations SHOULD log the
reason; the conformance tool checks the main cases.

## 12. Flow control and slow subscribers

A pipe has a finite buffer: when the reader does not read, the writer's writes stop
completing. The rules below make sure a slow or stuck peer never blocks the owner's
producers (a game-side recorder, a database writer) and never makes memory grow without
bound. Every peer:

- MUST NOT block a producing thread or task on a pipe write. Producers hand frames to a
  writer (a thread, a task, an async queue) that alone writes to the pipe; or write
  directly only when the writer is idle and the write completes at once.
- MUST keep a bounded queue per subscription it owns, with the policy of the channel's
  kind:

### 12.1 Streams: drop the oldest

Each stream subscription has a ring of at most N samples (RECOMMENDED one second at
`rate_hz`, at least 64). When it is full the oldest queued sample is discarded; the next
sample delivered on that subscription has `flags.dropped` set. Its `seq` shows how many
were lost. *Why:* a real-time sample is worthless once newer ones exist; the producer
(next to the game) must never wait.

### 12.2 Events: end the subscription

Each event subscription has a bounded queue (RECOMMENDED 1024 events or 8 MiB). When an
event does not fit, the owner MUST end the subscription with `end` `overloaded` (it MAY
discard the subscription's events still queued) instead of dropping events silently. The
subscriber learns that it lost events, MAY re-subscribe, and SHOULD resynchronise from a
method or a state channel if it needs to. *Why:* event channels carry facts (a lap was
completed, a setup was captured) where silent loss is worse than an explicit signal;
ending only that subscription keeps the connection, its requests and its other
subscriptions alive.

### 12.3 State: coalesce

A state subscription keeps at most one pending value: a newer value replaces a queued one
(`seq` skips). *Why:* each value is the whole state; only the latest matters.

### 12.4 Connections

- Requests and responses are not dropped. A peer that cannot write for a long time (the
  other side stopped reading) SHOULD close the connection after 30 s without write
  progress.
- An owner SHOULD bound the requests in flight per connection (RECOMMENDED 1024) and answer
  `busy` beyond.
- The same rules apply on the subscriber's side between its reader and the application's
  callbacks: an implementation that queues delivered items for application code MUST bound
  that queue with the same per-kind policies (or stop reading, which pushes the policy back
  to the owner), never grow without limit.

## 13. Ordering

13.1 **Frames.** Each direction of a connection delivers frames in the order they were
written.

13.2 **Messages.** Kind-1 messages (whole or chunked) are delivered in the order sent: a
chunked message is never overtaken by a later message (5.4). Stream samples MAY overtake
or be overtaken by messages, with two guarantees: an owner MUST NOT send an `evt` or a
sample for a `sid` before the `res` that created it, and MUST NOT send any after the `end`
it sent.

13.3 **Requests.** An owner MAY handle the requests of a connection concurrently;
responses may come in any order. Requests to a method declared `ordered` are handled one
at a time in arrival order per connection (so their effects and responses follow the
arrival order). Messages a handler sends before its `res` (an `evt` it publishes) are
sent before the `res`.

13.4 **Subscriptions.** On one subscription, items arrive in `seq` order. A state
subscription's snapshot (the `res`) comes before its first `evt`. Between different
subscriptions of one connection, messages keep their send order (13.2); between
connections there is no order.

## 14. Time

### 14.1 Timestamps

`t_ns` (stream samples, `evt.ts`, `snap` `ts`, `ping.ts` by convention) is the Windows
**performance counter** in nanoseconds:

```
t_ns = QueryPerformanceCounter() * 1_000_000_000 / QueryPerformanceFrequency()
```

computed without overflow (`(c / f) * 10^9 + (c % f) * 10^9 / f` in integers). One counter
serves every process of the PC and is monotonic, so a time stamped in one process can be
compared with the clock of another: "play this sound at T" needs no clock
synchronisation.

| Language | `t_ns` |
|---|---|
| Python | `time.perf_counter_ns()` (QPC on Windows) |
| C# | `Stopwatch.GetTimestamp()` scaled by `Stopwatch.Frequency` (QPC) |
| C/C++ | `QueryPerformanceCounter` / `QueryPerformanceFrequency` |
| Rust | `windows::Win32::System::Performance::QueryPerformanceCounter` (`Instant` hides it) |
| Go | `QueryPerformanceCounter` through `golang.org/x/sys/windows` (`time.Now` hides it) |
| Node | `process.hrtime.bigint()` (libuv uses QPC on Windows) |

Different conversions may round differently: consumers MUST NOT rely on agreement finer
than 1 µs between implementations.

### 14.2 Rules

- A stream sample's `t_ns` is when the sample was taken at its source, not when it was
  sent: schedule against it, never against arrival time.
- `t_ns` values are only comparable on one PC between boots; they are not wall-clock time
  and MUST NOT be stored as dates.
- Wall-clock times inside application payloads SHOULD be ISO 8601 strings with an offset,
  or Unix time.

### 14.3 Mapping to wall-clock time

`hello.clock` = `[t_ns, unix_ns]` read together by the server. A peer MAY use it to show a
`t_ns` as a wall-clock time (`unix = unix_ns + (t - t_ns)`), knowing the wall clock may be
adjusted later.

## 15. Timeouts, heartbeat and reconnection

### 15.1 Timeouts

| What | Rule |
|---|---|
| Handshake | The server closes a connection whose `auth` has not arrived 5 s after it connected (RECOMMENDED; at most 10 s). A client gives up if `hello` or `welcome` does not come within 5 s. |
| Requests | The caller chooses the deadline (RECOMMENDED default 5 s; long operations say theirs) and sends it as `dl`. On expiry it sends `cancel`, fails the call locally and ignores a late `res`. An owner SHOULD answer `timeout` without running a request whose deadline already passed when it would start, and MAY abandon work whose deadline passed. |
| Subscriptions | A `sub` has a deadline like a request (5 s). A late successful `res` for a `sub` the subscriber gave up on SHOULD be followed by an `unsub` of that `sid`. |
| `bye` | Wait at most 1 s for the peer to close. |
| Writes | 30 s without progress: close (12.4). |

### 15.2 Heartbeat

`welcome.heartbeat_ms` (H) applies to both peers:

- A peer that has sent nothing for H ms SHOULD send a `ping`.
- A peer that has received nothing (no byte) for 3 x H ms MUST consider the other dead and
  close the connection (a `bye` `timeout` MAY be attempted).
- A broken pipe is detected at once anyway (the OS closes the handles of a process that
  dies); the heartbeat catches a process that hangs.

### 15.3 Reconnection

A client that keeps a connection (a connector, a UI):

- on `ERROR_FILE_NOT_FOUND`, a broken connection or `bye shutting_down` retries with
  backoff 0.25, 0.5, 1, 2 s, then every 2 s (RECOMMENDED), each delay with +-20 % jitter;
- after `bye unauthorized` waits at least 10 s between attempts (a wrong key does not fix
  itself) and tells its user which key file it read;
- after `bye version_unsupported` SHOULD stop and report;
- after reconnecting MUST NOT assume anything about the old connection: it subscribes again
  (taking the snapshots of state channels), re-sends requests whose outcome it does not
  know only if they carry an idempotency key, and compares `welcome.run` with the previous
  one (a different `run` means the server restarted: sequence numbers and the server's
  in-memory state started over).

Programs that queue writes while the owner is down (10.4) replay them after the handshake,
oldest first.

## 16. Versioning and compatibility

- **Wire version** (the integer in `hello.versions`, `auth.v`, the MAC label): changes only
  for an incompatible change of this document. A new wire version gets its own MAC label
  (`stintmate-ipc/3`), so a MAC of one version is never valid in another. Servers MAY offer
  several versions during a transition; `hello` keeps the fields of 8.2 forever.
- **Document revision** (2.0, 2.1, ...): compatible changes within wire version 2: new
  OPTIONAL fields, new message types and frame kinds that receivers may ignore (6.3), new
  error codes, new features (8.8), clarifications. A peer of revision 2.x works with any
  peer of revision 2.y.
- **Methods and channels** version themselves (7.2).
- Every change is listed in [CHANGELOG.md](CHANGELOG.md), with the revision and date.

## 17. Conformance

An implementation conforms if it follows every MUST of this document for the roles it
implements (a client only, or a server too). The RECOMMENDED way to check:

1. Decode and verify every vector in [`vectors/`](vectors/README.md) (frames, handshake MACs,
   error cases, key files, layouts).
2. Run `stintmate-ipc-check server <pipe>` against your endpoint and
   `stintmate-ipc-check client <pipe>` with your client pointed at the checker's endpoint
   ([README](README.md#the-conformance-checker)). Both print a report with the section of
   this document behind each check; attach it to an issue if you need help.

The checks cover at least: the hello fields; MAC verification both ways; refusal of wrong
MACs, unsupported versions, malformed `auth` and silent clients; `ping`; ignoring unknown
types, keys and reserved kinds; `unknown_method`, `unknown_channel`,
`version_unsupported`; chunk reassembly; exact echo of large ids; protocol errors for
invalid kinds, oversized frames, non-map messages and messages inside chunk transfers;
every declared channel (response fields, snapshots, layouts, monotonic `seq` and `t_ns`,
body sizes, observed rate against `rate_hz`); duplicate subscriptions; heartbeat pings;
`bye`; a client's reconnection and its refusal of a wrong server MAC.

## 18. Security considerations

**What it protects against.** Other local users (the DACL, 3.2); remote machines
(`PIPE_REJECT_REMOTE_CLIENTS`); a program of the same user that does not have the key
squatting an endpoint name before the real server starts, or connecting to an endpoint
(mutual MACs, 8.5; the first-instance flag, 3.2); a squatting server impersonating its
clients (SQOS identification, 3.3); replay of a captured handshake (fresh nonces on both
sides, the pipe name in the MAC).

**What it does not.** Any process of the same user can read the key file and is therefore
trusted, as it could also read the user's other files; the key separates Stintmate's
programs from unrelated ones and from accidents, not from malware running as the user.
Messages are not encrypted or signed after the handshake: the pipe is local and its DACL
admits only the user.

**Denial of service.** Peers MUST enforce `max_frame` on the header (5.2), `max_message`
before buffering chunks (5.4), the handshake deadline (15.1), bounded queues (12) and a
bound on requests in flight (12.4). Parsers SHOULD bound MessagePack nesting (6.1) and
treat every length in the input as untrusted.

**Secrets.** The key MUST NOT be logged, put in messages, environment variables or
command lines (8.1). MACs MUST be compared in constant time. Nonces MUST come from a
cryptographically secure generator and MUST NOT be reused.

**Identity.** `peer` fields are claims by a key holder; use the OS-provided PID (3.4) for
anything that matters, remembering PID reuse. A server that grants different rights to
different apps can only do so among cooperating programs.

**Robustness.** Treat every message as untrusted input: validate types before use, never
evaluate payloads, never let a malformed message from one connection affect another.

## 19. Implementer's guide

### 19.1 A minimal client, step by step

1. Find the workspace (4.3) and read the key from `<workspace>\ipc\key` (8.1).
2. Build the pipe name (4.1) and open it (3.3) for overlapped / asynchronous I/O.
3. Read frames: 4 bytes of length (little-endian), then `length` bytes; the first byte is
   the kind. Keep a buffer: one read may return part of a frame or several frames.
4. The first frame is `hello`: check `proto`, pick the version, keep `nonce` (s_nonce).
5. Make 32 random bytes (c_nonce), compute `client_mac`, send `auth` with your identity and
   declarations.
6. Read `welcome` (or `bye`). Verify `server_mac` in constant time; close on mismatch.
7. Start a reader loop and a writer queue (or async tasks). In the reader:
   `ping` -> `pong`; `res` -> complete the waiting call; `req` -> run your handler (or
   answer `unknown_method`) and send `res`; `sub` -> answer `res` with a `sid`, then send
   samples; `unsub` -> stop; `evt` -> your callback; `bye` -> close; anything unknown ->
   ignore.
8. Send pings when idle for `heartbeat_ms`; reconnect with backoff when the pipe breaks
   (15.3).

### 19.2 Pseudo-code

```text
key       = hex_decode(read_file(workspace + "\ipc\key").strip())
name      = "stintmate-pitwall-" + ascii_lower(user_name())
pipe      = open_overlapped("\\.\pipe\" + name)            # CreateFile + SQOS flags

hello     = read_message(pipe)                             # kind 1, msgpack map
assert hello.t == "hello" and hello.proto == "stintmate-ipc" and 2 in hello.versions
c_nonce   = random_bytes(32)
mac       = hmac_sha256(key, "stintmate-ipc/2 client\0" + hello.nonce + c_nonce + name)
write_message(pipe, {t: "auth", v: 2, nonce: c_nonce, mac: mac,
                     peer: {app: "connector-mysim", name: "MySim connector",
                            version: "1.0.0"},
                     decl: {methods: {"connector.live_frame": {v: [1]}},
                            channels: {"connector.telemetry":
                                       {kind: "stream", v: [1], rate_hz: 200,
                                        layout: {size: 12, fields: [["speed", "f32"],
                                                                    ["rpm", "f32"],
                                                                    ["gear", "i32"]]}}}},
                     max_frame: 1048576, max_message: 268435456})
welcome   = read_message(pipe)
if welcome.t == "bye": fail(welcome.code)
expected  = hmac_sha256(key, "stintmate-ipc/2 server\0" + c_nonce + hello.nonce + name)
if not constant_time_equal(welcome.mac, expected): send_bye("unauthorized"); close(); fail()

spawn reader():
    loop:
        frame = read_frame(pipe)
        if frame.kind == 2: continue                       # we subscribe to no stream
        if frame.kind >= 0x80: continue
        if frame.kind == 3: assemble(frame); if not complete: continue
        m = msgpack_decode(frame.payload)
        switch m.t:
            "ping":   enqueue({t: "pong", ts: m.ts})
            "res":    pending[m.id].complete(m)
            "req":    if m.m == "connector.live_frame": enqueue({t: "res", id: m.id, ok: true,
                                                              r: current_frame()})
                      else: enqueue({t: "res", id: m.id, ok: false,
                                     e: {code: "unknown_method", msg: "unknown " + m.m}})
            "sub":    if m.ch == "connector.telemetry":
                          sid = next_sid++; enqueue({t: "res", id: m.id, ok: true,
                              r: {sid: sid, kind: "stream", seq: 0, layout: LAYOUT}})
                          streaming[sid] = {seq: 0, ring: Ring(200)}
                      else: enqueue({t: "res", id: m.id, ok: false,
                                     e: {code: "unknown_channel", msg: m.ch}})
            "unsub":  streaming.remove(m.sid)
            "bye":    close(); return
            default:  pass                                  # ignore unknown types

game loop (never blocks):
    for sid, s in streaming: s.seq += 1; s.ring.push(sample(s.seq, qpc_ns(), body))

writer thread: takes queued messages and ring samples, writes frames, pings when idle.

call(method, params):
    id = next_id++; pending[id] = Future()
    enqueue({t: "req", id: id, m: method, v: 1, p: params, dl: 5000})
    return pending[id].wait(5 s)                           # on timeout: enqueue cancel
```

### 19.3 C# sketch (.NET 8)

```csharp
using System.Buffers.Binary;
using System.IO.Pipes;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using MessagePack;            // MessagePack-CSharp; messages as Dictionary<string, object>

static class StintmateIpc
{
    public static async Task<NamedPipeClientStream> ConnectAsync(string app, string workspace)
    {
        byte[] key = Convert.FromHexString(File.ReadAllText(Path.Combine(workspace, "ipc", "key")).Trim());
        string user = AsciiLower(Environment.UserName);
        string name = $"stintmate-{app}-{user}";
        var pipe = new NamedPipeClientStream(".", name, PipeDirection.InOut,
            PipeOptions.Asynchronous, TokenImpersonationLevel.Identification);
        await pipe.ConnectAsync(5000);

        var hello = await ReadMessageAsync(pipe);
        byte[] sNonce = (byte[])hello["nonce"];
        byte[] cNonce = RandomNumberGenerator.GetBytes(32);
        byte[] mac = Mac(key, "client", sNonce, cNonce, name);
        await WriteMessageAsync(pipe, new Dictionary<string, object> {
            ["t"] = "auth", ["v"] = 2, ["nonce"] = cNonce, ["mac"] = mac,
            ["peer"] = new Dictionary<string, object> { ["app"] = "connector-mysim",
                ["name"] = "MySim connector", ["version"] = "1.0.0" },
            ["max_frame"] = 1 << 20, ["max_message"] = 1 << 28 });

        var welcome = await ReadMessageAsync(pipe);
        if ((string)welcome["t"] != "welcome") throw new Exception($"refused: {welcome["code"]}");
        byte[] expected = Mac(key, "server", cNonce, sNonce, name);
        if (!CryptographicOperations.FixedTimeEquals((byte[])welcome["mac"], expected))
            throw new Exception("the endpoint failed authentication");
        return pipe;     // then: one read loop task, one write queue (Channel<byte[]>)
    }

    static byte[] Mac(byte[] key, string side, byte[] first, byte[] second, string name) =>
        HMACSHA256.HashData(key, [.. Encoding.ASCII.GetBytes($"stintmate-ipc/2 {side}\0"),
                                  .. first, .. second, .. Encoding.UTF8.GetBytes(name)]);

    static string AsciiLower(string s) =>
        string.Concat(s.Select(c => c is >= 'A' and <= 'Z' ? (char)(c + 32) : c));

    public static async Task<Dictionary<string, object>> ReadMessageAsync(Stream s)
    {
        while (true)
        {
            byte[] head = new byte[5];
            await s.ReadExactlyAsync(head);
            int length = BinaryPrimitives.ReadInt32LittleEndian(head);   // check <= max_frame first
            byte[] payload = new byte[length - 1];
            await s.ReadExactlyAsync(payload);
            if (head[4] == 1)
                return MessagePackSerializer.Deserialize<Dictionary<string, object>>(payload);
            // kind 2: stream sample; kind 3: chunk (assemble); 0x80+: skip
        }
    }

    public static async Task WriteMessageAsync(Stream s, Dictionary<string, object> m)
    {
        byte[] body = MessagePackSerializer.Serialize(m);
        byte[] frame = new byte[5 + body.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, body.Length + 1);
        frame[4] = 1;
        body.CopyTo(frame, 5);
        await s.WriteAsync(frame);      // from ONE writer task only
    }

    public static byte[] StreamFrame(uint sid, uint seq, long tNs, bool dropped, ReadOnlySpan<byte> body)
    {
        byte[] f = new byte[25 + body.Length];
        BinaryPrimitives.WriteInt32LittleEndian(f, 21 + body.Length);
        f[4] = 2;
        BinaryPrimitives.WriteUInt32LittleEndian(f.AsSpan(5), sid);
        BinaryPrimitives.WriteUInt32LittleEndian(f.AsSpan(9), seq);
        BinaryPrimitives.WriteInt64LittleEndian(f.AsSpan(13), tNs);
        BinaryPrimitives.WriteUInt32LittleEndian(f.AsSpan(21), dropped ? 1u : 0u);
        body.CopyTo(f.AsSpan(25));
        return f;
    }

    public static long QpcNs()
    {
        long c = System.Diagnostics.Stopwatch.GetTimestamp(), f = System.Diagnostics.Stopwatch.Frequency;
        return c / f * 1_000_000_000 + c % f * 1_000_000_000 / f;
    }
}
```

Notes: configure MessagePack-CSharp so `byte[]` is written as `bin` (the default) and maps
are decoded with string keys; the server side is `NamedPipeServerStreamAcl.Create(name,
PipeDirection.InOut, NamedPipeServerStream.MaxAllowedServerInstances,
PipeTransmissionMode.Byte, PipeOptions.Asynchronous | PipeOptions.FirstPipeInstance, 65536,
65536, security)` with a `PipeSecurity` granting `FullControl` to
`WindowsIdentity.GetCurrent().User` only, and remote clients rejected
(`PipeOptions.CurrentUserOnly` also restricts to the user).

### 19.4 Rust sketch (Tokio)

```rust
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use rmpv::Value; // rmp + rmpv for dynamic MessagePack values

const SQOS: u32 = 0x0010_0000 | 0x0001_0000; // SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION

fn mac(key: &[u8], side: &str, a: &[u8], b: &[u8], name: &str) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
    m.update(format!("stintmate-ipc/2 {side}\0").as_bytes());
    m.update(a);
    m.update(b);
    m.update(name.as_bytes());
    m.finalize().into_bytes().to_vec()
}

async fn read_message(p: &mut NamedPipeClient) -> std::io::Result<Value> {
    loop {
        let len = p.read_u32_le().await? as usize;          // check <= max_frame first
        let kind = p.read_u8().await?;
        let mut payload = vec![0u8; len - 1];
        p.read_exact(&mut payload).await?;
        if kind == 1 {
            return Ok(rmpv::decode::read_value(&mut &payload[..]).expect("msgpack"));
        } // 2: stream sample, 3: chunk (assemble), 0x80..: skip
    }
}

async fn write_message(p: &mut NamedPipeClient, v: &Value) -> std::io::Result<()> {
    let mut body = Vec::new();
    rmpv::encode::write_value(&mut body, v).unwrap();
    let mut frame = Vec::with_capacity(5 + body.len());
    frame.extend_from_slice(&((body.len() + 1) as u32).to_le_bytes());
    frame.push(1);
    frame.extend_from_slice(&body);
    p.write_all(&frame).await                               // from ONE writer task only
}

fn get<'a>(v: &'a Value, k: &str) -> &'a Value {
    v.as_map().unwrap().iter().find(|(kk, _)| kk.as_str() == Some(k)).map(|(_, vv)| vv)
        .unwrap_or(&Value::Nil)
}

pub async fn connect(app: &str, user: &str, key: &[u8]) -> std::io::Result<NamedPipeClient> {
    let name = format!("stintmate-{app}-{}", user.to_ascii_lowercase());
    let mut p = ClientOptions::new().security_qos_flags(SQOS)
        .open(format!(r"\\.\pipe\{name}"))?;
    let hello = read_message(&mut p).await?;
    let s_nonce = get(&hello, "nonce").as_slice().unwrap().to_vec();
    let mut c_nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut c_nonce);
    let peer = Value::Map(vec![("app".into(), "connector-mysim".into()),
                               ("name".into(), "MySim connector".into()),
                               ("version".into(), "1.0.0".into())]);
    let auth = Value::Map(vec![
        ("t".into(), "auth".into()), ("v".into(), 2.into()),
        ("nonce".into(), Value::Binary(c_nonce.to_vec())),
        ("mac".into(), Value::Binary(mac(key, "client", &s_nonce, &c_nonce, &name))),
        ("peer".into(), peer), ("max_frame".into(), (1u32 << 20).into()),
        ("max_message".into(), (1u32 << 28).into())]);
    write_message(&mut p, &auth).await?;
    let welcome = read_message(&mut p).await?;
    let expected = mac(key, "server", &c_nonce, &s_nonce, &name);
    let got = get(&welcome, "mac").as_slice().unwrap_or(&[]);
    use subtle::ConstantTimeEq;
    if get(&welcome, "t").as_str() != Some("welcome") || got.ct_eq(&expected).unwrap_u8() == 0 {
        return Err(std::io::Error::other("refused or not the real endpoint"));
    }
    Ok(p) // then: tokio::io::split, one reader task, one writer task fed by an mpsc channel
}

pub fn stream_frame(sid: u32, seq: u32, t_ns: i64, dropped: bool, body: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(25 + body.len());
    f.extend_from_slice(&((21 + body.len()) as u32).to_le_bytes());
    f.push(2);
    f.extend_from_slice(&sid.to_le_bytes());
    f.extend_from_slice(&seq.to_le_bytes());
    f.extend_from_slice(&t_ns.to_le_bytes());
    f.extend_from_slice(&(dropped as u32).to_le_bytes());
    f.extend_from_slice(body);
    f
}
```

(`to_ascii_lowercase` lowercases A-Z only, as 4.1 requires.) The server side is
`ServerOptions::new().first_pipe_instance(true).reject_remote_clients(true)` with a
security descriptor for the user: Tokio's `create_with_security_attributes_raw`.

### 19.5 Pitfalls

- A blocking read on a synchronous pipe handle blocks writes on it (3.1).
- A writer that blocks when the pipe is full stalls whoever called it: queue and write from
  one writer (12).
- Lowercase the user name with ASCII rules only (4.1); MAC the short name, not the path.
- Read the 4-byte length as **unsigned little-endian**, check it against `max_frame`, then
  read exactly that many bytes; a read may return part of a frame or several frames.
- `bin` vs `str`: nonces, MACs and binary payloads are `bin`. Some MessagePack libraries
  default to `str` for byte strings; configure them.
- Answer `ping`; answer unknown methods with `unknown_method`; ignore unknown message
  types and keys.
- Never block a game loop on the pipe: hand samples to the writer and drop the oldest.

## 20. History

| Version | Date | |
|---|---|---|
| Connector protocol v1 | 2026 | The predecessor, `\\.\pipe\pitwall-<user>`: UTF-8 JSON Lines, `hello`/`welcome` with roles `connector` and `tool`, `request`/`response`/`notify`, live functions relayed by Pitwall's core, a user-only DACL and no authentication beyond it. Large payloads travelled as base64 inside JSON lines (a 1.7 MB lap per line). Each Stintmate app also had its own control pipe (one JSON line per connection), the voice radio a pipe of its own with an HMAC challenge, and events and messages travelled through files (`events.jsonl`, `inbox.jsonl`) read by polling. |
| **2 (revision 2.0)** | 2026-10-20 | One protocol for every channel: length-prefixed binary frames, MessagePack messages, fixed-layout binary stream samples, chunked messages; mutual HMAC-SHA256 handshake with a workspace key and nonces bound to the pipe name; symmetric sessions (either peer calls and subscribes); declarations with versions; event, state and stream channels with snapshots and per-kind slow-subscriber rules; idempotency keys; heartbeat; endpoint names per app and process, and a registry. |

## Appendix A: constants

| Name | Value |
|---|---|
| Protocol name (`hello.proto`) | `stintmate-ipc` |
| Wire version | `2` |
| Frame kinds | `0x01` message, `0x02` stream sample, `0x03` chunk; `0x80`-`0xFF` reserved (skip) |
| Stream sample header | 20 bytes: `sid u32`, `seq u32`, `t_ns i64`, `flags u32` (bit 0: dropped) |
| Chunk header | 8 bytes: `total u32`, `offset u32` |
| Handshake frame limit | 65536 |
| Minimum `max_frame` | 65536 |
| Nonce, MAC, key sizes | 32 bytes each |
| MAC labels | `stintmate-ipc/2 client\0`, `stintmate-ipc/2 server\0` |
| Handshake deadline | 5 s (RECOMMENDED; at most 10 s) |
| Default request deadline | 5 s (RECOMMENDED) |
| Heartbeat | `heartbeat_ms` from `welcome` (RECOMMENDED 5000); dead after 3 x |
| Reconnect backoff | 0.25, 0.5, 1, 2 s, then 2 s; +-20 % jitter; 10 s after `unauthorized` |
| Event queue per subscription | 1024 events or 8 MiB (RECOMMENDED) |
| Stream ring per subscription | 1 s at `rate_hz`, at least 64 (RECOMMENDED) |
| Requests in flight per connection | 1024 (RECOMMENDED) |
| Key file | `<workspace>\ipc\key`, 64 hex characters |
| Registry | `<workspace>\ipc\endpoints\<endpoint-id>.json` |
| Pipe DACL (SDDL) | `D:P(A;;GA;;;<user SID>)` |

## Appendix B: the Stintmate endpoints (informative)

The apps of Stintmate decide their endpoints in their own documentation; at the time of
writing the plan is:

| Endpoint | Pipe | Served by |
|---|---|---|
| `pitwall` | `stintmate-pitwall-<user>` | `pitwall-core` (database, write API, live relay) |
| `pitwall-ui` | `stintmate-pitwall-ui-<user>` | Pitwall's control app (control while the core is off) |
| `voice` | `stintmate-voice-<user>` | Stintmate Voice's control app (control, message tray) |
| `voice-radio` | `stintmate-voice-radio-<user>` | the radio (`voice.say`, radio state) |
| `connector-<id>` | `stintmate-connector-<id>-<user>` | a connector's control app |
| `connector-<id>-<process>` | `stintmate-connector-<id>-<process>-<user>` | a connector service that serves (optional: services usually dial Pitwall) |
