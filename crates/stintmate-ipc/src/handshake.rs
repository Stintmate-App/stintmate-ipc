//! The handshake (SPEC 8): messages, MACs, version negotiation, features and failures.
//!
//! Pure state machines over decoded messages: no I/O. The transport feeds the first decoded
//! message of each direction in and writes what comes out (each step yields the message to
//! send; a failure yields the `bye` to send before closing). Every frame of the handshake
//! must stay within [`HANDSHAKE_MAX_FRAME`](crate::wire::HANDSHAKE_MAX_FRAME); a `bye` from the
//! peer is for the transport to handle before it calls these.

use std::fmt;
use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::codec::{self, Value};
use crate::decl::Declarations;
use crate::error::{EncodeError, ProtocolError};
use crate::keyfile::Key;
use crate::naming::new_run_id;
use crate::wire::{
    self, DEFAULT_MAX_FRAME, DEFAULT_MAX_MESSAGE, HANDSHAKE_MAX_FRAME, MIN_MAX_FRAME,
};

/// The protocol name in `hello.proto`.
pub const PROTO: &str = "stintmate-ipc";
/// Size of a nonce.
pub const NONCE_SIZE: usize = 32;
/// Size of a MAC.
pub const MAC_SIZE: usize = 32;
/// The time a connection has to complete the handshake (SPEC 15.1).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// The wire versions this implementation speaks.
pub const SUPPORTED_VERSIONS: [u32; 1] = [2];
/// The recommended heartbeat period.
pub const DEFAULT_HEARTBEAT_MS: u32 = 5000;
/// The smallest heartbeat period a `welcome` may carry.
pub const MIN_HEARTBEAT_MS: u32 = 100;

/// A handshake nonce.
pub type Nonce = [u8; NONCE_SIZE];
/// A handshake MAC.
pub type MacBytes = [u8; MAC_SIZE];

fn label(version: u32, side: &str) -> Vec<u8> {
    let mut v = format!("stintmate-ipc/{version} {side}").into_bytes();
    v.push(0);
    v
}

fn mac_state(
    key: &Key,
    version: u32,
    side: &str,
    first: &Nonce,
    second: &Nonce,
    name: &str,
) -> Hmac<Sha256> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("HMAC takes any key length");
    m.update(&label(version, side));
    m.update(first);
    m.update(second);
    m.update(name.as_bytes());
    m
}

/// `HMAC-SHA256(key, "stintmate-ipc/<v> client" 0x00 | s_nonce | c_nonce | name)`; `name` is
/// the endpoint's short name (SPEC 8.5).
pub fn client_mac(
    key: &Key,
    version: u32,
    s_nonce: &Nonce,
    c_nonce: &Nonce,
    name: &str,
) -> MacBytes {
    mac_state(key, version, "client", s_nonce, c_nonce, name)
        .finalize()
        .into_bytes()
        .into()
}

/// `HMAC-SHA256(key, "stintmate-ipc/<v> server" 0x00 | c_nonce | s_nonce | name)`.
pub fn server_mac(
    key: &Key,
    version: u32,
    s_nonce: &Nonce,
    c_nonce: &Nonce,
    name: &str,
) -> MacBytes {
    mac_state(key, version, "server", c_nonce, s_nonce, name)
        .finalize()
        .into_bytes()
        .into()
}

/// Verifies a client MAC in constant time.
pub fn verify_client_mac(
    key: &Key,
    version: u32,
    s_nonce: &Nonce,
    c_nonce: &Nonce,
    name: &str,
    mac: &[u8],
) -> bool {
    mac_state(key, version, "client", s_nonce, c_nonce, name)
        .verify_slice(mac)
        .is_ok()
}

/// Verifies a server MAC in constant time.
pub fn verify_server_mac(
    key: &Key,
    version: u32,
    s_nonce: &Nonce,
    c_nonce: &Nonce,
    name: &str,
    mac: &[u8],
) -> bool {
    mac_state(key, version, "server", c_nonce, s_nonce, name)
        .verify_slice(mac)
        .is_ok()
}

/// 32 fresh random bytes.
pub fn new_nonce() -> Nonce {
    let mut n = [0u8; NONCE_SIZE];
    getrandom::fill(&mut n).expect("the system random generator failed");
    n
}

/// The highest version in `offered` that is in `supported` (SPEC 8.7).
pub fn choose_version(offered: &[u32], supported: &[u32]) -> Option<u32> {
    offered
        .iter()
        .copied()
        .filter(|v| supported.contains(v))
        .max()
}

// ----------------------------------------------------------------------------- failures

/// The `code` of the `bye` a failed handshake ends with (SPEC 8.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByeCode {
    /// A wrong MAC.
    Unauthorized,
    /// No version in common.
    VersionUnsupported,
    /// A malformed or unexpected message.
    ProtocolError,
    /// The server has no key.
    Unavailable,
}

impl ByeCode {
    /// The code on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::VersionUnsupported => "version_unsupported",
            Self::ProtocolError => "protocol_error",
            Self::Unavailable => "unavailable",
        }
    }
}

/// A failed handshake: send [`HandshakeFailure::bye`] (unless the peer's failure was the
/// cause) and close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeFailure {
    /// The `bye` code.
    pub code: ByeCode,
    /// The text for the `bye` and the local log; it never holds a key, MAC or nonce.
    pub message: String,
}

impl HandshakeFailure {
    fn new(code: ByeCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// The server has no key (no file).
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ByeCode::Unavailable, message)
    }

    /// The `bye` message to send: `{"t": "bye", "code": ..., "msg": ...}`.
    pub fn bye(&self) -> Value {
        bye_message(self.code.as_str(), &self.message)
    }
}

impl fmt::Display for HandshakeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "handshake failed ({}): {}",
            self.code.as_str(),
            self.message
        )
    }
}

impl std::error::Error for HandshakeFailure {}

impl From<ProtocolError> for HandshakeFailure {
    fn from(e: ProtocolError) -> Self {
        Self::new(ByeCode::ProtocolError, e.to_string())
    }
}

/// `{"t": "bye", "code": code, "msg": msg}`.
pub fn bye_message(code: &str, msg: &str) -> Value {
    codec::map([
        ("t", "bye".into()),
        ("code", code.into()),
        ("msg", msg.into()),
    ])
}

/// A message as one kind-1 frame, ready to write.
pub fn to_frame(message: &Value) -> Result<Vec<u8>, EncodeError> {
    let encoded = codec::encode(message)?;
    let mut out = Vec::with_capacity(encoded.len() + 5);
    wire::write_message_frame(&mut out, &encoded)?;
    Ok(out)
}

// ----------------------------------------------------------------------------- parsing

fn bad(t: &str, field: &'static str, expected: &'static str) -> ProtocolError {
    ProtocolError::BadField {
        message: t.to_owned(),
        field,
        expected,
    }
}

fn present<'a>(m: &'a Value, key: &str) -> Option<&'a Value> {
    codec::get(m, key).filter(|v| !v.is_nil())
}

fn req_u32(m: &Value, t: &str, field: &'static str) -> Result<u32, ProtocolError> {
    present(m, field)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| bad(t, field, "an unsigned 32-bit integer"))
}

fn opt_u32(m: &Value, t: &str, field: &'static str) -> Result<Option<u32>, ProtocolError> {
    match present(m, field) {
        None => Ok(None),
        Some(v) => v
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .map(Some)
            .ok_or_else(|| bad(t, field, "an unsigned 32-bit integer")),
    }
}

fn req_str<'a>(m: &'a Value, t: &str, field: &'static str) -> Result<&'a str, ProtocolError> {
    present(m, field)
        .and_then(Value::as_str)
        .ok_or_else(|| bad(t, field, "a string"))
}

fn fixed_bin<const N: usize>(
    m: &Value,
    t: &str,
    field: &'static str,
) -> Result<[u8; N], ProtocolError> {
    match present(m, field) {
        Some(Value::Binary(b)) if b.len() == N => {
            Ok(b.as_slice().try_into().expect("length checked"))
        }
        _ => Err(bad(t, field, "a bin of 32 bytes")),
    }
}

fn str_list(m: &Value, t: &str, field: &'static str) -> Result<Vec<String>, ProtocolError> {
    match present(m, field) {
        None => Ok(Vec::new()),
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| bad(t, field, "an array of strings"))
            })
            .collect(),
        Some(_) => Err(bad(t, field, "an array of strings")),
    }
}

fn str_array(v: &[String]) -> Value {
    Value::Array(v.iter().map(|s| Value::from(s.as_str())).collect())
}

fn parse_decl(m: &Value) -> Result<Declarations, ProtocolError> {
    match codec::get(m, "decl") {
        None => Ok(Declarations::default()),
        Some(d) => Ok(Declarations::from_value(d)?),
    }
}

fn check_type(m: &Value, want: &'static str) -> Result<(), ProtocolError> {
    let t = codec::message_type(m)?;
    if t == want {
        Ok(())
    } else {
        Err(ProtocolError::InvalidMessagePack(format!(
            "expected {want} but got {t}"
        )))
    }
}

/// The limits one peer announces for what it receives (SPEC 5.2, 8.2, 8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The largest frame `length` it accepts.
    pub max_frame: u32,
    /// The largest message it accepts after reassembly.
    pub max_message: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: DEFAULT_MAX_FRAME,
            max_message: DEFAULT_MAX_MESSAGE,
        }
    }
}

impl Limits {
    /// `max_frame` and `max_message` of a handshake message; absent means 65536 and
    /// `max_frame`; `max_frame` below 65536 or `max_message` below `max_frame` is refused.
    fn parse(m: &Value, t: &str) -> Result<Self, ProtocolError> {
        let max_frame = opt_u32(m, t, "max_frame")?.unwrap_or(HANDSHAKE_MAX_FRAME);
        if max_frame < MIN_MAX_FRAME {
            return Err(bad(t, "max_frame", "at least 65536"));
        }
        let max_message = opt_u32(m, t, "max_message")?.unwrap_or(max_frame);
        if max_message < max_frame {
            return Err(bad(t, "max_message", "at least max_frame"));
        }
        Ok(Self {
            max_frame,
            max_message,
        })
    }
}

// ----------------------------------------------------------------------------- peer

/// Who a peer says it is (`auth.peer` / `welcome.peer`, SPEC 8.4).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PeerInfo {
    /// The app id as in endpoint names (`pitwall`, `voice`, `hub`, `connector-<id>`).
    pub app: String,
    /// A display name.
    pub name: String,
    /// The program's version, for logs.
    pub version: String,
    /// Which process of the app.
    pub process: Option<String>,
    /// App-specific facts (a connector's `sim`), a map when present.
    pub meta: Option<Value>,
}

impl PeerInfo {
    /// A peer with only its app id.
    pub fn new(app: impl Into<String>) -> Self {
        Self {
            app: app.into(),
            ..Self::default()
        }
    }

    /// The wire form: `app`, `name`, `version`, then `process` and `meta` when set.
    pub fn to_value(&self) -> Value {
        let mut pairs = vec![
            ("app", Value::from(self.app.as_str())),
            ("name", Value::from(self.name.as_str())),
            ("version", Value::from(self.version.as_str())),
        ];
        if let Some(p) = &self.process {
            pairs.push(("process", Value::from(p.as_str())));
        }
        if let Some(m) = self
            .meta
            .as_ref()
            .filter(|m| m.as_map().is_some_and(|m| !m.is_empty()))
        {
            pairs.push(("meta", m.clone()));
        }
        codec::map(pairs)
    }

    /// Parses a `peer` map; `t` names the message for the error.
    pub fn from_value(d: &Value, t: &str) -> Result<Self, ProtocolError> {
        if !d.is_map() {
            return Err(bad(t, "peer", "a map"));
        }
        let s = |k: &str| present(d, k).and_then(Value::as_str).map(str::to_owned);
        Ok(Self {
            app: s("app").ok_or_else(|| bad(t, "peer.app", "a string"))?,
            name: s("name").unwrap_or_default(),
            version: s("version").unwrap_or_default(),
            process: s("process"),
            meta: present(d, "meta").filter(|m| m.is_map()).cloned(),
        })
    }
}

// ----------------------------------------------------------------------------- messages

/// The server's first frame (SPEC 8.2).
#[derive(Debug, Clone, PartialEq)]
pub struct Hello {
    /// Wire versions the server speaks.
    pub versions: Vec<u32>,
    /// `s_nonce`.
    pub nonce: Nonce,
    /// The server's limits.
    pub limits: Limits,
    /// Optional extensions the server supports.
    pub features: Vec<String>,
    /// The server's `t_ns` and Unix time in ns, read together (SPEC 14.3).
    pub clock: Option<(i64, i64)>,
}

impl Hello {
    /// The wire form, in the order of the reference encoder.
    pub fn to_value(&self) -> Value {
        let mut pairs = vec![
            ("t", Value::from("hello")),
            ("proto", Value::from(PROTO)),
            (
                "versions",
                Value::Array(self.versions.iter().map(|v| Value::from(*v)).collect()),
            ),
            ("nonce", Value::Binary(self.nonce.to_vec())),
            ("max_frame", Value::from(self.limits.max_frame)),
            ("features", str_array(&self.features)),
        ];
        if let Some((t_ns, unix_ns)) = self.clock {
            pairs.push((
                "clock",
                Value::Array(vec![Value::from(t_ns), Value::from(unix_ns)]),
            ));
        }
        pairs.push(("max_message", Value::from(self.limits.max_message)));
        codec::map(pairs)
    }

    /// Parses and validates a `hello`; `proto` must be `stintmate-ipc`.
    pub fn from_value(m: &Value) -> Result<Self, ProtocolError> {
        const T: &str = "hello";
        check_type(m, T)?;
        if req_str(m, T, "proto")? != PROTO {
            return Err(bad(T, "proto", "\"stintmate-ipc\""));
        }
        let versions = match present(m, "versions") {
            Some(Value::Array(a)) if !a.is_empty() => a
                .iter()
                .map(|v| v.as_u64().and_then(|v| u32::try_from(v).ok()))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| bad(T, "versions", "an array of unsigned integers"))?,
            _ => return Err(bad(T, "versions", "a non-empty array of unsigned integers")),
        };
        let clock = match present(m, "clock") {
            None => None,
            Some(Value::Array(a)) if a.len() == 2 => match (a[0].as_i64(), a[1].as_i64()) {
                (Some(x), Some(y)) => Some((x, y)),
                _ => return Err(bad(T, "clock", "two integers")),
            },
            Some(_) => return Err(bad(T, "clock", "two integers")),
        };
        Ok(Self {
            versions,
            nonce: fixed_bin(m, T, "nonce")?,
            limits: Limits::parse(m, T)?,
            features: str_list(m, T, "features")?,
            clock,
        })
    }
}

/// The client's answer (SPEC 8.3).
#[derive(Debug, Clone, PartialEq)]
pub struct Auth {
    /// The chosen wire version.
    pub v: u32,
    /// `c_nonce`.
    pub nonce: Nonce,
    /// The client MAC.
    pub mac: MacBytes,
    /// The client's identity.
    pub peer: PeerInfo,
    /// What the client offers.
    pub decl: Declarations,
    /// The client's limits.
    pub limits: Limits,
    /// The features of `hello.features` the client wants.
    pub features: Vec<String>,
}

impl Auth {
    /// The wire form.
    pub fn to_value(&self) -> Value {
        codec::map([
            ("t", Value::from("auth")),
            ("v", Value::from(self.v)),
            ("nonce", Value::Binary(self.nonce.to_vec())),
            ("mac", Value::Binary(self.mac.to_vec())),
            ("peer", self.peer.to_value()),
            ("decl", self.decl.to_value()),
            ("max_frame", Value::from(self.limits.max_frame)),
            ("max_message", Value::from(self.limits.max_message)),
            ("features", str_array(&self.features)),
        ])
    }

    /// Parses and validates an `auth` (the MAC is not verified here).
    pub fn from_value(m: &Value) -> Result<Self, ProtocolError> {
        const T: &str = "auth";
        check_type(m, T)?;
        Ok(Self {
            v: req_u32(m, T, "v")?,
            nonce: fixed_bin(m, T, "nonce")?,
            mac: fixed_bin(m, T, "mac")?,
            peer: PeerInfo::from_value(
                present(m, "peer").ok_or_else(|| bad(T, "peer", "a map"))?,
                T,
            )?,
            decl: parse_decl(m)?,
            limits: Limits::parse(m, T)?,
            features: str_list(m, T, "features")?,
        })
    }
}

/// The server's answer (SPEC 8.4).
#[derive(Debug, Clone, PartialEq)]
pub struct Welcome {
    /// Echoes `auth.v`.
    pub v: u32,
    /// The server MAC.
    pub mac: MacBytes,
    /// The server's random id for this start.
    pub run: String,
    /// The server's identity.
    pub peer: PeerInfo,
    /// What the server offers.
    pub decl: Declarations,
    /// The heartbeat period both peers use.
    pub heartbeat_ms: u32,
    /// The features in effect.
    pub features: Vec<String>,
}

impl Welcome {
    /// The wire form.
    pub fn to_value(&self) -> Value {
        codec::map([
            ("t", Value::from("welcome")),
            ("v", Value::from(self.v)),
            ("mac", Value::Binary(self.mac.to_vec())),
            ("run", Value::from(self.run.as_str())),
            ("peer", self.peer.to_value()),
            ("decl", self.decl.to_value()),
            ("heartbeat_ms", Value::from(self.heartbeat_ms)),
            ("features", str_array(&self.features)),
        ])
    }

    /// Parses and validates a `welcome` (the MAC is not verified here).
    pub fn from_value(m: &Value) -> Result<Self, ProtocolError> {
        const T: &str = "welcome";
        check_type(m, T)?;
        let heartbeat_ms = req_u32(m, T, "heartbeat_ms")?;
        if heartbeat_ms < MIN_HEARTBEAT_MS {
            return Err(bad(T, "heartbeat_ms", "at least 100"));
        }
        Ok(Self {
            v: req_u32(m, T, "v")?,
            mac: fixed_bin(m, T, "mac")?,
            run: req_str(m, T, "run")?.to_owned(),
            peer: PeerInfo::from_value(
                present(m, "peer").ok_or_else(|| bad(T, "peer", "a map"))?,
                T,
            )?,
            decl: parse_decl(m)?,
            heartbeat_ms,
            features: str_list(m, T, "features")?,
        })
    }
}

// ----------------------------------------------------------------------------- server

/// What a server brings to every handshake.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The endpoint's short name (`stintmate-<app>[-<process>]-<user>`), bound into the MACs.
    pub short_name: String,
    /// The server's identity.
    pub peer: PeerInfo,
    /// What the server offers.
    pub decl: Declarations,
    /// The server's limits.
    pub limits: Limits,
    /// The features the server supports.
    pub features: Vec<String>,
    /// The heartbeat period to announce.
    pub heartbeat_ms: u32,
    /// The id of this server start (`welcome.run`).
    pub run: String,
}

impl ServerConfig {
    /// A config with default limits, no features, a 5 s heartbeat and a fresh `run` id.
    pub fn new(short_name: impl Into<String>, peer: PeerInfo, decl: Declarations) -> Self {
        Self {
            short_name: short_name.into(),
            peer,
            decl,
            limits: Limits::default(),
            features: Vec::new(),
            heartbeat_ms: DEFAULT_HEARTBEAT_MS,
            run: new_run_id(),
        }
    }
}

/// The server's side of one connection's handshake.
#[derive(Debug)]
pub struct ServerHandshake {
    config: ServerConfig,
    key: Key,
    hello: Hello,
}

/// A client that passed the handshake, from the server's point of view.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerAccepted {
    /// The `welcome` to send.
    pub welcome: Value,
    /// The negotiated wire version.
    pub version: u32,
    /// The client's identity.
    pub peer: PeerInfo,
    /// What the client offers.
    pub decl: Declarations,
    /// The client's limits: what the server may send.
    pub peer_limits: Limits,
    /// The features in effect.
    pub features: Vec<String>,
}

impl ServerHandshake {
    /// Starts a handshake with a fresh nonce. `clock` is the server's `(t_ns, unix_ns)`.
    pub fn new(config: ServerConfig, key: Key, clock: Option<(i64, i64)>) -> Self {
        Self::with_nonce(config, key, new_nonce(), clock)
    }

    /// As [`ServerHandshake::new`] with a given nonce (tests and vectors).
    pub fn with_nonce(
        config: ServerConfig,
        key: Key,
        nonce: Nonce,
        clock: Option<(i64, i64)>,
    ) -> Self {
        let hello = Hello {
            versions: SUPPORTED_VERSIONS.to_vec(),
            nonce,
            limits: config.limits,
            features: config.features.clone(),
            clock,
        };
        Self { config, key, hello }
    }

    /// The `hello` to send first.
    pub fn hello(&self) -> Value {
        self.hello.to_value()
    }

    /// Handles the client's first message: verifies it and builds the `welcome`.
    pub fn on_auth(&self, message: &Value) -> Result<ServerAccepted, HandshakeFailure> {
        let auth = Auth::from_value(message)?;
        if !self.hello.versions.contains(&auth.v) {
            return Err(HandshakeFailure::new(
                ByeCode::VersionUnsupported,
                format!("wire version {} is not supported", auth.v),
            ));
        }
        let name = &self.config.short_name;
        if !verify_client_mac(
            &self.key,
            auth.v,
            &self.hello.nonce,
            &auth.nonce,
            name,
            &auth.mac,
        ) {
            return Err(HandshakeFailure::new(
                ByeCode::Unauthorized,
                "authentication failed",
            ));
        }
        let mut features: Vec<String> = Vec::new();
        for f in &auth.features {
            if self.config.features.contains(f) && !features.contains(f) {
                features.push(f.clone());
            }
        }
        let welcome = Welcome {
            v: auth.v,
            mac: server_mac(&self.key, auth.v, &self.hello.nonce, &auth.nonce, name),
            run: self.config.run.clone(),
            peer: self.config.peer.clone(),
            decl: self.config.decl.clone(),
            heartbeat_ms: self.config.heartbeat_ms,
            features: features.clone(),
        };
        Ok(ServerAccepted {
            welcome: welcome.to_value(),
            version: auth.v,
            peer: auth.peer,
            decl: auth.decl,
            peer_limits: auth.limits,
            features,
        })
    }
}

// ----------------------------------------------------------------------------- client

/// What a client brings to every handshake.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// The short name of the endpoint it connects to, bound into the MACs.
    pub short_name: String,
    /// The client's identity.
    pub peer: PeerInfo,
    /// What the client offers.
    pub decl: Declarations,
    /// The client's limits.
    pub limits: Limits,
    /// The features the client wants (those the server lists are requested).
    pub features: Vec<String>,
}

impl ClientConfig {
    /// A config with default limits and no features.
    pub fn new(short_name: impl Into<String>, peer: PeerInfo, decl: Declarations) -> Self {
        Self {
            short_name: short_name.into(),
            peer,
            decl,
            limits: Limits::default(),
            features: Vec::new(),
        }
    }
}

/// A server that passed the handshake, from the client's point of view.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientEstablished {
    /// The negotiated wire version.
    pub version: u32,
    /// The server's `run` id.
    pub run: String,
    /// The server's identity.
    pub peer: PeerInfo,
    /// What the server offers.
    pub decl: Declarations,
    /// The server's limits: what the client may send.
    pub peer_limits: Limits,
    /// The heartbeat period.
    pub heartbeat_ms: u32,
    /// The features in effect.
    pub features: Vec<String>,
    /// The server's clock sample from `hello`, if it sent one.
    pub clock: Option<(i64, i64)>,
}

struct Sent {
    hello: Hello,
    version: u32,
    nonce: Nonce,
    features: Vec<String>,
}

/// The client's side of one connection's handshake.
pub struct ClientHandshake {
    config: ClientConfig,
    key: Key,
    nonce: Nonce,
    sent: Option<Sent>,
}

impl fmt::Debug for ClientHandshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientHandshake")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ClientHandshake {
    /// Starts a handshake with a fresh nonce.
    pub fn new(config: ClientConfig, key: Key) -> Self {
        Self::with_nonce(config, key, new_nonce())
    }

    /// As [`ClientHandshake::new`] with a given nonce (tests and vectors).
    pub fn with_nonce(config: ClientConfig, key: Key, nonce: Nonce) -> Self {
        Self {
            config,
            key,
            nonce,
            sent: None,
        }
    }

    /// Handles the server's `hello`: negotiates the version and builds the `auth` to send.
    pub fn on_hello(&mut self, message: &Value) -> Result<Value, HandshakeFailure> {
        let hello = Hello::from_value(message)?;
        let version = choose_version(&hello.versions, &SUPPORTED_VERSIONS).ok_or_else(|| {
            HandshakeFailure::new(
                ByeCode::VersionUnsupported,
                format!(
                    "the server speaks versions {:?}, this client {:?}",
                    hello.versions, SUPPORTED_VERSIONS
                ),
            )
        })?;
        let mut features: Vec<String> = Vec::new();
        for f in &self.config.features {
            if hello.features.contains(f) && !features.contains(f) {
                features.push(f.clone());
            }
        }
        let auth = Auth {
            v: version,
            nonce: self.nonce,
            mac: client_mac(
                &self.key,
                version,
                &hello.nonce,
                &self.nonce,
                &self.config.short_name,
            ),
            peer: self.config.peer.clone(),
            decl: self.config.decl.clone(),
            limits: self.config.limits,
            features: features.clone(),
        };
        self.sent = Some(Sent {
            hello,
            version,
            nonce: self.nonce,
            features,
        });
        Ok(auth.to_value())
    }

    /// Handles the server's `welcome`: verifies the server MAC before anything is trusted.
    pub fn on_welcome(&mut self, message: &Value) -> Result<ClientEstablished, HandshakeFailure> {
        let sent = self
            .sent
            .as_ref()
            .ok_or_else(|| HandshakeFailure::new(ByeCode::ProtocolError, "welcome before hello"))?;
        let welcome = Welcome::from_value(message)?;
        if welcome.v != sent.version {
            return Err(HandshakeFailure::new(
                ByeCode::ProtocolError,
                format!(
                    "welcome.v {} differs from auth.v {}",
                    welcome.v, sent.version
                ),
            ));
        }
        if !verify_server_mac(
            &self.key,
            sent.version,
            &sent.hello.nonce,
            &sent.nonce,
            &self.config.short_name,
            &welcome.mac,
        ) {
            return Err(HandshakeFailure::new(
                ByeCode::Unauthorized,
                "the endpoint failed authentication: another key, or not the real endpoint",
            ));
        }
        if let Some(f) = welcome.features.iter().find(|f| !sent.features.contains(f)) {
            return Err(HandshakeFailure::new(
                ByeCode::ProtocolError,
                format!("welcome enables the feature {f:?} that was not requested"),
            ));
        }
        Ok(ClientEstablished {
            version: sent.version,
            run: welcome.run,
            peer: welcome.peer,
            decl: welcome.decl,
            peer_limits: sent.hello.limits,
            heartbeat_ms: welcome.heartbeat_ms,
            features: welcome.features,
            clock: sent.hello.clock,
        })
    }
}
