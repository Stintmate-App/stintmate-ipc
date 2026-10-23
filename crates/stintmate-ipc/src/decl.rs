//! Declarations: what a peer offers (SPEC section 9).
//!
//! ```text
//! {"methods":  {"pitwall.write": {"v": [1], "ordered": true, "idempotent": true, "doc": "..."}},
//!  "channels": {"pitwall.lap_completed": {"kind": "event", "v": [1]},
//!               "connector.telemetry": {"kind": "stream", "v": [1], "rate_hz": 200,
//!                                       "layout": {"size": 48, "fields": [...]}}}}
//! ```

use std::collections::BTreeMap;

use crate::codec::{self, Value};
use crate::error::DeclError;
use crate::layout::Layout;

/// Largest version number (SPEC 7.2).
pub const MAX_VERSION: u64 = 65535;
/// Longest method or channel name.
pub const MAX_NAME: usize = 64;

/// Whether `name` is a valid method or channel name (SPEC 7.1):
/// `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`, at most 64 characters.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && name.split('.').all(|seg| {
            let b = seg.as_bytes();
            !b.is_empty()
                && b[0].is_ascii_lowercase()
                && b[1..]
                    .iter()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
        })
}

fn err<T>(msg: String) -> Result<T, DeclError> {
    Err(DeclError(msg))
}

fn versions(v: Option<&Value>, what: &str) -> Result<Vec<u32>, DeclError> {
    let Some(items) = v.and_then(Value::as_array).filter(|a| !a.is_empty()) else {
        return err(format!("{what}: 'v' must be a non-empty list of versions"));
    };
    let mut out = Vec::with_capacity(items.len());
    for x in items {
        match x.as_u64() {
            Some(n) if (1..=MAX_VERSION).contains(&n) => out.push(n as u32),
            _ => {
                return err(format!(
                    "{what}: version {x} must be an integer 1..{MAX_VERSION}"
                ));
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn opt_str(d: &Value, key: &str) -> Option<String> {
    codec::get(d, key)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn opt_map(d: &Value, key: &str) -> Option<Value> {
    codec::get(d, key).filter(|v| v.is_map()).cloned()
}

/// A declared method.
#[derive(Debug, Clone, PartialEq)]
pub struct MethodDecl {
    /// Versions offered, sorted, not empty.
    pub versions: Vec<u32>,
    /// Requests from one connection are handled one at a time, in order (SPEC 13.3).
    pub ordered: bool,
    /// Requests with an idempotency key are deduplicated (SPEC 10.4).
    pub idempotent: bool,
    /// One line for people.
    pub doc: Option<String>,
    /// JSON Schema of `p` (informative).
    pub params: Option<Value>,
    /// JSON Schema of `r` (informative).
    pub result: Option<Value>,
}

impl Default for MethodDecl {
    fn default() -> Self {
        Self {
            versions: vec![1],
            ordered: false,
            idempotent: false,
            doc: None,
            params: None,
            result: None,
        }
    }
}

impl MethodDecl {
    /// The wire form.
    pub fn to_value(&self) -> Value {
        let mut p = vec![("v", versions_value(&self.versions))];
        if self.ordered {
            p.push(("ordered", Value::Boolean(true)));
        }
        if self.idempotent {
            p.push(("idempotent", Value::Boolean(true)));
        }
        if let Some(d) = self.doc.as_deref().filter(|d| !d.is_empty()) {
            p.push(("doc", Value::from(d)));
        }
        if let Some(v) = &self.params {
            p.push(("params", v.clone()));
        }
        if let Some(v) = &self.result {
            p.push(("result", v.clone()));
        }
        codec::map(p)
    }

    /// Parses and validates the wire form of method `name`.
    pub fn from_value(name: &str, d: &Value) -> Result<Self, DeclError> {
        if !d.is_map() {
            return err(format!("method {name}: must be a map"));
        }
        Ok(Self {
            versions: versions(codec::get(d, "v"), &format!("method {name}"))?,
            ordered: codec::get(d, "ordered") == Some(&Value::Boolean(true)),
            idempotent: codec::get(d, "idempotent") == Some(&Value::Boolean(true)),
            doc: opt_str(d, "doc"),
            params: opt_map(d, "params"),
            result: opt_map(d, "result"),
        })
    }
}

fn versions_value(v: &[u32]) -> Value {
    Value::Array(v.iter().map(|x| Value::from(*x)).collect())
}

/// The kind of a channel (SPEC 9.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    /// Events: never dropped.
    Event,
    /// State: the whole value, coalesced.
    State,
    /// Stream samples (kind-2 frames) with a fixed layout.
    Stream,
}

impl ChannelKind {
    /// The wire name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::State => "state",
            Self::Stream => "stream",
        }
    }

    /// Parses the wire name.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "event" => Some(Self::Event),
            "state" => Some(Self::State),
            "stream" => Some(Self::Stream),
            _ => None,
        }
    }
}

/// A declared channel.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelDecl {
    /// The kind.
    pub kind: ChannelKind,
    /// Versions offered, sorted, not empty.
    pub versions: Vec<u32>,
    /// One line for people.
    pub doc: Option<String>,
    /// Streams: the layout of the highest version.
    pub layout: Option<Layout>,
    /// Streams: the nominal rate.
    pub rate_hz: Option<f64>,
    /// Events and state: JSON Schema of `d` (informative).
    pub data: Option<Value>,
}

impl ChannelDecl {
    /// A channel of `kind` at version 1.
    pub fn new(kind: ChannelKind) -> Self {
        Self {
            kind,
            versions: vec![1],
            doc: None,
            layout: None,
            rate_hz: None,
            data: None,
        }
    }

    /// The wire form.
    pub fn to_value(&self) -> Value {
        let mut p = vec![
            ("kind", Value::from(self.kind.name())),
            ("v", versions_value(&self.versions)),
        ];
        if let Some(d) = self.doc.as_deref().filter(|d| !d.is_empty()) {
            p.push(("doc", Value::from(d)));
        }
        if let Some(l) = &self.layout {
            p.push(("layout", l.to_value()));
        }
        if let Some(r) = self.rate_hz {
            // Whole rates go as integers, like the reference implementation's JSON-like numbers.
            if r.fract() == 0.0 && r.abs() < 9e15 {
                p.push(("rate_hz", Value::from(r as i64)));
            } else {
                p.push(("rate_hz", Value::F64(r)));
            }
        }
        if let Some(v) = &self.data {
            p.push(("data", v.clone()));
        }
        codec::map(p)
    }

    /// Parses and validates the wire form of channel `name`.
    pub fn from_value(name: &str, d: &Value) -> Result<Self, DeclError> {
        if !d.is_map() {
            return err(format!("channel {name}: must be a map"));
        }
        let Some(kind) = codec::get(d, "kind")
            .and_then(Value::as_str)
            .and_then(ChannelKind::parse)
        else {
            return err(format!(
                "channel {name}: kind must be one of event, state, stream"
            ));
        };
        let layout = if kind == ChannelKind::Stream {
            let Some(l) = codec::get(d, "layout") else {
                return err(format!("stream {name}: 'layout' is required"));
            };
            Some(Layout::from_value(l)?)
        } else {
            None
        };
        Ok(Self {
            kind,
            versions: versions(codec::get(d, "v"), &format!("channel {name}"))?,
            doc: opt_str(d, "doc"),
            layout,
            rate_hz: codec::get(d, "rate_hz").and_then(Value::as_f64),
            data: opt_map(d, "data"),
        })
    }
}

/// What a peer offers: methods and channels by name.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Declarations {
    /// Methods by name.
    pub methods: BTreeMap<String, MethodDecl>,
    /// Channels by name.
    pub channels: BTreeMap<String, ChannelDecl>,
}

impl Declarations {
    /// The wire form (both maps always present).
    pub fn to_value(&self) -> Value {
        let m = self
            .methods
            .iter()
            .map(|(n, d)| (Value::from(n.as_str()), d.to_value()))
            .collect();
        let c = self
            .channels
            .iter()
            .map(|(n, d)| (Value::from(n.as_str()), d.to_value()))
            .collect();
        codec::map([("methods", Value::Map(m)), ("channels", Value::Map(c))])
    }

    /// Parses and validates; an invalid declaration is a protocol error for the receiver
    /// (SPEC 9.1, 11.2). Nil or absent is empty; unknown keys are ignored.
    pub fn from_value(d: &Value) -> Result<Self, DeclError> {
        let mut out = Self::default();
        if d.is_nil() {
            return Ok(out);
        }
        if !d.is_map() {
            return err("decl must be a map".into());
        }
        let section = |key: &str| -> Result<Vec<(String, Value)>, DeclError> {
            match codec::get(d, key) {
                None | Some(Value::Nil) => Ok(Vec::new()),
                Some(Value::Map(entries)) => entries
                    .iter()
                    .map(|(k, v)| {
                        let name = k.as_str().unwrap_or_default();
                        if !valid_name(name) {
                            return err(format!("invalid {key} name {name:?}"));
                        }
                        Ok((name.to_owned(), v.clone()))
                    })
                    .collect(),
                Some(_) => err(format!("decl.{key} must be a map")),
            }
        };
        for (name, v) in section("methods")? {
            let m = MethodDecl::from_value(&name, &v)?;
            out.methods.insert(name, m);
        }
        for (name, v) in section("channels")? {
            let c = ChannelDecl::from_value(&name, &v)?;
            out.channels.insert(name, c);
        }
        Ok(out)
    }
}
