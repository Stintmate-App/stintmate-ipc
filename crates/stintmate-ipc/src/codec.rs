//! Message encoding (SPEC section 6): the MessagePack subset and the shape of messages.
//!
//! Values are [`rmpv::Value`] (re-exported as [`Value`]). [`decode`] accepts every valid form
//! of the subset (an integer in a wider form than needed, float32, keys in any order) and
//! rejects what the subset forbids: extension types, non-string map keys, repeated keys,
//! invalid UTF-8, nesting deeper than [`MAX_DEPTH`], trailing bytes. [`encode`] writes the
//! shortest form of every value, keys in the order of the map.

pub use rmpv::Value;

use crate::error::{EncodeError, ProtocolError};

/// Deepest nesting accepted (SPEC 6.1 asks for at least 32).
pub const MAX_DEPTH: usize = 64;

/// Longest idempotency key `ik`, in bytes (SPEC 10.2).
pub const MAX_IK_LEN: usize = 256;

/// Checks that `value` is inside the subset of SPEC 6.1.
pub fn validate(value: &Value) -> Result<(), ProtocolError> {
    validate_depth(value, 0)
}

fn validate_depth(value: &Value, depth: usize) -> Result<(), ProtocolError> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::TooDeep);
    }
    match value {
        Value::Ext(..) => Err(ProtocolError::ExtensionType),
        Value::String(s) if !s.is_str() => Err(ProtocolError::InvalidUtf8),
        Value::Array(items) => items.iter().try_for_each(|v| validate_depth(v, depth + 1)),
        Value::Map(entries) => {
            for (i, (k, v)) in entries.iter().enumerate() {
                let Value::String(ks) = k else {
                    return Err(ProtocolError::NonStringKey);
                };
                let Some(key) = ks.as_str() else {
                    return Err(ProtocolError::InvalidUtf8);
                };
                if entries[..i]
                    .iter()
                    .any(|(other, _)| other.as_str() == Some(key))
                {
                    return Err(ProtocolError::DuplicateKey(key.to_owned()));
                }
                validate_depth(v, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Encodes `value`, appending to `out`. The value must be inside the subset.
pub fn encode_into(out: &mut Vec<u8>, value: &Value) -> Result<(), EncodeError> {
    validate(value).map_err(EncodeError::NotEncodable)?;
    rmpv::encode::write_value(out, value).expect("writing to a Vec cannot fail");
    Ok(())
}

/// Encodes `value` in shortest forms.
pub fn encode(value: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::with_capacity(128);
    encode_into(&mut out, value)?;
    Ok(out)
}

/// Decodes exactly one value from `data`, validating it against the subset as it goes
/// (extension types, non-string or repeated keys, invalid UTF-8, nesting deeper than
/// [`MAX_DEPTH`], truncated input and trailing bytes are protocol errors). Every valid form of
/// a value is accepted.
pub fn decode(data: &[u8]) -> Result<Value, ProtocolError> {
    let mut rd = Reader { data, pos: 0 };
    let value = rd.value(0)?;
    if rd.pos != data.len() {
        return Err(ProtocolError::TrailingData(data.len() - rd.pos));
    }
    Ok(value)
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

fn truncated() -> ProtocolError {
    ProtocolError::InvalidMessagePack("truncated value".into())
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self.pos.checked_add(n).filter(|e| *e <= self.data.len());
        let end = end.ok_or_else(truncated)?;
        let s = &self.data[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn be<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }

    fn len(&mut self, width: usize) -> Result<usize, ProtocolError> {
        Ok(match width {
            1 => usize::from(self.be::<1>()?[0]),
            2 => usize::from(u16::from_be_bytes(self.be()?)),
            _ => u32::from_be_bytes(self.be()?) as usize,
        })
    }

    fn string(&mut self, n: usize) -> Result<Value, ProtocolError> {
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(Value::from(s))
    }

    fn array(&mut self, n: usize, depth: usize) -> Result<Value, ProtocolError> {
        // Every element takes at least one byte: a length beyond the input is truncation,
        // and nothing larger than the input is ever allocated.
        if n > self.data.len() - self.pos {
            return Err(truncated());
        }
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            items.push(self.value(depth + 1)?);
        }
        Ok(Value::Array(items))
    }

    fn map(&mut self, n: usize, depth: usize) -> Result<Value, ProtocolError> {
        if n > (self.data.len() - self.pos) / 2 {
            return Err(truncated());
        }
        let mut entries: Vec<(Value, Value)> = Vec::with_capacity(n);
        for _ in 0..n {
            let key = self.value(depth + 1)?;
            if !key.is_str() {
                return Err(ProtocolError::NonStringKey);
            }
            let value = self.value(depth + 1)?;
            entries.push((key, value));
        }
        if n <= 16 {
            for (i, (k, _)) in entries.iter().enumerate() {
                if entries[..i].iter().any(|(o, _)| o == k) {
                    return Err(ProtocolError::DuplicateKey(
                        k.as_str().unwrap_or("").to_owned(),
                    ));
                }
            }
        } else {
            let mut seen = std::collections::HashSet::with_capacity(n);
            for (k, _) in &entries {
                let k = k.as_str().unwrap_or("");
                if !seen.insert(k) {
                    return Err(ProtocolError::DuplicateKey(k.to_owned()));
                }
            }
        }
        Ok(Value::Map(entries))
    }

    fn value(&mut self, depth: usize) -> Result<Value, ProtocolError> {
        if depth > MAX_DEPTH {
            return Err(ProtocolError::TooDeep);
        }
        let marker = self.be::<1>()?[0];
        Ok(match marker {
            0x00..=0x7f => Value::from(u64::from(marker)),
            0x80..=0x8f => return self.map(usize::from(marker & 0x0f), depth),
            0x90..=0x9f => return self.array(usize::from(marker & 0x0f), depth),
            0xa0..=0xbf => return self.string(usize::from(marker & 0x1f)),
            0xc0 => Value::Nil,
            0xc1 => {
                return Err(ProtocolError::InvalidMessagePack(
                    "marker 0xc1 is never used".into(),
                ));
            }
            0xc2 => Value::Boolean(false),
            0xc3 => Value::Boolean(true),
            0xc4..=0xc6 => {
                let n = self.len(1 << (marker - 0xc4))?;
                Value::Binary(self.take(n)?.to_vec())
            }
            0xc7..=0xc9 | 0xd4..=0xd8 => return Err(ProtocolError::ExtensionType),
            0xca => Value::F32(f32::from_be_bytes(self.be()?)),
            0xcb => Value::F64(f64::from_be_bytes(self.be()?)),
            0xcc => Value::from(u64::from(self.be::<1>()?[0])),
            0xcd => Value::from(u64::from(u16::from_be_bytes(self.be()?))),
            0xce => Value::from(u64::from(u32::from_be_bytes(self.be()?))),
            0xcf => Value::from(u64::from_be_bytes(self.be()?)),
            0xd0 => Value::from(i64::from(self.be::<1>()?[0] as i8)),
            0xd1 => Value::from(i64::from(i16::from_be_bytes(self.be()?))),
            0xd2 => Value::from(i64::from(i32::from_be_bytes(self.be()?))),
            0xd3 => Value::from(i64::from_be_bytes(self.be()?)),
            0xd9..=0xdb => {
                let n = self.len(1 << (marker - 0xd9))?;
                return self.string(n);
            }
            0xdc => {
                let n = self.len(2)?;
                return self.array(n, depth);
            }
            0xdd => {
                let n = self.len(4)?;
                return self.array(n, depth);
            }
            0xde => {
                let n = self.len(2)?;
                return self.map(n, depth);
            }
            0xdf => {
                let n = self.len(4)?;
                return self.map(n, depth);
            }
            0xe0..=0xff => Value::from(i64::from(marker as i8)),
        })
    }
}

/// Decodes a message: a map with a string `t` (SPEC 6.2).
pub fn decode_message(data: &[u8]) -> Result<Value, ProtocolError> {
    let value = decode(data)?;
    message_type(&value)?;
    Ok(value)
}

/// The value of the key `key` of a map value.
pub fn get<'a>(map: &'a Value, key: &str) -> Option<&'a Value> {
    map.as_map()?
        .iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}

/// Builds a map value from `(key, value)` pairs, in order.
pub fn map<I, K>(pairs: I) -> Value
where
    I: IntoIterator<Item = (K, Value)>,
    K: Into<String>,
{
    Value::Map(
        pairs
            .into_iter()
            .map(|(k, v)| (Value::from(k.into()), v))
            .collect(),
    )
}

/// The message type `t` of a decoded message.
pub fn message_type(message: &Value) -> Result<&str, ProtocolError> {
    if !message.is_map() {
        return Err(ProtocolError::NotAMap);
    }
    get(message, "t")
        .and_then(Value::as_str)
        .ok_or(ProtocolError::MissingType)
}

fn bad(t: &str, field: &'static str, expected: &'static str) -> ProtocolError {
    ProtocolError::BadField {
        message: t.to_owned(),
        field,
        expected,
    }
}

/// A field that must be present as an unsigned integer.
fn req_uint(m: &Value, t: &str, field: &'static str) -> Result<(), ProtocolError> {
    match get(m, field) {
        Some(v) if v.is_u64() => Ok(()),
        _ => Err(bad(t, field, "an unsigned integer")),
    }
}

fn opt_uint(m: &Value, t: &str, field: &'static str) -> Result<(), ProtocolError> {
    match get(m, field) {
        None | Some(Value::Nil) => Ok(()),
        Some(v) if v.is_u64() => Ok(()),
        _ => Err(bad(t, field, "an unsigned integer")),
    }
}

fn opt_int(m: &Value, t: &str, field: &'static str) -> Result<(), ProtocolError> {
    match get(m, field) {
        None | Some(Value::Nil) => Ok(()),
        Some(v) if v.is_i64() || v.is_u64() => Ok(()),
        _ => Err(bad(t, field, "an integer")),
    }
}

fn req_str(m: &Value, t: &str, field: &'static str) -> Result<(), ProtocolError> {
    match get(m, field) {
        Some(Value::String(_)) => Ok(()),
        _ => Err(bad(t, field, "a string")),
    }
}

fn opt_map(m: &Value, t: &str, field: &'static str) -> Result<(), ProtocolError> {
    match get(m, field) {
        None | Some(Value::Nil | Value::Map(_)) => Ok(()),
        _ => Err(bad(t, field, "a map")),
    }
}

/// Checks the shape of a decoded message of the types of SPEC 10.1 that can come after the
/// handshake: the REQUIRED fields exist with the right type (SPEC 11.2). A message whose `t`
/// is unknown is valid (it is ignored by the receiver, 6.3); unknown keys are ignored. The
/// handshake messages (`hello`, `auth`, `welcome`) are a protocol error here.
pub fn validate_message(message: &Value) -> Result<(), ProtocolError> {
    let t = message_type(message)?;
    match t {
        "hello" | "auth" | "welcome" => Err(ProtocolError::HandshakeAfterHandshake(t.to_owned())),
        "req" => {
            req_uint(message, t, "id")?;
            req_str(message, t, "m")?;
            opt_uint(message, t, "v")?;
            opt_uint(message, t, "dl")?;
            match get(message, "ik") {
                None | Some(Value::Nil) => Ok(()),
                Some(Value::String(s)) if s.as_bytes().len() <= MAX_IK_LEN => Ok(()),
                _ => Err(bad(t, "ik", "a string of at most 256 bytes")),
            }
        }
        "res" => {
            req_uint(message, t, "id")?;
            match get(message, "ok") {
                Some(Value::Boolean(true)) => Ok(()),
                Some(Value::Boolean(false)) => {
                    let e = get(message, "e").filter(|e| e.is_map());
                    let Some(e) = e else {
                        return Err(bad(t, "e", "a map"));
                    };
                    req_str(e, t, "code")?;
                    opt_uint(e, t, "retry_ms")
                }
                _ => Err(bad(t, "ok", "a boolean")),
            }
        }
        "prog" => {
            req_uint(message, t, "id")?;
            opt_uint(message, t, "dl")
        }
        "cancel" => req_uint(message, t, "id"),
        "sub" => {
            req_uint(message, t, "id")?;
            req_str(message, t, "ch")?;
            opt_uint(message, t, "v")?;
            opt_map(message, t, "o")
        }
        "unsub" => req_uint(message, t, "sid"),
        "end" => {
            req_uint(message, t, "sid")?;
            req_str(message, t, "code")
        }
        "evt" => {
            req_uint(message, t, "sid")?;
            req_uint(message, t, "seq")?;
            opt_int(message, t, "ts")
        }
        "decl" => match get(message, "decl") {
            Some(Value::Map(_)) => Ok(()),
            _ => Err(bad(t, "decl", "a map")),
        },
        "ping" | "pong" => opt_int(message, t, "ts"),
        "bye" => req_str(message, t, "code"),
        _ => Ok(()),
    }
}
