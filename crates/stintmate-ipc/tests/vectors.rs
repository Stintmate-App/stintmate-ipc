//! Every vector of `vectors/frames.json`, `errors.json` and `layouts.json` (encode must equal
//! the bytes, decode must equal the value).

use std::path::PathBuf;

use serde_json::Value as Json;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::layout::Layout;
use stintmate_ipc::wire::{self, Frame, FrameReader};

fn load(name: &str) -> Json {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../vectors")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// JSON with the `$bin` / `$int` escapes of `vectors/README.md` to a MessagePack value.
fn to_value(j: &Json) -> Value {
    match j {
        Json::Null => Value::Nil,
        Json::Bool(b) => Value::Boolean(*b),
        Json::Number(n) => {
            if let Some(u) = n.as_u64() {
                Value::from(u)
            } else if let Some(i) = n.as_i64() {
                Value::from(i)
            } else {
                Value::F64(n.as_f64().unwrap())
            }
        }
        Json::String(s) => Value::from(s.as_str()),
        Json::Array(a) => Value::Array(a.iter().map(to_value).collect()),
        Json::Object(o) => {
            if o.len() == 1 {
                if let Some(Json::String(h)) = o.get("$bin") {
                    return Value::Binary(hex::decode(h).unwrap());
                }
                if let Some(Json::String(d)) = o.get("$int") {
                    return match d.parse::<i64>() {
                        Ok(i) => Value::from(i),
                        Err(_) => Value::from(d.parse::<u64>().unwrap()),
                    };
                }
            }
            Value::Map(
                o.iter()
                    .map(|(k, v)| (Value::from(k.as_str()), to_value(v)))
                    .collect(),
            )
        }
    }
}

/// Sorts map keys recursively: decoding must match, key order is not significant.
fn norm(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(norm).collect()),
        Value::Map(m) => {
            let mut m: Vec<_> = m.iter().map(|(k, v)| (k.clone(), norm(v))).collect();
            m.sort_by(|a, b| a.0.as_str().cmp(&b.0.as_str()));
            Value::Map(m)
        }
        Value::F32(f) => Value::F64(f64::from(*f)),
        other => other.clone(),
    }
}

fn layout_of(j: &Json) -> Layout {
    Layout::from_value(&to_value(j)).unwrap()
}

#[test]
fn frames_vectors() {
    let doc = load("frames.json");
    let mut checked = 0;
    for case in doc["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let bytes = hex::decode(case["hex"].as_str().unwrap()).unwrap();
        let layout = case.get("layout").map(layout_of);
        let expected = case["frames"].as_array().unwrap();

        // Decode: the whole stream in one push, and byte by byte.
        for split in [usize::MAX, 1] {
            let mut reader = FrameReader::new(1 << 20, wire::DEFAULT_MAX_MESSAGE);
            let mut got = Vec::new();
            for part in bytes.chunks(split.min(bytes.len())) {
                reader.push(part);
                while let Some(f) = reader
                    .next_frame()
                    .unwrap_or_else(|e| panic!("{name}: {e}"))
                {
                    got.push(f);
                }
            }
            assert_eq!(got.len(), expected.len(), "{name}: frame count");
            for (g, e) in got.iter().zip(expected) {
                match g {
                    Frame::Message(b) => {
                        assert_eq!(e["kind"], 1, "{name}");
                        let msg = codec::decode_message(b).unwrap();
                        codec::validate_message(&msg).unwrap();
                        assert_eq!(norm(&msg), norm(&to_value(&e["message"])), "{name}");
                    }
                    Frame::Stream(s) => {
                        assert_eq!(e["kind"], 2, "{name}");
                        assert_eq!(u64::from(s.sid), e["sid"].as_u64().unwrap());
                        assert_eq!(u64::from(s.seq), e["seq"].as_u64().unwrap());
                        assert_eq!(s.t_ns, e["t_ns"].as_i64().unwrap());
                        assert_eq!(u64::from(s.flags), e["flags"].as_u64().unwrap());
                        assert_eq!(hex::encode(&s.body), e["body"].as_str().unwrap());
                        if let Some(l) = layout.as_ref() {
                            assert_eq!(
                                norm(&l.unpack_value(&s.body).unwrap()),
                                norm(&to_value(&e["values"])),
                                "{name}"
                            );
                        }
                    }
                }
            }
        }

        // Encode: the frames, re-encoded, give the bytes (reserved kinds are not re-encoded).
        if name == "reserved_kind" {
            continue;
        }
        let mut out = Vec::new();
        let mut messages = Vec::new();
        for e in expected {
            if e["kind"] == 1 {
                messages.push(codec::encode(&to_value(&e["message"])).unwrap());
            }
        }
        if let Some(chunks) = case.get("chunks") {
            // The stream sample goes between the first and the second chunk.
            let size = chunks[0]["length"].as_u64().unwrap() as usize;
            let mut stream = Vec::new();
            for e in expected.iter().filter(|e| e["kind"] == 2) {
                wire::write_stream_frame(
                    &mut stream,
                    e["sid"].as_u64().unwrap() as u32,
                    e["seq"].as_u64().unwrap() as u32,
                    e["t_ns"].as_i64().unwrap(),
                    e["flags"].as_u64().unwrap() as u32,
                    &hex::decode(e["body"].as_str().unwrap()).unwrap(),
                )
                .unwrap();
            }
            for (i, c) in wire::chunk_frames(&messages[0], size).unwrap().enumerate() {
                out.extend_from_slice(&c);
                if i == 0 {
                    out.extend_from_slice(&stream);
                }
            }
            assert_eq!(
                chunks.as_array().unwrap().len(),
                messages[0].len().div_ceil(size)
            );
        } else {
            let mut m = messages.iter();
            for e in expected {
                if e["kind"] == 1 {
                    wire::write_message_frame(&mut out, m.next().unwrap()).unwrap();
                } else {
                    wire::write_stream_frame(
                        &mut out,
                        e["sid"].as_u64().unwrap() as u32,
                        e["seq"].as_u64().unwrap() as u32,
                        e["t_ns"].as_i64().unwrap(),
                        e["flags"].as_u64().unwrap() as u32,
                        &hex::decode(e["body"].as_str().unwrap()).unwrap(),
                    )
                    .unwrap();
                }
            }
        }
        assert_eq!(
            hex::encode(&out),
            hex::encode(&bytes),
            "{name}: encoded bytes"
        );
        checked += 1;
    }
    assert!(checked >= 15, "checked {checked}");
}

#[test]
fn error_vectors() {
    let doc = load("errors.json");
    for case in doc["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        assert_eq!(case["expect"], "protocol_error");
        let bytes = hex::decode(case["hex"].as_str().unwrap()).unwrap();
        for split in [usize::MAX, 1] {
            let mut reader = FrameReader::new(1_048_576, wire::DEFAULT_MAX_MESSAGE);
            let mut failure = None;
            'feed: for part in bytes.chunks(split.min(bytes.len())) {
                reader.push(part);
                loop {
                    match reader.next_frame() {
                        Ok(Some(Frame::Message(b))) => {
                            let r =
                                codec::decode_message(&b).and_then(|m| codec::validate_message(&m));
                            if let Err(e) = r {
                                failure = Some(e);
                                break 'feed;
                            }
                        }
                        Ok(Some(Frame::Stream(_))) => {}
                        Ok(None) => break,
                        Err(e) => {
                            failure = Some(e);
                            break 'feed;
                        }
                    }
                }
            }
            assert!(
                failure.is_some(),
                "{name} (split {split}) must be a protocol error"
            );
        }
    }
}

#[test]
fn layout_vectors() {
    let doc = load("layouts.json");
    for case in doc["cases"].as_array().unwrap() {
        let layout = layout_of(&case["layout"]);
        assert_eq!(
            layout.size() as u64,
            case["layout"]["size"].as_u64().unwrap()
        );
        // The wire form round-trips.
        assert_eq!(norm(&layout.to_value()), norm(&to_value(&case["layout"])));
        let body = hex::decode(case["body"].as_str().unwrap()).unwrap();
        let values = to_value(&case["values"]);
        let mut packed = Vec::new();
        layout.pack_value(&values, &mut packed).unwrap();
        assert_eq!(hex::encode(&packed), hex::encode(&body));
        assert_eq!(norm(&layout.unpack_value(&body).unwrap()), norm(&values));
    }
}
