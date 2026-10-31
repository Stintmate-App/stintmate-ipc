//! Unit tests of the limits (SPEC 5.2), chunking (5.4), the MessagePack subset (6.1) and the
//! message shapes (6.2, 11.2).

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::decl::{Declarations, valid_name};
use stintmate_ipc::layout::{FieldType, Layout, Scalar};
use stintmate_ipc::wire::{self, Frame, FrameReader};
use stintmate_ipc::{EncodeError, ProtocolError};

fn msg_frame(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    wire::write_message_frame(&mut out, &codec::encode(v).unwrap()).unwrap();
    out
}

fn ping(ts: u64) -> Value {
    codec::map([("t", Value::from("ping")), ("ts", Value::from(ts))])
}

fn drain(r: &mut FrameReader) -> Result<Vec<Frame>, ProtocolError> {
    let mut v = Vec::new();
    while let Some(f) = r.next_frame()? {
        v.push(f);
    }
    Ok(v)
}

#[test]
fn length_is_checked_before_the_body_arrives() {
    let mut r = FrameReader::new(65536, wire::DEFAULT_MAX_MESSAGE);
    // Only the 4 length bytes (65537) and nothing else: refused at once.
    r.push(&65537u32.to_le_bytes());
    assert_eq!(
        r.next_frame(),
        Err(ProtocolError::FrameTooLarge {
            length: 65537,
            max: 65536
        })
    );
    // The reader is poisoned: it never resynchronises.
    r.push(&msg_frame(&ping(1)));
    assert!(r.next_frame().is_err());
    assert_eq!(r.buffered(), 0);
}

#[test]
fn frame_at_the_limit_is_accepted() {
    let mut r = FrameReader::new(65536, wire::DEFAULT_MAX_MESSAGE);
    let mut f = Vec::new();
    wire::write_stream_frame(&mut f, 1, 1, 0, 0, &vec![7u8; 65536 - 21]).unwrap();
    assert_eq!(f.len() - 4, 65536);
    r.push(&f);
    assert!(matches!(r.next_frame(), Ok(Some(Frame::Stream(_)))));
}

#[test]
fn zero_length_and_bad_kinds() {
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    r.push(&[0, 0, 0, 0]);
    assert_eq!(r.next_frame(), Err(ProtocolError::ZeroLength));
    for kind in [0u8, 4, 0x7f] {
        let mut r = FrameReader::new(1 << 20, 1 << 28);
        r.push(&[1, 0, 0, 0, kind]);
        assert_eq!(r.next_frame(), Err(ProtocolError::InvalidKind(kind)));
    }
}

#[test]
fn reserved_kinds_are_skipped_even_inside_a_transfer() {
    let enc = codec::encode(&ping(5)).unwrap();
    let mut bytes = Vec::new();
    let mut chunks = wire::chunk_frames(&enc, 3).unwrap();
    bytes.extend(chunks.next().unwrap());
    bytes.extend([2, 0, 0, 0, 0x90, 0xAA]);
    for c in chunks {
        bytes.extend(c);
    }
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    r.push(&bytes);
    assert_eq!(drain(&mut r).unwrap(), vec![Frame::Message(enc.into())]);
    assert!(!r.in_transfer());
}

#[test]
fn stream_header_and_flags() {
    let mut f = Vec::new();
    wire::write_stream_frame(&mut f, 9, u32::MAX, -5, 3, b"abc").unwrap();
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    r.push(&f);
    let Some(Frame::Stream(s)) = r.next_frame().unwrap() else {
        panic!()
    };
    assert_eq!((s.sid, s.seq, s.t_ns, s.flags), (9, u32::MAX, -5, 3));
    assert_eq!(&s.body[..], b"abc");
    // 19 payload bytes is too short.
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    r.push(&[20, 0, 0, 0, 2]);
    r.push(&[0u8; 19]);
    assert_eq!(r.next_frame(), Err(ProtocolError::StreamTooShort(19)));
}

#[test]
fn first_chunk_over_max_message_is_refused_without_the_rest() {
    let mut r = FrameReader::new(65536, 1000);
    let mut p = Vec::new();
    p.extend(1001u32.to_le_bytes());
    p.extend(0u32.to_le_bytes());
    p.push(1);
    let mut f = Vec::new();
    wire::write_frame(&mut f, wire::KIND_CHUNK, &p).unwrap();
    r.push(&f);
    assert_eq!(
        r.next_frame(),
        Err(ProtocolError::MessageTooLarge {
            total: 1001,
            max: 1000
        })
    );
}

fn chunk(total: u32, offset: u32, data: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend(total.to_le_bytes());
    p.extend(offset.to_le_bytes());
    p.extend(data);
    let mut f = Vec::new();
    wire::write_frame(&mut f, wire::KIND_CHUNK, &p).unwrap();
    f
}

#[test]
fn chunk_violations() {
    let cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("empty chunk", vec![chunk(10, 0, b"")]),
        ("zero total", vec![chunk(0, 0, b"x")]),
        ("more than total, first", vec![chunk(2, 0, b"xyz")]),
        (
            "overlap",
            vec![chunk(10, 0, b"abcd"), chunk(10, 2, b"cdef")],
        ),
        ("overrun", vec![chunk(6, 0, b"abcd"), chunk(6, 4, b"efgh")]),
        (
            "total changes",
            vec![chunk(10, 0, b"abcd"), chunk(11, 4, b"efgh")],
        ),
        (
            "empty second chunk",
            vec![chunk(10, 0, b"abcd"), chunk(10, 4, b"")],
        ),
    ];
    for (name, frames) in cases {
        let mut r = FrameReader::new(1 << 20, 1 << 28);
        for f in frames {
            r.push(&f);
        }
        assert!(drain(&mut r).is_err(), "{name}");
    }
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    r.push(&[8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(r.next_frame(), Err(ProtocolError::ChunkTooShort(7)));
}

#[test]
fn single_chunk_message_and_exact_total() {
    let enc = codec::encode(&ping(1)).unwrap();
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    for c in wire::chunk_frames(&enc, 1 << 20).unwrap() {
        r.push(&c);
    }
    assert_eq!(drain(&mut r).unwrap(), vec![Frame::Message(enc.into())]);
}

#[test]
fn chunk_sizes() {
    assert_eq!(wire::chunk_data_for(65536), 65536 - 9);
    assert_eq!(wire::chunk_data_for(1 << 24), wire::CHUNK_DATA);
    assert_eq!(wire::chunk_data_for(0), 1);
    assert!(matches!(
        wire::chunk_frames(b"x", 0),
        Err(EncodeError::ZeroChunk)
    ));
}

// ---------------------------------------------------------------- the MessagePack subset

#[test]
fn subset_rejections() {
    let cases: &[(&str, &[u8])] = &[
        ("ext type", &[0xd4, 0x05, 0x01]),
        ("timestamp ext", &[0xd6, 0xff, 0, 0, 0, 1]),
        ("never used marker", &[0xc1]),
        ("integer key", &[0x81, 0x01, 0x02]),
        ("duplicate key", &[0x82, 0xa1, b'a', 0x01, 0xa1, b'a', 0x02]),
        ("invalid utf-8", &[0xa2, 0xff, 0xfe]),
        ("trailing", &[0xc0, 0xc0]),
        ("empty", &[]),
        ("huge array length", &[0xdd, 0xff, 0xff, 0xff, 0xff]),
        ("huge map length", &[0xdf, 0xff, 0xff, 0xff, 0xff]),
        ("huge bin length", &[0xc6, 0xff, 0xff, 0xff, 0xff, 1]),
    ];
    for (name, bytes) in cases {
        assert!(codec::decode(bytes).is_err(), "{name}");
    }
}

#[test]
fn every_valid_form_is_accepted() {
    // 1 as uint64, float32, bin16, str32, map with keys in any order.
    let one = [0xcf, 0, 0, 0, 0, 0, 0, 0, 1];
    assert_eq!(codec::decode(&one).unwrap(), Value::from(1u64));
    assert_eq!(
        codec::decode(&[0xca, 0x3f, 0xc0, 0, 0]).unwrap(),
        Value::F32(1.5)
    );
    assert_eq!(
        codec::decode(&[0xc5, 0, 2, 9, 8]).unwrap(),
        Value::Binary(vec![9, 8])
    );
    assert_eq!(
        codec::decode(&[0xdb, 0, 0, 0, 1, b'x']).unwrap(),
        Value::from("x")
    );
    assert_eq!(
        codec::decode(&[0xd3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]).unwrap(),
        Value::from(-2i64)
    );
}

#[test]
fn shortest_forms() {
    let enc = |v: Value| codec::encode(&v).unwrap();
    assert_eq!(enc(Value::from(5u64)), [0x05]);
    assert_eq!(enc(Value::from(-1i64)), [0xff]);
    assert_eq!(enc(Value::from(-33i64)), [0xd0, 0xdf]);
    assert_eq!(enc(Value::from(200u64)), [0xcc, 200]);
    assert_eq!(enc(Value::from(70000u64)), [0xce, 0, 1, 0x11, 0x70]);
    assert_eq!(enc(Value::from("a")), [0xa1, b'a']);
    assert_eq!(enc(Value::Binary(vec![1])), [0xc4, 1, 1]);
    assert_eq!(enc(Value::F64(1.5)).len(), 9);
}

#[test]
fn encode_refuses_what_the_subset_forbids() {
    let ext = Value::Ext(5, vec![1]);
    assert!(codec::encode(&ext).is_err());
    let int_key = Value::Map(vec![(Value::from(1u64), Value::Nil)]);
    assert!(codec::encode(&int_key).is_err());
    let dup = Value::Map(vec![
        (Value::from("a"), Value::Nil),
        (Value::from("a"), Value::Nil),
    ]);
    assert!(codec::encode(&dup).is_err());
}

#[test]
fn nesting_depth() {
    let nest = |n: usize| {
        let mut v = Value::Nil;
        for _ in 0..n {
            v = Value::Array(vec![v]);
        }
        v
    };
    for depth in [1, 32, codec::MAX_DEPTH] {
        let b = codec::encode(&nest(depth)).unwrap();
        assert_eq!(codec::decode(&b).unwrap(), nest(depth), "depth {depth}");
    }
    assert!(codec::encode(&nest(codec::MAX_DEPTH + 2)).is_err());
    let mut deep = vec![0x91; 100_000];
    deep.push(0xc0);
    assert_eq!(codec::decode(&deep), Err(ProtocolError::TooDeep));
}

#[test]
fn big_ids_round_trip_exactly() {
    for id in [(1u64 << 53) + 1, u64::MAX] {
        let m = codec::map([("t", Value::from("cancel")), ("id", Value::from(id))]);
        let back = codec::decode_message(&codec::encode(&m).unwrap()).unwrap();
        assert_eq!(codec::get(&back, "id").unwrap().as_u64(), Some(id));
    }
}

// ---------------------------------------------------------------- messages

#[test]
fn message_shapes() {
    let ok = |v: Value| codec::validate_message(&v).is_ok();
    let m = |pairs: Vec<(&str, Value)>| codec::map(pairs);
    assert!(ok(m(vec![
        ("t", "req".into()),
        ("id", 1u64.into()),
        ("m", "a.b".into())
    ])));
    assert!(!ok(m(vec![
        ("t", "req".into()),
        ("id", "x".into()),
        ("m", "a.b".into())
    ])));
    assert!(!ok(m(vec![
        ("t", "req".into()),
        ("id", (-1i64).into()),
        ("m", "a.b".into())
    ])));
    assert!(!ok(m(vec![("t", "req".into()), ("id", 1u64.into())])));
    assert!(!ok(m(vec![
        ("t", "req".into()),
        ("id", 1u64.into()),
        ("m", "a".into()),
        ("ik", "x".repeat(257).into())
    ])));
    assert!(ok(m(vec![
        ("t", "req".into()),
        ("id", 1u64.into()),
        ("m", "a".into()),
        ("ik", "x".repeat(256).into())
    ])));
    assert!(!ok(m(vec![
        ("t", "res".into()),
        ("id", 1u64.into()),
        ("ok", false.into())
    ])));
    assert!(ok(m(vec![
        ("t", "res".into()),
        ("id", 1u64.into()),
        ("ok", true.into()),
        ("r", Value::Nil)
    ])));
    assert!(!ok(m(vec![("t", "unsub".into())])));
    assert!(ok(m(vec![
        ("t", "from_the_future".into()),
        ("x", 1u64.into())
    ])));
    assert!(!ok(m(vec![("t", "hello".into())])));
    assert!(!ok(Value::Array(vec![])));
    assert!(codec::decode_message(&codec::encode(&Value::from(1u64)).unwrap()).is_err());
}

// ---------------------------------------------------------------- layouts and declarations

#[test]
fn layout_validation() {
    let f = |n: &str, t, c| (n.to_owned(), t, c);
    assert!(Layout::new([f("a", FieldType::U8, 1)], None).is_ok());
    assert!(Layout::new(Vec::<(String, FieldType, usize)>::new(), None).is_err());
    assert!(Layout::new([f("A", FieldType::U8, 1)], None).is_err());
    assert!(Layout::new([f("a", FieldType::U8, 1), f("a", FieldType::U8, 1)], None).is_err());
    assert!(Layout::new([f("a", FieldType::U8, 0)], None).is_err());
    assert!(Layout::new([f("a", FieldType::U8, 4097)], None).is_err());
    let exact = [f("a", FieldType::F64, 4096), f("b", FieldType::F64, 4096)];
    assert!(Layout::new(exact.clone(), None).is_ok());
    assert!(
        Layout::new(
            [exact[0].clone(), exact[1].clone(), f("c", FieldType::U8, 1)],
            None
        )
        .is_err()
    );
    assert!(Layout::new([f(&"a".repeat(65), FieldType::U8, 1)], None).is_err());
    let many: Vec<_> = (0..1025)
        .map(|i| (format!("f{i}"), FieldType::U8, 1))
        .collect();
    assert!(Layout::new(many, None).is_err());
    let exact: Vec<_> = (0..1024)
        .map(|i| (format!("f{i}"), FieldType::U8, 1))
        .collect();
    assert!(Layout::new(exact, None).is_ok());
}

#[test]
fn layout_size_must_match() {
    let l = Layout::new([("a", FieldType::U32, 2)], Some("x")).unwrap();
    let mut v = l.to_value();
    assert_eq!(Layout::from_value(&v).unwrap(), l);
    if let Value::Map(m) = &mut v {
        m[0].1 = Value::from(7u64);
    }
    assert!(Layout::from_value(&v).is_err());
}

#[test]
fn layout_pack_checks_types_and_sizes() {
    let l = Layout::new([("a", FieldType::U16, 1), ("b", FieldType::F32, 2)], None).unwrap();
    let mut out = Vec::new();
    assert!(l.pack(&[Scalar::U16(1)], &mut out).is_err());
    assert!(
        l.pack(
            &[Scalar::U8(1), Scalar::F32(1.0), Scalar::F32(2.0)],
            &mut out
        )
        .is_err()
    );
    let mut flat = Vec::new();
    assert!(l.unpack(&[0; 9], &mut flat).is_err());
}

#[test]
fn declarations_round_trip_and_validation() {
    let d = Declarations::from_value(&Value::Nil).unwrap();
    assert!(d.methods.is_empty() && d.channels.is_empty());
    let l = Layout::new([("speed", FieldType::F32, 12)], Some("bench")).unwrap();
    let mut decl = Declarations::default();
    decl.methods.insert(
        "pitwall.write".into(),
        stintmate_ipc::decl::MethodDecl {
            ordered: true,
            idempotent: true,
            doc: Some("Store".into()),
            ..Default::default()
        },
    );
    let mut ch = stintmate_ipc::decl::ChannelDecl::new(stintmate_ipc::decl::ChannelKind::Stream);
    ch.layout = Some(l);
    ch.rate_hz = Some(200.0);
    decl.channels.insert("connector.telemetry".into(), ch);
    let v = decl.to_value();
    let back =
        Declarations::from_value(&codec::decode(&codec::encode(&v).unwrap()).unwrap()).unwrap();
    assert_eq!(back, decl);

    let bad = |v: Value| Declarations::from_value(&v).is_err();
    let one = |section: &str, name: &str, body: Value| {
        codec::map([(section, codec::map([(name, body)]))])
    };
    assert!(bad(one(
        "methods",
        "Bad.Name",
        codec::map([("v", Value::Array(vec![1u64.into()]))])
    )));
    assert!(bad(one(
        "methods",
        "a.b",
        codec::map([("v", Value::Array(vec![]))])
    )));
    assert!(bad(one(
        "methods",
        "a.b",
        codec::map([("v", Value::Array(vec![0u64.into()]))])
    )));
    assert!(bad(one(
        "channels",
        "a.b",
        codec::map([
            ("kind", "weird".into()),
            ("v", Value::Array(vec![1u64.into()]))
        ])
    )));
    assert!(bad(one(
        "channels",
        "a.b",
        codec::map([
            ("kind", "stream".into()),
            ("v", Value::Array(vec![1u64.into()]))
        ])
    )));
    assert!(bad(Value::from(1u64)));
    // Unknown keys are ignored.
    assert!(!bad(one(
        "methods",
        "a.b",
        codec::map([
            ("v", Value::Array(vec![1u64.into()])),
            ("future", 1u64.into())
        ])
    )));
}

#[test]
fn names() {
    for ok in ["a", "pitwall.write", "a_b.c9", "connector.telemetry"] {
        assert!(valid_name(ok), "{ok}");
    }
    for bad in [
        "",
        "A",
        "a.",
        ".a",
        "a..b",
        "1a",
        "a-b",
        "a.B",
        &"a".repeat(65),
    ] {
        assert!(!valid_name(bad), "{bad}");
    }
    assert!(valid_name(&"a".repeat(64)));
}

/// The fields SPEC 10.1 marks REQUIRED: `evt.d`, `res.r` when `ok` is true, and `e.msg`. A nil
/// value counts as given; only a missing key is refused.
#[test]
fn required_payload_fields() {
    let ok = |v: Value| codec::validate_message(&v).is_ok();
    let m = |pairs: Vec<(&str, Value)>| codec::map(pairs);
    let evt = |extra: Vec<(&str, Value)>| {
        let mut p = vec![
            ("t", "evt".into()),
            ("sid", 1u64.into()),
            ("seq", 1u64.into()),
        ];
        p.extend(extra);
        m(p)
    };
    assert!(!ok(evt(vec![])), "evt without d");
    assert!(ok(evt(vec![("d", Value::Nil)])), "evt with a nil d");
    assert!(ok(evt(vec![("d", 3u64.into())])));
    let res_ok = |extra: Vec<(&str, Value)>| {
        let mut p = vec![
            ("t", "res".into()),
            ("id", 1u64.into()),
            ("ok", true.into()),
        ];
        p.extend(extra);
        m(p)
    };
    assert!(!ok(res_ok(vec![])), "ok res without r");
    assert!(ok(res_ok(vec![("r", Value::Nil)])), "ok res with a nil r");
    assert!(ok(res_ok(vec![("r", "x".into())])));
    let res_err = |e: Vec<(&str, Value)>| {
        m(vec![
            ("t", "res".into()),
            ("id", 1u64.into()),
            ("ok", false.into()),
            ("e", m(e)),
        ])
    };
    assert!(!ok(res_err(vec![("code", "busy".into())])), "e without msg");
    assert!(!ok(res_err(vec![
        ("code", "busy".into()),
        ("msg", 1u64.into())
    ])));
    assert!(ok(res_err(vec![
        ("code", "busy".into()),
        ("msg", "".into())
    ])));
}
