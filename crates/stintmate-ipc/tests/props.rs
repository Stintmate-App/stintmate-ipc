//! Property tests: codec round trip, split invariance of the frame reader, chunk reassembly.

use proptest::prelude::*;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::layout::{FieldType, Layout, Scalar};
use stintmate_ipc::wire::{self, Frame, FrameReader};

fn arb_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Nil),
        any::<bool>().prop_map(Value::Boolean),
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::F64),
        any::<f32>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::F32),
        ".{0,40}".prop_map(|s| Value::from(s.as_str())),
        prop::collection::vec(any::<u8>(), 0..300).prop_map(Value::Binary),
    ];
    leaf.prop_recursive(5, 64, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            prop::collection::btree_map("[a-z_]{1,8}", inner, 0..6).prop_map(|m| {
                Value::Map(
                    m.into_iter()
                        .map(|(k, v)| (Value::from(k.as_str()), v))
                        .collect(),
                )
            }),
        ]
    })
}

fn read_all(bytes: &[u8], cuts: &[usize]) -> Result<Vec<Frame>, stintmate_ipc::ProtocolError> {
    let mut r = FrameReader::new(1 << 20, 1 << 28);
    let mut out = Vec::new();
    let mut last = 0;
    let mut points: Vec<usize> = cuts.iter().map(|c| c % (bytes.len() + 1)).collect();
    points.push(bytes.len());
    points.sort_unstable();
    for p in points {
        r.push(&bytes[last..p]);
        last = p;
        while let Some(f) = r.next_frame()? {
            out.push(f);
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
enum Item {
    Message(Vec<u8>),
    Stream(u32, u32, i64, u32, Vec<u8>),
    Reserved(u8, Vec<u8>),
    Chunked(Vec<u8>, usize),
}

fn arb_item() -> impl Strategy<Value = Item> {
    prop_oneof![
        arb_value().prop_map(|v| Item::Message(codec::encode(&v).unwrap())),
        (
            any::<u32>(),
            any::<u32>(),
            any::<i64>(),
            any::<u32>(),
            prop::collection::vec(any::<u8>(), 0..64)
        )
            .prop_map(|(a, b, c, d, e)| Item::Stream(a, b, c, d, e)),
        (0x80u8..=0xff, prop::collection::vec(any::<u8>(), 0..32))
            .prop_map(|(k, p)| Item::Reserved(k, p)),
        (prop::collection::vec(any::<u8>(), 1..400), 1usize..90)
            .prop_map(|(m, c)| Item::Chunked(m, c)),
    ]
}

fn serialise(items: &[Item]) -> Vec<u8> {
    let mut out = Vec::new();
    for it in items {
        match it {
            Item::Message(m) => wire::write_message_frame(&mut out, m).unwrap(),
            Item::Stream(a, b, c, d, e) => {
                wire::write_stream_frame(&mut out, *a, *b, *c, *d, e).unwrap()
            }
            Item::Reserved(k, p) => wire::write_frame(&mut out, *k, p).unwrap(),
            Item::Chunked(m, c) => {
                for f in wire::chunk_frames(m, *c).unwrap() {
                    out.extend(f);
                }
            }
        }
    }
    out
}

proptest! {
    #[test]
    fn codec_round_trip(v in arb_value()) {
        let bytes = codec::encode(&v).unwrap();
        let back = codec::decode(&bytes).unwrap();
        prop_assert_eq!(&back, &v);
        // Shortest forms are canonical: re-encoding gives the same bytes.
        prop_assert_eq!(codec::encode(&back).unwrap(), bytes);
    }

    #[test]
    fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
        let _ = codec::decode(&bytes);
        let mut r = FrameReader::new(65536, 1 << 20);
        r.push(&bytes);
        while let Ok(Some(_)) = r.next_frame() {}
    }

    #[test]
    fn any_split_gives_the_same_frames(
        items in prop::collection::vec(arb_item(), 0..8),
        cuts in prop::collection::vec(any::<usize>(), 0..12),
    ) {
        let bytes = serialise(&items);
        let whole = read_all(&bytes, &[]).unwrap();
        let split = read_all(&bytes, &cuts).unwrap();
        prop_assert_eq!(&whole, &split);
        let byte_by_byte: Vec<usize> = (0..bytes.len()).collect();
        prop_assert_eq!(&whole, &read_all(&bytes, &byte_by_byte).unwrap());
        // One frame per message, chunked message or stream sample; reserved kinds vanish.
        let expected = items.iter().filter(|i| !matches!(i, Item::Reserved(..))).count();
        prop_assert_eq!(whole.len(), expected);
    }

    #[test]
    fn corrupt_streams_fail_the_same_way_however_split(
        bytes in prop::collection::vec(any::<u8>(), 0..300),
        cuts in prop::collection::vec(any::<usize>(), 0..6),
    ) {
        let a = read_all(&bytes, &[]);
        let b = read_all(&bytes, &cuts);
        prop_assert_eq!(a.is_err(), b.is_err());
        if let (Ok(a), Ok(b)) = (a, b) {
            prop_assert_eq!(a, b);
        }
    }

    #[test]
    fn chunks_reassemble(
        msg in prop::collection::vec(any::<u8>(), 1..3000),
        chunk in 1usize..400,
        stream_at in 0usize..10,
    ) {
        let frames: Vec<Vec<u8>> = wire::chunk_frames(&msg, chunk).unwrap().collect();
        let mut sample = Vec::new();
        wire::write_stream_frame(&mut sample, 1, 2, 3, 0, b"body").unwrap();
        let mut bytes = Vec::new();
        for (i, f) in frames.iter().enumerate() {
            if i == stream_at { bytes.extend(&sample); }
            bytes.extend(f);
        }
        let got = read_all(&bytes, &[]).unwrap();
        let messages: Vec<&Frame> = got.iter().filter(|f| matches!(f, Frame::Message(_))).collect();
        prop_assert_eq!(messages.len(), 1);
        match messages[0] {
            Frame::Message(b) => prop_assert_eq!(&b[..], &msg[..]),
            Frame::Stream(_) => unreachable!(),
        }
    }

    #[test]
    fn layout_round_trip(
        spec in prop::collection::vec((0usize..10, 1usize..5), 1..12),
        seed in any::<u64>(),
    ) {
        const TYPES: [FieldType; 10] = [
            FieldType::U8, FieldType::I8, FieldType::U16, FieldType::I16, FieldType::U32,
            FieldType::I32, FieldType::U64, FieldType::I64, FieldType::F32, FieldType::F64,
        ];
        let fields: Vec<_> = spec.iter().enumerate()
            .map(|(i, (t, c))| (format!("f{i}"), TYPES[*t], *c)).collect();
        let layout = Layout::new(fields.clone(), None).unwrap();
        let mut x = seed;
        let mut next = || { x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); x };
        let mut flat = Vec::new();
        for (_, t, c) in &fields {
            for _ in 0..*c {
                let r = next();
                flat.push(match t {
                    FieldType::U8 => Scalar::U8(r as u8), FieldType::I8 => Scalar::I8(r as i8),
                    FieldType::U16 => Scalar::U16(r as u16), FieldType::I16 => Scalar::I16(r as i16),
                    FieldType::U32 => Scalar::U32(r as u32), FieldType::I32 => Scalar::I32(r as i32),
                    FieldType::U64 => Scalar::U64(r), FieldType::I64 => Scalar::I64(r as i64),
                    FieldType::F32 => Scalar::F32((r % 100_000) as f32 / 8.0),
                    FieldType::F64 => Scalar::F64((r % 100_000) as f64 / 8.0),
                });
            }
        }
        let mut body = Vec::new();
        layout.pack(&flat, &mut body).unwrap();
        prop_assert_eq!(body.len(), layout.size());
        let mut back = Vec::new();
        layout.unpack(&body, &mut back).unwrap();
        prop_assert_eq!(&back, &flat);
        // And through the map form.
        let m = layout.unpack_value(&body).unwrap();
        let mut again = Vec::new();
        layout.pack_value(&m, &mut again).unwrap();
        prop_assert_eq!(again, body);
    }
}
