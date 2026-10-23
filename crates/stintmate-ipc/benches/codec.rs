//! Micro-benchmarks of the codec (plan section 6): `req` encode/decode (~120 bytes), a stream
//! frame (48-byte body) and the 12 x f32 layout pack/unpack.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::layout::{FieldType, Layout, Scalar};
use stintmate_ipc::wire::{self, Frame, FrameReader};

fn req() -> Value {
    codec::map([
        ("t", Value::from("req")),
        ("id", Value::from(7u64)),
        ("m", Value::from("pitwall.write")),
        ("v", Value::from(1u64)),
        (
            "p",
            codec::map([
                ("type", Value::from("lap")),
                ("lap", Value::from(4u64)),
                (
                    "channels",
                    codec::map([("speed", Value::Binary(vec![0, 0, 0xc0, 0x3f]))]),
                ),
            ]),
        ),
        ("dl", Value::from(30000u64)),
        ("ik", Value::from("examplesim/2b9f1c/lap/4")),
    ])
}

fn benches(c: &mut Criterion) {
    let value = req();
    let bytes = codec::encode(&value).unwrap();
    println!("req is {} bytes", bytes.len());
    c.bench_function("req encode", |b| {
        b.iter(|| codec::encode(black_box(&value)).unwrap())
    });
    c.bench_function("req decode", |b| {
        b.iter(|| codec::decode_message(black_box(&bytes)).unwrap())
    });
    c.bench_function("req decode + validate", |b| {
        b.iter(|| {
            let m = codec::decode_message(black_box(&bytes)).unwrap();
            codec::validate_message(&m).unwrap()
        })
    });

    let body = [0u8; 48];
    let mut out = Vec::with_capacity(128);
    c.bench_function("stream frame write", |b| {
        b.iter(|| {
            out.clear();
            wire::write_stream_frame(&mut out, 2, black_box(41), 5_000_000_999, 0, &body).unwrap();
        })
    });
    let mut frame = Vec::new();
    wire::write_stream_frame(&mut frame, 2, 41, 5_000_000_999, 0, &body).unwrap();
    let mut reader = FrameReader::new(1 << 20, 1 << 28);
    c.bench_function("stream frame read", |b| {
        b.iter(|| {
            reader.push(black_box(&frame));
            let Some(Frame::Stream(s)) = reader.next_frame().unwrap() else {
                unreachable!()
            };
            s.seq
        })
    });

    let layout = Layout::new([("bench", FieldType::F32, 12)], None).unwrap();
    let values = [Scalar::F32(1.5); 12];
    let mut packed = Vec::with_capacity(48);
    c.bench_function("layout 12xf32 pack", |b| {
        b.iter(|| {
            packed.clear();
            layout.pack(black_box(&values), &mut packed).unwrap();
        })
    });
    let mut body = Vec::new();
    layout.pack(&values, &mut body).unwrap();
    let mut flat = Vec::with_capacity(12);
    c.bench_function("layout 12xf32 unpack", |b| {
        b.iter(|| layout.unpack(black_box(&body), &mut flat).unwrap())
    });
}

criterion_group!(codec_benches, benches);
criterion_main!(codec_benches);
