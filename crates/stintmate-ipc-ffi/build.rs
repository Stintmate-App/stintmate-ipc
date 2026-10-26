//! Writes `include/stintmate_ipc.h` with cbindgen when the generated text changed.

use std::path::PathBuf;

fn main() {
    let dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    let config = cbindgen::Config::from_file(dir.join("cbindgen.toml")).expect("cbindgen.toml");
    let bindings = cbindgen::Builder::new()
        .with_crate(&dir)
        .with_config(config)
        .generate()
        .expect("cbindgen failed");
    let mut raw = Vec::new();
    bindings.write(&mut raw);
    // The header is committed with LF. A checkout with autocrlf gives `cbindgen.toml` CRLF
    // endings and cbindgen copies them into the header: normalise before comparing and writing.
    let text = normalise_eol(&raw);
    let target = dir.join("include").join("stintmate_ipc.h");
    if std::fs::read(&target).ok().as_deref() != Some(text.as_slice()) {
        std::fs::create_dir_all(target.parent().unwrap()).expect("include dir");
        std::fs::write(&target, &text).expect("write header");
    }
}

/// `\r\n` and a lone `\r` become `\n`.
fn normalise_eol(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\r' {
            out.push(b'\n');
            if raw.get(i + 1) == Some(&b'\n') {
                i += 1;
            }
        } else {
            out.push(raw[i]);
        }
        i += 1;
    }
    out
}
