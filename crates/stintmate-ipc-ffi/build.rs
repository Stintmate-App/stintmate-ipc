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
    let mut text = Vec::new();
    bindings.write(&mut text);
    let target = dir.join("include").join("stintmate_ipc.h");
    if std::fs::read(&target).ok().as_deref() != Some(text.as_slice()) {
        std::fs::create_dir_all(target.parent().unwrap()).expect("include dir");
        std::fs::write(&target, &text).expect("write header");
    }
}
