//! The ABI contract: the version function, the header against the DLL, and the baseline of
//! every exported symbol with its signature hash (a change needs a MAJOR bump, an addition a
//! MINOR bump, once the ABI is released).

mod common;

use std::collections::BTreeMap;
use std::ffi::CStr;
use std::path::Path;

use common::*;

fn header() -> String {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("include/stintmate_ipc.h");
    let text = std::fs::read_to_string(p).unwrap();
    assert!(!text.contains('\r'), "the header is committed with LF");
    text
}

fn define(header: &str, name: &str) -> i64 {
    let prefix = format!("#define {name} ");
    let line = header
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("{name} is not defined in the header"));
    line.trim().trim_matches(['(', ')']).parse().unwrap()
}

/// Every function prototype of the header: name to the prototype with normalised whitespace.
fn prototypes(header: &str) -> BTreeMap<String, String> {
    // Drop block comments and preprocessor lines.
    let mut text = String::new();
    let mut rest = header;
    while let Some(i) = rest.find("/*") {
        text.push_str(&rest[..i]);
        match rest[i..].find("*/") {
            Some(j) => rest = &rest[i + j + 2..],
            None => rest = "",
        }
    }
    text.push_str(rest);
    let text: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with('#') && !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut out = BTreeMap::new();
    for chunk in text.split(';') {
        let flat = chunk.split_whitespace().collect::<Vec<_>>().join(" ");
        let flat = flat.trim_start_matches(['{', '}', ' ']);
        if flat.starts_with("typedef") || !flat.contains('(') {
            continue;
        }
        let Some(open) = flat.find('(') else { continue };
        let head = flat[..open].trim_end();
        let name = head.rsplit(['*', ' ']).next().unwrap();
        if let Some(short) = name.strip_prefix("stintmate_ipc_") {
            out.insert(short.to_owned(), flat.to_owned());
        }
    }
    out
}

fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[test]
fn the_abi_version_matches_the_header() {
    let h = header();
    let (major, minor) = (
        define(&h, "STINTMATE_IPC_ABI_MAJOR"),
        define(&h, "STINTMATE_IPC_ABI_MINOR"),
    );
    let v = unsafe { (api().abi_version)() };
    assert_eq!(i64::from(v >> 16), major);
    assert_eq!(i64::from(v & 0xffff), minor);
    // The compatibility rule a caller applies: same MAJOR, MINOR at least its own.
    assert_eq!(v >> 16, 1);
    assert_eq!(define(&h, "STINTMATE_IPC_FOREVER"), -1);
}

#[test]
fn the_static_strings_and_versions() {
    let a = api();
    let s = |p: *const std::ffi::c_char| unsafe { CStr::from_ptr(p) }.to_str().unwrap();
    assert_eq!(s(unsafe { (a.version)() }), env!("CARGO_PKG_VERSION"));
    assert_eq!(s(unsafe { (a.spec_revision)() }), "2.0");
    assert_eq!(unsafe { (a.protocol_version)() }, 2);
}

#[test]
fn the_status_codes_are_stable() {
    let h = header();
    let codes = [
        ("OK", 0),
        ("E_NOT_FOUND", -1),
        ("E_BUSY", -2),
        ("E_IN_USE", -3),
        ("E_HANDSHAKE", -4),
        ("E_KEYFILE", -5),
        ("E_NOT_CONNECTED", -6),
        ("E_LOST", -7),
        ("E_TIMEOUT", -8),
        ("E_CANCELLED", -9),
        ("E_PROTOCOL", -10),
        ("E_INVALID_ARG", -11),
        ("E_PANIC", -12),
        ("E_REMOTE", -13),
        ("E_TOO_LARGE", -14),
        ("E_IO", -15),
        ("E_INTERNAL", -16),
        ("E_WAIT_TIMEOUT", -17),
        ("E_ENCODE", -18),
        ("E_CLOSED", -19),
        ("E_SHUTDOWN", -20),
    ];
    for (name, value) in codes {
        assert_eq!(
            define(&h, &format!("STINTMATE_IPC_{name}")),
            value,
            "{name}"
        );
    }
}

#[test]
fn every_prototype_of_the_header_is_exported_and_matches_the_baseline() {
    let _s = serial();
    let h = header();
    let protos = prototypes(&h);
    assert!(protos.len() > 40, "parsed only {} prototypes", protos.len());
    // Every prototype resolves in the DLL (loading `api()` resolved the ones the tests use;
    // the rest are looked up here by name).
    let _ = api();
    for name in protos.keys() {
        assert!(
            exported(name),
            "stintmate_ipc_{name} is in the header but not exported"
        );
    }

    let current: BTreeMap<String, u64> = protos.iter().map(|(k, v)| (k.clone(), fnv(v))).collect();
    let (major, minor) = (
        define(&h, "STINTMATE_IPC_ABI_MAJOR"),
        define(&h, "STINTMATE_IPC_ABI_MINOR"),
    );
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/abi_baseline.txt");
    if std::env::var_os("STINTMATE_ABI_UPDATE").is_some() {
        let status = std::fs::read_to_string(&path)
            .ok()
            .and_then(|t| t.lines().next().map(str::to_owned))
            .and_then(|l| l.split_whitespace().nth(2).map(str::to_owned))
            .unwrap_or_else(|| "unreleased".into());
        let mut text = format!("abi {major}.{minor} {status}\n");
        for (k, v) in &current {
            text.push_str(&format!("{k} {v:016x}\n"));
        }
        std::fs::write(&path, text).unwrap();
        return;
    }
    let baseline = std::fs::read_to_string(&path).expect("tests/abi_baseline.txt");
    let mut lines = baseline.lines();
    let first: Vec<&str> = lines.next().unwrap().split_whitespace().collect();
    assert_eq!(first[0], "abi");
    let (bmajor, bminor) = first[1].split_once('.').unwrap();
    let (bmajor, bminor): (i64, i64) = (bmajor.parse().unwrap(), bminor.parse().unwrap());
    let released = first[2] == "released";
    let base: BTreeMap<String, u64> = lines
        .map(|l| {
            let (k, v) = l.split_once(' ').unwrap();
            (k.to_owned(), u64::from_str_radix(v, 16).unwrap())
        })
        .collect();

    let changed: Vec<_> = base
        .iter()
        .filter(|(k, v)| current.get(*k) != Some(*v))
        .map(|(k, _)| k.as_str())
        .collect();
    let added: Vec<_> = current.keys().filter(|k| !base.contains_key(*k)).collect();
    if !released {
        // Until the first release the baseline only has to be current: regenerate it with
        // STINTMATE_ABI_UPDATE=1 and review the diff.
        assert!(
            changed.is_empty() && added.is_empty() && (bmajor, bminor) == (major, minor),
            "the ABI changed (changed {changed:?}, added {added:?}); run the tests with \
             STINTMATE_ABI_UPDATE=1 and commit tests/abi_baseline.txt"
        );
        return;
    }
    assert!(major >= bmajor, "the ABI MAJOR went down");
    if major == bmajor {
        assert!(
            changed.is_empty(),
            "symbols removed or changed without a MAJOR bump: {changed:?}"
        );
        assert!(
            added.is_empty() || minor > bminor,
            "symbols added without a MINOR bump: {added:?}"
        );
    }
}

/// Whether the loaded DLL exports `stintmate_ipc_<name>`.
fn exported(name: &str) -> bool {
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleA, GetProcAddress};
    let module = unsafe { GetModuleHandleA(c"stintmate_ipc.dll".as_ptr().cast()) };
    assert!(!module.is_null());
    let sym = c(&format!("stintmate_ipc_{name}"));
    unsafe { GetProcAddress(module, sym.as_ptr().cast()) }.is_some()
}
