//! Keys, naming and layouts through the C functions.

mod common;

use std::ffi::c_char;

use common::*;
use stintmate_ipc::codec::{self, Value};

const E_KEYFILE: i32 = -5;
const E_INVALID_ARG: i32 = -11;

fn temp_key_path(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("stintmate-ffi-keys-{}", random_tag()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(tag)
}

fn status_of_last_error() -> i64 {
    let mut b = NO_BUFFER;
    assert_eq!(unsafe { (api().last_error)(&mut b) }, 0);
    let v = codec::decode(&take(b)).unwrap();
    codec::get(&v, "status").and_then(|s| s.as_i64()).unwrap()
}

#[test]
fn ensure_read_parse_and_format_a_key() {
    let _s = serial();
    let a = api();
    let path = temp_key_path("key");
    let p = c(path.to_str().unwrap());

    // The file does not exist yet: read fails, ensure creates it (key_out may be null).
    let mut key = NO_BUFFER;
    assert_eq!(unsafe { (a.read_key)(p.as_ptr(), &mut key) }, E_KEYFILE);
    assert!(last_message().contains("not found"), "{}", last_message());
    assert_eq!(status_of_last_error(), i64::from(E_KEYFILE));
    assert_eq!(
        unsafe { (a.ensure_key)(p.as_ptr(), std::ptr::null_mut()) },
        0
    );
    assert!(path.exists());

    let mut k1 = NO_BUFFER;
    assert_eq!(unsafe { (a.ensure_key)(p.as_ptr(), &mut k1) }, 0);
    let mut k2 = NO_BUFFER;
    assert_eq!(unsafe { (a.read_key)(p.as_ptr(), &mut k2) }, 0);
    let (k1, k2) = (take(k1), take(k2));
    assert_eq!(k1.len(), 32);
    assert_eq!(k1, k2);

    // format and parse are inverse; the text is what the file holds.
    let mut t = NO_BUFFER;
    assert_eq!(unsafe { (a.format_key)(k1.as_ptr(), k1.len(), &mut t) }, 0);
    let t = take(t);
    assert_eq!(
        String::from_utf8(t.clone()).unwrap().trim(),
        std::fs::read_to_string(&path).unwrap().trim()
    );
    let mut back = NO_BUFFER;
    assert_eq!(unsafe { (a.parse_key)(t.as_ptr(), t.len(), &mut back) }, 0);
    assert_eq!(take(back), k1);

    // Bad input.
    let mut out = NO_BUFFER;
    assert_eq!(
        unsafe { (a.parse_key)(b"nothex".as_ptr(), 6, &mut out) },
        E_KEYFILE
    );
    assert_eq!(
        unsafe { (a.format_key)(k1.as_ptr(), 31, &mut out) },
        E_INVALID_ARG
    );
    assert_eq!(
        unsafe { (a.read_key)(std::ptr::null(), &mut out) },
        E_INVALID_ARG
    );
    assert_eq!(unsafe { (a.live_buffers)() }, 0);
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn naming_functions() {
    let _s = serial();
    let a = api();
    let name = |f: unsafe extern "C" fn(
        *const c_char,
        *const c_char,
        *const c_char,
        *mut Buffer,
    ) -> i32,
                app: &str,
                process: Option<&str>,
                user: Option<&str>| {
        let (app, process, user) = (c(app), process.map(c), user.map(c));
        let mut out = NO_BUFFER;
        let st = unsafe {
            f(
                app.as_ptr(),
                process.as_ref().map_or(std::ptr::null(), |p| p.as_ptr()),
                user.as_ref().map_or(std::ptr::null(), |u| u.as_ptr()),
                &mut out,
            )
        };
        (st, text(out))
    };
    assert_eq!(
        name(a.short_name, "pitwall", None, Some("Leo")),
        (0, "stintmate-pitwall-leo".to_owned())
    );
    assert_eq!(
        name(a.short_name, "pitwall", Some("ui"), Some("LEO")),
        (0, "stintmate-pitwall-ui-leo".to_owned())
    );
    assert_eq!(
        name(a.pipe_path, "pitwall", None, Some("leo")),
        (0, r"\\.\pipe\stintmate-pitwall-leo".to_owned())
    );
    assert_eq!(
        name(a.pipe_path, "Bad App", None, Some("leo")).0,
        E_INVALID_ARG
    );
    assert_eq!(
        name(a.pipe_path, "pitwall", Some("Bad"), None).0,
        E_INVALID_ARG
    );

    // The default user is the current Windows user, lowercased.
    let mut u = NO_BUFFER;
    assert_eq!(unsafe { (a.pipe_user)(std::ptr::null(), &mut u) }, 0);
    let user = text(u);
    assert!(!user.is_empty() && user == user.to_ascii_lowercase());
    let (st, short) = name(a.short_name, "pitwall", None, None);
    assert_eq!((st, short), (0, format!("stintmate-pitwall-{user}")));
    let mut u = NO_BUFFER;
    assert_eq!(unsafe { (a.pipe_user)(c("MiXed").as_ptr(), &mut u) }, 0);
    assert_eq!(text(u), "mixed");

    assert_eq!(unsafe { (a.valid_app)(c("pitwall").as_ptr()) }, 1);
    assert_eq!(unsafe { (a.valid_app)(c("Bad App").as_ptr()) }, 0);
    assert_eq!(unsafe { (a.valid_app)(std::ptr::null()) }, 0);
    assert_eq!(unsafe { (a.valid_name)(c("session.lap_done").as_ptr()) }, 1);
    assert_eq!(unsafe { (a.valid_name)(c("Bad Name").as_ptr()) }, 0);
    assert_eq!(unsafe { (a.live_buffers)() }, 0);
}

#[test]
fn workspace_paths() {
    let _s = serial();
    let a = api();
    let ws = c(r"C:\ws");
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.default_key_path)(ws.as_ptr(), &mut out) }, 0);
    assert_eq!(text(out), r"C:\ws\ipc\key");
    assert_eq!(unsafe { (a.registry_dir)(ws.as_ptr(), &mut out) }, 0);
    assert_eq!(text(out), r"C:\ws\ipc\endpoints");

    // The found workspace follows STINTMATE_WORKSPACE; empty when there is none.
    let dir = std::env::temp_dir().join(format!("stintmate-ffi-ws-{}", random_tag()));
    // SAFETY: the tests of this binary run one at a time (`serial`).
    unsafe { std::env::set_var("STINTMATE_WORKSPACE", &dir) };
    assert_eq!(unsafe { (a.find_workspace)(&mut out) }, 0);
    assert_eq!(text(out), dir.to_str().unwrap());
    assert_eq!(unsafe { (a.registry_dir)(std::ptr::null(), &mut out) }, 0);
    assert!(text(out).ends_with(r"\ipc\endpoints"));
    // SAFETY: as above.
    unsafe {
        std::env::set_var("STINTMATE_WORKSPACE", "");
        std::env::set_var("STINTMATE_SETTINGS_FILE", dir.join("none.json"));
    }
    assert_eq!(unsafe { (a.find_workspace)(&mut out) }, 0);
    assert_eq!(out.len, 0);
    assert_eq!(
        unsafe { (a.registry_dir)(std::ptr::null(), &mut out) },
        E_KEYFILE
    );
    assert_eq!(unsafe { (a.live_buffers)() }, 0);
}

#[test]
fn layout_normalize_gives_offsets_and_size() {
    let _s = serial();
    let a = api();
    let layout = codec::map([
        ("name", Value::from("lap")),
        (
            "fields",
            Value::Array(vec![
                Value::Array(vec![Value::from("gear"), Value::from("u8")]),
                Value::Array(vec![
                    Value::from("temps"),
                    Value::from("f32"),
                    Value::from(2u64),
                ]),
                Value::Array(vec![Value::from("t"), Value::from("i64")]),
            ]),
        ),
    ]);
    let bytes = codec::encode(&layout).unwrap();
    let mut out = NO_BUFFER;
    assert_eq!(
        unsafe { (a.layout_normalize)(bytes.as_ptr(), bytes.len(), &mut out) },
        0
    );
    let v = codec::decode(&take(out)).unwrap();
    assert_eq!(codec::get(&v, "size").and_then(|s| s.as_u64()), Some(17));
    assert_eq!(codec::get(&v, "name").and_then(|s| s.as_str()), Some("lap"));
    let fields = codec::get(&v, "fields").and_then(|f| f.as_array()).unwrap();
    let row = |i: usize| -> (String, String, u64, u64) {
        let f = fields[i].as_array().unwrap();
        (
            f[0].as_str().unwrap().to_owned(),
            f[1].as_str().unwrap().to_owned(),
            f[2].as_u64().unwrap(),
            f[3].as_u64().unwrap(),
        )
    };
    assert_eq!(row(0), ("gear".into(), "u8".into(), 1, 0));
    assert_eq!(row(1), ("temps".into(), "f32".into(), 2, 1));
    assert_eq!(row(2), ("t".into(), "i64".into(), 1, 9));

    // Invalid layouts are invalid arguments with a message.
    let bad = codec::encode(&codec::map([("fields", Value::Array(vec![]))])).unwrap();
    assert_eq!(
        unsafe { (a.layout_normalize)(bad.as_ptr(), bad.len(), &mut out) },
        E_INVALID_ARG
    );
    assert!(last_message().contains("field"), "{}", last_message());
    assert_eq!(unsafe { (a.live_buffers)() }, 0);
}

#[test]
fn key_source_forms_open_an_endpoint() {
    let _s = serial();
    let a = api();
    let info = PeerInfo {
        app: c"server".as_ptr(),
        name: std::ptr::null(),
        version: std::ptr::null(),
        process: std::ptr::null(),
        meta: std::ptr::null(),
        meta_len: 0,
    };
    let pipe = c(&format!("stintmate-ffi-ks-{}", random_tag()));
    let opts = EndpointOpts {
        struct_size: size_of::<EndpointOpts>(),
        pipe: pipe.as_ptr(),
        handler_threads: 1,
    };
    let make = |ks: &KeySource| {
        let mut ep = std::ptr::null_mut();
        let st = unsafe { (a.endpoint_new)(&info, ks, &opts, &mut ep) };
        (st, ep)
    };
    let raw = [7u8; 32];
    // Raw bytes.
    let (st, ep) = make(&key_source(&raw));
    assert_eq!(st, 0, "{}", last_message());
    unsafe { (a.endpoint_free)(ep) };
    // A file that is created on demand.
    let path = temp_key_path("created");
    let p = c(path.to_str().unwrap());
    let ks = KeySource {
        struct_size: size_of::<KeySource>(),
        path: p.as_ptr(),
        key: std::ptr::null(),
        key_len: 0,
        create: 1,
    };
    let (st, ep) = make(&ks);
    assert_eq!(st, 0, "{}", last_message());
    unsafe { (a.endpoint_free)(ep) };
    // A shorter struct (an older caller): the fields beyond struct_size are not read.
    let short = KeySource {
        struct_size: std::mem::offset_of!(KeySource, key),
        path: p.as_ptr(),
        key: raw.as_ptr(), // beyond struct_size: ignored
        key_len: 99,
        create: 0,
    };
    let (st, ep) = make(&short);
    assert_eq!(st, 0, "{}", last_message());
    unsafe { (a.endpoint_free)(ep) };
    // Wrong forms.
    let both = KeySource {
        path: p.as_ptr(),
        ..key_source(&raw)
    };
    assert_eq!(make(&both).0, E_INVALID_ARG);
    let len = KeySource {
        key_len: 31,
        ..key_source(&raw)
    };
    assert_eq!(make(&len).0, E_INVALID_ARG);
    let tiny = KeySource {
        struct_size: 4,
        ..key_source(&raw)
    };
    assert_eq!(make(&tiny).0, E_INVALID_ARG);
    assert_eq!(unsafe { (a.live_handles)() }, 0);
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}
