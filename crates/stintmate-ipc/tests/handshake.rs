//! Every vector of `vectors/handshake.json` and `vectors/keyfile.json`, plus the failure paths of
//! SPEC 8.6 and the features of 8.8.

use std::path::PathBuf;

use serde_json::Value as Json;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::decl::Declarations;
use stintmate_ipc::handshake::*;
use stintmate_ipc::keyfile::{Key, format_key, parse_key};
use stintmate_ipc::wire::{Frame, FrameReader};

fn load(name: &str) -> Json {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../vectors")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn hx<const N: usize>(j: &Json) -> [u8; N] {
    hex::decode(j.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}

/// The one message of a one-frame hex string.
fn message_of(hex_str: &str) -> Value {
    let mut r = FrameReader::for_handshake();
    r.push(&hex::decode(hex_str).unwrap());
    let Some(Frame::Message(b)) = r.next_frame().unwrap() else {
        panic!("no message")
    };
    assert!(r.next_frame().unwrap().is_none(), "one frame only");
    codec::decode_message(&b).unwrap()
}

/// JSON of a decl in the vector (plain maps, arrays and numbers) to a value.
fn decl_of(auth: &Value) -> Declarations {
    Declarations::from_value(codec::get(auth, "decl").unwrap()).unwrap()
}

struct Fixture {
    key: Key,
    name: String,
    s_nonce: Nonce,
    c_nonce: Nonce,
    hello_hex: String,
    auth_hex: String,
    welcome_hex: String,
}

fn fixture() -> Fixture {
    let doc = load("handshake.json");
    let t = doc["transcript"].as_array().unwrap();
    Fixture {
        key: hx(&doc["key"]),
        name: doc["pipe_short_name"].as_str().unwrap().to_owned(),
        s_nonce: hx(&doc["server_nonce"]),
        c_nonce: hx(&doc["client_nonce"]),
        hello_hex: t[0]["hex"].as_str().unwrap().to_owned(),
        auth_hex: t[1]["hex"].as_str().unwrap().to_owned(),
        welcome_hex: t[2]["hex"].as_str().unwrap().to_owned(),
    }
}

fn server_cfg(f: &Fixture, welcome: &Value) -> ServerConfig {
    let mut cfg = ServerConfig::new(
        f.name.clone(),
        PeerInfo::from_value(codec::get(welcome, "peer").unwrap(), "welcome").unwrap(),
        decl_of(welcome),
    );
    cfg.run = "5f1c2a9be03d4471".into();
    cfg
}

fn client_cfg(f: &Fixture, auth: &Value) -> ClientConfig {
    ClientConfig::new(
        f.name.clone(),
        PeerInfo::from_value(codec::get(auth, "peer").unwrap(), "auth").unwrap(),
        decl_of(auth),
    )
}

const CLOCK: (i64, i64) = (5_000_000_000, 1_792_500_000_000_000_000);

#[test]
fn macs_match_the_vector() {
    let doc = load("handshake.json");
    let f = fixture();
    let v = doc["version"].as_u64().unwrap() as u32;
    let cm: MacBytes = hx(&doc["client_mac"]);
    let sm: MacBytes = hx(&doc["server_mac"]);
    assert_eq!(client_mac(&f.key, v, &f.s_nonce, &f.c_nonce, &f.name), cm);
    assert_eq!(server_mac(&f.key, v, &f.s_nonce, &f.c_nonce, &f.name), sm);
    assert!(verify_client_mac(
        &f.key, v, &f.s_nonce, &f.c_nonce, &f.name, &cm
    ));
    assert!(verify_server_mac(
        &f.key, v, &f.s_nonce, &f.c_nonce, &f.name, &sm
    ));
    // the two MACs are not interchangeable, nor valid for other inputs
    assert!(!verify_client_mac(
        &f.key, v, &f.s_nonce, &f.c_nonce, &f.name, &sm
    ));
    assert!(!verify_client_mac(
        &f.key, 3, &f.s_nonce, &f.c_nonce, &f.name, &cm
    ));
    assert!(!verify_client_mac(
        &f.key, v, &f.c_nonce, &f.s_nonce, &f.name, &cm
    ));
    assert!(!verify_client_mac(
        &f.key,
        v,
        &f.s_nonce,
        &f.c_nonce,
        &f.name,
        &cm[..31]
    ));
    assert!(!verify_client_mac(
        &f.key,
        v,
        &f.s_nonce,
        &f.c_nonce,
        &f.name,
        &[]
    ));

    // the wrong key and the other pipe name
    let wrong: Key = hx(&doc["wrong_key"]["key"]);
    let wrong_mac: MacBytes = hx(&doc["wrong_key"]["client_mac"]);
    assert_eq!(
        client_mac(&wrong, v, &f.s_nonce, &f.c_nonce, &f.name),
        wrong_mac
    );
    assert!(!verify_client_mac(
        &f.key, v, &f.s_nonce, &f.c_nonce, &f.name, &wrong_mac
    ));
    let other = doc["other_pipe"]["pipe_short_name"].as_str().unwrap();
    let other_mac: MacBytes = hx(&doc["other_pipe"]["client_mac"]);
    assert_eq!(
        client_mac(&f.key, v, &f.s_nonce, &f.c_nonce, other),
        other_mac
    );
    assert!(!verify_client_mac(
        &f.key, v, &f.s_nonce, &f.c_nonce, &f.name, &other_mac
    ));
}

#[test]
fn key_file_contents_of_the_handshake_vector() {
    let doc = load("handshake.json");
    let f = fixture();
    let bytes = hex::decode(doc["key_file_contents"].as_str().unwrap()).unwrap();
    assert_eq!(parse_key(&bytes).unwrap(), f.key);
    assert_eq!(format_key(&f.key), bytes);
}

#[test]
fn transcript_bytes_are_reproduced_and_parsed() {
    let f = fixture();
    let (hello, auth, welcome) = (
        message_of(&f.hello_hex),
        message_of(&f.auth_hex),
        message_of(&f.welcome_hex),
    );

    // the server side: the exact hello, then welcome for the exact auth
    let server =
        ServerHandshake::with_nonce(server_cfg(&f, &welcome), f.key, f.s_nonce, Some(CLOCK));
    assert_eq!(hex::encode(to_frame(&server.hello()).unwrap()), f.hello_hex);
    assert_eq!(Hello::from_value(&hello).unwrap().clock, Some(CLOCK));
    let accepted = server.on_auth(&auth).unwrap();
    assert_eq!(
        hex::encode(to_frame(&accepted.welcome).unwrap()),
        f.welcome_hex
    );
    assert_eq!(accepted.version, 2);
    assert_eq!(accepted.peer.app, "connector-examplesim");
    assert_eq!(accepted.peer.process.as_deref(), Some("recorder"));
    assert!(accepted.peer.meta.is_some());
    assert_eq!(accepted.peer_limits.max_frame, 1_048_576);
    assert_eq!(accepted.decl.channels.len(), 1);
    assert!(accepted.features.is_empty());

    // the client side: the exact auth for the exact hello, then the welcome is accepted
    let mut client = ClientHandshake::with_nonce(client_cfg(&f, &auth), f.key, f.c_nonce);
    let sent = client.on_hello(&hello).unwrap();
    // the declaration encoder orders channel keys differently from the reference script:
    // decoding is what must match (vectors/README.md)
    assert_eq!(
        norm(&message_of(&hex::encode(to_frame(&sent).unwrap()))),
        norm(&auth)
    );
    let est = client.on_welcome(&welcome).unwrap();
    assert_eq!(est.version, 2);
    assert_eq!(est.run, "5f1c2a9be03d4471");
    assert_eq!(est.peer.app, "pitwall");
    assert_eq!(est.heartbeat_ms, 5000);
    assert_eq!(
        est.peer_limits,
        Limits {
            max_frame: 1_048_576,
            max_message: 268_435_456
        }
    );
    assert_eq!(est.clock, Some(CLOCK));
    assert!(est.decl.methods["pitwall.write"].ordered);
}

#[test]
fn a_wrong_key_ends_in_bye_unauthorized() {
    let doc = load("handshake.json");
    let f = fixture();
    let (hello, auth, welcome) = (
        message_of(&f.hello_hex),
        message_of(&f.auth_hex),
        message_of(&f.welcome_hex),
    );
    let wrong: Key = hx(&doc["wrong_key"]["key"]);

    // the client holds the wrong key: the server refuses with the vector's bye
    let mut client = ClientHandshake::with_nonce(client_cfg(&f, &auth), wrong, f.c_nonce);
    let bad_auth = client.on_hello(&hello).unwrap();
    let mac = codec::get(&bad_auth, "mac")
        .and_then(Value::as_slice)
        .unwrap();
    assert_eq!(
        hex::encode(mac),
        doc["wrong_key"]["client_mac"].as_str().unwrap()
    );
    let server =
        ServerHandshake::with_nonce(server_cfg(&f, &welcome), f.key, f.s_nonce, Some(CLOCK));
    let failure = server.on_auth(&bad_auth).unwrap_err();
    assert_eq!(failure.code, ByeCode::Unauthorized);
    assert_eq!(
        hex::encode(to_frame(&failure.bye()).unwrap()),
        doc["wrong_key"]["server_reply_hex"].as_str().unwrap()
    );

    // the server holds the wrong key: the client refuses its welcome
    let impostor =
        ServerHandshake::with_nonce(server_cfg(&f, &welcome), wrong, f.s_nonce, Some(CLOCK));
    let mut client = ClientHandshake::with_nonce(client_cfg(&f, &auth), f.key, f.c_nonce);
    // (the impostor's hello is the same; its auth check fails first, so build its welcome by hand)
    let auth_ok = client.on_hello(&impostor.hello()).unwrap();
    assert_eq!(
        impostor.on_auth(&auth_ok).unwrap_err().code,
        ByeCode::Unauthorized
    );
    let mut forged = Welcome::from_value(&welcome).unwrap();
    forged.mac = server_mac(&wrong, 2, &f.s_nonce, &f.c_nonce, &f.name);
    let failure = client.on_welcome(&forged.to_value()).unwrap_err();
    assert_eq!(failure.code, ByeCode::Unauthorized);
    assert!(failure.message.contains("another key"));
}

#[test]
fn an_auth_for_another_pipe_is_refused() {
    let f = fixture();
    let (hello, auth, welcome) = (
        message_of(&f.hello_hex),
        message_of(&f.auth_hex),
        message_of(&f.welcome_hex),
    );
    let mut cfg = client_cfg(&f, &auth);
    cfg.short_name = load("handshake.json")["other_pipe"]["pipe_short_name"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut client = ClientHandshake::with_nonce(cfg, f.key, f.c_nonce);
    let other = client.on_hello(&hello).unwrap();
    let server = ServerHandshake::with_nonce(server_cfg(&f, &welcome), f.key, f.s_nonce, None);
    assert_eq!(
        server.on_auth(&other).unwrap_err().code,
        ByeCode::Unauthorized
    );
}

#[test]
fn version_negotiation() {
    assert_eq!(choose_version(&[1, 2, 3], &[2]), Some(2));
    assert_eq!(choose_version(&[3, 4], &[2]), None);
    assert_eq!(choose_version(&[2, 5, 3], &[2, 3]), Some(3));
    assert_eq!(choose_version(&[], &[2]), None);

    let f = fixture();
    let (hello, auth, welcome) = (
        message_of(&f.hello_hex),
        message_of(&f.auth_hex),
        message_of(&f.welcome_hex),
    );
    // a server that speaks only version 3: the client has nothing in common
    let mut h = Hello::from_value(&hello).unwrap();
    h.versions = vec![3];
    let mut client = ClientHandshake::new(client_cfg(&f, &auth), f.key);
    let failure = client.on_hello(&h.to_value()).unwrap_err();
    assert_eq!(failure.code, ByeCode::VersionUnsupported);
    // an auth with a version the server did not offer
    let mut a = Auth::from_value(&auth).unwrap();
    a.v = 3;
    let server = ServerHandshake::with_nonce(server_cfg(&f, &welcome), f.key, f.s_nonce, None);
    assert_eq!(
        server.on_auth(&a.to_value()).unwrap_err().code,
        ByeCode::VersionUnsupported
    );
    // a welcome that echoes another version
    let mut client = ClientHandshake::with_nonce(client_cfg(&f, &auth), f.key, f.c_nonce);
    client.on_hello(&hello).unwrap();
    let mut w = Welcome::from_value(&welcome).unwrap();
    w.v = 3;
    assert_eq!(
        client.on_welcome(&w.to_value()).unwrap_err().code,
        ByeCode::ProtocolError
    );
}

fn without(m: &Value, key: &str) -> Value {
    let Value::Map(pairs) = m else { panic!() };
    Value::Map(
        pairs
            .iter()
            .filter(|(k, _)| k.as_str() != Some(key))
            .cloned()
            .collect(),
    )
}

fn with(m: &Value, key: &str, v: Value) -> Value {
    let Value::Map(pairs) = without(m, key) else {
        panic!()
    };
    let mut pairs = pairs;
    pairs.push((Value::from(key), v));
    Value::Map(pairs)
}

#[test]
fn malformed_messages_are_protocol_errors() {
    let f = fixture();
    let (hello, auth, welcome) = (
        message_of(&f.hello_hex),
        message_of(&f.auth_hex),
        message_of(&f.welcome_hex),
    );
    let server = ServerHandshake::with_nonce(server_cfg(&f, &welcome), f.key, f.s_nonce, None);
    let new_client = || ClientHandshake::with_nonce(client_cfg(&f, &auth), f.key, f.c_nonce);
    let short = Value::Binary(vec![0; 31]);

    for bad in [
        without(&auth, "v"),
        without(&auth, "nonce"),
        without(&auth, "mac"),
        without(&auth, "peer"),
        with(&auth, "nonce", short.clone()),
        with(&auth, "mac", short.clone()),
        with(&auth, "mac", Value::from("text")),
        with(&auth, "peer", Value::from(1)),
        with(&auth, "peer", codec::map([("name", Value::from("x"))])),
        with(&auth, "features", Value::from("x")),
        with(&auth, "max_frame", Value::from(1000)),
        with(&auth, "decl", Value::from(1)),
        with(&auth, "t", Value::from("hello")),
        codec::map([("t", Value::from("req"))]),
        Value::from(1),
    ] {
        let failure = server.on_auth(&bad).unwrap_err();
        assert_eq!(failure.code, ByeCode::ProtocolError, "{bad:?}");
    }
    for bad in [
        without(&hello, "proto"),
        with(&hello, "proto", Value::from("other")),
        without(&hello, "versions"),
        with(&hello, "versions", Value::Array(vec![])),
        with(&hello, "versions", Value::from("2")),
        without(&hello, "nonce"),
        with(&hello, "nonce", short.clone()),
        with(&hello, "clock", Value::Array(vec![Value::from(1)])),
        with(&hello, "features", Value::from(1)),
        with(&hello, "max_frame", Value::from(10)),
        with(&hello, "max_message", Value::from(70000)),
        with(&hello, "t", Value::from("auth")),
    ] {
        let failure = new_client().on_hello(&bad).unwrap_err();
        assert_eq!(failure.code, ByeCode::ProtocolError, "{bad:?}");
    }
    for bad in [
        without(&welcome, "mac"),
        with(&welcome, "mac", short.clone()),
        without(&welcome, "run"),
        without(&welcome, "v"),
        without(&welcome, "heartbeat_ms"),
        with(&welcome, "heartbeat_ms", Value::from(99)),
        without(&welcome, "peer"),
        with(&welcome, "t", Value::from("hello")),
    ] {
        let mut c = new_client();
        c.on_hello(&hello).unwrap();
        assert_eq!(
            c.on_welcome(&bad).unwrap_err().code,
            ByeCode::ProtocolError,
            "{bad:?}"
        );
    }
    // a welcome before any hello
    assert_eq!(
        new_client().on_welcome(&welcome).unwrap_err().code,
        ByeCode::ProtocolError
    );
    // optional fields may be absent
    let minimal = without(
        &without(
            &without(&without(&auth, "decl"), "max_frame"),
            "max_message",
        ),
        "features",
    );
    let ok = server.on_auth(&minimal).unwrap();
    assert_eq!(
        ok.peer_limits,
        Limits {
            max_frame: 65536,
            max_message: 65536
        }
    );
    assert!(ok.decl.methods.is_empty());
}

#[test]
fn features_are_the_intersection() {
    let f = fixture();
    let (hello, auth, welcome) = (
        message_of(&f.hello_hex),
        message_of(&f.auth_hex),
        message_of(&f.welcome_hex),
    );
    let mut scfg = server_cfg(&f, &welcome);
    scfg.features = vec!["acme.fast".into(), "acme.zip".into()];
    let server = ServerHandshake::with_nonce(scfg, f.key, f.s_nonce, None);
    let mut ccfg = client_cfg(&f, &auth);
    ccfg.features = vec!["acme.zip".into(), "acme.other".into(), "acme.zip".into()];
    let mut client = ClientHandshake::with_nonce(ccfg, f.key, f.c_nonce);

    let hello_msg = server.hello();
    let a = client.on_hello(&hello_msg).unwrap();
    assert_eq!(Auth::from_value(&a).unwrap().features, vec!["acme.zip"]);
    let accepted = server.on_auth(&a).unwrap();
    assert_eq!(accepted.features, vec!["acme.zip"]);
    let est = client.on_welcome(&accepted.welcome).unwrap();
    assert_eq!(est.features, vec!["acme.zip"]);

    // a server that enables what was not requested is refused
    let mut client = ClientHandshake::with_nonce(client_cfg(&f, &auth), f.key, f.c_nonce);
    client.on_hello(&hello).unwrap();
    let mut w = Welcome::from_value(&welcome).unwrap();
    w.features = vec!["acme.zip".into()];
    w.mac = server_mac(&f.key, 2, &f.s_nonce, &f.c_nonce, &f.name);
    assert_eq!(
        client.on_welcome(&w.to_value()).unwrap_err().code,
        ByeCode::ProtocolError
    );
    // a client that asks for something the server lacks gets it dropped, not an error
    let a = Auth {
        features: vec!["x.y".into()],
        ..Auth::from_value(&auth).unwrap()
    };
    let a = Auth {
        mac: client_mac(&f.key, 2, &f.s_nonce, &f.c_nonce, &f.name),
        ..a
    };
    let plain = ServerHandshake::with_nonce(server_cfg(&f, &welcome), f.key, f.s_nonce, None);
    assert!(plain.on_auth(&a.to_value()).unwrap().features.is_empty());
}

#[test]
fn unavailable_and_nonces() {
    let failure = HandshakeFailure::unavailable("no key");
    assert_eq!(failure.code.as_str(), "unavailable");
    assert_eq!(
        codec::get(&failure.bye(), "code").and_then(Value::as_str),
        Some("unavailable")
    );
    assert_ne!(new_nonce(), new_nonce());
}

#[test]
fn keyfile_vectors() {
    let doc = load("keyfile.json");
    let key: Key = hx(&doc["key"]);
    for v in doc["valid"].as_array().unwrap() {
        let bytes = hex::decode(v.as_str().unwrap()).unwrap();
        assert_eq!(parse_key(&bytes).unwrap(), key, "{v}");
    }
    for v in doc["invalid"].as_array().unwrap() {
        let bytes = hex::decode(v.as_str().unwrap()).unwrap();
        assert!(parse_key(&bytes).is_err(), "{v}");
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
        other => other.clone(),
    }
}
