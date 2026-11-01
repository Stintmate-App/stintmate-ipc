//! Endpoint options, info, stop and the handshake-failed hook.

mod common;

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use stintmate_ipc::handshake::PeerInfo as CorePeerInfo;
use stintmate_ipc::keyfile::KeySource;

#[test]
fn info_reports_the_pipe_registry_and_identity_and_options_are_applied() {
    let _s = serial();
    let a = api();
    let dir = std::env::temp_dir().join(format!("stintmate-ffi-ep-{}", random_tag()));
    std::fs::create_dir_all(&dir).unwrap();
    let registry = c(dir.to_str().unwrap());
    let features = mp(&Value::Array(vec![Value::from("direct_writes")]));
    let fx = EpFix::with(|o| {
        o.registry = registry.as_ptr();
        o.handshake_timeout_ms = 3000;
        o.heartbeat_ms = 500;
        o.event_queue = 64;
        o.event_queue_bytes = 1 << 20;
        o.max_inflight = 16;
        o.features = features.as_ptr();
        o.features_len = features.len();
    });
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.endpoint_info)(fx.ep, &mut out) }, 0);
    let info = val(out);
    let path = key_of(&info, "path");
    assert!(path.as_str().unwrap().contains(fx.pipe.to_str().unwrap()));
    assert!(key_of(&info, "short_name").as_str().is_some());
    assert!(!key_of(&info, "run").as_str().unwrap().is_empty());
    assert_eq!(
        key_of(&info, "registry"),
        Value::from(dir.to_str().unwrap())
    );
    assert_eq!(key_of(&key_of(&info, "info"), "app"), Value::from("server"));

    // The options do not get in the way of a connection.
    let peer = fx.start();
    assert!(peer.is_open());
    fx.free();
    assert_clean();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_short_options_struct_takes_defaults_and_bad_options_fail() {
    let _s = serial();
    let a = api();
    let pipe = c(&format!("stintmate-ffi-t2o-{}", random_tag()));
    let key = new_key();
    let ks = key_source(&key);
    let app = c("server");
    let mut opts = EndpointOpts::new(pipe.as_ptr(), 3);
    opts.struct_size = std::mem::offset_of!(EndpointOpts, registry);
    opts.registry = c"never-read".as_ptr();
    let mut ep = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.endpoint_new)(&peer_info(&app), &ks, &opts, &mut ep) },
        0,
        "{}",
        last_message()
    );
    let mut out = NO_BUFFER;
    assert_eq!(unsafe { (a.endpoint_info)(ep, &mut out) }, 0);
    let info = val(out);
    assert_eq!(key_of(&info, "registry"), Value::Nil);
    unsafe { (a.endpoint_free)(ep) };

    // Features that are not an array of strings.
    let bad = mp(&Value::from("direct_writes"));
    let mut opts = EndpointOpts::new(pipe.as_ptr(), 1);
    opts.features = bad.as_ptr();
    opts.features_len = bad.len();
    let mut ep = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.endpoint_new)(&peer_info(&app), &ks, &opts, &mut ep) },
        -11
    );
    assert!(ep.is_null());
    assert_clean();
}

unsafe extern "C-unwind" fn failed(user: *mut c_void, pid: i64, err: *const u8, len: usize) {
    let v = codec::decode(unsafe { std::slice::from_raw_parts(err, len) }).unwrap();
    unsafe { log(user) }.push(format!(
        "failed:{}:{}:{}",
        key_of(&v, "status").as_i64().unwrap(),
        key_of(&v, "code").as_str().unwrap_or("none"),
        pid > 0
    ));
}

#[test]
fn the_handshake_failed_hook_reports_a_client_with_the_wrong_key() {
    let _s = serial();
    let a = api();
    let fx = EpFix::new();
    let l = Arc::new(Log::default());
    assert_eq!(
        unsafe { (a.on_handshake_failed)(fx.ep, Some(failed), l.ud(), Some(release_log)) },
        0
    );
    assert_eq!(unsafe { (a.endpoint_start)(fx.ep) }, 0);
    let wrong = ipc::Client::new(
        fx.pipe.to_str().unwrap(),
        CorePeerInfo::new("intruder"),
        KeySource::Bytes(new_key()),
    )
    .unwrap();
    wrong.start();
    wait_for("the report", || {
        l.lines()
            .iter()
            .any(|s| s.starts_with("failed:-4:unauthorized:"))
    });
    wrong.close();

    // Stopping says bye with the code given; the right client sees the connection end.
    fx.core.start();
    fx.core.wait_connected(Duration::from_secs(10)).unwrap();
    let st = unsafe { (a.endpoint_stop)(fx.ep, c"maintenance".as_ptr(), c"back soon".as_ptr()) };
    assert_eq!(st, 0, "{}", last_message());
    wait_for("the disconnect", || !fx.core.is_connected());
    // Stopping twice is fine, and the handle is still valid until freed.
    assert_eq!(
        unsafe { (a.endpoint_stop)(fx.ep, std::ptr::null(), std::ptr::null()) },
        0
    );
    fx.free();
    wait_for("the release", || l.released.load(Ordering::SeqCst) == 1);
    assert_clean();
}
