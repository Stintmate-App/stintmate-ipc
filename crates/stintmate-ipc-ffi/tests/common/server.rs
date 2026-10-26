//! A core endpoint with a few methods and one channel of each kind, for the peer and client
//! tests (`#[path]`-included after `papi`).
#![allow(dead_code)]

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use stintmate_ipc::blocking::Endpoint;
use stintmate_ipc::codec::Value;
use stintmate_ipc::layout::{FieldType, Layout};
use stintmate_ipc::{
    EventChannel, KeySource as CoreKey, MethodOpts, RemoteError, StateChannel, StreamChannel,
};

use crate::common;
use crate::papi::{new_client, new_key, p};

pub struct Server {
    pub ep: Endpoint,
    pub pipe: String,
    pub key: [u8; 32],
    pub release_slow: Arc<AtomicBool>,
    pub ev: EventChannel,
    pub st: StateChannel,
    pub sm: StreamChannel,
}

pub fn random_pipe() -> String {
    format!("stintmate-ffi-t3-{}", common::random_tag())
}

impl Server {
    pub fn start(pipe: &str, key: [u8; 32]) -> Server {
        let ep = Endpoint::builder(
            stintmate_ipc::handshake::PeerInfo::new("server"),
            CoreKey::Bytes(key),
        )
        .pipe(pipe)
        .handler_threads(4)
        .build()
        .unwrap();
        let release_slow = Arc::new(AtomicBool::new(false));
        ep.method("t.echo", MethodOpts::default(), |_c, p| Ok(p))
            .unwrap();
        ep.method("t.progress", MethodOpts::default(), |ctx, _p| {
            for i in 0..3i64 {
                ctx.progress(Some(Value::from(i)), None);
            }
            Ok(Value::from("finished"))
        })
        .unwrap();
        ep.method(
            "t.fail",
            MethodOpts::default(),
            |_c, _p| -> Result<Value, RemoteError> { Err(RemoteError::new("boom", "it broke")) },
        )
        .unwrap();
        let flag = release_slow.clone();
        ep.method("t.slow", MethodOpts::default(), move |ctx, p| {
            let end = Instant::now() + Duration::from_secs(10);
            while !flag.load(Ordering::SeqCst) && Instant::now() < end {
                if ctx.wait_cancelled(Duration::from_millis(10)) {
                    break;
                }
            }
            Ok(if p.is_nil() { Value::from("slow") } else { p })
        })
        .unwrap();
        let ev = ep.event_channel("t.ev", &[1]).unwrap();
        let st = ep
            .state_channel("t.st", Value::from("start"), &[1])
            .unwrap();
        let layout = Layout::new([("n", FieldType::U32, 1)], Some("t")).unwrap();
        let sm = ep.stream_channel("t.sm", layout, &[1]).unwrap();
        ep.start().unwrap();
        Server {
            ep,
            pipe: pipe.to_owned(),
            key,
            release_slow,
            ev,
            st,
            sm,
        }
    }

    pub fn new() -> Server {
        Server::start(&random_pipe(), new_key())
    }

    /// A started client of this server, connected: the client and a reference to its peer.
    pub fn connect(&self) -> (*mut c_void, *mut c_void) {
        let c = new_client(&self.pipe, &self.key, true);
        assert_eq!(unsafe { (p().client_start)(c) }, 0);
        let mut peer = std::ptr::null_mut();
        let st = unsafe { (p().client_wait_connected)(c, 10_000, &mut peer) };
        assert_eq!(st, 0, "{}", common::last_message());
        (c, peer)
    }
}
