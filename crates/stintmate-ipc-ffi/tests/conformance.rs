//! The conformance tool against the DLL: `stintmate-ipc-check` in server mode against an
//! endpoint made through the DLL, and in client mode against a client made through the DLL.

mod common;
#[path = "common/peer_api.rs"]
mod papi;

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{
    EpFix, NO_BUFFER, api, assert_clean, c, codec, key_of, last_message, mp, serial, take,
};
use stintmate_ipc::codec::Value;
use stintmate_ipc::layout::{FieldType, Layout};

fn hex(key: &[u8; 32]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

fn argv(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).to_owned()).collect()
}

unsafe extern "C-unwind" fn echo(_user: *mut c_void, req: *mut c_void, p: *const u8, len: usize) {
    unsafe { (api().respond)(req, p, len) };
}

/// Addresses of channel handles, for a publisher thread.
#[derive(Clone, Copy)]
struct Chans {
    ev: usize,
    st: usize,
    sm: usize,
}

fn publish_loop(ch: Chans, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let a = api();
        let mut n = 0u32;
        while !stop.load(Ordering::SeqCst) {
            n += 1;
            let v = mp(&codec::map([("n", Value::from(n))]));
            let mut q = 0usize;
            let mut body = n.to_le_bytes().to_vec();
            body.extend_from_slice(&1.5f32.to_le_bytes());
            let mut sent = 0usize;
            unsafe {
                for h in [ch.ev, ch.st] {
                    (a.publish)(
                        h as *mut c_void,
                        v.as_ptr(),
                        v.len(),
                        i64::MIN,
                        0,
                        None,
                        std::ptr::null_mut(),
                        &mut q,
                    );
                }
                (a.stream_send)(
                    ch.sm as *mut c_void,
                    body.as_ptr(),
                    body.len(),
                    i64::MIN,
                    &mut sent,
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    })
}

#[test]
fn the_server_checks_pass_against_an_endpoint_made_through_the_dll() {
    let _s = serial();
    let a = api();
    let fx = EpFix::with(|o| o.heartbeat_ms = 300);
    assert_eq!(
        unsafe {
            (a.method)(
                fx.node,
                c"test.echo".as_ptr(),
                std::ptr::null(),
                Some(echo),
                std::ptr::null_mut(),
                None,
            )
        },
        0,
        "{}",
        last_message()
    );
    let (mut ev, mut st, mut sm) = (
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    let v1 = [1u32];
    let init = mp(&codec::map([("n", Value::from(0))]));
    let layout = mp(&Layout::new(
        [("t_ms", FieldType::U32, 1), ("x", FieldType::F32, 1)],
        Some("sample"),
    )
    .unwrap()
    .to_value());
    unsafe {
        let s = (a.event_channel)(fx.node, c"test.events".as_ptr(), v1.as_ptr(), 1, &mut ev);
        assert_eq!(s, 0, "{}", last_message());
        let s = (a.state_channel)(
            fx.node,
            c"test.state".as_ptr(),
            init.as_ptr(),
            init.len(),
            v1.as_ptr(),
            1,
            &mut st,
        );
        assert_eq!(s, 0, "{}", last_message());
        let s = (a.stream_channel)(
            fx.node,
            c"test.stream".as_ptr(),
            layout.as_ptr(),
            layout.len(),
            v1.as_ptr(),
            1,
            100.0,
            0,
            &mut sm,
        );
        assert_eq!(s, 0, "{}", last_message());
        assert_eq!((a.endpoint_start)(fx.ep), 0, "{}", last_message());
    }
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = publish_loop(
        Chans {
            ev: ev as usize,
            st: st as usize,
            sm: sm as usize,
        },
        stop.clone(),
    );

    let key = hex(&fx.key);
    let code = stintmate_ipc_check::cli::main(&argv(&[
        "server",
        fx.pipe.to_str().unwrap(),
        "--key-hex",
        &key,
        "--listen",
        "0.5",
        "--call",
        "test.echo={\"a\":1}",
    ]));
    stop.store(true, Ordering::SeqCst);
    publisher.join().unwrap();
    assert_eq!(code, 0, "the server checks failed (see the output above)");

    unsafe {
        (a.channel_free)(ev);
        (a.channel_free)(st);
        (a.channel_free)(sm);
    }
    fx.free();
    assert_clean();
}

#[test]
fn the_client_checks_pass_against_a_client_made_through_the_dll() {
    let _s = serial();
    let a = api();
    let p = papi::p();
    let pipe = c(&format!("stintmate-ffi-t3conf-{}", common::random_tag()));
    let key = common::new_key();
    let key_hex = hex(&key);

    let pipe_str = pipe.to_str().unwrap().to_owned();
    let checker = std::thread::spawn(move || {
        stintmate_ipc_check::cli::main(&argv(&[
            "client",
            &pipe_str,
            "--key-hex",
            &key_hex,
            "--timeout",
            "30",
            "--serve",
            "1",
            "--reconnect",
            "30",
        ]))
    });
    std::thread::sleep(Duration::from_millis(500));

    let app = c("check-client");
    let ks = common::key_source(&key);
    let mut client = std::ptr::null_mut();
    let st = unsafe {
        (a.client_new)(
            pipe.as_ptr(),
            &common::peer_info(&app),
            &ks,
            std::ptr::null(),
            &mut client,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    let node = unsafe { (a.client_node)(client) };
    let mut ev = std::ptr::null_mut();
    assert_eq!(
        unsafe { (a.event_channel)(node, c"client.events".as_ptr(), [1u32].as_ptr(), 1, &mut ev) },
        0,
        "{}",
        last_message()
    );
    assert_eq!(unsafe { (a.client_start)(client) }, 0);
    assert_eq!(
        unsafe { (a.client_wait_connected)(client, 10_000, std::ptr::null_mut()) },
        0,
        "{}",
        last_message()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let (s2, evp) = (stop.clone(), ev as usize);
    let pubt = std::thread::spawn(move || {
        while !s2.load(Ordering::SeqCst) {
            let v = mp(&codec::map([("k", Value::from(1))]));
            let mut q = 0usize;
            unsafe {
                (api().publish)(
                    evp as *mut c_void,
                    v.as_ptr(),
                    v.len(),
                    i64::MIN,
                    0,
                    None,
                    std::ptr::null_mut(),
                    &mut q,
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    });

    let params = mp(&codec::map([("x", Value::from(1))]));
    let mut out = NO_BUFFER;
    let st = unsafe {
        (p.client_call)(
            client,
            c"check.echo".as_ptr(),
            params.as_ptr(),
            params.len(),
            std::ptr::null(),
            &mut out,
        )
    };
    assert_eq!(st, 0, "{}", last_message());
    assert_eq!(
        key_of(&codec::decode(&take(out)).unwrap(), "x"),
        Value::from(1)
    );
    let opts = papi::SubscribeOpts::new(Some(papi::on_delivery), papi::Probe::leak().ptr());
    let mut sub = std::ptr::null_mut();
    let st = unsafe { (p.client_subscribe)(client, c"check.events".as_ptr(), &opts, &mut sub) };
    assert_eq!(st, 0, "{}", last_message());

    let code = checker.join().unwrap();
    stop.store(true, Ordering::SeqCst);
    pubt.join().unwrap();
    assert_eq!(code, 0, "the client checks failed (see the output above)");

    unsafe {
        (p.unsubscribe)(sub);
        (a.channel_free)(ev);
        (a.client_free)(client);
    }
    papi::settle();
    assert_clean();
}
