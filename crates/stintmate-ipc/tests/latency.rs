//! Same-process latency probes of the blocking façade (`cargo test --release --test latency
//! -- --ignored --nocapture`): the floor the bindings add their own costs to.

mod common;

use std::time::{Duration, Instant};

use common::{key, ks, pipe_name};
use stintmate_ipc::MethodOpts;
use stintmate_ipc::blocking::{Client, Endpoint};
use stintmate_ipc::codec::Value;
use stintmate_ipc::connection::CallOptions;
use stintmate_ipc::handshake::PeerInfo;

fn pct(mut v: Vec<f64>, q: f64) -> f64 {
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 * q) as usize).min(v.len() - 1)]
}

#[test]
#[ignore = "a measurement, not a check"]
fn rr_latency() {
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .build()
        .unwrap();
    ep.method("t.echo", MethodOpts::default(), |_c, p| Ok(p))
        .unwrap();
    ep.method(
        "t.inline",
        MethodOpts {
            inline: true,
            ..MethodOpts::default()
        },
        |_c, p| Ok(p),
    )
    .unwrap();
    ep.start().unwrap();
    let c = Client::new(&pipe, PeerInfo::new("client"), ks(k)).unwrap();
    c.start();
    c.wait_connected(Duration::from_secs(5)).unwrap();
    for (m, dl) in [
        ("t.echo", true),
        ("t.inline", true),
        ("t.echo", false),
        ("t.inline", false),
    ] {
        let mut lat = Vec::new();
        for i in 0..3000 {
            let t = Instant::now();
            let o = if dl {
                CallOptions::new()
            } else {
                CallOptions::new().no_timeout()
            };
            c.call_with(m, Value::from(i), o).unwrap();
            lat.push(t.elapsed().as_secs_f64() * 1e6);
        }
        println!(
            "{m} (deadline {dl}): p50 {:.1} us p99 {:.1} us",
            pct(lat.clone(), 0.5),
            pct(lat, 0.99)
        );
    }
    c.close();
    ep.stop();
}

/// The floor: a 64-byte ping-pong over a Tokio named pipe, echoed by a task, timed from a
/// caller thread blocked in `block_on` (the façade's shape).
#[test]
#[ignore = "a measurement, not a check"]
fn pipe_floor() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
    let rt = stintmate_ipc::blocking::runtime();
    let name = format!(r"\\.\pipe\{}", pipe_name());
    let server = rt
        .block_on(async { ServerOptions::new().create(&name) })
        .unwrap();
    let mut client = rt
        .block_on(async { ClientOptions::new().open(&name) })
        .unwrap();
    rt.block_on(server.connect()).unwrap();
    rt.spawn(async move {
        let mut s = server;
        let mut b = [0u8; 64];
        while s.read_exact(&mut b).await.is_ok() {
            if s.write_all(&b).await.is_err() {
                break;
            }
        }
    });
    let mut lat = Vec::new();
    let mut b = [7u8; 64];
    for _ in 0..3000 {
        let t = Instant::now();
        rt.block_on(async {
            client.write_all(&b).await.unwrap();
            client.read_exact(&mut b).await.unwrap();
        });
        lat.push(t.elapsed().as_secs_f64() * 1e6);
    }
    println!(
        "pipe floor: p50 {:.1} us p99 {:.1} us",
        pct(lat.clone(), 0.5),
        pct(lat, 0.99)
    );
}

/// Event and stream delivery Rust to Rust (one process, blocking façade): 1000 items at
/// 200 Hz, latency from the publishing call to the subscriber's callback.
#[test]
#[ignore = "a measurement, not a check"]
fn event_and_stream_latency() {
    use std::sync::{Arc, Mutex, OnceLock};
    use stintmate_ipc::SubscribeOptions;
    use stintmate_ipc::layout::{FieldType, Layout};
    use stintmate_ipc::{Delivery, codec};

    static BASE: OnceLock<Instant> = OnceLock::new();
    let stamp = || BASE.get_or_init(Instant::now).elapsed().as_nanos() as i64;
    let (pipe, k) = (pipe_name(), key());
    let ep = Endpoint::builder(PeerInfo::new("server"), ks(k))
        .pipe(&pipe)
        .build()
        .unwrap();
    let ev = ep.event_channel("t.e", &[1]).unwrap();
    let layout = Layout::new([("v", FieldType::F32, 12)], Some("bench48")).unwrap();
    let sm = ep
        .stream_channel_with("t.s", layout, &[1], Some(200.0), None)
        .unwrap();
    ep.start().unwrap();
    let c = Client::new(&pipe, PeerInfo::new("client"), ks(k)).unwrap();
    c.start();
    c.wait_connected(Duration::from_secs(5)).unwrap();
    let lat = Arc::new(Mutex::new(Vec::<f64>::new()));
    let l2 = lat.clone();
    let _h = c.subscribe("t.e", SubscribeOptions::default(), move |d| {
        if let Delivery::Event(e) = d
            && let Some(t) = e.data.as_i64()
        {
            l2.lock().unwrap().push((stamp() - t) as f64 / 1e3);
        }
    });
    let l3 = lat.clone();
    let _h2 = c.subscribe("t.s", SubscribeOptions::default(), move |d| {
        if let Delivery::Sample(s) = d {
            l3.lock().unwrap().push((stamp() - s.t_ns) as f64 / 1e3);
        }
    });
    while ev.subscriber_count() + sm.subscriber_count() < 2 {
        std::thread::sleep(Duration::from_millis(5));
    }
    let body = vec![0u8; 48];
    for what in ["event", "stream"] {
        std::thread::sleep(Duration::from_millis(200));
        lat.lock().unwrap().clear();
        for _ in 0..1000 {
            std::thread::sleep(Duration::from_millis(5));
            if what == "event" {
                ev.publish(&codec::Value::from(stamp())).unwrap();
            } else {
                sm.send(&body, Some(stamp())).unwrap();
            }
        }
        std::thread::sleep(Duration::from_millis(200));
        let v = std::mem::take(&mut *lat.lock().unwrap());
        println!(
            "{what} 200 Hz: n {} p50 {:.1} us p99 {:.1} us",
            v.len(),
            pct(v.clone(), 0.5),
            pct(v, 0.99)
        );
    }
    c.close();
    ep.stop();
}
