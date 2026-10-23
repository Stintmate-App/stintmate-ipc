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
