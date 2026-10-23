//! The tool against the Rust implementation: every check passes against a Rust endpoint and a
//! Rust client (the same checks the Python binding and other implementations are held to).

use std::time::Duration;

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::handshake::{PeerInfo, new_nonce};
use stintmate_ipc::layout::{FieldType, Layout};
use stintmate_ipc::{Client, Endpoint, KeySource, MethodOpts, SubscribeOptions};
use stintmate_ipc_check::client::ClientChecker;
use stintmate_ipc_check::report::{Report, Status};
use stintmate_ipc_check::server::ServerChecker;

fn pipe_name() -> String {
    let n: String = new_nonce()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("stintmate-t5-{n}")
}

fn assert_conformant(report: &Report, wanted: &[&str]) {
    let text = report.to_text();
    assert!(report.ok(), "not conformant:\n{text}");
    for id in wanted {
        let r = report.results.iter().find(|r| r.id == *id);
        assert!(
            r.is_some_and(|r| r.status == Status::Pass),
            "{id} did not pass:\n{text}"
        );
    }
    assert!(report.to_json().contains("\"ok\": true"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_checks_pass_against_a_rust_endpoint() {
    let (pipe, key) = (pipe_name(), new_nonce());
    let ep = Endpoint::builder(PeerInfo::new("check-target"), KeySource::Bytes(key))
        .pipe(&pipe)
        .heartbeat_ms(300)
        .build()
        .unwrap();
    ep.method(
        "test.echo",
        MethodOpts::default(),
        |_c, p| async move { Ok(p) },
    )
    .unwrap();
    let events = ep.event_channel("test.events", &[1]).unwrap();
    let state = ep
        .state_channel("test.state", codec::map([("n", Value::from(0))]), &[1])
        .unwrap();
    let layout = Layout::new(
        [("t_ms", FieldType::U32, 1), ("x", FieldType::F32, 1)],
        Some("sample"),
    )
    .unwrap();
    let stream = ep
        .stream_channel_with("test.stream", layout, &[1], Some(100.0), None)
        .unwrap();
    ep.start().await.unwrap();
    let publisher = tokio::spawn(async move {
        let mut n = 0u32;
        loop {
            n += 1;
            let _ = events.publish(&codec::map([("n", Value::from(n))]));
            let _ = state.set(&codec::map([("n", Value::from(n))]));
            let mut body = n.to_le_bytes().to_vec();
            body.extend_from_slice(&1.5f32.to_le_bytes());
            let _ = stream.send(&body, None);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let calls = vec![("test.echo".to_owned(), codec::map([("a", Value::from(1))]))];
    let checker = ServerChecker::new(&pipe, key, 0.5, true, None, calls);
    let report = checker.run().await;
    publisher.abort();
    ep.stop().await;
    assert_conformant(
        &report,
        &[
            "S01",
            "S02",
            "S03",
            "S04",
            "S05",
            "S06",
            "S07",
            "S08",
            "S09",
            "S10",
            "S11",
            "S12",
            "S13",
            "S14",
            "S15",
            "S16",
            "S17",
            "S18",
            "S19.test.events",
            "S19.test.state",
            "S19.test.stream",
            "S20",
            "S22",
            "S23",
            "S24",
        ],
    );
    assert!(
        report
            .results
            .iter()
            .any(|r| r.id == "S21.1" && r.status == Status::Info)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_checks_pass_against_a_rust_client() {
    let (pipe, key) = (pipe_name(), new_nonce());
    let checker = ClientChecker::new(&pipe, key, 20.0, 1.0, 20.0).unwrap();
    let run = tokio::spawn(checker.run());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let client = Client::new(&pipe, PeerInfo::new("check-client"), KeySource::Bytes(key)).unwrap();
    let own = client.event_channel("client.events", &[1]).unwrap();
    client.start();
    client
        .wait_connected(Duration::from_secs(10))
        .await
        .unwrap();
    let pub_task = tokio::spawn(async move {
        loop {
            let _ = own.publish(&codec::map([("k", Value::from(1))]));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let _echo = client
        .call("check.echo", codec::map([("x", Value::from(1))]))
        .await;
    let _sub = client
        .subscribe("check.events", SubscribeOptions::default(), |_| {})
        .await;
    let report = run.await.unwrap();
    pub_task.abort();
    client.close().await;
    assert_conformant(
        &report,
        &[
            "C01", "C02", "C03", "C04", "C05", "C06", "C08", "C10", "C11",
        ],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_key_is_reported_as_a_failure() {
    let (pipe, key) = (pipe_name(), new_nonce());
    let ep = Endpoint::builder(PeerInfo::new("check-target"), KeySource::Bytes(key))
        .pipe(&pipe)
        .build()
        .unwrap();
    ep.start().await.unwrap();
    let report = ServerChecker::new(&pipe, new_nonce(), 0.2, false, None, Vec::new())
        .run()
        .await;
    ep.stop().await;
    assert!(!report.ok());
    assert!(
        report
            .results
            .iter()
            .any(|r| r.id == "S02" && r.status == Status::Fail)
    );
    assert!(report.to_text().contains("NOT CONFORMANT"));
}
