//! The transport and the connection over real named pipes (random names, random keys) and, for
//! the cases that need a scripted peer, over in-memory duplex streams.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::connection::{
    CallOptions, ClosedBy, Connection, ConnectionOptions, FnHandler, Handler, NoHandler,
    RequestContext, accept, accept_io, connect, connect_io,
};
use stintmate_ipc::decl::Declarations;
use stintmate_ipc::error::{Error, RemoteError};
use stintmate_ipc::handshake::{
    ByeCode, ClientConfig, ClientHandshake, PeerInfo, ServerConfig, ServerHandshake, new_nonce,
    to_frame,
};
use stintmate_ipc::keyfile::Key;
use stintmate_ipc::security::pipe_exists;
use stintmate_ipc::transport::{PipeListener, open_pipe};
use stintmate_ipc::wire::{Frame, FrameReader};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::windows::named_pipe::ServerOptions;
use tokio::sync::{Notify, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

const NAME: &str = "test-endpoint";

fn random_hex() -> String {
    new_nonce()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn pipe_path() -> String {
    format!(r"\\.\pipe\stintmate-test-{}", random_hex())
}

fn key() -> Key {
    new_nonce()
}

fn server_cfg(heartbeat_ms: u32) -> ServerConfig {
    let mut c = ServerConfig::new(NAME, PeerInfo::new("server"), Declarations::default());
    c.heartbeat_ms = heartbeat_ms;
    c
}

fn client_cfg() -> ClientConfig {
    ClientConfig::new(NAME, PeerInfo::new("client"), Declarations::default())
}

fn fast() -> ConnectionOptions {
    ConnectionOptions {
        bye_wait: Duration::from_millis(300),
        ..ConnectionOptions::default()
    }
}

fn int(v: i64) -> Value {
    Value::from(v)
}

/// Echoes `echo`; `wait` waits for the caller's cancel; `panic` panics; `slow` sleeps its
/// params in milliseconds; `progress` reports 1 and 2 first.
fn test_handler(cancelled: Arc<Notify>) -> Arc<dyn Handler> {
    Arc::new(FnHandler(move |ctx: RequestContext| {
        let cancelled = cancelled.clone();
        async move {
            match ctx.method.as_str() {
                "echo" => Ok(ctx.params.clone()),
                "wait" => {
                    ctx.cancelled().await;
                    cancelled.notify_one();
                    // The answer to a cancelled request is still sent; the caller ignores it.
                    Ok(Value::from("late"))
                }
                "panic" => panic!("boom"),
                "slow" => {
                    let ms = ctx.params.as_u64().unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(Value::from(ms))
                }
                "progress" => {
                    ctx.progress(Some(int(1)), None);
                    ctx.progress(Some(int(2)), None);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(Value::from("done"))
                }
                "extend" => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    ctx.progress(None, Some(2000));
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    Ok(Value::from("extended"))
                }
                "fail" => {
                    let mut e = RemoteError::new("game_not_running", "AMS2 is not running.");
                    e.retry_ms = Some(2000);
                    Err(e)
                }
                other => Err(RemoteError::new("unknown_method", other)),
            }
        }
    }))
}

struct Harness {
    path: String,
    key: Key,
    stop: CancellationToken,
    task: JoinHandle<()>,
    conns: mpsc::UnboundedReceiver<Result<Connection, Error>>,
}

impl Harness {
    fn start(handler: Arc<dyn Handler>, heartbeat_ms: u32) -> Self {
        Self::start_with(handler, heartbeat_ms, key())
    }

    fn start_with(handler: Arc<dyn Handler>, heartbeat_ms: u32, key: Key) -> Self {
        let path = pipe_path();
        let mut listener = PipeListener::bind(&path).expect("bind");
        let stop = CancellationToken::new();
        let (tx, conns) = mpsc::unbounded_channel();
        let stop2 = stop.clone();
        let task = tokio::spawn(async move {
            let mut handshakes = JoinSet::new();
            loop {
                tokio::select! {
                    () = stop2.cancelled() => break,
                    a = listener.accept() => {
                        let a = a.expect("accept");
                        let (tx, handler) = (tx.clone(), handler.clone());
                        handshakes.spawn(async move {
                            let r = accept(a, server_cfg(heartbeat_ms), key, handler, fast()).await;
                            let _ = tx.send(r);
                        });
                    }
                }
            }
            while handshakes.join_next().await.is_some() {}
        });
        Self {
            path,
            key,
            stop,
            task,
            conns,
        }
    }

    async fn client(&self) -> Connection {
        connect(
            &self.path,
            client_cfg(),
            self.key,
            Arc::new(NoHandler),
            fast(),
        )
        .await
        .expect("connect")
    }

    async fn next(&mut self) -> Result<Connection, Error> {
        tokio::time::timeout(Duration::from_secs(5), self.conns.recv())
            .await
            .expect("a connection in time")
            .expect("the listener runs")
    }

    /// Stops listening and checks that nothing of the pipe is left.
    async fn finish(self) {
        self.stop.cancel();
        self.task.await.unwrap();
        wait_gone(&self.path).await;
    }
}

/// A server and a connected pair, both sides ready.
async fn pair(handler: Arc<dyn Handler>, heartbeat_ms: u32) -> (Harness, Connection, Connection) {
    let mut h = Harness::start(handler, heartbeat_ms);
    let c = h.client().await;
    let s = h.next().await.expect("server side");
    (h, c, s)
}

async fn close_pair(h: Harness, c: Connection, s: Connection) {
    c.shutdown("normal", "test over").await;
    s.wait_closed().await;
    h.finish().await;
}

// ----------------------------------------------------------------------------- pipes

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_instance_is_exclusive() {
    let path = pipe_path();
    let first = PipeListener::bind(&path).unwrap();
    assert!(matches!(
        PipeListener::bind(&path),
        Err(Error::EndpointInUse(p)) if p == path
    ));
    drop(first);
    wait_gone(&path).await;
    drop(PipeListener::bind(&path).expect("free again"));
}

#[tokio::test]
async fn a_missing_pipe_is_not_found() {
    let path = pipe_path();
    assert!(matches!(
        open_pipe(&path, Duration::from_secs(1)).await,
        Err(Error::PipeNotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_next_instance_exists_before_the_connected_one_is_handed_over() {
    let path = pipe_path();
    let mut listener = PipeListener::bind(&path).unwrap();
    let p2 = path.clone();
    let client = tokio::spawn(async move { open_pipe(&p2, Duration::from_secs(2)).await });
    let accepted = listener.accept().await.unwrap();
    assert_eq!(accepted.client_pid, Some(std::process::id()));
    drop(client.await.unwrap().unwrap());
    drop(accepted);
    // Both ends are gone, yet the name still exists: a client arriving now is not told
    // "not found" (SPEC 3.2).
    assert!(pipe_exists(&path));
    let again = open_pipe(&path, Duration::from_secs(2)).await;
    assert!(again.is_ok());
    drop(again);
    drop(listener);
    wait_gone(&path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_waits_for_a_free_instance_within_the_deadline() {
    let path = pipe_path();
    let server = ServerOptions::new()
        .first_pipe_instance(true)
        .max_instances(1)
        .create(&path)
        .unwrap();
    let first = open_pipe(&path, Duration::from_secs(1)).await.unwrap();
    server.connect().await.unwrap();

    // Every instance is taken: a short deadline ends in PipeBusy.
    assert!(matches!(
        open_pipe(&path, Duration::from_millis(300)).await,
        Err(Error::PipeBusy(_))
    ));

    // A longer one succeeds once the instance is free again.
    let p2 = path.clone();
    let started = Instant::now();
    let waiting = tokio::spawn(async move { open_pipe(&p2, Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(first);
    server.disconnect().unwrap();
    server.connect().await.unwrap();
    let second = waiting.await.unwrap().expect("opened after the wait");
    assert!(started.elapsed() >= Duration::from_millis(250));
    drop(second);
    drop(server);
}

// ----------------------------------------------------------------------------- handshake

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_and_a_call() {
    let (h, c, s) = pair(test_handler(Arc::new(Notify::new())), 5000).await;
    assert_eq!(s.info().peer.app, "client");
    assert_eq!(c.info().peer.app, "server");
    assert_eq!(s.info().client_pid, Some(std::process::id()));
    assert_eq!(c.info().run, s.info().run);
    let params = codec::map([("a", int(1)), ("b", Value::from("two"))]);
    let r = c.call("echo", params.clone(), CallOptions::new()).await;
    assert_eq!(r.unwrap(), params);
    // The server calls the client too (sessions are symmetric): the client has no methods.
    let r = s.call("anything", Value::Nil, CallOptions::new()).await;
    assert!(matches!(r, Err(Error::Remote(e)) if e.code == "unknown_method"));
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_key_is_unauthorized_on_both_sides() {
    let mut h = Harness::start(Arc::new(NoHandler), 5000);
    let wrong = key();
    let r = connect(&h.path, client_cfg(), wrong, Arc::new(NoHandler), fast()).await;
    match r {
        Err(Error::PeerBye { code, .. }) => assert_eq!(code, "unauthorized"),
        other => panic!("expected the server's bye, got {other:?}"),
    }
    match h.next().await {
        Err(Error::Handshake(f)) => assert_eq!(f.code, ByeCode::Unauthorized),
        other => panic!("expected a handshake failure, got {other:?}"),
    }
    // The server keeps serving.
    let c = h.client().await;
    let s = h.next().await.unwrap();
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_client_times_out_in_the_handshake() {
    let h = Harness::start(Arc::new(NoHandler), 5000);
    // Override the 5 s default with a quicker server is not possible through the harness, so
    // use the pipe directly with a short-timeout server.
    h.stop.cancel();
    let path = pipe_path();
    let mut listener = PipeListener::bind(&path).unwrap();
    let p2 = path.clone();
    let silent = tokio::spawn(async move {
        let pipe = open_pipe(&p2, Duration::from_secs(1)).await.unwrap();
        let mut pipe = pipe;
        let mut buf = vec![0u8; 4096];
        let n = pipe.read(&mut buf).await.unwrap();
        (n, pipe)
    });
    let a = listener.accept().await.unwrap();
    let opts = ConnectionOptions {
        handshake_timeout: Duration::from_millis(200),
        ..fast()
    };
    let r = accept(a, server_cfg(5000), key(), Arc::new(NoHandler), opts).await;
    assert!(matches!(r, Err(Error::HandshakeTimeout)));
    let (n, pipe) = silent.await.unwrap();
    assert!(n > 0, "the hello was sent");
    drop(pipe);
    drop(listener);
    h.task.await.unwrap();
}

// ----------------------------------------------------------------------------- calls

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_errors_unknown_methods_and_panics() {
    let (h, c, s) = pair(test_handler(Arc::new(Notify::new())), 5000).await;
    match c.call("fail", Value::Nil, CallOptions::new()).await {
        Err(Error::Remote(e)) => {
            assert_eq!(e.code, "game_not_running");
            assert_eq!(e.retry_ms, Some(2000));
        }
        other => panic!("{other:?}"),
    }
    let r = c.call("nope", Value::Nil, CallOptions::new()).await;
    assert!(matches!(r, Err(Error::Remote(e)) if e.code == "unknown_method"));
    let r = c.call("panic", Value::Nil, CallOptions::new()).await;
    assert!(matches!(r, Err(Error::Remote(e)) if e.code == "internal"));
    // The connection survived the panic.
    assert_eq!(
        c.call("echo", int(5), CallOptions::new()).await.unwrap(),
        int(5)
    );
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_fails_the_call_cancels_the_owner_and_ignores_the_late_res() {
    let seen = Arc::new(Notify::new());
    let (h, c, s) = pair(test_handler(seen.clone()), 5000).await;
    let r = c
        .call(
            "wait",
            Value::Nil,
            CallOptions::new().timeout(Duration::from_millis(100)),
        )
        .await;
    assert!(matches!(r, Err(Error::CallTimeout)), "{r:?}");
    assert_eq!(c.pending_calls(), 0);
    // The owner saw the `cancel`, and its (late) answer changes nothing.
    tokio::time::timeout(Duration::from_secs(2), seen.notified())
        .await
        .expect("the owner was cancelled");
    assert_eq!(
        c.call("echo", int(1), CallOptions::new()).await.unwrap(),
        int(1)
    );
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_fails_the_call_once() {
    let seen = Arc::new(Notify::new());
    let (h, c, s) = pair(test_handler(seen.clone()), 5000).await;
    let pending = c
        .call_async("wait", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    let handle = pending.handle();
    let waiter = tokio::spawn(pending);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.cancel());
    assert!(!handle.cancel(), "a call is given up once");
    assert!(matches!(waiter.await.unwrap(), Err(Error::CallCancelled)));
    tokio::time::timeout(Duration::from_secs(2), seen.notified())
        .await
        .expect("the owner was cancelled");
    // The `res` that follows is ignored; the connection works.
    assert_eq!(
        c.call("echo", int(2), CallOptions::new()).await.unwrap(),
        int(2)
    );
    assert_eq!(c.pending_calls(), 0);
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progress_reaches_the_caller_and_can_extend_the_deadline() {
    let (h, c, s) = pair(test_handler(Arc::new(Notify::new())), 5000).await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let r = c
        .call(
            "progress",
            Value::Nil,
            CallOptions::new().on_progress(move |d| seen2.lock().unwrap().push(d)),
        )
        .await;
    assert_eq!(r.unwrap(), Value::from("done"));
    assert_eq!(*seen.lock().unwrap(), vec![int(1), int(2)]);

    // 150 ms would not do: the owner extends the deadline to 2 s after 50 ms.
    let r = c
        .call(
            "extend",
            Value::Nil,
            CallOptions::new().timeout(Duration::from_millis(150)),
        )
        .await;
    assert_eq!(r.unwrap(), Value::from("extended"));
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_larger_than_a_frame_travel_in_chunks() {
    let (h, c, s) = pair(test_handler(Arc::new(Notify::new())), 5000).await;
    // The default max_frame is 1 MiB: 3 MiB takes several chunks each way.
    let big = Value::Binary(vec![7u8; 3 * 1024 * 1024]);
    let r = c
        .call(
            "echo",
            big.clone(),
            CallOptions::new().timeout(Duration::from_secs(20)),
        )
        .await;
    assert_eq!(r.unwrap(), big);
    close_pair(h, c, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bye_from_the_peer_fails_pending_calls_with_its_code() {
    let (h, c, s) = pair(test_handler(Arc::new(Notify::new())), 5000).await;
    let pending = c
        .call_async("wait", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    s.close("shutting_down", "the server stops");
    match pending.await {
        Err(Error::ConnectionLost { code, .. }) => {
            assert_eq!(code.as_deref(), Some("shutting_down"));
        }
        other => panic!("{other:?}"),
    }
    let info = c.wait_closed().await;
    assert_eq!(info.by, ClosedBy::Peer);
    assert_eq!(info.code, "shutting_down");
    let info = s.wait_closed().await;
    assert_eq!(info.by, ClosedBy::Local);
    // New calls fail at once.
    assert!(matches!(
        c.call("echo", Value::Nil, CallOptions::new()).await,
        Err(Error::ConnectionLost { .. })
    ));
    h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_peer_completes_pending_calls_with_connection_lost() {
    let (a, b) = tokio::io::duplex(64 * 1024);
    let k = key();
    let hold = CancellationToken::new();
    let mute = tokio::spawn(mute_server(a, k, hold.clone()));
    let c = connect_io(b, client_cfg(), k, Arc::new(NoHandler), fast())
        .await
        .unwrap();
    let pending = c
        .call_async("wait", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    // Hard loss: the peer's end of the stream is dropped, with no bye.
    hold.cancel();
    mute.await.unwrap();
    assert!(matches!(
        pending.await,
        Err(Error::ConnectionLost { code: None, .. })
    ));
    assert_eq!(c.wait_closed().await.by, ClosedBy::Transport);
}

// ----------------------------------------------------------------------------- heartbeat

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pings_keep_an_idle_connection_alive_and_ping_measures() {
    // H = 100 ms: an idle connection pings every ~100 ms; it would be closed after 300 ms
    // of silence.
    let (h, c, s) = pair(Arc::new(NoHandler), 100).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(!c.is_closed() && !s.is_closed());
    let rtt = c.ping(Duration::from_secs(2)).await.unwrap();
    assert!(rtt < Duration::from_secs(1));
    close_pair(h, c, s).await;
}

/// A scripted raw peer: runs the client's handshake by hand and returns the stream.
async fn raw_client(mut io: DuplexStream, k: Key) -> (DuplexStream, FrameReader) {
    let mut reader = FrameReader::for_handshake();
    let mut hs = ClientHandshake::new(client_cfg(), k);
    let hello = read_raw(&mut io, &mut reader).await.unwrap();
    let auth = hs.on_hello(&hello).unwrap();
    io.write_all(&to_frame(&auth).unwrap()).await.unwrap();
    let welcome = read_raw(&mut io, &mut reader).await.unwrap();
    hs.on_welcome(&welcome).unwrap();
    (io, reader)
}

async fn read_raw(io: &mut DuplexStream, reader: &mut FrameReader) -> Option<Value> {
    let mut buf = [0u8; 4096];
    loop {
        if let Some(Frame::Message(b)) = reader.next_frame().ok()? {
            return Some(codec::decode_message(&b).unwrap());
        }
        let n = io.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        reader.push(&buf[..n]);
    }
}

/// The server's handshake by hand; afterwards the peer says nothing.
async fn mute_server(mut io: DuplexStream, k: Key, hold: CancellationToken) {
    let hs = ServerHandshake::new(server_cfg(100), k, None);
    let mut reader = FrameReader::for_handshake();
    io.write_all(&to_frame(&hs.hello()).unwrap()).await.unwrap();
    let auth = read_raw(&mut io, &mut reader).await.unwrap();
    let accepted = hs.on_auth(&auth).unwrap();
    io.write_all(&to_frame(&accepted.welcome).unwrap())
        .await
        .unwrap();
    hold.cancelled().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_peer_is_dropped_after_three_heartbeats() {
    let (a, b) = tokio::io::duplex(64 * 1024);
    let k = key();
    let hold = CancellationToken::new();
    let mute = tokio::spawn(mute_server(a, k, hold.clone()));
    let c = connect_io(b, client_cfg(), k, Arc::new(NoHandler), fast())
        .await
        .unwrap();
    assert_eq!(c.info().heartbeat_ms, 100);
    let started = Instant::now();
    let pending = c
        .call_async("echo", Value::Nil, CallOptions::new().no_timeout())
        .await
        .unwrap();
    let r = pending.await;
    let took = started.elapsed();
    assert!(
        matches!(r, Err(Error::ConnectionLost { .. })),
        "pending call failed by the heartbeat: {r:?}"
    );
    assert!(
        took >= Duration::from_millis(250) && took < Duration::from_secs(2),
        "{took:?}"
    );
    let info = c.wait_closed().await;
    assert_eq!((info.by, info.code.as_str()), (ClosedBy::Local, "timeout"));
    hold.cancel();
    mute.await.unwrap();
}

// ----------------------------------------------------------------------------- protocol

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_framing_error_ends_the_connection_with_a_protocol_error_bye() {
    let (a, b) = tokio::io::duplex(64 * 1024);
    let k = key();
    let server = tokio::spawn(accept_io(
        a,
        None,
        server_cfg(5000),
        k,
        Arc::new(NoHandler),
        fast(),
    ));
    let (mut raw, mut reader) = raw_client(b, k).await;
    let s = server.await.unwrap().unwrap();
    raw.write_all(&[0, 0, 0, 0]).await.unwrap(); // a frame of length 0
    let bye = read_raw(&mut raw, &mut reader).await.expect("a bye");
    assert_eq!(codec::get(&bye, "t").unwrap().as_str(), Some("bye"));
    assert_eq!(
        codec::get(&bye, "code").unwrap().as_str(),
        Some("protocol_error")
    );
    drop(raw);
    let info = s.wait_closed().await;
    assert_eq!(info.code, "protocol_error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reused_request_id_is_a_protocol_error() {
    let (a, b) = tokio::io::duplex(64 * 1024);
    let k = key();
    let server = tokio::spawn(accept_io(
        a,
        None,
        server_cfg(5000),
        k,
        test_handler(Arc::new(Notify::new())),
        fast(),
    ));
    let (mut raw, mut reader) = raw_client(b, k).await;
    let s = server.await.unwrap().unwrap();
    let req = codec::map([
        ("t", Value::from("req")),
        ("id", int(9)),
        ("m", Value::from("wait")),
    ]);
    let frame = to_frame(&req).unwrap();
    raw.write_all(&frame).await.unwrap();
    raw.write_all(&frame).await.unwrap();
    let bye = loop {
        let m = read_raw(&mut raw, &mut reader).await.expect("a bye");
        if codec::get(&m, "t").unwrap().as_str() == Some("bye") {
            break m;
        }
    };
    assert_eq!(
        codec::get(&bye, "code").unwrap().as_str(),
        Some("protocol_error")
    );
    drop(raw);
    s.wait_closed().await;
}

// ----------------------------------------------------------------------------- shutdown

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_shutdown_leaves_no_task_and_no_pipe() {
    let mut h = Harness::start(test_handler(Arc::new(Notify::new())), 5000);
    let mut pairs = Vec::new();
    for _ in 0..3 {
        let c = h.client().await;
        let s = h.next().await.unwrap();
        pairs.push((c, s));
    }
    // Requests in flight on both ends when the server closes.
    let mut pend = Vec::new();
    for (c, _) in &pairs {
        pend.push(
            c.call_async("wait", Value::Nil, CallOptions::new().no_timeout())
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    for (_, s) in &pairs {
        s.shutdown("shutting_down", "stopping").await;
    }
    for p in pend {
        assert!(matches!(p.await, Err(Error::ConnectionLost { .. })));
    }
    for (c, _) in &pairs {
        let info = tokio::time::timeout(Duration::from_secs(3), c.wait_closed())
            .await
            .expect("the client closes after the bye");
        assert_eq!(info.code, "shutting_down");
    }
    h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_racing_deadlines_and_cancels_complete_once_and_leave_nothing() {
    let (a, b) = tokio::io::duplex(1024 * 1024);
    let k = key();
    let server = tokio::spawn(accept_io(
        a,
        None,
        server_cfg(5000),
        k,
        test_handler(Arc::new(Notify::new())),
        fast(),
    ));
    let c = connect_io(b, client_cfg(), k, Arc::new(NoHandler), fast())
        .await
        .unwrap();
    let s = server.await.unwrap().unwrap();
    let outcomes = Arc::new([
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ]);
    let mut set = JoinSet::new();
    for i in 0..600u64 {
        let (c, outcomes) = (c.clone(), outcomes.clone());
        set.spawn(async move {
            let opts = CallOptions::new().timeout(Duration::from_millis(1 + i % 4));
            let pending = c
                .call_async("slow", Value::from(i % 6), opts)
                .await
                .unwrap();
            let handle = pending.handle();
            if i % 3 == 0 {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(i % 5)).await;
                    handle.cancel();
                });
            }
            match pending.await {
                Ok(_) => outcomes[0].fetch_add(1, Ordering::Relaxed),
                Err(Error::CallTimeout) => outcomes[1].fetch_add(1, Ordering::Relaxed),
                Err(Error::CallCancelled) => outcomes[2].fetch_add(1, Ordering::Relaxed),
                Err(e) => panic!("unexpected {e:?}"),
            };
        });
    }
    while let Some(r) = set.join_next().await {
        r.unwrap();
    }
    let total: usize = outcomes.iter().map(|o| o.load(Ordering::Relaxed)).sum();
    assert_eq!(total, 600, "every call completed once");
    assert_eq!(c.pending_calls(), 0);
    // The connection is still good.
    assert_eq!(
        c.call("echo", int(3), CallOptions::new()).await.unwrap(),
        int(3)
    );
    c.shutdown("normal", "done").await;
    s.wait_closed().await;
}

/// The pipe's name goes away once the last handle is closed; the runtime reaps a dropped
/// pipe's cancelled operations a moment after the drop.
async fn wait_gone(path: &str) {
    for _ in 0..200 {
        if !pipe_exists(path) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the pipe {path} still exists");
}
