//! Client checks (C01-C11): serve a test endpoint and check what a client does (SPEC 17).
//!
//! The test endpoint declares:
//!
//! * methods `check.echo` (v1: returns its params) and `check.fail` (v1: error `check_failure`);
//! * channels `check.events` (event, about 10 per second), `check.state` (state, changes about
//!   5 per second) and `check.stream` (stream, 100 Hz, layout `t_ms u32, x f32`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use serde_json::json;
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::decl::{ChannelDecl, ChannelKind, Declarations, MethodDecl};
use stintmate_ipc::handshake::PeerInfo;
use stintmate_ipc::keyfile::Key;
use stintmate_ipc::layout::{FieldType, Layout, Scalar};
use stintmate_ipc::transport::PipeListener;
use stintmate_ipc::{naming, wire};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::raw::{
    CResult, CheckError, RawConn, Recv, err_code, msg_type, now_ns, tool_peer, vget, vis, vstr,
    vu64,
};
use crate::report::{Report, Status};
use crate::server::value_json;

fn sample_layout() -> Layout {
    Layout::new(
        [("t_ms", FieldType::U32, 1), ("x", FieldType::F32, 1)],
        Some("check_sample"),
    )
    .expect("the check layout is valid")
}

fn test_decl() -> Declarations {
    let mut d = Declarations::default();
    d.methods.insert(
        "check.echo".into(),
        MethodDecl {
            doc: Some("returns its params".into()),
            ..MethodDecl::default()
        },
    );
    d.methods.insert(
        "check.fail".into(),
        MethodDecl {
            doc: Some("always fails with check_failure".into()),
            ..MethodDecl::default()
        },
    );
    d.channels
        .insert("check.events".into(), ChannelDecl::new(ChannelKind::Event));
    d.channels
        .insert("check.state".into(), ChannelDecl::new(ChannelKind::State));
    let mut s = ChannelDecl::new(ChannelKind::Stream);
    s.rate_hz = Some(100.0);
    s.layout = Some(sample_layout());
    d.channels.insert("check.stream".into(), s);
    d
}

fn peer() -> PeerInfo {
    tool_peer("stintmate-ipc-check (test endpoint)")
}

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s.max(0.0))
}

fn err_res(rid: u64, code: &str, msg: &str) -> Value {
    codec::map([
        ("t", Value::from("res")),
        ("id", Value::from(rid)),
        ("ok", Value::Boolean(false)),
        (
            "e",
            codec::map([("code", Value::from(code)), ("msg", Value::from(msg))]),
        ),
    ])
}

/// Checks a client: serves the test endpoint on `pipe` and watches what the client does.
pub struct ClientChecker {
    path: String,
    short: String,
    key: Key,
    timeout: f64,
    serve_s: f64,
    reconnect_s: f64,
    listen_s: f64,
    /// The report being built.
    pub report: Report,
    state: Served,
}

impl ClientChecker {
    /// A checker serving the endpoint `pipe` (path or short name). `timeout`: seconds to wait for
    /// the client; `serve_s`: seconds to serve it after the checks; `reconnect_s`: seconds to wait
    /// for it to reconnect.
    pub fn new(
        pipe: &str,
        key: Key,
        timeout: f64,
        serve_s: f64,
        reconnect_s: f64,
    ) -> CResult<Self> {
        let (path, short) = naming::split_pipe(pipe).map_err(|e| CheckError::Io(e.to_string()))?;
        Ok(Self {
            report: Report::new("client", &path),
            path,
            short,
            key,
            timeout,
            serve_s,
            reconnect_s,
            listen_s: 2.0,
            state: Served::new(),
        })
    }

    fn pass(&mut self, cid: &str, name: &str, spec: &str, detail: &str) {
        self.report.add(cid, name, Status::Pass, detail, spec);
    }

    fn fail(&mut self, cid: &str, name: &str, spec: &str, detail: &str) {
        self.report.add(cid, name, Status::Fail, detail, spec);
    }

    /// Serves the endpoint, runs every check and returns the report.
    pub async fn run(mut self) -> Report {
        let mut listener = match PipeListener::bind(&self.path) {
            Ok(l) => l,
            Err(e) => {
                self.fail(
                    "C01",
                    "the client connects",
                    "3.3",
                    &format!("cannot serve: {e}"),
                );
                return self.report;
            }
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accept = tokio::spawn(async move {
            while let Ok(a) = listener.accept().await {
                if tx.send(a).is_err() {
                    break;
                }
            }
        });
        self.report.fact("endpoint", json!(self.path));
        match timeout(secs(self.timeout), rx.recv()).await {
            Ok(Some(a)) => {
                self.pass(
                    "C01",
                    "the client connects",
                    "3.3",
                    &format!("pid {:?}", a.client_pid),
                );
                self.report
                    .fact("client pid (from the OS)", json!(a.client_pid));
                let mut c = RawConn::new(a.pipe, &self.short, a.client_pid);
                let again = match self.session(&mut c).await {
                    Ok(b) => b,
                    Err(e) => {
                        self.fail("C00", "the session with the client", "", &e.to_string());
                        false
                    }
                };
                drop(c);
                if again {
                    self.reconnect(&mut rx).await;
                }
            }
            _ => {
                let detail = format!("no connection within {} s", self.timeout);
                self.fail("C01", "the client connects", "3.3", &detail);
            }
        }
        accept.abort();
        self.report
    }

    // ---------------------------------------------------------------- session 1

    async fn session(&mut self, c: &mut RawConn) -> CResult<bool> {
        const NAME: &str = "auth: version, nonce, MAC, identity";
        let decl = test_decl();
        let auth = match c
            .server_handshake(&self.key, &peer(), &decl, false, 5000)
            .await
        {
            Ok(a) => a,
            Err(CheckError::Unauthorized) => {
                self.fail(
                    "C02",
                    NAME,
                    "8.3, 8.5",
                    "wrong MAC (does the client read the same key file?)",
                );
                return Ok(false);
            }
            Err(
                e @ (CheckError::Timeout(_)
                | CheckError::Closed(_)
                | CheckError::Protocol(_)
                | CheckError::Io(_)),
            ) => {
                self.fail("C02", NAME, "8.3", &e.to_string());
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        let mut problems = Vec::new();
        if vu64(&auth, "v") != Some(2) {
            problems.push(format!("v {:?}", vget(&auth, "v")));
        }
        let peer_v = vget(&auth, "peer");
        let app_ok = peer_v
            .and_then(|p| vstr(p, "app"))
            .is_some_and(naming::valid_app);
        if !app_ok {
            problems.push(format!("peer.app {peer_v:?} invalid"));
        }
        let mf = vu64(&auth, "max_frame");
        if !mf.is_some_and(|v| v >= u64::from(wire::MIN_MAX_FRAME)) {
            problems.push(format!("max_frame {mf:?}"));
        }
        let client_decl = match Declarations::from_value(vget(&auth, "decl").unwrap_or(&Value::Nil))
        {
            Ok(d) => d,
            Err(e) => {
                problems.push(format!("decl: {e}"));
                Declarations::default()
            }
        };
        if !problems.is_empty() {
            self.fail("C02", NAME, "8.3", &problems.join("; "));
            return Ok(false);
        }
        let app = peer_v.and_then(|p| vstr(p, "app")).unwrap_or("");
        self.pass(
            "C02",
            NAME,
            "8.3, 8.5",
            &format!(
                "app {app:?}, {} methods, {} channels",
                client_decl.methods.len(),
                client_decl.channels.len()
            ),
        );
        self.report
            .fact("client", peer_v.map(value_json).unwrap_or_default());
        self.check_ping(c).await?;
        self.check_unknown(c).await?;
        self.check_unknown_method(c).await?;
        self.check_chunked(c).await?;
        for (name, ch) in &client_decl.channels {
            self.check_client_channel(c, name, ch).await?;
        }
        self.serve(c).await?;
        c.send(&codec::map([
            ("t", Value::from("bye")),
            ("code", Value::from("shutting_down")),
            ("msg", Value::from("check: reconnect test")),
        ]))
        .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(true)
    }

    /// Serves the client until `until`; returns the first message `want` accepts.
    async fn pump(
        &mut self,
        c: &mut RawConn,
        until: Instant,
        want: impl Fn(&Value) -> bool,
    ) -> CResult<Option<Value>> {
        loop {
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.state.tick(c).await;
            match c.recv(left.min(Duration::from_millis(10))).await {
                Err(CheckError::Timeout(_)) => continue,
                Err(e) => return Err(e),
                Ok(Recv::Stream(s)) => self.state.on_stream(&s),
                Ok(Recv::Msg(m)) => {
                    if want(&m) {
                        return Ok(Some(m));
                    }
                    self.state.on_message(c, &m).await;
                }
            }
        }
    }

    async fn check_ping(&mut self, c: &mut RawConn) -> CResult<()> {
        const NAME: &str = "ping is answered";
        let ts = now_ns();
        c.send(&codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(ts)),
        ]))
        .await?;
        let m = self
            .pump(c, Instant::now() + Duration::from_secs(3), |m| {
                msg_type(m) == "pong"
            })
            .await?;
        match m {
            None => self.fail("C03", NAME, "10.10", "no pong within 3 s"),
            Some(m) if vget(&m, "ts").and_then(Value::as_i64) != Some(ts) => {
                self.fail("C03", NAME, "10.10", "pong.ts differs");
            }
            Some(_) => self.pass("C03", NAME, "10.10", ""),
        }
        Ok(())
    }

    async fn check_unknown(&mut self, c: &mut RawConn) -> CResult<()> {
        const NAME: &str = "unknown message types and keys are ignored";
        c.send(&codec::map([
            ("t", Value::from("x_check_unknown")),
            ("zz", Value::from(1)),
        ]))
        .await?;
        c.send(&codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(99)),
            ("zz_unknown", Value::Array(vec![Value::from(1)])),
        ]))
        .await?;
        let m = self
            .pump(c, Instant::now() + Duration::from_secs(3), |m| {
                matches!(msg_type(m), "pong" | "bye")
            })
            .await?;
        match m {
            Some(m) if msg_type(&m) == "pong" => self.pass("C04", NAME, "6.3, 10.1", ""),
            other => self.fail("C04", NAME, "6.3, 10.1", &format!("got {other:?}")),
        }
        Ok(())
    }

    async fn check_unknown_method(&mut self, c: &mut RawConn) -> CResult<()> {
        const NAME: &str = "unknown method -> unknown_method";
        let rid = 7_000_001u64;
        c.send(&codec::map([
            ("t", Value::from("req")),
            ("id", Value::from(rid)),
            ("m", Value::from("ipc_check.no_such_method")),
            ("v", Value::from(1)),
            ("p", codec::map::<_, &str>([])),
        ]))
        .await?;
        let m = self
            .pump(c, Instant::now() + Duration::from_secs(3), |m| {
                msg_type(m) == "res" && vu64(m, "id") == Some(rid)
            })
            .await?;
        match m {
            None => self.fail("C05", NAME, "10.2", "no res in 3 s"),
            Some(m) if !vis(&m, "ok", false) || err_code(&m) != Some("unknown_method") => {
                self.fail(
                    "C05",
                    NAME,
                    "10.2",
                    &format!("ok={:?} code={:?}", vget(&m, "ok"), err_code(&m)),
                );
            }
            Some(_) => self.pass("C05", NAME, "10.2", ""),
        }
        Ok(())
    }

    async fn check_chunked(&mut self, c: &mut RawConn) -> CResult<()> {
        const NAME: &str = "a chunked message is reassembled";
        let msg = codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(4242)),
            ("pad", Value::from("x".repeat(100))),
        ]);
        c.send_chunked(&msg, 16).await?;
        let m = self
            .pump(c, Instant::now() + Duration::from_secs(3), |m| {
                msg_type(m) == "pong"
            })
            .await?;
        if m.is_some_and(|m| vu64(&m, "ts") == Some(4242)) {
            self.pass("C06", NAME, "5.4", "");
        } else {
            self.fail("C06", NAME, "5.4", "no pong");
        }
        Ok(())
    }

    async fn check_client_channel(
        &mut self,
        c: &mut RawConn,
        name: &str,
        ch: &ChannelDecl,
    ) -> CResult<()> {
        let cid = format!("C07.{name}");
        let title = format!("the client's {} channel {name}", ch.kind.name());
        let rid = 7_100_000 + self.report.results.len() as u64;
        let version = ch.versions.iter().max().copied().unwrap_or(1);
        c.send(&codec::map([
            ("t", Value::from("sub")),
            ("id", Value::from(rid)),
            ("ch", Value::from(name)),
            ("v", Value::from(version)),
        ]))
        .await?;
        let res = self
            .pump(c, Instant::now() + Duration::from_secs(3), |m| {
                msg_type(m) == "res" && vu64(m, "id") == Some(rid)
            })
            .await?;
        let Some(res) = res else {
            self.fail(&cid, &title, "10.6", "no res within 3 s");
            return Ok(());
        };
        if !vis(&res, "ok", true) {
            let e = vget(&res, "e").map(ToString::to_string).unwrap_or_default();
            self.report
                .add(&cid, &title, Status::Warn, &format!("refused: {e}"), "10.6");
            return Ok(());
        }
        let r = vget(&res, "r").cloned().unwrap_or(Value::Nil);
        let sid = vu64(&r, "sid").and_then(|s| u32::try_from(s).ok());
        let Some(sid) = sid.filter(|_| vstr(&r, "kind") == Some(ch.kind.name())) else {
            self.fail(&cid, &title, "10.6", &format!("bad res.r {r}"));
            return Ok(());
        };
        if ch.kind == ChannelKind::Stream {
            match Layout::from_value(vget(&r, "layout").unwrap_or(&Value::Nil)) {
                Ok(l) => {
                    self.state.stream_watch.insert(sid, l);
                }
                Err(e) => {
                    self.fail(&cid, &title, "9.3", &format!("layout: {e}"));
                    return Ok(());
                }
            }
        } else {
            self.state.event_watch.insert(sid);
        }
        self.pump(c, Instant::now() + secs(self.listen_s), |_| false)
            .await?;
        c.send(&codec::map([
            ("t", Value::from("unsub")),
            ("sid", Value::from(sid)),
        ]))
        .await?;
        let err = self
            .state
            .stream_errors
            .get(&sid)
            .or_else(|| self.state.event_errors.get(&sid))
            .cloned();
        let n = self.state.counts.get(&sid).copied().unwrap_or(0);
        match err {
            Some(e) => self.fail(&cid, &title, "10.8, 10.9", &e),
            None => self.pass(
                &cid,
                &title,
                "10.6-10.9",
                &format!("sid {sid}, {n} items in {} s", self.listen_s),
            ),
        }
        Ok(())
    }

    async fn serve(&mut self, c: &mut RawConn) -> CResult<()> {
        self.pump(c, Instant::now() + secs(self.serve_s), |_| false)
            .await?;
        let calls = self.state.calls.clone();
        let subs: Vec<String> = self.state.subs_seen.iter().cloned().collect();
        self.report.fact("client requests", json!(calls));
        self.report.fact("client subscriptions", json!(subs));
        let total: usize = calls.values().sum();
        if self.state.errors.is_empty() {
            self.pass(
                "C08",
                "the client's messages are well formed",
                "10",
                &format!("{total} requests, {} subscriptions", subs.len()),
            );
        } else {
            let detail = self
                .state
                .errors
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join("; ");
            self.fail(
                "C08",
                "the client's messages are well formed",
                "10",
                &detail,
            );
        }
        if calls.is_empty() && subs.is_empty() {
            self.report.add(
                "C09",
                "the client uses the test endpoint",
                Status::Info,
                "it called nothing and subscribed to nothing",
                "",
            );
        } else {
            self.pass(
                "C09",
                "the client uses the test endpoint",
                "",
                &format!("calls {calls:?}, subscriptions {subs:?}"),
            );
        }
        Ok(())
    }

    // ---------------------------------------------------------------- session 2

    async fn reconnect(
        &mut self,
        rx: &mut mpsc::UnboundedReceiver<stintmate_ipc::transport::Accepted>,
    ) {
        const C10: &str = "the client reconnects after bye shutting_down";
        const C11: &str = "a wrong server MAC makes the client close";
        let Ok(Some(a)) = timeout(secs(self.reconnect_s), rx.recv()).await else {
            self.report.add(
                "C10",
                C10,
                Status::Warn,
                &format!("no reconnection within {} s", self.reconnect_s),
                "15.3",
            );
            self.report
                .add("C11", C11, Status::Skip, "needs a reconnection", "8.6");
            return;
        };
        self.pass("C10", C10, "15.3", "");
        let mut c = RawConn::new(a.pipe, &self.short, a.client_pid);
        if let Err(e) = c
            .server_handshake(&self.key, &peer(), &test_decl(), true, 5000)
            .await
        {
            self.fail("C11", C11, "8.6", &format!("handshake: {e}"));
            return;
        }
        let t0 = Instant::now();
        let seen = match c.wait_closed(Duration::from_secs(3)).await {
            Ok(s) => s,
            Err(CheckError::Timeout(_)) => {
                self.fail(
                    "C11",
                    C11,
                    "8.6",
                    "still connected 3 s after a wrong welcome MAC",
                );
                return;
            }
            Err(e) => {
                self.fail("C11", C11, "8.6", &e.to_string());
                return;
            }
        };
        let dt = t0.elapsed().as_secs_f64();
        let other: Vec<&str> = seen.iter().map(msg_type).filter(|t| *t != "bye").collect();
        if dt >= 2.9 {
            self.fail(
                "C11",
                C11,
                "8.6",
                "still connected 3 s after a wrong welcome MAC",
            );
        } else if !other.is_empty() {
            self.fail(
                "C11",
                C11,
                "8.6",
                &format!("sent {other:?} after a wrong MAC"),
            );
        } else {
            self.pass(
                "C11",
                C11,
                "8.6",
                &format!("closed after {:.0} ms", 1000.0 * dt),
            );
        }
    }
}

/// The test endpoint's behaviour during a session: answers, publishes, validates.
struct Served {
    calls: BTreeMap<String, usize>,
    subs_seen: BTreeSet<String>,
    errors: Vec<String>,
    /// sid -> channel (the client subscribed).
    my_subs: BTreeMap<u32, String>,
    next_sid: u32,
    seq: HashMap<u32, u32>,
    state_n: u64,
    t0: Instant,
    next_evt: Instant,
    next_state: Instant,
    next_sample: Instant,
    stream_watch: HashMap<u32, Layout>,
    event_watch: BTreeSet<u32>,
    stream_errors: HashMap<u32, String>,
    event_errors: HashMap<u32, String>,
    counts: HashMap<u32, usize>,
    last: HashMap<u32, (u64, i64)>,
}

impl Served {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            calls: BTreeMap::new(),
            subs_seen: BTreeSet::new(),
            errors: Vec::new(),
            my_subs: BTreeMap::new(),
            next_sid: 1,
            seq: HashMap::new(),
            state_n: 0,
            t0: now,
            next_evt: now,
            next_state: now,
            next_sample: now,
            stream_watch: HashMap::new(),
            event_watch: BTreeSet::new(),
            stream_errors: HashMap::new(),
            event_errors: HashMap::new(),
            counts: HashMap::new(),
            last: HashMap::new(),
        }
    }

    fn on_stream(&mut self, s: &wire::StreamSample) {
        let Some(lay) = self.stream_watch.get(&s.sid) else {
            return;
        };
        *self.counts.entry(s.sid).or_default() += 1;
        if s.body.len() != lay.size() {
            self.stream_errors.insert(
                s.sid,
                format!("sample of {} bytes, layout {}", s.body.len(), lay.size()),
            );
        }
        if let Some(&(pseq, pt)) = self.last.get(&s.sid) {
            if (u64::from(s.seq).wrapping_sub(pseq) & 0xFFFF_FFFF) < 1 {
                self.stream_errors
                    .insert(s.sid, format!("seq not increasing {pseq} -> {}", s.seq));
            }
            if s.t_ns < pt {
                self.stream_errors
                    .insert(s.sid, "t_ns went backwards".to_owned());
            }
        }
        self.last.insert(s.sid, (u64::from(s.seq), s.t_ns));
    }

    async fn send(c: &mut RawConn, m: Value) {
        let _ = c.send(&m).await;
    }

    async fn on_message(&mut self, c: &mut RawConn, m: &Value) {
        match msg_type(m) {
            "ping" => {
                let mut pairs = vec![("t", Value::from("pong"))];
                if let Some(ts) = vget(m, "ts") {
                    pairs.push(("ts", ts.clone()));
                }
                Self::send(c, codec::map(pairs)).await;
            }
            "evt" => {
                let Some(sid) = vu64(m, "sid").and_then(|s| u32::try_from(s).ok()) else {
                    return;
                };
                if self.event_watch.contains(&sid) {
                    *self.counts.entry(sid).or_default() += 1;
                    let seq = vu64(m, "seq").unwrap_or(0);
                    if self.last.get(&sid).is_some_and(|&(p, _)| seq <= p) {
                        self.event_errors
                            .insert(sid, "seq not increasing".to_owned());
                    }
                    self.last.insert(sid, (seq, 0));
                }
            }
            "req" => self.on_req(c, m).await,
            "sub" => self.on_sub(c, m).await,
            "unsub" => match vu64(m, "sid").and_then(|s| u32::try_from(s).ok()) {
                Some(sid) => {
                    self.my_subs.remove(&sid);
                }
                None => self.errors.push(truncate(format!("malformed unsub {m}"))),
            },
            _ => {}
        }
    }

    async fn on_req(&mut self, c: &mut RawConn, m: &Value) {
        let rid = vu64(m, "id");
        let meth = vstr(m, "m");
        let v = match vget(m, "v") {
            None => Some(1),
            Some(x) => x.as_u64(),
        };
        let (Some(rid), Some(meth), Some(v)) = (rid, meth, v) else {
            self.errors.push(truncate(format!("malformed req {m}")));
            return;
        };
        *self.calls.entry(meth.to_owned()).or_default() += 1;
        let reply = match (meth, v) {
            ("check.echo", 1) => codec::map([
                ("t", Value::from("res")),
                ("id", Value::from(rid)),
                ("ok", Value::Boolean(true)),
                ("r", vget(m, "p").cloned().unwrap_or(Value::Nil)),
            ]),
            ("check.fail", 1) => err_res(rid, "check_failure", "as requested"),
            ("check.echo" | "check.fail", _) => codec::map([
                ("t", Value::from("res")),
                ("id", Value::from(rid)),
                ("ok", Value::Boolean(false)),
                (
                    "e",
                    codec::map([
                        ("code", Value::from("version_unsupported")),
                        ("msg", Value::from("v1 only")),
                        (
                            "details",
                            codec::map([("versions", Value::Array(vec![Value::from(1)]))]),
                        ),
                    ]),
                ),
            ]),
            _ => err_res(rid, "unknown_method", &format!("unknown method {meth}")),
        };
        Self::send(c, reply).await;
    }

    async fn on_sub(&mut self, c: &mut RawConn, m: &Value) {
        let rid = vu64(m, "id");
        let ch = vstr(m, "ch");
        let v = match vget(m, "v") {
            None => Some(1),
            Some(x) => x.as_u64(),
        };
        let (Some(rid), Some(ch), Some(v)) = (rid, ch, v) else {
            self.errors.push(truncate(format!("malformed sub {m}")));
            return;
        };
        self.subs_seen.insert(ch.to_owned());
        let decl = test_decl();
        let Some(d) = decl.channels.get(ch) else {
            Self::send(
                c,
                err_res(rid, "unknown_channel", &format!("unknown channel {ch}")),
            )
            .await;
            return;
        };
        if v != 1 {
            Self::send(c, err_res(rid, "version_unsupported", "v1 only")).await;
            return;
        }
        let sid = match self.my_subs.iter().find(|(_, n)| n.as_str() == ch) {
            Some((sid, _)) => *sid,
            None => {
                let sid = self.next_sid;
                self.next_sid += 1;
                self.my_subs.insert(sid, ch.to_owned());
                self.seq.insert(sid, 0);
                sid
            }
        };
        let mut r = vec![
            ("sid", Value::from(sid)),
            ("kind", Value::from(d.kind.name())),
            ("seq", Value::from(self.seq[&sid])),
        ];
        if d.kind == ChannelKind::State {
            r.push(("snap", codec::map([("n", Value::from(self.state_n))])));
            r.push(("ts", Value::from(now_ns())));
        }
        if d.kind == ChannelKind::Stream {
            r.push(("layout", sample_layout().to_value()));
        }
        Self::send(
            c,
            codec::map([
                ("t", Value::from("res")),
                ("id", Value::from(rid)),
                ("ok", Value::Boolean(true)),
                ("r", codec::map(r)),
            ]),
        )
        .await;
    }

    async fn tick(&mut self, c: &mut RawConn) {
        let now = Instant::now();
        if now >= self.next_evt {
            self.next_evt = now + Duration::from_millis(100);
            for sid in self.subs_of("check.events") {
                let seq = self.bump(sid);
                Self::send(c, evt(sid, seq, Value::from(seq))).await;
            }
        }
        if now >= self.next_state {
            self.next_state = now + Duration::from_millis(200);
            self.state_n += 1;
            for sid in self.subs_of("check.state") {
                let seq = self.bump(sid);
                Self::send(c, evt(sid, seq, Value::from(self.state_n))).await;
            }
        }
        if now >= self.next_sample {
            self.next_sample = now + Duration::from_millis(10);
            for sid in self.subs_of("check.stream") {
                let seq = self.bump(sid);
                let ms = u32::try_from((now - self.t0).as_millis()).unwrap_or(u32::MAX);
                let mut body = Vec::new();
                sample_layout()
                    .pack(&[Scalar::U32(ms), Scalar::F32(1.5)], &mut body)
                    .expect("the sample matches the layout");
                let mut f = Vec::new();
                if wire::write_stream_frame(&mut f, sid, seq, now_ns(), 0, &body).is_ok() {
                    let _ = c.send_frame(&f).await;
                }
            }
        }
    }

    fn subs_of(&self, channel: &str) -> Vec<u32> {
        self.my_subs
            .iter()
            .filter(|(_, n)| n.as_str() == channel)
            .map(|(s, _)| *s)
            .collect()
    }

    fn bump(&mut self, sid: u32) -> u32 {
        let s = self.seq.entry(sid).or_default();
        *s += 1;
        *s
    }
}

fn evt(sid: u32, seq: u32, n: Value) -> Value {
    codec::map([
        ("t", Value::from("evt")),
        ("sid", Value::from(sid)),
        ("seq", Value::from(seq)),
        ("ts", Value::from(now_ns())),
        ("d", codec::map([("n", n)])),
    ])
}

fn truncate(s: String) -> String {
    s.chars().take(200).collect()
}
