//! Server checks (S00-S24): connect to an endpoint and test what it does (SPEC 17).

use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};
use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::decl::{ChannelDecl, ChannelKind, Declarations};
use stintmate_ipc::handshake::HANDSHAKE_TIMEOUT;
use stintmate_ipc::keyfile::Key;
use stintmate_ipc::layout::Layout;
use stintmate_ipc::wire;

use crate::raw::{
    CResult, CheckError, RawConn, Recv, err_code, msg_type, now_ns, vget, vis, vstr, vu64,
};
use crate::report::{Report, Status};

const UNKNOWN_METHOD: &str = "ipc_check.no_such_method";
const UNKNOWN_CHANNEL: &str = "ipc_check.no_such_channel";

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s.max(0.0))
}

fn expect(cond: bool, message: impl FnOnce() -> String) -> CResult<()> {
    if cond {
        Ok(())
    } else {
        Err(CheckError::Fail(message()))
    }
}

fn fail<T>(message: impl Into<String>) -> CResult<T> {
    Err(CheckError::Fail(message.into()))
}

/// A MessagePack value as JSON (binary as hex), for the facts.
pub fn value_json(v: &Value) -> Json {
    match v {
        Value::Nil => Json::Null,
        Value::Boolean(b) => json!(b),
        Value::Integer(i) => i
            .as_i64()
            .map(|i| json!(i))
            .or_else(|| i.as_u64().map(|u| json!(u)))
            .unwrap_or(Json::Null),
        Value::F32(f) => json!(f),
        Value::F64(f) => json!(f),
        Value::String(s) => json!(s.as_str().unwrap_or("")),
        Value::Binary(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            json!(hex)
        }
        Value::Array(a) => Json::Array(a.iter().map(value_json).collect()),
        Value::Map(m) => Json::Object(
            m.iter()
                .map(|(k, v)| {
                    (
                        k.as_str().map_or_else(|| k.to_string(), str::to_owned),
                        value_json(v),
                    )
                })
                .collect(),
        ),
        Value::Ext(..) => Json::Null,
    }
}

/// Checks a server (an endpoint).
pub struct ServerChecker {
    pipe: String,
    key: Key,
    listen_s: f64,
    slow: bool,
    only_channels: Option<Vec<String>>,
    calls: Vec<(String, Value)>,
    /// The report being built.
    pub report: Report,
    decl: Option<Declarations>,
    heartbeat_ms: u32,
    next_id: u64,
}

macro_rules! check {
    ($s:ident, $id:expr, $name:expr, $spec:expr, $call:expr) => {{
        let r = $call.await;
        $s.record(&$id, &$name, $spec, r);
    }};
}

impl ServerChecker {
    /// A checker for the endpoint `pipe` (path or short name). `listen_s`: seconds to listen to
    /// each channel; `slow`: also run the checks that wait for timeouts.
    pub fn new(
        pipe: &str,
        key: Key,
        listen_s: f64,
        slow: bool,
        channels: Option<Vec<String>>,
        calls: Vec<(String, Value)>,
    ) -> Self {
        Self {
            pipe: pipe.to_owned(),
            key,
            listen_s,
            slow,
            only_channels: channels,
            calls,
            report: Report::new("server", pipe),
            decl: None,
            heartbeat_ms: 5000,
            next_id: 1,
        }
    }

    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    async fn open(&self) -> CResult<RawConn> {
        RawConn::open(&self.pipe, Duration::from_secs(5)).await
    }

    async fn session(&self) -> CResult<RawConn> {
        let mut c = self.open().await?;
        let reply = c.client_handshake(&self.key, false, 2).await?;
        expect(msg_type(&reply) == "welcome", || {
            format!("no welcome: {reply}")
        })?;
        Ok(c)
    }

    fn record(&mut self, cid: &str, name: &str, spec: &str, r: CResult<String>) {
        let (status, detail) = match r {
            Ok(d) => {
                if let Some(x) = d.strip_prefix("SKIP:") {
                    (Status::Skip, x.trim().to_owned())
                } else if let Some(x) = d.strip_prefix("WARN:") {
                    (Status::Warn, x.trim().to_owned())
                } else if let Some(x) = d.strip_prefix("INFO:") {
                    (Status::Info, x.trim().to_owned())
                } else {
                    (Status::Pass, d)
                }
            }
            Err(e) => (Status::Fail, e.to_string()),
        };
        self.report.add(cid, name, status, &detail, spec);
    }

    async fn req(
        &mut self,
        c: &mut RawConn,
        method: &str,
        params: Option<Value>,
        v: u64,
        chunked: bool,
        rid: Option<u64>,
    ) -> CResult<Value> {
        let rid = rid.unwrap_or_else(|| self.id());
        let msg = codec::map([
            ("t", Value::from("req")),
            ("id", Value::from(rid)),
            ("m", Value::from(method)),
            ("v", Value::from(v)),
            ("p", params.unwrap_or_else(|| codec::map::<_, &str>([]))),
            ("dl", Value::from(5000)),
        ]);
        if chunked {
            c.send_chunked(&msg, 16).await?;
        } else {
            c.send(&msg).await?;
        }
        loop {
            let m = c
                .recv_msg(
                    Duration::from_secs(5),
                    &["ping", "pong", "decl", "evt", "end"],
                )
                .await?;
            if msg_type(&m) == "res" && vu64(&m, "id") == Some(rid) {
                return Ok(m);
            }
        }
    }

    async fn closes_with(
        &self,
        c: &mut RawConn,
        code: Option<&str>,
        wait: Duration,
    ) -> CResult<String> {
        let seen = c.wait_closed(wait).await?;
        let byes: Vec<&Value> = seen.iter().filter(|m| msg_type(m) == "bye").collect();
        if let (Some(code), Some(last)) = (code, byes.last())
            && vstr(last, "code") != Some(code)
        {
            return fail(format!(
                "closed with bye {:?}, expected {code:?}",
                vstr(last, "code").unwrap_or("")
            ));
        }
        Ok(match byes.last() {
            Some(b) => format!("closed with bye {}", vstr(b, "code").unwrap_or("")),
            None => "closed (no bye)".to_owned(),
        })
    }

    fn channels(&self) -> Vec<(String, ChannelDecl)> {
        let Some(d) = &self.decl else {
            return Vec::new();
        };
        d.channels
            .iter()
            .filter(|(n, _)| {
                self.only_channels
                    .as_ref()
                    .is_none_or(|only| only.iter().any(|o| o == *n))
            })
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect()
    }

    /// Runs every check and returns the report.
    pub async fn run(mut self) -> Report {
        let s = &mut self;
        check!(s, "S01", "hello: first frame, fields", "8.2", s.c_hello());
        check!(
            s,
            "S02",
            "handshake: welcome, server MAC, declarations",
            "8.4, 8.5, 9",
            s.c_welcome()
        );
        if s.decl.is_none() {
            s.report.add(
                "S00",
                "remaining checks",
                Status::Skip,
                "no successful handshake",
                "",
            );
            return self.report;
        }
        check!(
            s,
            "S03",
            "wrong client MAC is refused (bye unauthorized)",
            "8.6",
            s.c_bad_mac()
        );
        check!(
            s,
            "S04",
            "unsupported protocol version is refused",
            "8.7",
            s.c_bad_version()
        );
        check!(s, "S05", "malformed auth is refused", "8.6", s.c_bad_auth());
        if s.slow {
            check!(
                s,
                "S06",
                "handshake deadline: silent client is closed",
                "15.1",
                s.c_silent()
            );
        }
        check!(
            s,
            "S07",
            "ping is answered with the same ts",
            "10.10",
            s.c_ping()
        );
        check!(
            s,
            "S08",
            "unknown message type and unknown keys are ignored",
            "6.3, 10.1",
            s.c_unknown()
        );
        check!(
            s,
            "S09",
            "unknown method -> unknown_method",
            "10.2, 11",
            s.c_unknown_method()
        );
        check!(
            s,
            "S10",
            "declared method, unsupported version -> version_unsupported",
            "7.2",
            s.c_method_version()
        );
        check!(
            s,
            "S11",
            "unknown channel -> unknown_channel",
            "10.6, 11",
            s.c_unknown_channel()
        );
        check!(
            s,
            "S12",
            "chunked request is reassembled",
            "5.4",
            s.c_chunked()
        );
        check!(
            s,
            "S13",
            "large request id is echoed exactly",
            "10.2",
            s.c_big_id()
        );
        check!(
            s,
            "S14",
            "reserved frame kind 0x80 is skipped",
            "5.2",
            s.c_reserved_kind()
        );
        check!(
            s,
            "S15",
            "unknown frame kind -> protocol_error",
            "5.5",
            s.c_bad_kind()
        );
        check!(
            s,
            "S16",
            "frame over max_frame -> closed",
            "5.5",
            s.c_oversize()
        );
        check!(
            s,
            "S17",
            "message that is not a map -> protocol_error",
            "6.2",
            s.c_not_map()
        );
        check!(
            s,
            "S18",
            "message frame inside a chunked transfer -> protocol_error",
            "5.4",
            s.c_chunk_interleave()
        );
        for (name, ch) in s.channels() {
            let title = format!("subscription to {} channel {name}", ch.kind.name());
            check!(
                s,
                format!("S19.{name}"),
                title,
                "10.6-10.9, 12",
                s.c_channel(&name, &ch)
            );
        }
        check!(
            s,
            "S20",
            "the same subscription twice -> the same sid",
            "10.6",
            s.c_resub()
        );
        let calls = std::mem::take(&mut s.calls);
        for (i, (m, p)) in calls.iter().enumerate() {
            check!(
                s,
                format!("S21.{}", i + 1),
                format!("call {m}"),
                "10.2",
                s.c_call(m, p)
            );
        }
        if s.slow {
            check!(
                s,
                "S22",
                "idle connection gets a ping within heartbeat_ms",
                "15.2",
                s.c_heartbeat()
            );
        }
        check!(
            s,
            "S24",
            "cancel for an unknown id is ignored",
            "10.3",
            s.c_cancel_unknown()
        );
        check!(s, "S23", "bye closes the connection", "10.12", s.c_bye());
        self.report
    }

    async fn c_hello(&mut self) -> CResult<String> {
        let mut c = self.open().await?;
        let t0 = Instant::now();
        let Recv::Msg(m) = c.recv(Duration::from_secs(5)).await? else {
            return fail("the first frame is not a message (kind 1)");
        };
        expect(msg_type(&m) == "hello", || {
            format!("first message is {:?}, not hello", msg_type(&m))
        })?;
        expect(vstr(&m, "proto") == Some("stintmate-ipc"), || {
            format!("proto is {:?}", vget(&m, "proto"))
        })?;
        let vs = vget(&m, "versions");
        let has2 = matches!(vs, Some(Value::Array(a)) if a.iter().any(|v| v.as_u64() == Some(2)));
        expect(has2, || format!("versions {vs:?} lacks 2"))?;
        let nonce_ok = matches!(vget(&m, "nonce"), Some(Value::Binary(b)) if b.len() == 32);
        expect(nonce_ok, || "nonce is not a 32-byte bin".into())?;
        let mf = vu64(&m, "max_frame");
        expect(
            mf.is_some_and(|v| v >= u64::from(wire::MIN_MAX_FRAME)),
            || format!("max_frame {mf:?} < {}", wire::MIN_MAX_FRAME),
        )?;
        self.report.fact("server max_frame", json!(mf));
        self.report
            .fact("server max_message", json!(vu64(&m, "max_message")));
        let versions = vs.map(value_json).unwrap_or(Json::Null);
        Ok(format!(
            "hello after {:.0} ms, versions {versions}",
            1000.0 * t0.elapsed().as_secs_f64()
        ))
    }

    async fn c_welcome(&mut self) -> CResult<String> {
        let mut c = self.open().await?;
        let reply = c.client_handshake(&self.key, false, 2).await?;
        expect(msg_type(&reply) == "welcome", || {
            format!(
                "got {} {}: wrong key file?",
                msg_type(&reply),
                vstr(&reply, "code").unwrap_or("")
            )
        })?;
        expect(c.server_mac_ok, || "the server's MAC is wrong".into())?;
        expect(vu64(&reply, "v") == Some(2), || {
            format!("welcome.v is {:?}", vget(&reply, "v"))
        })?;
        expect(vstr(&reply, "run").is_some_and(|r| !r.is_empty()), || {
            "no run id".into()
        })?;
        let peer = vget(&reply, "peer");
        expect(peer.and_then(|p| vstr(p, "app")).is_some(), || {
            "welcome.peer.app missing".into()
        })?;
        let hb = vu64(&reply, "heartbeat_ms");
        expect(hb.is_some_and(|h| h >= 100), || {
            format!("heartbeat_ms {hb:?}")
        })?;
        let decl = Declarations::from_value(vget(&reply, "decl").unwrap_or(&Value::Nil))
            .map_err(|e| CheckError::Fail(format!("declarations invalid: {e}")))?;
        self.heartbeat_ms = u32::try_from(hb.unwrap_or(5000)).unwrap_or(5000);
        self.report
            .fact("server", peer.map(value_json).unwrap_or(Json::Null));
        self.report
            .fact("server pid (from the OS)", json!(c.peer_pid));
        let methods: serde_json::Map<String, Json> = decl
            .methods
            .iter()
            .map(|(n, m)| (n.clone(), json!(m.versions)))
            .collect();
        self.report.fact("methods", Json::Object(methods));
        let channels: serde_json::Map<String, Json> = decl
            .channels
            .iter()
            .map(|(n, ch)| (n.clone(), json!([ch.kind.name(), ch.versions])))
            .collect();
        self.report.fact("channels", Json::Object(channels));
        let detail = format!(
            "{} methods, {} channels, heartbeat {} ms",
            decl.methods.len(),
            decl.channels.len(),
            self.heartbeat_ms
        );
        self.decl = Some(decl);
        Ok(detail)
    }

    async fn c_bad_mac(&mut self) -> CResult<String> {
        let mut c = self.open().await?;
        let reply = match c.client_handshake(&self.key, true, 2).await {
            Err(CheckError::Closed(_)) => {
                return Ok("WARN: closed without bye unauthorized".into());
            }
            r => r?,
        };
        expect(msg_type(&reply) == "bye", || {
            format!("got {:?} instead of bye", msg_type(&reply))
        })?;
        expect(vstr(&reply, "code") == Some("unauthorized"), || {
            format!("bye code {:?}", vstr(&reply, "code"))
        })?;
        c.wait_closed(Duration::from_secs(3)).await?;
        Ok("bye unauthorized, closed".into())
    }

    async fn c_bad_version(&mut self) -> CResult<String> {
        let mut c = self.open().await?;
        let reply = match c.client_handshake(&self.key, false, 99).await {
            Err(CheckError::Closed(_)) => {
                return Ok("WARN: closed without bye version_unsupported".into());
            }
            r => r?,
        };
        expect(msg_type(&reply) != "welcome", || {
            "welcomed protocol version 99".into()
        })?;
        expect(vstr(&reply, "code") == Some("version_unsupported"), || {
            format!(
                "bye code {:?}, expected version_unsupported",
                vstr(&reply, "code")
            )
        })?;
        Ok("bye version_unsupported".into())
    }

    async fn c_bad_auth(&mut self) -> CResult<String> {
        let mut c = self.open().await?;
        c.recv_msg(Duration::from_secs(5), &[]).await?;
        c.send(&codec::map([
            ("t", Value::from("auth")),
            ("v", Value::from(2)),
            ("nonce", Value::Binary(b"short".to_vec())),
            ("mac", Value::Binary(b"x".to_vec())),
        ]))
        .await?;
        let seen = c.wait_closed(Duration::from_secs(5)).await?;
        expect(!seen.iter().any(|m| msg_type(m) == "welcome"), || {
            "welcomed a bad auth".into()
        })?;
        let byes: Vec<&str> = seen
            .iter()
            .filter(|m| msg_type(m) == "bye")
            .filter_map(|m| vstr(m, "code"))
            .collect();
        Ok(format!("refused ({})", byes.first().unwrap_or(&"closed")))
    }

    async fn c_silent(&mut self) -> CResult<String> {
        let mut c = self.open().await?;
        c.recv_msg(Duration::from_secs(5), &[]).await?;
        let t0 = Instant::now();
        c.wait_closed(HANDSHAKE_TIMEOUT + Duration::from_secs(4))
            .await?;
        let dt = t0.elapsed().as_secs_f64();
        expect(dt < 8.5, || {
            "still open 8.5 s after hello without auth".into()
        })?;
        Ok(format!("closed after {dt:.1} s"))
    }

    async fn c_ping(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let ts = now_ns();
        c.send(&codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(ts)),
        ]))
        .await?;
        let m = c
            .recv_msg(Duration::from_secs(5), &["ping", "decl"])
            .await?;
        expect(msg_type(&m) == "pong", || format!("got {:?}", msg_type(&m)))?;
        expect(vget(&m, "ts").and_then(Value::as_i64) == Some(ts), || {
            "pong.ts differs from ping.ts".into()
        })?;
        Ok(format!("rtt {} us", (now_ns() - ts) / 1000))
    }

    async fn c_unknown(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        c.send(&codec::map([
            ("t", Value::from("x_check_unknown")),
            ("zz", Value::from(1)),
        ]))
        .await?;
        let ping = codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(12345)),
            (
                "zz_unknown",
                codec::map([("a", Value::Array(vec![Value::from(1), Value::from(2)]))]),
            ),
        ]);
        c.send(&ping).await?;
        self.expect_pong(&mut c, 12345).await
    }

    async fn expect_pong(&self, c: &mut RawConn, ts: u64) -> CResult<String> {
        let m = c
            .recv_msg(Duration::from_secs(5), &["ping", "decl"])
            .await?;
        expect(msg_type(&m) == "pong" && vu64(&m, "ts") == Some(ts), || {
            format!(
                "expected pong, got {:?} {}",
                msg_type(&m),
                vstr(&m, "code").unwrap_or("")
            )
        })?;
        Ok(String::new())
    }

    async fn c_cancel_unknown(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        c.send(&codec::map([
            ("t", Value::from("cancel")),
            ("id", Value::from(424_242)),
        ]))
        .await?;
        c.send(&codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(54321)),
        ]))
        .await?;
        self.expect_pong(&mut c, 54321).await
    }

    async fn c_unknown_method(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let m = self
            .req(&mut c, UNKNOWN_METHOD, None, 1, false, None)
            .await?;
        expect(vis(&m, "ok", false), || "ok is not false".into())?;
        expect(err_code(&m) == Some("unknown_method"), || {
            format!("code {:?}", err_code(&m))
        })?;
        let msg_ok = vget(&m, "e").and_then(|e| vstr(e, "msg")).is_some();
        expect(msg_ok, || "e.msg is not a string".into())?;
        Ok(String::new())
    }

    async fn c_method_version(&mut self) -> CResult<String> {
        let name = match self.decl.as_ref().and_then(|d| d.methods.keys().next()) {
            Some(n) => n.clone(),
            None => return Ok("SKIP: no methods declared".into()),
        };
        let mut c = self.session().await?;
        let m = self.req(&mut c, &name, None, 65535, false, None).await?;
        expect(
            vis(&m, "ok", false) && err_code(&m) == Some("version_unsupported"),
            || {
                format!(
                    "{name} v65535: ok={:?} code={:?}",
                    vget(&m, "ok"),
                    err_code(&m)
                )
            },
        )?;
        Ok(format!("{name} v65535 refused"))
    }

    async fn c_unknown_channel(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let rid = self.id();
        c.send(&codec::map([
            ("t", Value::from("sub")),
            ("id", Value::from(rid)),
            ("ch", Value::from(UNKNOWN_CHANNEL)),
            ("v", Value::from(1)),
        ]))
        .await?;
        let m = c
            .recv_msg(Duration::from_secs(5), &["ping", "pong", "decl", "evt"])
            .await?;
        expect(msg_type(&m) == "res" && vu64(&m, "id") == Some(rid), || {
            format!("got {m}")
        })?;
        expect(
            vis(&m, "ok", false) && err_code(&m) == Some("unknown_channel"),
            || format!("code {:?}", err_code(&m)),
        )?;
        Ok(String::new())
    }

    async fn c_chunked(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let p = codec::map([("pad", Value::from("x".repeat(200)))]);
        let m = self
            .req(&mut c, UNKNOWN_METHOD, Some(p), 1, true, None)
            .await?;
        expect(err_code(&m) == Some("unknown_method"), || {
            format!("code {:?}", err_code(&m))
        })?;
        Ok("16-byte chunks".into())
    }

    async fn c_big_id(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let rid = (1u64 << 53) + 1;
        let m = self
            .req(&mut c, UNKNOWN_METHOD, None, 1, false, Some(rid))
            .await?;
        expect(vu64(&m, "id") == Some(rid), || "id changed".into())?;
        Ok(String::new())
    }

    async fn c_reserved_kind(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let mut f = Vec::new();
        wire::write_frame(&mut f, 0x80, b"reserved for extensions")
            .map_err(|e| CheckError::Io(e.to_string()))?;
        c.send_frame(&f).await?;
        c.send(&codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(7)),
        ]))
        .await?;
        let m = c
            .recv_msg(Duration::from_secs(5), &["ping", "decl"])
            .await?;
        expect(msg_type(&m) == "pong", || {
            format!("got {:?} {}", msg_type(&m), vstr(&m, "code").unwrap_or(""))
        })?;
        Ok(String::new())
    }

    async fn c_bad_kind(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let mut f = Vec::new();
        wire::write_frame(&mut f, 0x10, &[0]).map_err(|e| CheckError::Io(e.to_string()))?;
        c.send_frame(&f).await?;
        self.closes_with(&mut c, Some("protocol_error"), Duration::from_secs(5))
            .await
    }

    async fn c_oversize(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let mf = c.peer_max_frame;
        let mut f = (mf + 1).to_le_bytes().to_vec();
        f.push(wire::KIND_MESSAGE);
        c.send_frame(&f).await?;
        self.closes_with(&mut c, Some("protocol_error"), Duration::from_secs(5))
            .await
    }

    async fn c_not_map(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let enc = codec::encode(&Value::Array(vec![
            Value::from(1),
            Value::from(2),
            Value::from(3),
        ]))
        .map_err(|e| CheckError::Io(e.to_string()))?;
        let mut f = Vec::new();
        wire::write_message_frame(&mut f, &enc).map_err(|e| CheckError::Io(e.to_string()))?;
        c.send_frame(&f).await?;
        self.closes_with(&mut c, Some("protocol_error"), Duration::from_secs(5))
            .await
    }

    async fn c_chunk_interleave(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let ping = codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(1)),
            ("pad", Value::from("x".repeat(64))),
        ]);
        let enc = codec::encode(&ping).map_err(|e| CheckError::Io(e.to_string()))?;
        let first = wire::chunk_frames(&enc, 16)
            .map_err(|e| CheckError::Io(e.to_string()))?
            .next()
            .expect("at least one chunk");
        c.send_frame(&first).await?;
        c.send(&codec::map([
            ("t", Value::from("ping")),
            ("ts", Value::from(2)),
        ]))
        .await?;
        self.closes_with(&mut c, Some("protocol_error"), Duration::from_secs(5))
            .await
    }

    async fn c_channel(&mut self, name: &str, decl: &ChannelDecl) -> CResult<String> {
        let mut c = self.session().await?;
        let v = decl.versions.iter().max().copied().unwrap_or(1);
        let rid = self.id();
        c.send(&codec::map([
            ("t", Value::from("sub")),
            ("id", Value::from(rid)),
            ("ch", Value::from(name)),
            ("v", Value::from(v)),
        ]))
        .await?;
        let mut early: Vec<Recv> = Vec::new();
        let end = Instant::now() + Duration::from_secs(5);
        let res = loop {
            let left = end
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            match c.recv(left).await? {
                Recv::Msg(m) if msg_type(&m) == "res" && vu64(&m, "id") == Some(rid) => break m,
                Recv::Msg(m) if msg_type(&m) == "ping" => {
                    let mut pairs = vec![("t", Value::from("pong"))];
                    if let Some(ts) = vget(&m, "ts") {
                        pairs.push(("ts", ts.clone()));
                    }
                    c.send(&codec::map(pairs)).await?;
                }
                other => early.push(other),
            }
        };
        if !vis(&res, "ok", true) {
            let e = vget(&res, "e");
            return Ok(format!(
                "WARN: refused: {} {}",
                e.and_then(|e| vstr(e, "code")).unwrap_or(""),
                e.and_then(|e| vstr(e, "msg")).unwrap_or("")
            ));
        }
        let r = vget(&res, "r").cloned().unwrap_or(Value::Nil);
        let sid = vu64(&r, "sid");
        expect(sid.is_some(), || "res.r.sid is not an integer".into())?;
        let sid = sid.unwrap_or_default();
        expect(vstr(&r, "kind") == Some(decl.kind.name()), || {
            format!(
                "res.r.kind {:?} != declared {:?}",
                vstr(&r, "kind"),
                decl.kind.name()
            )
        })?;
        let premature = early.iter().any(|e| match e {
            Recv::Stream(s) => u64::from(s.sid) == sid,
            Recv::Msg(m) => msg_type(m) == "evt" && vu64(m, "sid") == Some(sid),
        });
        expect(!premature, || {
            "data for the subscription arrived before its res".into()
        })?;
        let mut layout: Option<Layout> = None;
        if decl.kind == ChannelKind::State {
            expect(vget(&r, "snap").is_some(), || {
                "state res has no snap".into()
            })?;
        }
        if decl.kind == ChannelKind::Stream {
            let l = Layout::from_value(vget(&r, "layout").unwrap_or(&Value::Nil))
                .map_err(|e| CheckError::Fail(format!("res.r.layout invalid: {e}")))?;
            expect(decl.layout.as_ref() == Some(&l), || {
                "res layout differs from the declared layout".into()
            })?;
            layout = Some(l);
        }
        let mut seqs: Vec<u64> = Vec::new();
        let mut times: Vec<i64> = Vec::new();
        let mut dropped = 0u32;
        let end = Instant::now() + secs(self.listen_s);
        while Instant::now() < end {
            let left = end
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            let item = match c.recv(left).await {
                Err(CheckError::Timeout(_)) => break,
                r => r?,
            };
            match item {
                Recv::Stream(s) => {
                    if u64::from(s.sid) != sid {
                        continue;
                    }
                    expect(decl.kind == ChannelKind::Stream, || {
                        "stream frame on a non-stream subscription".into()
                    })?;
                    let size = layout.as_ref().map_or(0, Layout::size);
                    expect(s.body.len() == size, || {
                        format!("sample of {} bytes, layout {size}", s.body.len())
                    })?;
                    seqs.push(u64::from(s.seq));
                    times.push(s.t_ns);
                    dropped += s.flags & 1;
                }
                Recv::Msg(m) => match msg_type(&m) {
                    "evt" if vu64(&m, "sid") == Some(sid) => {
                        expect(decl.kind != ChannelKind::Stream, || {
                            "evt on a stream subscription".into()
                        })?;
                        let seq = vu64(&m, "seq");
                        expect(seq.is_some(), || "evt.seq is not an integer".into())?;
                        seqs.push(seq.unwrap_or_default());
                        expect(vget(&m, "ts").is_some_and(Value::is_i64), || {
                            "evt.ts is not an integer".into()
                        })?;
                    }
                    "ping" => {
                        let mut pairs = vec![("t", Value::from("pong"))];
                        if let Some(ts) = vget(&m, "ts") {
                            pairs.push(("ts", ts.clone()));
                        }
                        c.send(&codec::map(pairs)).await?;
                    }
                    "end" if vu64(&m, "sid") == Some(sid) => {
                        return Ok(format!(
                            "WARN: ended by the owner: {}",
                            vstr(&m, "code").unwrap_or("")
                        ));
                    }
                    _ => {}
                },
            }
        }
        for w in seqs.windows(2) {
            let (a, b) = (w[0], w[1]);
            let increasing = if decl.kind == ChannelKind::Stream {
                (b.wrapping_sub(a) & 0xFFFF_FFFF) >= 1
            } else {
                b > a
            };
            expect(increasing, || format!("seq not increasing: {a} -> {b}"))?;
        }
        for w in times.windows(2) {
            expect(w[1] >= w[0], || "t_ns went backwards".into())?;
        }
        c.send(&codec::map([
            ("t", Value::from("unsub")),
            ("sid", Value::from(sid)),
        ]))
        .await?;
        let mut detail = format!("sid {sid}, {} items in {} s", seqs.len(), self.listen_s);
        if decl.kind == ChannelKind::Stream && times.len() > 2 {
            let span = (times[times.len() - 1] - times[0]) as f64 / 1e9;
            if span > 0.0 {
                let rate = (times.len() - 1) as f64 / span;
                detail.push_str(&format!(", {rate:.0} Hz"));
                if let Some(nominal) = decl.rate_hz.filter(|r| *r > 0.0)
                    && rate > 1.5 * nominal
                {
                    return Ok(format!("WARN: {detail} (declared {nominal} Hz)"));
                }
            }
            if dropped > 0 {
                detail.push_str(&format!(", {dropped} dropped flags"));
            }
        }
        Ok(detail)
    }

    async fn c_resub(&mut self) -> CResult<String> {
        let chans: Vec<(String, ChannelDecl)> = self
            .channels()
            .into_iter()
            .filter(|(_, d)| d.kind != ChannelKind::Stream)
            .collect();
        let Some((name, d)) = chans.into_iter().next() else {
            return Ok("SKIP: no event or state channel declared".into());
        };
        let mut c = self.session().await?;
        let v = d.versions.iter().max().copied().unwrap_or(1);
        let mut sids = Vec::new();
        for _ in 0..2 {
            let rid = self.id();
            c.send(&codec::map([
                ("t", Value::from("sub")),
                ("id", Value::from(rid)),
                ("ch", Value::from(name.as_str())),
                ("v", Value::from(v)),
            ]))
            .await?;
            let m = loop {
                let m = c
                    .recv_msg(Duration::from_secs(5), &["ping", "pong", "decl", "evt"])
                    .await?;
                if msg_type(&m) == "res" && vu64(&m, "id") == Some(rid) {
                    break m;
                }
            };
            if !vis(&m, "ok", true) {
                return Ok(format!("SKIP: {name} refused the subscription"));
            }
            sids.push(vget(&m, "r").and_then(|r| vu64(r, "sid")));
        }
        expect(sids[0] == sids[1], || {
            format!("sids {:?} and {:?}", sids[0], sids[1])
        })?;
        Ok(format!("{name}: sid {}", sids[0].unwrap_or_default()))
    }

    async fn c_call(&mut self, method: &str, params: &Value) -> CResult<String> {
        let mut c = self.session().await?;
        let m = self
            .req(&mut c, method, Some(params.clone()), 1, false, None)
            .await?;
        if vis(&m, "ok", true) {
            let r = vget(&m, "r").map_or_else(|| "None".to_owned(), ToString::to_string);
            return Ok(format!(
                "INFO: result {}",
                r.chars().take(300).collect::<String>()
            ));
        }
        let e = vget(&m, "e");
        let code = e.and_then(|e| vstr(e, "code"));
        expect(code.is_some(), || "error response without e.code".into())?;
        Ok(format!(
            "INFO: error {}: {}",
            code.unwrap_or(""),
            e.and_then(|e| vstr(e, "msg")).unwrap_or("")
        ))
    }

    async fn c_heartbeat(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        let hb = self.heartbeat_ms;
        let end = Instant::now() + secs(f64::from(hb) / 1000.0 * 1.6 + 0.5);
        while Instant::now() < end {
            let left = end
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            match c.recv(left).await {
                Err(CheckError::Timeout(_)) => break,
                Ok(Recv::Msg(m)) if msg_type(&m) == "ping" => {
                    let mut pairs = vec![("t", Value::from("pong"))];
                    if let Some(ts) = vget(&m, "ts") {
                        pairs.push(("ts", ts.clone()));
                    }
                    c.send(&codec::map(pairs)).await?;
                    return Ok(format!("ping after idle (heartbeat {hb} ms)"));
                }
                Err(e) => return Err(e),
                Ok(_) => {}
            }
        }
        Ok(format!("WARN: no ping within 1.6 x heartbeat_ms ({hb} ms)"))
    }

    async fn c_bye(&mut self) -> CResult<String> {
        let mut c = self.session().await?;
        c.send(&codec::map([
            ("t", Value::from("bye")),
            ("code", Value::from("normal")),
            ("msg", Value::from("check done")),
        ]))
        .await?;
        let t0 = Instant::now();
        c.wait_closed(Duration::from_secs(3)).await?;
        let dt = t0.elapsed().as_secs_f64();
        expect(dt < 2.9, || "still open 3 s after bye".into())?;
        Ok(format!("closed after {:.0} ms", 1000.0 * dt))
    }
}
