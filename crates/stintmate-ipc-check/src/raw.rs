//! A raw protocol connection: frames by hand, handshakes with chosen versions or wrong MACs,
//! on the codec (`wire`, `codec`) and the transport of the library. It is how the checks send
//! malformed and adversarial frames that the library itself never produces.

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::decl::Declarations;
use stintmate_ipc::handshake::{
    Auth, Hello, Limits, MacBytes, Nonce, PeerInfo, Welcome, bye_message, client_mac, new_nonce,
    server_mac, verify_client_mac, verify_server_mac,
};
use stintmate_ipc::keyfile::Key;
use stintmate_ipc::naming;
use stintmate_ipc::transport::{self, Pipe};
use stintmate_ipc::wire::{self, Frame, FrameReader, StreamSample};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

/// Why a check step could not go on.
#[derive(Debug)]
pub enum CheckError {
    /// A check's own verdict: the implementation under test is not conformant.
    Fail(String),
    /// Nothing arrived in time.
    Timeout(String),
    /// The peer closed the connection.
    Closed(String),
    /// The peer sent bytes the codec refuses.
    Protocol(String),
    /// The operating system failed.
    Io(String),
    /// The client's MAC was wrong (after `bye unauthorized`).
    Unauthorized,
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fail(m) => f.write_str(m),
            Self::Timeout(m) => write!(f, "Timeout: {m}"),
            Self::Closed(m) => write!(f, "Closed: {m}"),
            Self::Protocol(m) => write!(f, "Protocol: {m}"),
            Self::Io(m) => write!(f, "Io: {m}"),
            Self::Unauthorized => f.write_str("wrong client MAC"),
        }
    }
}

/// The result of a check step.
pub type CResult<T> = Result<T, CheckError>;

/// What [`RawConn::recv`] returns.
#[derive(Debug)]
pub enum Recv {
    /// A message (kind 1, or reassembled kind 3).
    Msg(Value),
    /// A stream sample (kind 2).
    Stream(StreamSample),
}

/// The monotonic nanoseconds since the first call in this process (the `ts` of pings).
pub fn now_ns() -> i64 {
    use std::sync::OnceLock;
    static T0: OnceLock<Instant> = OnceLock::new();
    i64::try_from(T0.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(i64::MAX)
}

/// `msg[key]`.
pub fn vget<'a>(m: &'a Value, key: &str) -> Option<&'a Value> {
    codec::get(m, key)
}

/// `msg["t"]` as text (empty when absent).
pub fn msg_type(m: &Value) -> &str {
    vget(m, "t").and_then(Value::as_str).unwrap_or("")
}

/// `msg[key]` as text.
pub fn vstr<'a>(m: &'a Value, key: &str) -> Option<&'a str> {
    vget(m, key).and_then(Value::as_str)
}

/// `msg[key]` as an unsigned integer.
pub fn vu64(m: &Value, key: &str) -> Option<u64> {
    vget(m, key).and_then(Value::as_u64)
}

/// Whether `msg[key]` is the boolean `b`.
pub fn vis(m: &Value, key: &str, b: bool) -> bool {
    vget(m, key).and_then(Value::as_bool) == Some(b)
}

/// `msg["e"]["code"]` of a `res`.
pub fn err_code(m: &Value) -> Option<&str> {
    vget(m, "e").and_then(|e| vstr(e, "code"))
}

/// The identity the tool announces.
pub fn tool_peer(name: &str) -> PeerInfo {
    PeerInfo {
        app: "ipc-check".to_owned(),
        name: name.to_owned(),
        version: stintmate_ipc::VERSION.to_owned(),
        process: None,
        meta: None,
    }
}

/// One pipe connection driven by hand.
pub struct RawConn {
    pipe: Pipe,
    short: String,
    reader: FrameReader,
    frames: VecDeque<Frame>,
    /// The peer's announced `max_frame` (after a handshake).
    pub peer_max_frame: u32,
    /// Whether the server's MAC in `welcome` was right (after a client handshake).
    pub server_mac_ok: bool,
    /// The peer process id as the operating system reports it.
    pub peer_pid: Option<u32>,
}

impl RawConn {
    /// Wraps a connected pipe; `short` is the endpoint's short name (bound into the MACs).
    pub fn new(pipe: Pipe, short: &str, peer_pid: Option<u32>) -> Self {
        Self {
            pipe,
            short: naming::ascii_lower(short),
            reader: FrameReader::new(wire::DEFAULT_MAX_FRAME, wire::DEFAULT_MAX_MESSAGE),
            frames: VecDeque::new(),
            peer_max_frame: wire::MIN_MAX_FRAME,
            server_mac_ok: false,
            peer_pid,
        }
    }

    /// Opens the pipe `pipe` (a path or a short name) as a client, waiting for a free instance.
    pub async fn open(pipe: &str, wait: Duration) -> CResult<Self> {
        let (path, short) = naming::split_pipe(pipe).map_err(|e| CheckError::Io(e.to_string()))?;
        let p = transport::open_pipe(&path, wait)
            .await
            .map_err(|e| CheckError::Io(e.to_string()))?;
        let pid = transport::server_pid(&p);
        Ok(Self::new(p, &short, pid))
    }

    // ------------------------------------------------------------------ I/O

    /// Writes bytes as they are (a frame, or garbage).
    pub async fn send_frame(&mut self, data: &[u8]) -> CResult<()> {
        match timeout(Duration::from_secs(5), self.pipe.write_all(data)).await {
            Err(_) => Err(CheckError::Timeout(
                "the pipe did not take the write".into(),
            )),
            Ok(Err(e)) => Err(CheckError::Closed(e.to_string())),
            Ok(Ok(())) => Ok(()),
        }
    }

    /// Sends a message as one kind-1 frame.
    pub async fn send(&mut self, msg: &Value) -> CResult<()> {
        let enc = codec::encode(msg).map_err(|e| CheckError::Io(e.to_string()))?;
        let mut out = Vec::new();
        wire::write_message_frame(&mut out, &enc).map_err(|e| CheckError::Io(e.to_string()))?;
        self.send_frame(&out).await
    }

    /// Sends a message as kind-3 chunks of `chunk` data bytes.
    pub async fn send_chunked(&mut self, msg: &Value, chunk: usize) -> CResult<()> {
        let enc = codec::encode(msg).map_err(|e| CheckError::Io(e.to_string()))?;
        let frames: Vec<Vec<u8>> = wire::chunk_frames(&enc, chunk)
            .map_err(|e| CheckError::Io(e.to_string()))?
            .collect();
        for f in frames {
            self.send_frame(&f).await?;
        }
        Ok(())
    }

    async fn fill(&mut self, deadline: Instant) -> CResult<()> {
        let mut buf = [0u8; 16 * 1024];
        while self.frames.is_empty() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(CheckError::Timeout("nothing received in time".into()));
            }
            match timeout(left, self.pipe.read(&mut buf)).await {
                Err(_) => return Err(CheckError::Timeout("nothing received in time".into())),
                Ok(Ok(0) | Err(_)) => {
                    return Err(CheckError::Closed("the peer closed the connection".into()));
                }
                Ok(Ok(n)) => self.reader.push(&buf[..n]),
            }
            loop {
                match self.reader.next_frame() {
                    Ok(Some(f)) => self.frames.push_back(f),
                    Ok(None) => break,
                    Err(e) => {
                        if self.frames.is_empty() {
                            return Err(CheckError::Protocol(e.to_string()));
                        }
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// The next message or stream sample, waiting at most `wait`.
    pub async fn recv(&mut self, wait: Duration) -> CResult<Recv> {
        self.fill(Instant::now() + wait).await?;
        match self.frames.pop_front().expect("fill returned with a frame") {
            Frame::Stream(s) => Ok(Recv::Stream(s)),
            Frame::Message(b) => codec::decode_message(&b)
                .map(Recv::Msg)
                .map_err(|e| CheckError::Protocol(e.to_string())),
        }
    }

    /// The next message whose `t` is not in `skip`; pings are answered.
    pub async fn recv_msg(&mut self, wait: Duration, skip: &[&str]) -> CResult<Value> {
        let end = Instant::now() + wait;
        loop {
            let left = end
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            let Recv::Msg(m) = self.recv(left).await? else {
                continue;
            };
            let t = msg_type(&m).to_owned();
            if t == "ping" {
                let mut pairs = vec![("t", Value::from("pong"))];
                if let Some(ts) = vget(&m, "ts") {
                    pairs.push(("ts", ts.clone()));
                }
                self.send(&codec::map(pairs)).await?;
            }
            if skip.contains(&t.as_str()) {
                continue;
            }
            return Ok(m);
        }
    }

    /// Reads until the peer closes; the messages seen (a `bye` among them). A timeout is an
    /// error.
    pub async fn wait_closed(&mut self, wait: Duration) -> CResult<Vec<Value>> {
        let mut seen = Vec::new();
        let end = Instant::now() + wait;
        loop {
            let left = end
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            match self.recv(left).await {
                Ok(Recv::Msg(m)) => seen.push(m),
                Ok(Recv::Stream(_)) => {}
                Err(CheckError::Closed(_) | CheckError::Protocol(_)) => return Ok(seen),
                Err(e) => return Err(e),
            }
        }
    }

    // ------------------------------------------------------------------ handshakes

    /// Acts as a client: reads `hello`, sends `auth` (with a wrong MAC when `bad_mac`, with
    /// protocol version `version`); returns the `welcome` (or the `bye`). Sets
    /// `server_mac_ok` and `peer_max_frame`.
    pub async fn client_handshake(
        &mut self,
        key: &Key,
        bad_mac: bool,
        version: u32,
    ) -> CResult<Value> {
        let hello = self.recv_msg(Duration::from_secs(5), &[]).await?;
        let s_nonce: Nonce = match vget(&hello, "nonce") {
            Some(Value::Binary(b)) => b
                .as_slice()
                .try_into()
                .map_err(|_| CheckError::Fail("hello.nonce is not 32 bytes".into()))?,
            _ => return Err(CheckError::Fail("hello.nonce is not a bin".into())),
        };
        let c_nonce = new_nonce();
        let mut mac: MacBytes = client_mac(key, version, &s_nonce, &c_nonce, &self.short);
        if bad_mac {
            mac[0] ^= 0xFF;
        }
        let auth = Auth {
            v: version,
            nonce: c_nonce,
            mac,
            peer: tool_peer("stintmate-ipc-check"),
            decl: Declarations::default(),
            limits: Limits::default(),
            features: Vec::new(),
        };
        self.send(&auth.to_value()).await?;
        let reply = self.recv_msg(Duration::from_secs(5), &["decl"]).await?;
        if msg_type(&reply) == "welcome" {
            self.server_mac_ok = matches!(vget(&reply, "mac"), Some(Value::Binary(m))
                if verify_server_mac(key, version, &s_nonce, &c_nonce, &self.short, m));
            self.peer_max_frame = vu64(&hello, "max_frame")
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(wire::MIN_MAX_FRAME);
        }
        Ok(reply)
    }

    /// Acts as a server on an accepted pipe: sends `hello`, reads and verifies `auth`, sends
    /// `welcome` (with a wrong MAC when `bad_mac`). Returns the raw `auth`; a wrong client MAC
    /// is answered with `bye unauthorized` and is [`CheckError::Unauthorized`].
    pub async fn server_handshake(
        &mut self,
        key: &Key,
        peer: &PeerInfo,
        decl: &Declarations,
        bad_mac: bool,
        heartbeat_ms: u32,
    ) -> CResult<Value> {
        let s_nonce = new_nonce();
        let hello = Hello {
            versions: vec![2],
            nonce: s_nonce,
            limits: Limits::default(),
            features: Vec::new(),
            clock: None,
        };
        self.send(&hello.to_value()).await?;
        let auth = self.recv_msg(Duration::from_secs(5), &[]).await?;
        if msg_type(&auth) != "auth" {
            return Err(CheckError::Protocol(format!(
                "expected auth, got {}",
                msg_type(&auth)
            )));
        }
        let v = vu64(&auth, "v")
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(2);
        let c_nonce: Option<Nonce> = match vget(&auth, "nonce") {
            Some(Value::Binary(b)) => b.as_slice().try_into().ok(),
            _ => None,
        };
        let verified = match (&c_nonce, vget(&auth, "mac")) {
            (Some(n), Some(Value::Binary(m))) => {
                verify_client_mac(key, v, &s_nonce, n, &self.short, m)
            }
            _ => false,
        };
        let Some(c_nonce) = c_nonce.filter(|_| verified) else {
            self.send(&bye_message("unauthorized", "authentication failed"))
                .await?;
            return Err(CheckError::Unauthorized);
        };
        let mut mac = server_mac(key, 2, &s_nonce, &c_nonce, &self.short);
        if bad_mac {
            mac[0] ^= 0xFF;
        }
        let welcome = Welcome {
            v: 2,
            mac,
            run: "check-run".to_owned(),
            peer: peer.clone(),
            decl: decl.clone(),
            heartbeat_ms,
            features: Vec::new(),
        };
        self.send(&welcome.to_value()).await?;
        self.peer_max_frame = vu64(&auth, "max_frame")
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(wire::MIN_MAX_FRAME);
        Ok(auth)
    }
}
