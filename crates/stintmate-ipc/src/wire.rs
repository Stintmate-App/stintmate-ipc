//! Framing (SPEC section 5).
//!
//! ```text
//! frame   = length:u32le  kind:u8  payload        ; length = 1 + len(payload)
//! kind 1  = message: one MessagePack map with a string "t"
//! kind 2  = stream sample: sid:u32le seq:u32le t_ns:i64le flags:u32le body
//! kind 3  = chunk: total:u32le offset:u32le data
//! kinds 0x80-0xFF: reserved, skipped; 0x00 and 0x04-0x7F: protocol error
//! ```

use bytes::{Buf, Bytes, BytesMut};

use crate::error::{EncodeError, ProtocolError};

/// Frame kind 1: one MessagePack message.
pub const KIND_MESSAGE: u8 = 0x01;
/// Frame kind 2: a stream sample.
pub const KIND_STREAM: u8 = 0x02;
/// Frame kind 3: a chunk of a message too large for one frame.
pub const KIND_CHUNK: u8 = 0x03;

/// Size of the stream sample header (`sid`, `seq`, `t_ns`, `flags`).
pub const STREAM_HEADER_LEN: usize = 20;
/// Size of the chunk header (`total`, `offset`).
pub const CHUNK_HEADER_LEN: usize = 8;
/// Stream `flags` bit 0: samples were dropped before this one (SPEC 12.1).
pub const STREAM_FLAG_DROPPED: u32 = 0x1;

/// Largest `length` of any frame before the handshake completes.
pub const HANDSHAKE_MAX_FRAME: u32 = 65536;
/// The smallest `max_frame` a peer may announce.
pub const MIN_MAX_FRAME: u32 = 65536;
/// The reference implementation's `max_frame` (1 MiB).
pub const DEFAULT_MAX_FRAME: u32 = 1024 * 1024;
/// The reference implementation's `max_message` (256 MiB).
pub const DEFAULT_MAX_MESSAGE: u32 = 256 * 1024 * 1024;
/// The largest `max_message` a peer may announce.
pub const MAX_MESSAGE_LIMIT: u32 = u32::MAX;
/// Data bytes per chunk this implementation sends (256 KiB, SPEC 5.4).
pub const CHUNK_DATA: usize = 256 * 1024;

/// A stream sample (kind 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSample {
    /// The subscription.
    pub sid: u32,
    /// The sample's sequence number (wraps at 2^32).
    pub seq: u32,
    /// When the sample was taken at the source.
    pub t_ns: i64,
    /// Bit 0: [`STREAM_FLAG_DROPPED`]; the other bits are reserved.
    pub flags: u32,
    /// The packed layout body.
    pub body: Bytes,
}

/// What a [`FrameReader`] yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// One complete encoded message: a kind-1 payload or reassembled chunks. Decode it with
    /// [`crate::codec::decode_message`].
    Message(Bytes),
    /// A stream sample.
    Stream(StreamSample),
}

/// Appends a frame of `kind` with `payload`.
pub fn write_frame(out: &mut Vec<u8>, kind: u8, payload: &[u8]) -> Result<(), EncodeError> {
    let length = u32::try_from(payload.len() + 1)
        .map_err(|_| EncodeError::TooLarge(payload.len().saturating_add(1)))?;
    out.reserve(payload.len() + 5);
    out.extend_from_slice(&length.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    Ok(())
}

/// Appends a kind-1 frame carrying an encoded message.
pub fn write_message_frame(out: &mut Vec<u8>, encoded: &[u8]) -> Result<(), EncodeError> {
    write_frame(out, KIND_MESSAGE, encoded)
}

/// Appends a kind-2 frame.
pub fn write_stream_frame(
    out: &mut Vec<u8>,
    sid: u32,
    seq: u32,
    t_ns: i64,
    flags: u32,
    body: &[u8],
) -> Result<(), EncodeError> {
    let payload = STREAM_HEADER_LEN + body.len();
    let length =
        u32::try_from(payload + 1).map_err(|_| EncodeError::TooLarge(payload.saturating_add(1)))?;
    out.reserve(payload + 5);
    out.extend_from_slice(&length.to_le_bytes());
    out.push(KIND_STREAM);
    out.extend_from_slice(&sid.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&t_ns.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(body);
    Ok(())
}

/// The kind-3 frames carrying one encoded message, `chunk_data` data bytes each (the last one
/// may be shorter). SPEC 5.4.
pub fn chunk_frames(
    encoded: &[u8],
    chunk_data: usize,
) -> Result<impl Iterator<Item = Vec<u8>> + '_, EncodeError> {
    if chunk_data == 0 {
        return Err(EncodeError::ZeroChunk);
    }
    let total = u32::try_from(encoded.len()).map_err(|_| EncodeError::TooLarge(encoded.len()))?;
    Ok(encoded
        .chunks(chunk_data)
        .enumerate()
        .map(move |(i, part)| {
            let offset = (i * chunk_data) as u32;
            let mut payload = Vec::with_capacity(CHUNK_HEADER_LEN + part.len());
            payload.extend_from_slice(&total.to_le_bytes());
            payload.extend_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(part);
            let mut frame = Vec::with_capacity(payload.len() + 5);
            write_frame(&mut frame, KIND_CHUNK, &payload).expect("chunk fits a frame");
            frame
        }))
}

/// Data bytes per chunk so a chunk frame fits a peer's `max_frame`.
pub fn chunk_data_for(max_frame: u32) -> usize {
    let room = (max_frame as usize).saturating_sub(1 + CHUNK_HEADER_LEN);
    room.clamp(1, CHUNK_DATA)
}

#[derive(Debug)]
struct Transfer {
    total: u32,
    data: Vec<u8>,
}

/// Incremental receiver: [`push`](Self::push) bytes as they arrive, then call
/// [`next_frame`](Self::next_frame) until it returns `Ok(None)`.
///
/// The length of a frame is checked against `max_frame` as soon as its 4 bytes are there, so a
/// refused frame's body is never waited for or buffered (SPEC 5.2). Frames of reserved kinds
/// are skipped. After the first error the reader is poisoned: every later call returns the same
/// error (SPEC 5.5: no resynchronisation). However the input is split across `push` calls, the
/// frames yielded are the same.
#[derive(Debug)]
pub struct FrameReader {
    max_frame: u32,
    max_message: u32,
    buf: BytesMut,
    transfer: Option<Transfer>,
    failed: Option<ProtocolError>,
}

impl FrameReader {
    /// A reader with the given limits.
    pub fn new(max_frame: u32, max_message: u32) -> Self {
        Self {
            max_frame,
            max_message,
            buf: BytesMut::new(),
            transfer: None,
            failed: None,
        }
    }

    /// A reader for the handshake: every frame at most [`HANDSHAKE_MAX_FRAME`].
    pub fn for_handshake() -> Self {
        Self::new(HANDSHAKE_MAX_FRAME, DEFAULT_MAX_MESSAGE)
    }

    /// Replaces the limits (after the handshake, with the ones this peer announced).
    pub fn set_limits(&mut self, max_frame: u32, max_message: u32) {
        self.max_frame = max_frame;
        self.max_message = max_message;
    }

    /// The `max_frame` in force.
    pub fn max_frame(&self) -> u32 {
        self.max_frame
    }

    /// The `max_message` in force.
    pub fn max_message(&self) -> u32 {
        self.max_message
    }

    /// Whether a chunked message is being received.
    pub fn in_transfer(&self) -> bool {
        self.transfer.is_some()
    }

    /// Bytes received and not yet consumed.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Appends received bytes.
    pub fn push(&mut self, data: &[u8]) {
        if self.failed.is_none() {
            self.buf.extend_from_slice(data);
        }
    }

    fn fail<T>(&mut self, e: ProtocolError) -> Result<T, ProtocolError> {
        self.buf.clear();
        self.transfer = None;
        self.failed = Some(e.clone());
        Err(e)
    }

    /// The next complete frame, `Ok(None)` when more bytes are needed.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ProtocolError> {
        loop {
            if let Some(e) = &self.failed {
                return Err(e.clone());
            }
            if self.buf.len() < 4 {
                return Ok(None);
            }
            let length = u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
            if length == 0 {
                return self.fail(ProtocolError::ZeroLength);
            }
            if length > self.max_frame {
                let max = self.max_frame;
                return self.fail(ProtocolError::FrameTooLarge { length, max });
            }
            if self.buf.len() - 4 < length as usize {
                return Ok(None);
            }
            self.buf.advance(4);
            let mut payload = self.buf.split_to(length as usize).freeze();
            let kind = payload.get_u8();
            match kind {
                KIND_MESSAGE => {
                    if self.transfer.is_some() {
                        return self.fail(ProtocolError::MessageInsideTransfer);
                    }
                    return Ok(Some(Frame::Message(payload)));
                }
                KIND_STREAM => {
                    if payload.len() < STREAM_HEADER_LEN {
                        return self.fail(ProtocolError::StreamTooShort(payload.len()));
                    }
                    let sid = payload.get_u32_le();
                    let seq = payload.get_u32_le();
                    let t_ns = payload.get_i64_le();
                    let flags = payload.get_u32_le();
                    return Ok(Some(Frame::Stream(StreamSample {
                        sid,
                        seq,
                        t_ns,
                        flags,
                        body: payload,
                    })));
                }
                KIND_CHUNK => match self.on_chunk(payload) {
                    Ok(Some(msg)) => return Ok(Some(Frame::Message(msg))),
                    Ok(None) => {}
                    Err(e) => return self.fail(e),
                },
                0x80..=0xFF => {}
                other => return self.fail(ProtocolError::InvalidKind(other)),
            }
        }
    }

    fn on_chunk(&mut self, mut payload: Bytes) -> Result<Option<Bytes>, ProtocolError> {
        if payload.len() < CHUNK_HEADER_LEN {
            return Err(ProtocolError::ChunkTooShort(payload.len()));
        }
        let total = payload.get_u32_le();
        let offset = payload.get_u32_le();
        let data = payload;
        match &mut self.transfer {
            None => {
                if offset != 0 {
                    return Err(ProtocolError::FirstChunkOffset(offset));
                }
                if total > self.max_message {
                    return Err(ProtocolError::MessageTooLarge {
                        total,
                        max: self.max_message,
                    });
                }
                if total == 0 {
                    return Err(ProtocolError::EmptyTotal);
                }
                if data.is_empty() {
                    return Err(ProtocolError::EmptyChunk);
                }
                if data.len() as u64 > u64::from(total) {
                    return Err(ProtocolError::ChunkOverrun(total));
                }
                if data.len() as u64 == u64::from(total) {
                    return Ok(Some(data));
                }
                let mut buf = Vec::with_capacity((total as usize).min(CHUNK_DATA));
                buf.extend_from_slice(&data);
                self.transfer = Some(Transfer { total, data: buf });
                Ok(None)
            }
            Some(t) => {
                if total != t.total || offset as usize != t.data.len() {
                    return Err(ProtocolError::ChunkOutOfSequence {
                        offset,
                        total,
                        expected_offset: t.data.len() as u32,
                        expected_total: t.total,
                    });
                }
                if data.is_empty() {
                    return Err(ProtocolError::EmptyChunk);
                }
                if (t.data.len() + data.len()) as u64 > u64::from(t.total) {
                    return Err(ProtocolError::ChunkOverrun(t.total));
                }
                t.data.extend_from_slice(&data);
                if t.data.len() as u64 == u64::from(t.total) {
                    let done = self.transfer.take().expect("transfer in progress");
                    return Ok(Some(Bytes::from(done.data)));
                }
                Ok(None)
            }
        }
    }
}
