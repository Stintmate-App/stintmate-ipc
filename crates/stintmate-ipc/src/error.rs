//! Error types of the codec.

use thiserror::Error;

/// A framing or encoding violation (SPEC 5.5, 6.2, 11.2). The receiver stops processing the
/// connection's input, sends `bye protocol_error` with the error's text and closes: it must not
/// try to resynchronise.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    /// A frame with `length` 0 (a frame has at least its kind byte).
    #[error("frame length 0")]
    ZeroLength,
    /// A frame longer than the receiver's `max_frame` (refused on its 4 length bytes).
    #[error("frame of {length} bytes exceeds max_frame {max}")]
    FrameTooLarge {
        /// The announced `length`.
        length: u32,
        /// The receiver's limit.
        max: u32,
    },
    /// A kind that is neither defined nor reserved (`0x00`, `0x04`-`0x7F`).
    #[error("unknown frame kind 0x{0:02x}")]
    InvalidKind(u8),
    /// A kind-2 frame with a payload shorter than its 20-byte header.
    #[error("stream frame of {0} payload bytes is shorter than its header")]
    StreamTooShort(usize),
    /// A stream sample whose body is not the subscription's layout size.
    #[error("stream body of {actual} bytes, layout of {expected}")]
    StreamBodySize {
        /// The layout size.
        expected: usize,
        /// The body length received.
        actual: usize,
    },
    /// A kind-3 frame with a payload shorter than its 8-byte header.
    #[error("chunk frame of {0} payload bytes is shorter than its header")]
    ChunkTooShort(usize),
    /// A kind-1 frame between the chunks of one message.
    #[error("a message frame inside a chunked transfer")]
    MessageInsideTransfer,
    /// The first chunk of a message with an offset other than 0.
    #[error("first chunk with offset {0} (must be 0)")]
    FirstChunkOffset(u32),
    /// A chunked message with `total` 0.
    #[error("chunked message of 0 bytes")]
    EmptyTotal,
    /// A chunk without data.
    #[error("empty chunk")]
    EmptyChunk,
    /// A chunked message larger than the receiver's `max_message`.
    #[error("message of {total} bytes exceeds max_message {max}")]
    MessageTooLarge {
        /// The announced `total`.
        total: u32,
        /// The receiver's limit.
        max: u32,
    },
    /// A gap, an overlap or a changed `total`.
    #[error(
        "chunk out of sequence (offset {offset} total {total}, expected offset {expected_offset} total {expected_total})"
    )]
    ChunkOutOfSequence {
        /// The offset received.
        offset: u32,
        /// The total received.
        total: u32,
        /// Where the next chunk had to start.
        expected_offset: u32,
        /// The total the first chunk announced.
        expected_total: u32,
    },
    /// More chunk data than `total`.
    #[error("chunks longer than their total of {0} bytes")]
    ChunkOverrun(u32),
    /// Bytes that are not valid MessagePack.
    #[error("invalid MessagePack: {0}")]
    InvalidMessagePack(String),
    /// An extension type (including timestamp, -1).
    #[error("MessagePack extension types are not allowed")]
    ExtensionType,
    /// A map key that is not a string.
    #[error("map keys must be strings")]
    NonStringKey,
    /// A repeated map key.
    #[error("duplicate map key {0:?}")]
    DuplicateKey(String),
    /// A `str` that is not valid UTF-8.
    #[error("a str that is not valid UTF-8")]
    InvalidUtf8,
    /// Nesting deeper than [`MAX_DEPTH`](crate::codec::MAX_DEPTH).
    #[error("nesting deeper than the supported depth")]
    TooDeep,
    /// Bytes after the value.
    #[error("{0} bytes after the MessagePack value")]
    TrailingData(usize),
    /// A message that is not a map.
    #[error("a message must be a map")]
    NotAMap,
    /// A message without a string `t`.
    #[error("a message must have a string 't'")]
    MissingType,
    /// A required field is missing or has the wrong type.
    #[error("malformed {message} message: field {field:?} must be {expected}")]
    BadField {
        /// The message type (`t`).
        message: String,
        /// The field.
        field: &'static str,
        /// What it must be.
        expected: &'static str,
    },
    /// A handshake message (`hello`, `auth`, `welcome`) after the handshake.
    #[error("{0} message after the handshake")]
    HandshakeAfterHandshake(String),
    /// An invalid declaration (section 9).
    #[error("invalid declaration: {0}")]
    InvalidDeclaration(String),
}

/// Something that cannot be encoded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EncodeError {
    /// The value is outside the subset of SPEC 6.1.
    #[error("not encodable: {0}")]
    NotEncodable(ProtocolError),
    /// A frame or message whose length does not fit 32 bits.
    #[error("{0} bytes do not fit a frame")]
    TooLarge(usize),
    /// A chunk size of 0.
    #[error("chunk size must be positive")]
    ZeroChunk,
}

/// An invalid layout, or values that do not fit one (SPEC 9.3).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LayoutError {
    /// A malformed layout map.
    #[error("invalid layout: {0}")]
    Invalid(String),
    /// Values or a body that do not fit the layout.
    #[error("{0}")]
    Mismatch(String),
}

/// An invalid declaration (SPEC 9.1-9.3).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct DeclError(pub String);

impl From<DeclError> for ProtocolError {
    fn from(e: DeclError) -> Self {
        ProtocolError::InvalidDeclaration(e.0)
    }
}

impl From<LayoutError> for DeclError {
    fn from(e: LayoutError) -> Self {
        DeclError(e.to_string())
    }
}

/// An invalid endpoint name part (SPEC 4.1).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NameError {
    /// An app id that does not match `^[a-z0-9][a-z0-9_-]{0,47}$`.
    #[error("invalid app id {0:?} (^[a-z0-9][a-z0-9_-]{{0,47}}$)")]
    InvalidApp(String),
    /// A process id that does not match `^[a-z0-9][a-z0-9_-]{0,31}$`.
    #[error("invalid process id {0:?} (^[a-z0-9][a-z0-9_-]{{0,31}}$)")]
    InvalidProcess(String),
    /// A pipe name that is empty or has a backslash after the pipe prefix.
    #[error("invalid pipe name {0:?}")]
    InvalidPipe(String),
    /// The Windows user name could not be read.
    #[error("cannot read the Windows user name: {0}")]
    User(String),
}

/// A problem with the key file (SPEC 8.1). The messages never contain the key.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KeyFileError {
    /// There is no workspace, so no key file.
    #[error(
        "no Stintmate workspace (set STINTMATE_WORKSPACE or run the hub onboarding), so no key file"
    )]
    NoWorkspace,
    /// The file does not exist.
    #[error("key file not found: {0}")]
    NotFound(String),
    /// The file cannot be read or created.
    #[error("cannot access the key file {path}: {message}")]
    Io {
        /// The file.
        path: String,
        /// The system message.
        message: String,
    },
    /// The contents are not 64 hexadecimal characters.
    #[error("{path}: {reason}")]
    Invalid {
        /// The file (empty when parsing bytes).
        path: String,
        /// What is wrong.
        reason: String,
    },
}

/// The error a method returns to its caller: the `e` of a failed `res` (SPEC 10.2).
#[derive(Debug, Clone, PartialEq, Error)]
#[error("{code}: {message}")]
pub struct RemoteError {
    /// A code of SPEC 11.1 or an application code (lowercase snake case).
    pub code: String,
    /// One sentence for people.
    pub message: String,
    /// Machine-readable details.
    pub details: Option<crate::codec::Value>,
    /// When retrying makes sense, after how many milliseconds.
    pub retry_ms: Option<u64>,
}

impl RemoteError {
    /// An error with a code and a message.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
            retry_ms: None,
        }
    }

    /// The `e` map of a `res`.
    pub fn to_value(&self) -> crate::codec::Value {
        use crate::codec::Value;
        let mut pairs: Vec<(&str, Value)> = vec![
            ("code", self.code.as_str().into()),
            ("msg", self.message.as_str().into()),
        ];
        if let Some(d) = &self.details {
            pairs.push(("details", d.clone()));
        }
        if let Some(r) = self.retry_ms {
            pairs.push(("retry_ms", r.into()));
        }
        crate::codec::map(pairs)
    }

    /// Reads an `e` map; missing or ill-typed fields get placeholders (the receiver of a
    /// `res` never fails on the text of an error).
    pub fn from_value(e: &crate::codec::Value) -> Self {
        use crate::codec::get;
        Self {
            code: get(e, "code")
                .and_then(|v| v.as_str())
                .unwrap_or("internal")
                .to_owned(),
            message: get(e, "msg")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
            details: get(e, "details").filter(|v| !v.is_nil()).cloned(),
            retry_ms: get(e, "retry_ms").and_then(|v| v.as_u64()),
        }
    }
}

/// Everything the transport and the connection can fail with.
#[derive(Debug, Error)]
pub enum Error {
    /// Nobody serves the pipe (`ERROR_FILE_NOT_FOUND`): the program is not running.
    #[error("nobody serves {0}")]
    PipeNotFound(String),
    /// Every instance stayed taken until the deadline (`ERROR_PIPE_BUSY`).
    #[error("every instance of {0} is busy")]
    PipeBusy(String),
    /// Another server already listens on the pipe (SPEC 3.2).
    #[error("another process already serves {0}")]
    EndpointInUse(String),
    /// This side ended the handshake (the `bye` was sent unless the pipe was already gone).
    #[error(transparent)]
    Handshake(#[from] crate::handshake::HandshakeFailure),
    /// The handshake did not complete in time (SPEC 15.1).
    #[error("the handshake did not complete in time")]
    HandshakeTimeout,
    /// The peer sent `bye` (during the handshake: why it refused).
    #[error("the peer closed the connection: {code}: {message}")]
    PeerBye {
        /// The `bye` code.
        code: String,
        /// The `bye` text.
        message: String,
    },
    /// The key file.
    #[error(transparent)]
    KeyFile(#[from] KeyFileError),
    /// There is no connection (never established, or closed before the call started).
    #[error("not connected")]
    NotConnected,
    /// The connection ended while the call waited (`disconnected`, SPEC 10.12).
    #[error("connection lost: {message}")]
    ConnectionLost {
        /// The peer's `bye` code, when it sent one.
        code: Option<String>,
        /// Why the connection ended.
        message: String,
    },
    /// The deadline passed before the `res`.
    #[error("no response in time")]
    CallTimeout,
    /// The caller cancelled.
    #[error("call cancelled")]
    CallCancelled,
    /// The owner answered with an error.
    #[error(transparent)]
    Remote(#[from] RemoteError),
    /// A message larger than the peer accepts.
    #[error("message of {size} bytes exceeds the peer's max_message {max}")]
    TooLarge {
        /// The encoded size.
        size: usize,
        /// The peer's limit.
        max: u32,
    },
    /// The peer broke the protocol (or this side could not encode).
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// A value that cannot be encoded.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// An operating-system error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
