//! Stintmate IPC v2: the reference implementation of the protocol described in `SPEC.md`.
//!
//! This crate holds, so far (PRY-085 T1), the **codec**: no I/O.
//!
//! * [`wire`]: frames, the incremental [`wire::FrameReader`] with the limits of SPEC 5.2, stream
//!   samples (5.3) and chunked messages (5.4).
//! * [`codec`]: the MessagePack subset of SPEC 6.1 (validated on decode, shortest forms on
//!   encode) and the shape of the messages of 10.1.
//! * [`layout`]: stream layouts and the packing of samples (9.3).
//! * [`decl`]: declarations of methods and channels (9.1-9.3).
//! * [`error`]: the error types.
//!
//! PRY-085 T2 adds the connection setup, still without I/O:
//!
//! * [`naming`]: endpoint names, the workspace and the registry (SPEC 4).
//! * [`keyfile`]: the shared key file (8.1).
//! * [`handshake`]: `hello`, `auth`, `welcome`, the MACs and the failures (8.2-8.8).
//! * [`security`]: the current user, user-only DACLs (shared with the pipe listener).
//!
//! PRY-085 T3 adds the I/O, on Tokio:
//!
//! * [`transport`]: the pipe listener and the client open (SPEC 3).
//! * [`connection`]: the handshake over a stream, reader/writer tasks, heartbeat, `bye`, calls,
//!   `prog`/`cancel`, and the [`connection::Handler`] the nodes build on.
//! * [`calls`]: the table of calls, with one completion path.

pub mod calls;
pub mod codec;
pub mod connection;
pub mod decl;
pub mod error;
pub mod handshake;
pub mod keyfile;
pub mod layout;
pub mod naming;
pub mod security;
pub mod transport;
pub mod wire;

pub use codec::Value;
pub use error::{
    DeclError, EncodeError, Error, KeyFileError, LayoutError, NameError, ProtocolError, RemoteError,
};
pub use keyfile::{KeyFile, KeySource};

/// The crate's version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The revision of `SPEC.md` this implementation follows.
pub const SPEC_REVISION: &str = "2.0";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_set() {
        assert!(!VERSION.is_empty());
        assert_eq!(SPEC_REVISION, "2.0");
    }
}
