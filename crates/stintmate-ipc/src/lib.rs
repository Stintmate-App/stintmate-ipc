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

pub mod codec;
pub mod decl;
pub mod error;
pub mod layout;
pub mod wire;

pub use codec::Value;
pub use error::{DeclError, EncodeError, LayoutError, ProtocolError};

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
