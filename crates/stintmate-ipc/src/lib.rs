//! Stintmate IPC v2: the reference implementation of the protocol described in `SPEC.md`.
//!
//! Skeleton (PRY-085 T0): the codec, handshake, transport and nodes arrive in the next tasks.

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
