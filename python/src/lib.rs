//! The native module `stintmate_ipc._native` (skeleton; the binding arrives in PRY-085 T6-T7).

use pyo3::prelude::*;

#[pymodule]
mod _native {
    use super::*;

    /// The library's version.
    #[pyfunction]
    fn version() -> &'static str {
        stintmate_ipc::VERSION
    }

    /// The revision of SPEC.md the library follows.
    #[pyfunction]
    fn spec_revision() -> &'static str {
        stintmate_ipc::SPEC_REVISION
    }
}
