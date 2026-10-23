//! The C ABI of Stintmate IPC v2 (skeleton; the API arrives in PRY-085 T8).

use std::ffi::{CStr, c_char};

static VERSION: &CStr = c"0.2.0";

/// The library's version as a static NUL-terminated string (never freed by the caller).
#[unsafe(no_mangle)]
pub extern "C" fn stintmate_ipc_version() -> *const c_char {
    VERSION.as_ptr()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_matches_the_crate() {
        let v = unsafe { CStr::from_ptr(stintmate_ipc_version()) };
        assert_eq!(v.to_str().unwrap(), stintmate_ipc::VERSION);
    }
}
