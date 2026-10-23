//! The clock of `t_ns` (SPEC 14): the performance counter in nanoseconds.

use std::sync::OnceLock;

use windows_sys::Win32::System::Performance::{QueryPerformanceCounter, QueryPerformanceFrequency};

fn frequency() -> i64 {
    static F: OnceLock<i64> = OnceLock::new();
    *F.get_or_init(|| {
        let mut f = 0i64;
        // SAFETY: `f` is a valid out-pointer.
        unsafe { QueryPerformanceFrequency(&mut f) };
        f.max(1)
    })
}

/// `QueryPerformanceCounter` in nanoseconds: monotonic, comparable between the processes of one
/// boot.
pub fn now_ns() -> i64 {
    let mut c = 0i64;
    // SAFETY: `c` is a valid out-pointer.
    unsafe { QueryPerformanceCounter(&mut c) };
    (i128::from(c) * 1_000_000_000 / i128::from(frequency())) as i64
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_clock_moves_forward() {
        let a = super::now_ns();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(super::now_ns() - a >= 1_000_000);
    }
}
