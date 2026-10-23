//! Windows security helpers: the current user, a DACL for that user only (SPEC 3.2 and 8.1)
//! and the liveness test of a pipe (SPEC 4.4). The pipe listener of the transport reuses
//! [`SecurityDescriptor`].

use std::io;
use std::ptr;

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_FILE_NOT_FOUND, GetLastError, HANDLE, LocalFree,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSecurityDescriptorToStringSecurityDescriptorW, ConvertSidToStringSidW,
    ConvertStringSecurityDescriptorToSecurityDescriptorW, GetNamedSecurityInfoW, SDDL_REVISION_1,
    SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    DACL_SECURITY_INFORMATION, GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES,
    TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::System::Pipes::WaitNamedPipeW;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
use windows_sys::Win32::System::WindowsProgramming::GetUserNameW;

/// `s` as a NUL-terminated UTF-16 string.
pub fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// A NUL-terminated UTF-16 pointer to a Rust string, lossless.
fn from_wide_ptr(p: *const u16) -> String {
    let mut len = 0;
    // SAFETY: `p` is a NUL-terminated string returned by the system.
    unsafe {
        while *p.add(len) != 0 {
            len += 1;
        }
        String::from_utf16_lossy(std::slice::from_raw_parts(p, len))
    }
}

/// The Windows user name of this process (`GetUserNameW`), as the system spells it.
pub fn user_name() -> io::Result<String> {
    let mut buf = [0u16; 257];
    let mut len = buf.len() as u32;
    // SAFETY: `buf` holds `len` UTF-16 units.
    if unsafe { GetUserNameW(buf.as_mut_ptr(), &mut len) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let n = (len as usize).saturating_sub(1).min(buf.len());
    Ok(String::from_utf16_lossy(&buf[..n]))
}

/// The SID of the user of this process, in string form (`S-1-5-21-...`).
pub fn current_user_sid() -> io::Result<String> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: standard token query on the current process.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        let mut needed = 0u32;
        // SAFETY: the first call only asks for the size.
        unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) };
        if needed == 0 {
            return Err(io::Error::last_os_error());
        }
        // u64 elements: the buffer is aligned for TOKEN_USER.
        let mut buf = vec![0u64; (needed as usize).div_ceil(8)];
        // SAFETY: `buf` has at least `needed` bytes.
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buf.as_mut_ptr().cast(),
                needed,
                &mut needed,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the buffer now holds a TOKEN_USER.
        let sid = unsafe { (*buf.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        let mut s: *mut u16 = ptr::null_mut();
        // SAFETY: `sid` is valid while `buf` lives; `s` is freed with LocalFree.
        if unsafe { ConvertSidToStringSidW(sid, &mut s) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let out = from_wide_ptr(s);
        // SAFETY: allocated by ConvertSidToStringSidW.
        unsafe { LocalFree(s.cast()) };
        Ok(out)
    })();
    // SAFETY: `token` was opened above.
    unsafe { CloseHandle(token) };
    result
}

/// The SDDL of a protected DACL that grants `rights` (`"FA"` file all, `"GA"` generic all) to
/// the current user only: `D:P(A;;<rights>;;;<user SID>)`.
pub fn user_only_sddl(rights: &str) -> io::Result<String> {
    Ok(format!("D:P(A;;{rights};;;{})", current_user_sid()?))
}

/// A security descriptor built from SDDL, freed on drop.
pub struct SecurityDescriptor {
    psd: PSECURITY_DESCRIPTOR,
}

// SAFETY: the descriptor is plain memory, never mutated after creation.
unsafe impl Send for SecurityDescriptor {}
// SAFETY: as above, read-only.
unsafe impl Sync for SecurityDescriptor {}

impl SecurityDescriptor {
    /// Parses an SDDL string.
    pub fn from_sddl(sddl: &str) -> io::Result<Self> {
        let w = wide(sddl);
        let mut psd: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `w` is NUL-terminated; `psd` is freed in Drop.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                w.as_ptr(),
                SDDL_REVISION_1,
                &mut psd,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { psd })
    }

    /// A descriptor for the current user only, with `rights` as in [`user_only_sddl`].
    pub fn user_only(rights: &str) -> io::Result<Self> {
        Self::from_sddl(&user_only_sddl(rights)?)
    }

    /// A non-inheritable `SECURITY_ATTRIBUTES` pointing at this descriptor; valid while
    /// `self` lives.
    pub fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.psd,
            bInheritHandle: 0,
        }
    }
}

impl Drop for SecurityDescriptor {
    fn drop(&mut self) {
        // SAFETY: allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW.
        unsafe { LocalFree(self.psd.cast()) };
    }
}

/// The DACL of a file or folder as SDDL (`D:...`), for tests and diagnostics.
pub fn dacl_sddl_of_path(path: &std::path::Path) -> io::Result<String> {
    let w = wide(&path.to_string_lossy());
    let mut psd: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: `w` is NUL-terminated; `psd` is freed below.
    let rc = unsafe {
        GetNamedSecurityInfoW(
            w.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut psd,
        )
    };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc as i32));
    }
    let mut s: *mut u16 = ptr::null_mut();
    // SAFETY: `psd` is a valid descriptor; `s` is freed below.
    let ok = unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            psd,
            SDDL_REVISION_1,
            DACL_SECURITY_INFORMATION,
            &mut s,
            ptr::null_mut(),
        )
    };
    let result = if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(from_wide_ptr(s))
    };
    // SAFETY: both were allocated by the system with LocalAlloc.
    unsafe {
        if !s.is_null() {
            LocalFree(s.cast());
        }
        LocalFree(psd.cast());
    }
    result
}

/// Whether the named pipe `path` (`\.\pipe\...`) exists: `WaitNamedPipe(path, 1)` does not
/// fail with `ERROR_FILE_NOT_FOUND` (SPEC 4.4).
pub fn pipe_exists(path: &str) -> bool {
    let w = wide(path);
    // SAFETY: `w` is NUL-terminated.
    if unsafe { WaitNamedPipeW(w.as_ptr(), 1) } != 0 {
        return true;
    }
    // SAFETY: reads the thread's last error.
    unsafe { GetLastError() != ERROR_FILE_NOT_FOUND }
}
