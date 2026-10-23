//! The shared key file `<workspace>\ipc\key` (SPEC 8.1).
//!
//! Format: 64 hexadecimal characters (32 bytes), written lowercase, optionally followed by
//! ASCII whitespace (a line end). Created by whoever needs it first, atomically and never
//! replaced: a temporary file with a user-only DACL in the same folder, then a rename that
//! fails if `key` exists (in which case the existing key wins). The key never appears in an
//! error message or in `Debug` output.

use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::FromRawHandle;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Mutex;
use std::time::SystemTime;

use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_WRITE, MoveFileExW,
};

use crate::error::KeyFileError;
use crate::naming::{find_workspace, ipc_dir, new_run_id};
use crate::security::SecurityDescriptor;

/// The size of the key in bytes.
pub const KEY_SIZE: usize = 32;

/// A key. Compare with `==` only for tests; the protocol compares MACs, not keys.
pub type Key = [u8; KEY_SIZE];

/// `<workspace>\ipc\key`; the workspace is found with [`find_workspace`] when not given.
pub fn default_key_path(workspace: Option<&Path>) -> Result<PathBuf, KeyFileError> {
    let ws = match workspace {
        Some(w) => w.to_path_buf(),
        None => find_workspace().ok_or(KeyFileError::NoWorkspace)?,
    };
    Ok(ipc_dir(&ws).join("key"))
}

fn invalid(reason: impl Into<String>) -> KeyFileError {
    KeyFileError::Invalid {
        path: String::new(),
        reason: reason.into(),
    }
}

/// The 32 key bytes from the file's contents (a BOM and ASCII whitespace around the 64 hex
/// digits are accepted, upper and lower case).
pub fn parse_key(text: &[u8]) -> Result<Key, KeyFileError> {
    let text = text.strip_prefix(b"\xef\xbb\xbf").unwrap_or(text);
    let ws = |c: &u8| matches!(c, b' ' | b'\t' | b'\r' | b'\n');
    let start = text.iter().position(|c| !ws(c)).unwrap_or(text.len());
    let end = text.iter().rposition(|c| !ws(c)).map_or(start, |i| i + 1);
    let s = &text[start..end];
    if s.len() != 2 * KEY_SIZE {
        return Err(invalid(format!(
            "the key file must hold {} hex characters, found {}",
            2 * KEY_SIZE,
            s.len()
        )));
    }
    let nibble = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut key = [0u8; KEY_SIZE];
    for (i, pair) in s.as_chunks::<2>().0.iter().enumerate() {
        match (nibble(pair[0]), nibble(pair[1])) {
            (Some(h), Some(l)) => key[i] = (h << 4) | l,
            _ => {
                return Err(invalid(
                    "the key file holds characters that are not hexadecimal",
                ));
            }
        }
    }
    Ok(key)
}

/// The file contents for `key`: 64 lowercase hex digits and `\r\n`.
pub fn format_key(key: &Key) -> Vec<u8> {
    let mut out: Vec<u8> = key
        .iter()
        .flat_map(|b| format!("{b:02x}").into_bytes())
        .collect();
    out.extend_from_slice(b"\r\n");
    out
}

fn io_err(path: &Path, e: &io::Error) -> KeyFileError {
    KeyFileError::Io {
        path: path.display().to_string(),
        message: e.to_string(),
    }
}

/// Reads and parses the key file.
pub fn read_key(path: &Path) -> Result<Key, KeyFileError> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(KeyFileError::NotFound(path.display().to_string()));
        }
        Err(e) => return Err(io_err(path, &e)),
    };
    parse_key(&data).map_err(|e| match e {
        KeyFileError::Invalid { reason, .. } => KeyFileError::Invalid {
            path: path.display().to_string(),
            reason,
        },
        other => other,
    })
}

/// Creates `path` (it must not exist) with a DACL for the current user only and writes `data`.
fn write_new_user_only(path: &Path, data: &[u8]) -> io::Result<()> {
    let sd = SecurityDescriptor::user_only("FA")?;
    let attrs = sd.attributes();
    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: `wide` is NUL-terminated and `attrs` points at `sd`, alive for the call.
    let h = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_GENERIC_WRITE,
            0,
            &attrs,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            ptr::null_mut(),
        )
    };
    if h == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `h` is a valid handle we own; the File closes it.
    let mut f = unsafe { fs::File::from_raw_handle(h) };
    f.write_all(data)?;
    f.sync_all()
}

/// Renames `from` to `to`, failing if `to` exists (`MoveFileExW` without replace).
fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    let w = |p: &Path| -> Vec<u16> {
        p.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    };
    let (a, b) = (w(from), w(to));
    // SAFETY: both are NUL-terminated.
    if unsafe { MoveFileExW(a.as_ptr(), b.as_ptr(), 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Reads the key, creating the file first if it does not exist: atomic, never replacing. When
/// two creators race, one wins and the other reads the winner's key.
pub fn ensure_key(path: &Path) -> Result<Key, KeyFileError> {
    if path.exists() {
        return read_key(path);
    }
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(dir).map_err(|e| io_err(dir, &e))?;
    let mut key = [0u8; KEY_SIZE];
    getrandom::fill(&mut key).map_err(|e| io_err(path, &io::Error::other(e)))?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        new_run_id()
    ));
    write_new_user_only(&tmp, &format_key(&key)).map_err(|e| io_err(&tmp, &e))?;
    let renamed = rename_no_replace(&tmp, path);
    let _ = fs::remove_file(&tmp);
    match renamed {
        Ok(()) => {}
        // the key exists now: it wins (ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS or, while
        // the winner's handle is closing, ERROR_ACCESS_DENIED / sharing violation)
        Err(_) if path.exists() => {}
        Err(e) => return Err(io_err(path, &e)),
    }
    read_key(path)
}

#[derive(Default)]
struct State {
    stamp: Option<(SystemTime, u64)>,
    key: Key,
}

/// A key source that re-reads the file when its modification time or size changes (a cheap
/// `stat` per handshake), so a replaced key takes effect without a restart.
pub struct KeyFile {
    path: PathBuf,
    create: bool,
    state: Mutex<State>,
}

impl fmt::Debug for KeyFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyFile")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl KeyFile {
    /// A source for an existing key file (a missing file is an error at [`KeyFile::key`]).
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            create: false,
            state: Mutex::default(),
        }
    }

    /// A source that creates the file when it is missing (see [`ensure_key`]).
    pub fn create(path: impl Into<PathBuf>) -> Self {
        Self {
            create: true,
            ..Self::open(path)
        }
    }

    /// The file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The current key, re-reading the file if it changed since the last call.
    pub fn key(&self) -> Result<Key, KeyFileError> {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let meta = match fs::metadata(&self.path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if !self.create {
                    return Err(KeyFileError::NotFound(self.path.display().to_string()));
                }
                st.key = ensure_key(&self.path)?;
                st.stamp = stamp(&self.path);
                return Ok(st.key);
            }
            Err(e) => return Err(io_err(&self.path, &e)),
        };
        let now = meta.modified().ok().map(|t| (t, meta.len()));
        if now.is_none() || now != st.stamp {
            st.key = read_key(&self.path)?;
            st.stamp = now;
        }
        Ok(st.key)
    }
}

fn stamp(path: &Path) -> Option<(SystemTime, u64)> {
    let m = fs::metadata(path).ok()?;
    Some((m.modified().ok()?, m.len()))
}

/// Where the handshake gets its key from.
pub enum KeySource {
    /// A fixed key (tests, embedders).
    Bytes(Key),
    /// A key file, re-read when it changes.
    File(KeyFile),
}

impl fmt::Debug for KeySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(_) => f.write_str("KeySource::Bytes(..)"),
            Self::File(k) => write!(f, "KeySource::File({k:?})"),
        }
    }
}

impl KeySource {
    /// The current key.
    pub fn key(&self) -> Result<Key, KeyFileError> {
        match self {
            Self::Bytes(k) => Ok(*k),
            Self::File(f) => f.key(),
        }
    }
}
