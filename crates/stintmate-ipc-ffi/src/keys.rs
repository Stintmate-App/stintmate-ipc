//! Keys, naming and layouts (spec 2.4): the stateless helpers of the C ABI.

use std::ffi::c_char;
use std::mem::offset_of;
use std::path::Path;

use stintmate_ipc::codec::{self, Value};
use stintmate_ipc::error::{Error, KeyFileError};
use stintmate_ipc::keyfile::{self, KEY_SIZE, Key, KeyFile, KeySource};
use stintmate_ipc::layout::Layout;
use stintmate_ipc::{decl, naming};

use crate::{
    STINTMATE_IPC_OK, cstr, fail, guard, invalid, opt_cstr, put_buffer, put_value,
    stintmate_ipc_buffer, value_arg,
};

/// Where a key comes from: a key file, or 32 raw bytes.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct stintmate_ipc_key_source {
    /// `sizeof` this struct.
    pub struct_size: usize,
    /// The key file; null with `key` null: the default key file.
    pub path: *const c_char,
    /// 32 raw bytes, instead of a file.
    pub key: *const u8,
    /// The length of `key` (32).
    pub key_len: usize,
    /// Create the file when it is missing (with `path`, or the default path).
    pub create: i32,
}

/// Whether the field at `offset` of `len` bytes is inside the `size` the caller declared.
fn fits(size: usize, offset: usize, len: usize) -> bool {
    size >= offset + len
}

/// Reads a key source; null is the default key file (read, never created).
///
/// # Safety
/// `src` must be null or point to a struct that follows the field docs for its `struct_size`.
pub(crate) unsafe fn key_source_arg(
    src: *const stintmate_ipc_key_source,
) -> Result<KeySource, i32> {
    if src.is_null() {
        return default_source(false);
    }
    // SAFETY: the first field is readable per the contract.
    let size = unsafe { src.cast::<usize>().read_unaligned() };
    if size < offset_of!(stintmate_ipc_key_source, path) + size_of::<*const c_char>() {
        return Err(invalid("key.struct_size is too small"));
    }
    // SAFETY (all reads below): each field is read only when it lies inside `struct_size`.
    let (path, key, key_len, create) = unsafe {
        let field = |off: usize| src.cast::<u8>().add(off);
        let path = field(offset_of!(stintmate_ipc_key_source, path))
            .cast::<*const c_char>()
            .read_unaligned();
        let key = if fits(
            size,
            offset_of!(stintmate_ipc_key_source, key),
            size_of::<*const u8>(),
        ) {
            field(offset_of!(stintmate_ipc_key_source, key))
                .cast::<*const u8>()
                .read_unaligned()
        } else {
            std::ptr::null()
        };
        let key_len = if fits(
            size,
            offset_of!(stintmate_ipc_key_source, key_len),
            size_of::<usize>(),
        ) {
            field(offset_of!(stintmate_ipc_key_source, key_len))
                .cast::<usize>()
                .read_unaligned()
        } else {
            0
        };
        let create = if fits(
            size,
            offset_of!(stintmate_ipc_key_source, create),
            size_of::<i32>(),
        ) {
            field(offset_of!(stintmate_ipc_key_source, create))
                .cast::<i32>()
                .read_unaligned()
        } else {
            0
        };
        (path, key, key_len, create)
    };
    let create = create != 0;
    if !key.is_null() {
        if !path.is_null() {
            return Err(invalid("key.path and key.key are exclusive"));
        }
        if key_len != KEY_SIZE {
            return Err(invalid("key.key_len must be 32"));
        }
        let mut k: Key = [0; KEY_SIZE];
        // SAFETY: valid for 32 bytes per the field docs.
        k.copy_from_slice(unsafe { std::slice::from_raw_parts(key, KEY_SIZE) });
        return Ok(KeySource::Bytes(k));
    }
    // SAFETY: NUL-terminated or null per the field docs.
    match unsafe { opt_cstr(path, "key.path") }? {
        Some(p) => Ok(KeySource::File(if create {
            KeyFile::create(p)
        } else {
            KeyFile::open(p)
        })),
        None => default_source(create),
    }
}

fn default_source(create: bool) -> Result<KeySource, i32> {
    keyfile::default_key_path(None)
        .map(|p| {
            KeySource::File(if create {
                KeyFile::create(p)
            } else {
                KeyFile::open(p)
            })
        })
        .map_err(|e| fail(&Error::KeyFile(e)))
}

/// Writes a 32-byte key to `out` (null: dropped).
///
/// # Safety
/// `out` null or valid for a write.
unsafe fn write_key(out: *mut stintmate_ipc_buffer, key: &Key) {
    // SAFETY: forwarded contract.
    unsafe { put_buffer(out, key.to_vec()) };
}

/// Reads the key file at `path`, creating it first when missing (atomic, never replacing).
/// The 32 key bytes go to `key_out` (may be null).
///
/// # Safety
/// `path` NUL-terminated; `key_out` null or valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_ensure_key(
    path: *const c_char,
    key_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let path = match unsafe { cstr(path, "path") } {
            Ok(p) => p,
            Err(s) => return s,
        };
        match keyfile::ensure_key(Path::new(path)) {
            Ok(k) => {
                // SAFETY: forwarded contract.
                unsafe { write_key(key_out, &k) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&Error::KeyFile(e)),
        }
    })
}

/// Reads the key file at `path` (it must exist) into `key_out` as 32 raw bytes.
///
/// # Safety
/// `path` NUL-terminated; `key_out_buf` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_read_key(
    path: *const c_char,
    key_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let path = match unsafe { cstr(path, "path") } {
            Ok(p) => p,
            Err(s) => return s,
        };
        match keyfile::read_key(Path::new(path)) {
            Ok(k) => {
                // SAFETY: forwarded contract.
                unsafe { write_key(key_out, &k) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&Error::KeyFile(e)),
        }
    })
}

/// Parses key file text (64 hex digits, a BOM and whitespace around are accepted) into 32 raw
/// bytes.
///
/// # Safety
/// `text` valid for `len` bytes; `key_out_buf` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_parse_key(
    text: *const u8,
    len: usize,
    key_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if text.is_null() && len > 0 {
            return invalid("text is null");
        }
        let bytes = if len == 0 {
            &[][..]
        } else {
            // SAFETY: valid for `len` bytes per the contract.
            unsafe { std::slice::from_raw_parts(text, len) }
        };
        match keyfile::parse_key(bytes) {
            Ok(k) => {
                // SAFETY: forwarded contract.
                unsafe { write_key(key_out, &k) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&Error::KeyFile(e)),
        }
    })
}

/// Formats 32 raw key bytes as key file text (64 lowercase hex digits and a newline).
///
/// # Safety
/// `key` valid for `len` bytes; `text_out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_format_key(
    key: *const u8,
    len: usize,
    text_out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        if key.is_null() || len != KEY_SIZE {
            return invalid("key must be 32 bytes");
        }
        let mut k: Key = [0; KEY_SIZE];
        // SAFETY: valid for 32 bytes per the contract.
        k.copy_from_slice(unsafe { std::slice::from_raw_parts(key, KEY_SIZE) });
        // SAFETY: `text_out` per the contract.
        unsafe { put_buffer(text_out, keyfile::format_key(&k)) };
        STINTMATE_IPC_OK
    })
}

/// Writes the default key file path (`<workspace>\ipc\key`) into `out`; `workspace` may be
/// null to find it (`STINTMATE_WORKSPACE`, then the settings file).
///
/// # Safety
/// `workspace` NUL-terminated or null; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_default_key_path(
    workspace: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let ws = match unsafe { opt_cstr(workspace, "workspace") } {
            Ok(w) => w,
            Err(s) => return s,
        };
        match keyfile::default_key_path(ws.map(Path::new)) {
            Ok(p) => {
                // SAFETY: `out` per the contract.
                unsafe { put_buffer(out, p.to_string_lossy().into_owned().into_bytes()) };
                STINTMATE_IPC_OK
            }
            Err(e) => fail(&Error::KeyFile(e)),
        }
    })
}

fn name_result(
    r: Result<String, stintmate_ipc::error::NameError>,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    match r {
        Ok(s) => {
            // SAFETY: `out` is valid per the callers' contracts.
            unsafe { put_buffer(out, s.into_bytes()) };
            STINTMATE_IPC_OK
        }
        Err(e) => invalid(&e.to_string()),
    }
}

/// The `<user>` part of endpoint names: `user` ASCII-lowercased, or the current Windows user
/// when `user` is null.
///
/// # Safety
/// `user` NUL-terminated or null; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_pipe_user(
    user: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        match unsafe { opt_cstr(user, "user") } {
            Ok(Some(u)) => name_result(Ok(naming::ascii_lower(u)), out),
            Ok(None) => name_result(naming::pipe_user(), out),
            Err(s) => s,
        }
    })
}

/// Shared by `short_name` and `pipe_path`.
///
/// # Safety
/// Strings NUL-terminated (`process` and `user` may be null); `out` valid for a write.
unsafe fn named(
    app: *const c_char,
    process: *const c_char,
    user: *const c_char,
    out: *mut stintmate_ipc_buffer,
    f: impl FnOnce(&str, Option<&str>, Option<&str>) -> Result<String, stintmate_ipc::error::NameError>,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of the callers.
        let (app, process, user) = unsafe {
            match (
                cstr(app, "app"),
                opt_cstr(process, "process"),
                opt_cstr(user, "user"),
            ) {
                (Ok(a), Ok(p), Ok(u)) => (a, p, u),
                (Err(s), _, _) | (_, Err(s), _) | (_, _, Err(s)) => return s,
            }
        };
        name_result(f(app, process, user), out)
    })
}

/// `stintmate-<app>[-<process>]-<user>`: the pipe name without `\\.\pipe\`. `process` and
/// `user` may be null (no process; the current user).
///
/// # Safety
/// Strings NUL-terminated or null where allowed; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_short_name(
    app: *const c_char,
    process: *const c_char,
    user: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    crate::run_status(|| {
        // SAFETY: the contract of this function.
        unsafe {
            named(app, process, user, out, |a, p, u| match u {
                Some(u) => naming::short_name_for(a, p, u),
                None => naming::short_name(a, p),
            })
        }
    })
}

/// `\\.\pipe\stintmate-<app>[-<process>]-<user>`; `process` and `user` may be null (no
/// process; the current user).
///
/// # Safety
/// As [`stintmate_ipc_short_name`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_pipe_path(
    app: *const c_char,
    process: *const c_char,
    user: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    crate::run_status(|| {
        // SAFETY: the contract of this function.
        unsafe {
            named(app, process, user, out, |a, p, u| match u {
                Some(u) => naming::pipe_path_for(a, p, u),
                None => naming::pipe_path(a, p),
            })
        }
    })
}

/// The workspace folder (`STINTMATE_WORKSPACE`, then the settings file); an empty buffer when
/// there is none.
///
/// # Safety
/// `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_find_workspace(out: *mut stintmate_ipc_buffer) -> i32 {
    guard(|| {
        let ws = naming::find_workspace()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        // SAFETY: `out` per the contract.
        unsafe { put_buffer(out, ws.into_bytes()) };
        STINTMATE_IPC_OK
    })
}

/// `<workspace>\ipc\endpoints`; `workspace` null: the found workspace (else
/// `STINTMATE_IPC_E_KEYFILE`, as for the key file).
///
/// # Safety
/// `workspace` NUL-terminated or null; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_registry_dir(
    workspace: *const c_char,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let ws = match unsafe { opt_cstr(workspace, "workspace") } {
            Ok(w) => w
                .map(std::path::PathBuf::from)
                .or_else(naming::find_workspace),
            Err(s) => return s,
        };
        match ws {
            Some(w) => {
                let dir = naming::registry_dir(&w);
                // SAFETY: `out` per the contract.
                unsafe { put_buffer(out, dir.to_string_lossy().into_owned().into_bytes()) };
                STINTMATE_IPC_OK
            }
            None => fail(&Error::KeyFile(KeyFileError::NoWorkspace)),
        }
    })
}

/// 1 when `app` is a valid app id, else 0.
///
/// # Safety
/// `app` NUL-terminated or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_valid_app(app: *const c_char) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        match unsafe { cstr(app, "app") } {
            Ok(a) => i32::from(naming::valid_app(a)),
            Err(_) => 0,
        }
    })
}

/// 1 when `name` is a valid method or channel name, else 0.
///
/// # Safety
/// `name` NUL-terminated or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_valid_name(name: *const c_char) -> i32 {
    crate::run(0, || {
        // SAFETY: the contract of this function.
        match unsafe { cstr(name, "name") } {
            Ok(n) => i32::from(decl::valid_name(n)),
            Err(_) => 0,
        }
    })
}

/// Validates a layout (MessagePack `{name?, fields: [[name, type, count?], ...]}`) and returns
/// its canonical wire form with offsets and size: `{name?, size, fields: [[name, type, count,
/// offset], ...]}`.
///
/// # Safety
/// `layout` valid for `len` bytes; `out` valid for a write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stintmate_ipc_layout_normalize(
    layout: *const u8,
    len: usize,
    out: *mut stintmate_ipc_buffer,
) -> i32 {
    guard(|| {
        // SAFETY: the contract of this function.
        let value = match unsafe { value_arg(layout, len, "layout") } {
            Ok(v) => v,
            Err(s) => return s,
        };
        let layout = match Layout::from_value(&value) {
            Ok(l) => l,
            Err(e) => return invalid(&e.to_string()),
        };
        let mut offset = 0u64;
        let mut fields = Vec::new();
        for f in layout.fields() {
            fields.push(Value::Array(vec![
                Value::from(f.name.as_str()),
                Value::from(f.ty.name()),
                Value::from(f.count as u64),
                Value::from(offset),
            ]));
            offset += (f.ty.size() * f.count) as u64;
        }
        let mut pairs = vec![
            ("size", Value::from(layout.size() as u64)),
            ("fields", Value::Array(fields)),
        ];
        if let Some(n) = layout.name() {
            pairs.push(("name", Value::from(n)));
        }
        // SAFETY: `out` per the contract.
        unsafe { put_value(out, &codec::map(pairs)) }
    })
}
