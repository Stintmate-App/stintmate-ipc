//! The named pipe transport (SPEC 3): the server's listener and the client's open.
//!
//! The listener keeps one instance waiting at all times and creates the next one before it
//! hands a connected one over (3.2); its first instance carries `FILE_FLAG_FIRST_PIPE_INSTANCE`,
//! so a second server on the same name fails with [`Error::EndpointInUse`]. Every instance is
//! created with a DACL for the current user only and refuses remote clients.

use std::ffi::c_void;
use std::io;
use std::os::windows::io::AsRawHandle;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, PipeMode, ServerOptions,
};
use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_FILE_NOT_FOUND, ERROR_PIPE_BUSY};
use windows_sys::Win32::Storage::FileSystem::SECURITY_IDENTIFICATION;
use windows_sys::Win32::System::Pipes::{
    GetNamedPipeClientProcessId, GetNamedPipeServerProcessId, WaitNamedPipeW,
};

use crate::error::Error;
use crate::security::{SecurityDescriptor, wide};

/// Buffer size of each direction (SPEC 3.1 recommends 64 KiB).
const BUFFER: u32 = 64 * 1024;

/// One end of a connected pipe, whichever side opened it.
#[derive(Debug)]
pub enum Pipe {
    /// An instance accepted by a [`PipeListener`].
    Server(NamedPipeServer),
    /// A pipe opened by [`open_pipe`].
    Client(NamedPipeClient),
}

impl AsyncRead for Pipe {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Pipe::Server(p) => Pin::new(p).poll_read(cx, buf),
            Pipe::Client(p) => Pin::new(p).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Pipe::Server(p) => Pin::new(p).poll_write(cx, buf),
            Pipe::Client(p) => Pin::new(p).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Pipe::Server(p) => Pin::new(p).poll_flush(cx),
            Pipe::Client(p) => Pin::new(p).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Pipe::Server(p) => Pin::new(p).poll_shutdown(cx),
            Pipe::Client(p) => Pin::new(p).poll_shutdown(cx),
        }
    }
}

/// A client that connected to a listener.
#[derive(Debug)]
pub struct Accepted {
    /// The connected instance.
    pub pipe: Pipe,
    /// The client's process id as the operating system reports it (SPEC 3.4), read once right
    /// after the connection.
    pub client_pid: Option<u32>,
}

/// The server side of an endpoint's pipe.
pub struct PipeListener {
    path: String,
    sd: SecurityDescriptor,
    next: NamedPipeServer,
}

impl std::fmt::Debug for PipeListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipeListener")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

fn create_instance(
    path: &str,
    sd: &SecurityDescriptor,
    first: bool,
) -> io::Result<NamedPipeServer> {
    let mut attrs = sd.attributes();
    let mut options = ServerOptions::new();
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true)
        .pipe_mode(PipeMode::Byte)
        .in_buffer_size(BUFFER)
        .out_buffer_size(BUFFER);
    // SAFETY: `attrs` points at the descriptor owned by `sd`, which outlives the call.
    unsafe { options.create_with_security_attributes_raw(path, (&raw mut attrs).cast::<c_void>()) }
}

impl PipeListener {
    /// Creates the first instance of the pipe `path` (`\.\pipe\<name>`). Fails with
    /// [`Error::EndpointInUse`] when another server already has an instance of that name.
    pub fn bind(path: &str) -> Result<Self, Error> {
        let sd = SecurityDescriptor::user_only("GA")?;
        match create_instance(path, &sd, true) {
            Ok(next) => Ok(Self {
                path: path.to_owned(),
                sd,
                next,
            }),
            Err(e) if e.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) => {
                Err(Error::EndpointInUse(path.to_owned()))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// The pipe path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Waits for a client. The next instance exists before this returns, so the name never
    /// disappears while the listener lives. Cancel-safe: dropping the future loses no client.
    pub async fn accept(&mut self) -> Result<Accepted, Error> {
        self.next.connect().await?;
        let fresh = create_instance(&self.path, &self.sd, false)?;
        let connected = std::mem::replace(&mut self.next, fresh);
        let mut pid = 0u32;
        // SAFETY: the handle is a connected server instance owned by `connected`.
        let ok = unsafe { GetNamedPipeClientProcessId(connected.as_raw_handle(), &mut pid) };
        Ok(Accepted {
            pipe: Pipe::Server(connected),
            client_pid: (ok != 0 && pid != 0).then_some(pid),
        })
    }
}

/// Opens the pipe `path` as a client, waiting up to `deadline` for a free instance (SPEC 3.3):
/// identification-level impersonation only; `ERROR_FILE_NOT_FOUND` is [`Error::PipeNotFound`]
/// at once; `ERROR_PIPE_BUSY` waits with `WaitNamedPipe` and tries again until the deadline,
/// then is [`Error::PipeBusy`].
pub async fn open_pipe(path: &str, deadline: Duration) -> Result<Pipe, Error> {
    let end = Instant::now() + deadline;
    loop {
        match ClientOptions::new()
            .security_qos_flags(SECURITY_IDENTIFICATION)
            .open(path)
        {
            Ok(c) => return Ok(Pipe::Client(c)),
            Err(e) if e.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) => {
                return Err(Error::PipeNotFound(path.to_owned()));
            }
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                let left = end.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(Error::PipeBusy(path.to_owned()));
                }
                let w = wide(path);
                let ms = left.as_millis().clamp(1, 1000) as u32;
                // A bounded blocking wait (at most one second) off the runtime's workers.
                let waited = tokio::task::spawn_blocking(move || {
                    // SAFETY: `w` is NUL-terminated.
                    unsafe { WaitNamedPipeW(w.as_ptr(), ms) }
                })
                .await;
                if waited.is_err() {
                    return Err(Error::PipeBusy(path.to_owned()));
                }
                // The wait may have timed out or an instance freed up and was taken by
                // someone else: try to open again, the deadline decides.
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// The process id of the server of a pipe this side opened (`None` for an accepted one).
pub fn server_pid(pipe: &Pipe) -> Option<u32> {
    let Pipe::Client(c) = pipe else { return None };
    let mut pid = 0u32;
    // SAFETY: a valid pipe handle and a valid out pointer.
    let ok = unsafe { GetNamedPipeServerProcessId(c.as_raw_handle(), &mut pid) };
    (ok != 0 && pid != 0).then_some(pid)
}
