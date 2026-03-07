//! AsyncRead/AsyncWrite adapter over Rapidbyte host-proxied sockets.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::error::PluginError;
use crate::host_ffi::{self, SocketReadResult, SocketWriteResult};

/// TCP stream backed by host-side socket operations.
pub struct HostTcpStream {
    handle: u64,
    closed: bool,
}

impl HostTcpStream {
    /// Connect to a remote host via the host-proxied TCP stack.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the host denies the connection or the TCP handshake fails.
    pub fn connect(host: &str, port: u16) -> Result<Self, PluginError> {
        let handle = host_ffi::connect_tcp(host, port)?;
        Ok(Self {
            handle,
            closed: false,
        })
    }

    pub fn handle(&self) -> u64 {
        self.handle
    }

    fn close_inner(&mut self) {
        if !self.closed {
            host_ffi::socket_close(self.handle);
            self.closed = true;
        }
    }
}

impl Drop for HostTcpStream {
    fn drop(&mut self) {
        self.close_inner();
    }
}

fn to_io_error(err: PluginError) -> io::Error {
    io::Error::other(err)
}

impl AsyncRead for HostTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Ok(()));
        }

        let remaining = buf.remaining();
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        match host_ffi::socket_read(this.handle, remaining as u64) {
            Ok(SocketReadResult::Data(data)) => {
                if data.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                let copy_len = data.len().min(buf.remaining());
                buf.put_slice(&data[..copy_len]);
                Poll::Ready(Ok(()))
            }
            Ok(SocketReadResult::Eof) => Poll::Ready(Ok(())),
            Ok(SocketReadResult::WouldBlock) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(to_io_error(err))),
        }
    }
}

impl AsyncWrite for HostTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "socket closed",
            )));
        }

        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }

        match host_ffi::socket_write(this.handle, data) {
            Ok(SocketWriteResult::Written(n)) => Poll::Ready(Ok(n as usize)),
            Ok(SocketWriteResult::WouldBlock) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(to_io_error(err))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.close_inner();
        Poll::Ready(Ok(()))
    }
}
