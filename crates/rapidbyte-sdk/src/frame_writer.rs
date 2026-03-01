//! A `std::io::Write` adapter that streams bytes directly into a host frame
//! via `frame-write` calls, eliminating the guest-side `Vec<u8>` allocation
//! that would otherwise be needed for IPC encoding.

use std::io;

use crate::host_ffi::HostImports;

/// Wraps a host frame handle and implements `std::io::Write`.
///
/// Arrow's `StreamWriter` can write directly into this, streaming IPC
/// bytes across the WIT boundary without an intermediate guest buffer.
pub struct FrameWriter<'a> {
    handle: u64,
    imports: &'a dyn HostImports,
}

impl<'a> FrameWriter<'a> {
    pub fn new(handle: u64, imports: &'a dyn HostImports) -> Self {
        Self { handle, imports }
    }
}

impl io::Write for FrameWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.imports
            .frame_write(self.handle, buf)
            .map_err(io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
