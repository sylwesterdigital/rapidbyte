//! Host-side frame table for V3 zero-copy batch transport.
//!
//! Frames go through a strict lifecycle: **Writable** -> **Sealed** -> **Consumed/Dropped**.
//! Writable frames are backed by `BytesMut`; sealing freezes them to `Bytes`.
//! The table is embedded in `ComponentHostState` and called by host import
//! implementations for `alloc-frame`, `write-frame`, `seal-frame`, etc.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};

/// Errors returned by [`FrameTable`] operations.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The handle does not exist in the table.
    #[error("invalid frame handle: {0}")]
    InvalidHandle(u64),

    /// The frame is not sealed (required for read/consume).
    #[error("frame {0} is not sealed")]
    NotSealed(u64),

    /// The frame is already sealed (write/seal not allowed).
    #[error("frame {0} is already sealed")]
    AlreadySealed(u64),

    /// Read offset+len exceeds the frame length.
    #[error("out of bounds: offset={offset}, len={len}, frame_len={frame_len}")]
    OutOfBounds {
        /// Starting byte offset of the read.
        offset: u64,
        /// Requested byte length.
        len: u64,
        /// Actual byte length of the frame.
        frame_len: u64,
    },
}

/// Internal state of a single frame.
enum FrameState {
    Writable(BytesMut),
    Sealed(Bytes),
}

/// Host-managed table mapping opaque `u64` handles to frame buffers.
///
/// Used by the V3 transport layer to pass Arrow IPC batches between
/// guest (connector) and host without copying through linear memory.
pub struct FrameTable {
    frames: HashMap<u64, FrameState>,
    next_handle: u64,
}

impl FrameTable {
    /// Create an empty frame table. Handles start at 1 (0 is reserved as
    /// a sentinel / null handle).
    #[must_use]
    pub fn new() -> Self {
        Self {
            frames: HashMap::new(),
            next_handle: 1,
        }
    }

    /// Allocate a new writable frame with the given byte capacity.
    /// Returns the opaque handle.
    #[allow(clippy::cast_possible_truncation)] // host is 64-bit; u64 == usize
    pub fn alloc(&mut self, capacity: u64) -> u64 {
        let handle = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        let buf = BytesMut::with_capacity(capacity as usize);
        self.frames.insert(handle, FrameState::Writable(buf));
        handle
    }

    /// Append `chunk` to a writable frame. Returns the total byte length
    /// after appending.
    ///
    /// # Errors
    ///
    /// Returns [`FrameError::InvalidHandle`] if `handle` does not exist, or
    /// [`FrameError::AlreadySealed`] if the frame is sealed.
    pub fn write(&mut self, handle: u64, chunk: &[u8]) -> Result<u64, FrameError> {
        let state = self
            .frames
            .get_mut(&handle)
            .ok_or(FrameError::InvalidHandle(handle))?;
        match state {
            FrameState::Writable(buf) => {
                buf.extend_from_slice(chunk);
                Ok(buf.len() as u64)
            }
            FrameState::Sealed(_) => Err(FrameError::AlreadySealed(handle)),
        }
    }

    /// Freeze a writable frame into an immutable `Bytes` buffer.
    ///
    /// # Errors
    ///
    /// Returns [`FrameError::InvalidHandle`] if `handle` does not exist, or
    /// [`FrameError::AlreadySealed`] if the frame is already sealed.
    pub fn seal(&mut self, handle: u64) -> Result<(), FrameError> {
        let state = self
            .frames
            .remove(&handle)
            .ok_or(FrameError::InvalidHandle(handle))?;
        match state {
            FrameState::Writable(buf) => {
                self.frames.insert(handle, FrameState::Sealed(buf.freeze()));
                Ok(())
            }
            sealed @ FrameState::Sealed(_) => {
                self.frames.insert(handle, sealed);
                Err(FrameError::AlreadySealed(handle))
            }
        }
    }

    /// Get the byte length of a frame (works for both writable and sealed).
    ///
    /// # Errors
    ///
    /// Returns [`FrameError::InvalidHandle`] if `handle` does not exist.
    pub fn len(&self, handle: u64) -> Result<u64, FrameError> {
        let state = self
            .frames
            .get(&handle)
            .ok_or(FrameError::InvalidHandle(handle))?;
        let n = match state {
            FrameState::Writable(buf) => buf.len(),
            FrameState::Sealed(buf) => buf.len(),
        };
        Ok(n as u64)
    }

    /// Read a byte slice from a sealed frame. Returns a `Vec<u8>` copy of
    /// the requested range.
    ///
    /// # Errors
    ///
    /// Returns [`FrameError::InvalidHandle`] if `handle` does not exist,
    /// [`FrameError::NotSealed`] if the frame is still writable, or
    /// [`FrameError::OutOfBounds`] if `offset + len` exceeds the frame length.
    #[allow(clippy::cast_possible_truncation)] // host is 64-bit; u64 == usize
    pub fn read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, FrameError> {
        let state = self
            .frames
            .get(&handle)
            .ok_or(FrameError::InvalidHandle(handle))?;
        match state {
            FrameState::Writable(_) => Err(FrameError::NotSealed(handle)),
            FrameState::Sealed(buf) => {
                let off = offset as usize;
                let l = len as usize;
                if off.saturating_add(l) > buf.len() {
                    return Err(FrameError::OutOfBounds {
                        offset,
                        len,
                        frame_len: buf.len() as u64,
                    });
                }
                Ok(buf[off..off + l].to_vec())
            }
        }
    }

    /// Remove a sealed frame from the table and return its `Bytes`.
    /// This is the primary way `emit-batch` takes ownership of frame data.
    ///
    /// If the frame is not sealed, the frame is **put back** and an error
    /// is returned (the handle remains valid).
    ///
    /// # Errors
    ///
    /// Returns [`FrameError::InvalidHandle`] if `handle` does not exist, or
    /// [`FrameError::NotSealed`] if the frame is still writable.
    pub fn consume(&mut self, handle: u64) -> Result<Bytes, FrameError> {
        let state = self
            .frames
            .remove(&handle)
            .ok_or(FrameError::InvalidHandle(handle))?;
        match state {
            FrameState::Sealed(buf) => Ok(buf),
            FrameState::Writable(_) => {
                // Put it back so the handle stays valid.
                self.frames.insert(handle, state);
                Err(FrameError::NotSealed(handle))
            }
        }
    }

    /// Insert existing `Bytes` as a sealed read-only frame. Returns the handle.
    /// Used by `next-batch` to wrap incoming channel bytes as a guest-readable
    /// frame handle.
    pub fn insert_sealed(&mut self, data: Bytes) -> u64 {
        let handle = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        self.frames.insert(handle, FrameState::Sealed(data));
        handle
    }

    /// Remove a frame from the table. No-op if the handle does not exist.
    pub fn drop_frame(&mut self, handle: u64) {
        self.frames.remove(&handle);
    }

    /// Drop all frames (teardown).
    pub fn clear(&mut self) {
        self.frames.clear();
    }

    /// Number of live frames in the table (diagnostics).
    #[must_use]
    pub fn live_count(&self) -> usize {
        self.frames.len()
    }
}

impl Default for FrameTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FrameTable {
    fn drop(&mut self) {
        let count = self.frames.len();
        if count > 0 {
            tracing::debug!(
                count,
                "FrameTable dropped with live frames — cleaning up orphaned handles"
            );
            self.frames.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_write_seal_read_drop() {
        let mut table = FrameTable::new();

        let h = table.alloc(64);
        assert_eq!(table.live_count(), 1);

        let len = table.write(h, b"hello ").unwrap();
        assert_eq!(len, 6);

        let len = table.write(h, b"world").unwrap();
        assert_eq!(len, 11);

        assert_eq!(table.len(h).unwrap(), 11);

        table.seal(h).unwrap();

        let data = table.read(h, 0, 11).unwrap();
        assert_eq!(&data, b"hello world");

        let slice = table.read(h, 6, 5).unwrap();
        assert_eq!(&slice, b"world");

        table.drop_frame(h);
        assert_eq!(table.live_count(), 0);
    }

    #[test]
    fn write_after_seal_fails() {
        let mut table = FrameTable::new();
        let h = table.alloc(16);
        table.write(h, b"data").unwrap();
        table.seal(h).unwrap();

        let err = table.write(h, b"more").unwrap_err();
        assert!(
            matches!(err, FrameError::AlreadySealed(handle) if handle == h),
            "expected AlreadySealed, got {err:?}"
        );
    }

    #[test]
    fn double_seal_fails() {
        let mut table = FrameTable::new();
        let h = table.alloc(16);
        table.write(h, b"data").unwrap();
        table.seal(h).unwrap();

        let err = table.seal(h).unwrap_err();
        assert!(
            matches!(err, FrameError::AlreadySealed(handle) if handle == h),
            "expected AlreadySealed, got {err:?}"
        );
    }

    #[test]
    fn read_before_seal_fails() {
        let mut table = FrameTable::new();
        let h = table.alloc(16);
        table.write(h, b"data").unwrap();

        let err = table.read(h, 0, 4).unwrap_err();
        assert!(
            matches!(err, FrameError::NotSealed(handle) if handle == h),
            "expected NotSealed, got {err:?}"
        );
    }

    #[test]
    fn read_out_of_bounds_fails() {
        let mut table = FrameTable::new();
        let h = table.alloc(16);
        table.write(h, b"data").unwrap();
        table.seal(h).unwrap();

        let err = table.read(h, 2, 5).unwrap_err();
        assert!(
            matches!(
                err,
                FrameError::OutOfBounds {
                    offset: 2,
                    len: 5,
                    frame_len: 4,
                }
            ),
            "expected OutOfBounds, got {err:?}"
        );
    }

    #[test]
    fn consume_returns_bytes_and_removes_handle() {
        let mut table = FrameTable::new();
        let h = table.alloc(16);
        table.write(h, b"payload").unwrap();
        table.seal(h).unwrap();

        let bytes = table.consume(h).unwrap();
        assert_eq!(&bytes[..], b"payload");
        assert_eq!(table.live_count(), 0);

        // Handle is gone.
        let err = table.len(h).unwrap_err();
        assert!(matches!(err, FrameError::InvalidHandle(_)));
    }

    #[test]
    fn consume_unsealed_fails() {
        let mut table = FrameTable::new();
        let h = table.alloc(16);
        table.write(h, b"data").unwrap();

        let err = table.consume(h).unwrap_err();
        assert!(
            matches!(err, FrameError::NotSealed(handle) if handle == h),
            "expected NotSealed, got {err:?}"
        );

        // Handle must still exist after failed consume.
        assert_eq!(table.live_count(), 1);
        assert_eq!(table.len(h).unwrap(), 4);
    }

    #[test]
    fn insert_sealed_creates_readonly_frame() {
        let mut table = FrameTable::new();
        let data = Bytes::from_static(b"readonly data");
        let h = table.insert_sealed(data);

        // Can read it.
        assert_eq!(table.len(h).unwrap(), 13);
        let slice = table.read(h, 0, 13).unwrap();
        assert_eq!(&slice, b"readonly data");

        // Cannot write to it.
        let err = table.write(h, b"nope").unwrap_err();
        assert!(matches!(err, FrameError::AlreadySealed(_)));

        // Cannot seal again.
        let err = table.seal(h).unwrap_err();
        assert!(matches!(err, FrameError::AlreadySealed(_)));
    }

    #[test]
    fn clear_drops_all_frames() {
        let mut table = FrameTable::new();
        table.alloc(8);
        table.alloc(8);
        let h3 = table.alloc(8);
        table.write(h3, b"x").unwrap();
        table.seal(h3).unwrap();
        assert_eq!(table.live_count(), 3);

        table.clear();
        assert_eq!(table.live_count(), 0);
    }

    #[test]
    fn drop_nonexistent_is_noop() {
        let mut table = FrameTable::new();
        // Should not panic.
        table.drop_frame(999);
        table.drop_frame(0);
        table.drop_frame(u64::MAX);
        assert_eq!(table.live_count(), 0);
    }
}
