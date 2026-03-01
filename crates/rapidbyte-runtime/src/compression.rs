//! IPC channel compression helpers for Arrow batch payload transfer.

use std::io::Cursor;

use bytes::Bytes;
use thiserror::Error;

pub use rapidbyte_types::compression::CompressionCodec;

/// Zstd level used for stage-to-stage IPC compression.
const ZSTD_COMPRESSION_LEVEL: i32 = 1;

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("zstd compression failed: {0}")]
    ZstdCompress(#[from] std::io::Error),
    #[error("lz4 decompression failed: {0}")]
    Lz4Decompress(String),
    #[error("zstd decompression failed: {0}")]
    ZstdDecompress(std::io::Error),
}

/// Compress bytes using the given codec.
///
/// # Errors
///
/// Returns an error if compression fails.
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match codec {
        CompressionCodec::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        CompressionCodec::Zstd => Ok(zstd::bulk::compress(data, ZSTD_COMPRESSION_LEVEL)
            .map_err(CompressionError::ZstdCompress)?),
    }
}

/// Decompress bytes using the given codec.
///
/// # Errors
///
/// Returns an error if decompression fails.
pub fn decompress(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match codec {
        CompressionCodec::Lz4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| CompressionError::Lz4Decompress(e.to_string())),
        CompressionCodec::Zstd => {
            zstd::decode_all(Cursor::new(data)).map_err(CompressionError::ZstdDecompress)
        }
    }
}

/// Compress bytes using the given codec. Returns owned `Bytes`.
///
/// # Errors
///
/// Returns an error if compression fails.
pub fn compress_bytes(codec: CompressionCodec, data: &[u8]) -> Result<Bytes, CompressionError> {
    compress(codec, data).map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"hello world repeated hello world repeated hello world repeated";
        let compressed = compress(CompressionCodec::Lz4, data).unwrap();
        let decompressed = decompress(CompressionCodec::Lz4, &compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"hello world repeated hello world repeated hello world repeated";
        let compressed = compress(CompressionCodec::Zstd, data).unwrap();
        let decompressed = decompress(CompressionCodec::Zstd, &compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_lz4_compresses_repetitive_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress(CompressionCodec::Lz4, &data).unwrap();
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_zstd_compresses_repetitive_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress(CompressionCodec::Zstd, &data).unwrap();
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_codec_serde_roundtrip() {
        let lz4 = serde_json::from_str::<CompressionCodec>("\"lz4\"").unwrap();
        let zstd = serde_json::from_str::<CompressionCodec>("\"zstd\"").unwrap();
        assert_eq!(lz4, CompressionCodec::Lz4);
        assert_eq!(zstd, CompressionCodec::Zstd);
    }

    #[test]
    fn test_empty_data_roundtrip() {
        let data = b"";
        let compressed = compress(CompressionCodec::Lz4, data).unwrap();
        let decompressed = decompress(CompressionCodec::Lz4, &compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());

        let compressed = compress(CompressionCodec::Zstd, data).unwrap();
        let decompressed = decompress(CompressionCodec::Zstd, &compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_invalid_lz4_decompress_returns_error() {
        let bad = vec![1u8, 2, 3, 4];
        assert!(decompress(CompressionCodec::Lz4, &bad).is_err());
    }

    #[test]
    fn test_invalid_zstd_decompress_returns_error() {
        let bad = vec![1u8, 2, 3, 4];
        assert!(decompress(CompressionCodec::Zstd, &bad).is_err());
    }

    #[test]
    fn test_lz4_roundtrip_bytes() {
        let data = bytes::Bytes::from_static(
            b"hello world repeated hello world repeated hello world repeated",
        );
        let compressed = compress_bytes(CompressionCodec::Lz4, &data).unwrap();
        let decompressed = decompress(CompressionCodec::Lz4, &compressed).unwrap();
        assert_eq!(data.as_ref(), decompressed.as_slice());
    }

    #[test]
    fn test_zstd_roundtrip_bytes() {
        let data = bytes::Bytes::from_static(
            b"hello world repeated hello world repeated hello world repeated",
        );
        let compressed = compress_bytes(CompressionCodec::Zstd, &data).unwrap();
        let decompressed = decompress(CompressionCodec::Zstd, &compressed).unwrap();
        assert_eq!(data.as_ref(), decompressed.as_slice());
    }
}
