//! Signed preview tickets with HMAC-SHA256 and preview metadata store.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

const HMAC_LEN: usize = 32;

#[derive(Debug, Error)]
pub enum TicketError {
    #[error("ticket too short")]
    TooShort,
    #[error("invalid HMAC signature")]
    InvalidSignature,
    #[error("ticket expired")]
    Expired,
    #[error("malformed payload: {0}")]
    MalformedPayload(String),
}

/// Payload embedded in a signed ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketPayload {
    pub run_id: String,
    pub task_id: String,
    pub stream_name: String,
    pub lease_epoch: u64,
    pub expires_at_unix: u64,
}

impl TicketPayload {
    /// Serialize to bytes: length-prefixed strings + u64 fields.
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_string(&mut buf, &self.run_id);
        write_string(&mut buf, &self.task_id);
        write_string(&mut buf, &self.stream_name);
        buf.extend_from_slice(&self.lease_epoch.to_le_bytes());
        buf.extend_from_slice(&self.expires_at_unix.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    fn from_bytes(data: &[u8]) -> Result<Self, TicketError> {
        let mut cursor = 0;
        let run_id = read_string(data, &mut cursor).map_err(TicketError::MalformedPayload)?;
        let task_id = read_string(data, &mut cursor).map_err(TicketError::MalformedPayload)?;
        let stream_name = read_string(data, &mut cursor).map_err(TicketError::MalformedPayload)?;

        if cursor + 16 > data.len() {
            return Err(TicketError::MalformedPayload(
                "not enough bytes for u64 fields".into(),
            ));
        }
        let lease_epoch = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let expires_at_unix = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());

        Ok(Self {
            run_id,
            task_id,
            stream_name,
            lease_epoch,
            expires_at_unix,
        })
    }
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = u32::try_from(s.len()).expect("string length exceeds u32::MAX");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn read_string(data: &[u8], cursor: &mut usize) -> Result<String, String> {
    if *cursor + 4 > data.len() {
        return Err("not enough bytes for string length".into());
    }
    let len = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap()) as usize; // u32 -> usize never truncates
    *cursor += 4;
    if *cursor + len > data.len() {
        return Err("not enough bytes for string data".into());
    }
    let s = String::from_utf8(data[*cursor..*cursor + len].to_vec())
        .map_err(|e| format!("invalid UTF-8: {e}"))?;
    *cursor += len;
    Ok(s)
}

/// Signs and verifies preview tickets using HMAC-SHA256.
pub struct TicketSigner {
    key: Vec<u8>,
}

impl TicketSigner {
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }

    /// Sign a payload, returning `payload_bytes || hmac_32bytes`.
    ///
    /// # Panics
    ///
    /// Panics if HMAC initialization fails (should never happen as HMAC accepts
    /// any key size).
    #[must_use]
    pub fn sign(&self, payload: &TicketPayload) -> bytes::Bytes {
        let payload_bytes = payload.to_bytes();
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(&payload_bytes);
        let signature = mac.finalize().into_bytes();

        let mut ticket = payload_bytes;
        ticket.extend_from_slice(&signature);
        bytes::Bytes::from(ticket)
    }

    /// Verify a ticket and extract the payload.
    /// Checks both HMAC integrity and expiration.
    ///
    /// # Errors
    ///
    /// Returns `TicketError` if the ticket is too short, has an invalid
    /// HMAC signature, contains a malformed payload, or has expired.
    ///
    /// # Panics
    ///
    /// Panics if HMAC initialization fails (should never happen as HMAC accepts
    /// any key size).
    pub fn verify(&self, ticket: &[u8]) -> Result<TicketPayload, TicketError> {
        if ticket.len() < HMAC_LEN {
            return Err(TicketError::TooShort);
        }

        let split = ticket.len() - HMAC_LEN;
        let payload_bytes = &ticket[..split];
        let signature = &ticket[split..];

        // Verify HMAC
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(payload_bytes);
        mac.verify_slice(signature)
            .map_err(|_| TicketError::InvalidSignature)?;

        // Decode payload
        let payload = TicketPayload::from_bytes(payload_bytes)?;

        // Check expiration
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now > payload.expires_at_unix {
            return Err(TicketError::Expired);
        }

        Ok(payload)
    }
}

/// Metadata about a stored preview.
#[derive(Debug, Clone)]
pub struct PreviewEntry {
    pub run_id: String,
    pub task_id: String,
    pub flight_endpoint: String,
    pub ticket: bytes::Bytes,
    pub streams: Vec<PreviewStreamEntry>,
    pub created_at: Instant,
    pub ttl: Duration,
}

/// Metadata for a single preview stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewStreamEntry {
    pub stream: String,
    pub rows: u64,
    pub ticket: bytes::Bytes,
}

/// Stores preview metadata indexed by `run_id`.
pub struct PreviewStore {
    entries: HashMap<String, PreviewEntry>,
}

impl PreviewStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn store(&mut self, entry: PreviewEntry) {
        self.entries.insert(entry.run_id.clone(), entry);
    }

    pub fn restore(&mut self, entry: PreviewEntry) {
        self.store(entry);
    }

    pub fn remove(&mut self, run_id: &str) -> Option<PreviewEntry> {
        self.entries.remove(run_id)
    }

    #[must_use]
    pub fn get(&self, run_id: &str) -> Option<&PreviewEntry> {
        let entry = self.entries.get(run_id)?;
        if entry.created_at.elapsed() >= entry.ttl {
            return None;
        }
        Some(entry)
    }

    pub fn remove_expired(&mut self) -> Vec<String> {
        let expired_run_ids = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.created_at.elapsed() >= entry.ttl)
            .map(|(run_id, _)| run_id.clone())
            .collect::<Vec<_>>();
        for run_id in &expired_run_ids {
            let _ = self.entries.remove(run_id);
        }
        expired_run_ids
    }

    #[must_use]
    pub fn all_entries(&self) -> Vec<PreviewEntry> {
        self.entries.values().cloned().collect()
    }
}

impl Default for PreviewStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_signer() -> TicketSigner {
        TicketSigner::new(b"test-secret-key-32-bytes-long!!!")
    }

    fn make_payload(ttl_secs: u64) -> TicketPayload {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + ttl_secs;
        TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            stream_name: "users".into(),
            lease_epoch: 42,
            expires_at_unix: expires,
        }
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let signer = make_signer();
        let payload = make_payload(300);
        let ticket = signer.sign(&payload);
        let verified = signer.verify(&ticket).unwrap();
        assert_eq!(verified, payload);
    }

    #[test]
    fn tampered_ticket_fails_verification() {
        let signer = make_signer();
        let payload = make_payload(300);
        let mut ticket = signer.sign(&payload).to_vec();
        // Tamper with payload
        if let Some(byte) = ticket.first_mut() {
            *byte ^= 0xFF;
        }
        assert!(matches!(
            signer.verify(&ticket),
            Err(TicketError::InvalidSignature)
        ));
    }

    #[test]
    fn expired_ticket_fails_verification() {
        let signer = make_signer();
        let payload = TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            stream_name: "users".into(),
            lease_epoch: 1,
            expires_at_unix: 0, // epoch 0 = already expired
        };
        let ticket = signer.sign(&payload);
        assert!(matches!(signer.verify(&ticket), Err(TicketError::Expired)));
    }

    #[test]
    fn preview_store_stores_and_retrieves() {
        let mut store = PreviewStore::new();
        store.store(PreviewEntry {
            run_id: "r1".into(),
            task_id: "t1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![PreviewStreamEntry {
                stream: "users".into(),
                rows: 42,
                ticket: bytes::Bytes::from_static(b"users-ticket"),
            }],
            created_at: Instant::now(),
            ttl: Duration::from_secs(60),
        });
        let entry = store.get("r1").unwrap();
        assert_eq!(entry.streams.len(), 1);
        assert_eq!(entry.streams[0].stream, "users");
    }

    #[test]
    fn preview_store_expired_returns_none() {
        let mut store = PreviewStore::new();
        store.store(PreviewEntry {
            run_id: "r1".into(),
            task_id: "t1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![],
            created_at: Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap(),
            ttl: Duration::from_secs(60),
        });
        assert!(store.get("r1").is_none());
        // get() no longer removes expired entries; cleanup is done by remove_expired()
        assert_eq!(store.remove_expired().len(), 1);
    }

    #[test]
    fn preview_store_cleanup_removes_expired() {
        let mut store = PreviewStore::new();
        store.store(PreviewEntry {
            run_id: "r1".into(),
            task_id: "t1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![],
            created_at: Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap(),
            ttl: Duration::from_secs(60),
        });
        store.store(PreviewEntry {
            run_id: "r2".into(),
            task_id: "t2".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket2"),
            streams: vec![],
            created_at: Instant::now(),
            ttl: Duration::from_secs(60),
        });

        let removed = store.remove_expired().len();
        assert_eq!(removed, 1);
        assert!(store.get("r1").is_none());
        assert!(store.get("r2").is_some());
    }

    #[test]
    fn preview_store_restore_rehydrates_entry() {
        let mut store = PreviewStore::new();
        store.restore(PreviewEntry {
            run_id: "r1".into(),
            task_id: "t1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![PreviewStreamEntry {
                stream: "users".into(),
                rows: 2,
                ticket: bytes::Bytes::from_static(b"users-ticket"),
            }],
            created_at: Instant::now(),
            ttl: Duration::from_secs(60),
        });

        let entry = store.get("r1").unwrap();
        assert_eq!(entry.task_id, "t1");
        assert_eq!(entry.streams[0].rows, 2);
    }
}
