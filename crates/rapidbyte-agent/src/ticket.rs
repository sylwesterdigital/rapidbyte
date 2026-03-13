//! Ticket validation for Flight preview access.
//!
//! Mirrors the controller's HMAC-SHA256 ticket verification logic.
//! The agent receives the signing key via shared configuration.

use std::time::{SystemTime, UNIX_EPOCH};

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

fn read_string(data: &[u8], cursor: &mut usize) -> Result<String, String> {
    if *cursor + 4 > data.len() {
        return Err("not enough bytes for string length".into());
    }
    let len = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap()) as usize;
    *cursor += 4;
    if *cursor + len > data.len() {
        return Err("not enough bytes for string data".into());
    }
    let s = String::from_utf8(data[*cursor..*cursor + len].to_vec())
        .map_err(|e| format!("invalid UTF-8: {e}"))?;
    *cursor += len;
    Ok(s)
}

/// Verifies preview tickets using HMAC-SHA256.
pub struct TicketVerifier {
    key: Vec<u8>,
}

impl TicketVerifier {
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }

    /// Verify a ticket and extract the payload.
    /// Checks both HMAC integrity and expiration.
    ///
    /// # Errors
    ///
    /// Returns [`TicketError::TooShort`] if the ticket is smaller than the HMAC length,
    /// [`TicketError::InvalidSignature`] if the HMAC does not match,
    /// [`TicketError::Expired`] if the ticket has expired, or
    /// [`TicketError::MalformedPayload`] if the payload bytes cannot be decoded.
    ///
    /// # Panics
    ///
    /// Panics if the system clock is before the Unix epoch.
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

/// Signs preview tickets using the same HMAC-SHA256 format as the controller.
pub struct TicketSigner {
    key: Vec<u8>,
}

impl TicketSigner {
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }

    #[must_use]
    ///
    /// # Panics
    ///
    /// Panics if any ticket string field exceeds `u32::MAX` bytes.
    pub fn sign(&self, payload: &TicketPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(
            &u32::try_from(payload.run_id.len())
                .expect("run_id length exceeds u32::MAX")
                .to_le_bytes(),
        );
        buf.extend_from_slice(payload.run_id.as_bytes());
        buf.extend_from_slice(
            &u32::try_from(payload.task_id.len())
                .expect("task_id length exceeds u32::MAX")
                .to_le_bytes(),
        );
        buf.extend_from_slice(payload.task_id.as_bytes());
        buf.extend_from_slice(
            &u32::try_from(payload.stream_name.len())
                .expect("stream_name length exceeds u32::MAX")
                .to_le_bytes(),
        );
        buf.extend_from_slice(payload.stream_name.as_bytes());
        buf.extend_from_slice(&payload.lease_epoch.to_le_bytes());
        buf.extend_from_slice(&payload.expires_at_unix.to_le_bytes());

        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(&buf);
        buf.extend_from_slice(&mac.finalize().into_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn future_expiry() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300
    }

    #[test]
    fn verify_valid_ticket() {
        let key = b"test-secret-key-32-bytes-long!!!";
        let verifier = TicketVerifier::new(key);
        let payload = TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            stream_name: "users".into(),
            lease_epoch: 42,
            expires_at_unix: future_expiry(),
        };
        let ticket = TicketSigner::new(key).sign(&payload);
        let actual = verifier.verify(&ticket).unwrap();
        assert_eq!(actual, payload);
    }

    #[test]
    fn reject_tampered_ticket() {
        let key = b"test-secret-key-32-bytes-long!!!";
        let verifier = TicketVerifier::new(key);
        let payload = TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            stream_name: "users".into(),
            lease_epoch: 42,
            expires_at_unix: future_expiry(),
        };
        let mut ticket = TicketSigner::new(key).sign(&payload);
        ticket[0] ^= 0xFF;
        assert!(matches!(
            verifier.verify(&ticket),
            Err(TicketError::InvalidSignature)
        ));
    }

    #[test]
    fn reject_expired_ticket() {
        let key = b"test-secret-key-32-bytes-long!!!";
        let verifier = TicketVerifier::new(key);
        let payload = TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            stream_name: "users".into(),
            lease_epoch: 1,
            expires_at_unix: 0, // already expired
        };
        let ticket = TicketSigner::new(key).sign(&payload);
        assert!(matches!(
            verifier.verify(&ticket),
            Err(TicketError::Expired)
        ));
    }

    #[test]
    fn reject_too_short_ticket() {
        let verifier = TicketVerifier::new(b"key");
        assert!(matches!(
            verifier.verify(&[0; 10]),
            Err(TicketError::TooShort)
        ));
    }

    #[test]
    fn signer_round_trips_with_verifier() {
        let key = b"test-secret-key-32-bytes-long!!!";
        let signer = TicketSigner::new(key);
        let verifier = TicketVerifier::new(key);
        let payload = TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            stream_name: "users".into(),
            lease_epoch: 42,
            expires_at_unix: future_expiry(),
        };

        let ticket = signer.sign(&payload);
        assert_eq!(verifier.verify(&ticket).unwrap(), payload);
    }
}
