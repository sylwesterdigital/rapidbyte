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
    pub lease_epoch: u64,
    pub expires_at_unix: u64,
}

impl TicketPayload {
    /// Deserialize from bytes.
    fn from_bytes(data: &[u8]) -> Result<Self, TicketError> {
        let mut cursor = 0;
        let run_id = read_string(data, &mut cursor).map_err(TicketError::MalformedPayload)?;
        let task_id = read_string(data, &mut cursor).map_err(TicketError::MalformedPayload)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a ticket using the same binary format as the controller's TicketSigner.
    fn sign_ticket(key: &[u8], payload: &TicketPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        // Write run_id
        buf.extend_from_slice(&(payload.run_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload.run_id.as_bytes());
        // Write task_id
        buf.extend_from_slice(&(payload.task_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload.task_id.as_bytes());
        // Write u64 fields
        buf.extend_from_slice(&payload.lease_epoch.to_le_bytes());
        buf.extend_from_slice(&payload.expires_at_unix.to_le_bytes());

        // HMAC
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(&buf);
        let sig = mac.finalize().into_bytes();
        buf.extend_from_slice(&sig);
        buf
    }

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
            lease_epoch: 42,
            expires_at_unix: future_expiry(),
        };
        let ticket = sign_ticket(key, &payload);
        let verified = verifier.verify(&ticket).unwrap();
        assert_eq!(verified, payload);
    }

    #[test]
    fn reject_tampered_ticket() {
        let key = b"test-secret-key-32-bytes-long!!!";
        let verifier = TicketVerifier::new(key);
        let payload = TicketPayload {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 42,
            expires_at_unix: future_expiry(),
        };
        let mut ticket = sign_ticket(key, &payload);
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
            lease_epoch: 1,
            expires_at_unix: 0, // already expired
        };
        let ticket = sign_ticket(key, &payload);
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
}
