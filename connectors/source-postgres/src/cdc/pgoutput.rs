//! Pure binary decoder for `PostgreSQL` `pgoutput` logical replication messages.
//!
//! No I/O, no async, no host imports -- only deterministic binary parsing
//! of the wire-format messages produced by the `pgoutput` replication plugin.
//!
//! Reference: <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>

use std::fmt;
use std::io::{Cursor, Read as _};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur while decoding a `pgoutput` binary message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodeError {
    /// The input buffer is too short to contain the expected data.
    UnexpectedEof,
    /// A null-terminated string contained invalid UTF-8.
    InvalidUtf8,
    /// The leading type byte does not match any known message type.
    UnknownMessageType(u8),
    /// Unknown column value kind byte in `TupleData`.
    UnknownColumnKind(u8),
    /// Unknown replica identity byte.
    UnknownReplicaIdentity(u8),
    /// The message buffer is completely empty.
    EmptyMessage,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of pgoutput message"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in pgoutput cstring"),
            Self::UnknownMessageType(b) => {
                write!(f, "unknown pgoutput message type byte: 0x{b:02X}")
            }
            Self::UnknownColumnKind(b) => {
                write!(f, "unknown pgoutput column kind: 0x{b:02X}")
            }
            Self::UnknownReplicaIdentity(b) => {
                write!(f, "unknown replica identity: 0x{b:02X}")
            }
            Self::EmptyMessage => write!(f, "empty pgoutput message"),
        }
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// Replica identity setting for a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

/// Definition of a single column within a `Relation` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnDef {
    pub(crate) flags: u8,
    pub(crate) name: String,
    pub(crate) type_oid: u32,
    pub(crate) type_modifier: i32,
}

/// A single column value inside a `TupleData` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnValue {
    /// SQL NULL.
    Null,
    /// The column is part of a TOAST-ed value that has not changed.
    UnchangedToast,
    /// Text representation of the column value.
    Text(String),
}

/// Row data consisting of an ordered list of column values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TupleData {
    pub(crate) columns: Vec<ColumnValue>,
}

/// CDC operation label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// A fully-decoded `pgoutput` logical replication message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PgOutputMessage {
    Begin {
        final_lsn: u64,
        commit_ts: u64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_ts: u64,
    },
    Relation {
        oid: u32,
        namespace: String,
        name: String,
        replica_identity: ReplicaIdentity,
        columns: Vec<ColumnDef>,
    },
    Insert {
        relation_oid: u32,
        new_tuple: TupleData,
    },
    Update {
        relation_oid: u32,
        key_tuple: Option<TupleData>,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
    },
    Delete {
        relation_oid: u32,
        key_tuple: Option<TupleData>,
        old_tuple: Option<TupleData>,
    },
    Truncate {
        num_relations: i32,
        options: u8,
        relation_oids: Vec<u32>,
    },
    Origin {
        origin_lsn: u64,
        name: String,
    },
    Type {
        type_oid: u32,
        namespace: String,
        name: String,
    },
    Message {
        flags: u8,
        message_lsn: u64,
        prefix: String,
        content: Vec<u8>,
    },
}

// ---------------------------------------------------------------------------
// Binary read helpers
// ---------------------------------------------------------------------------

fn read_u8(cur: &mut Cursor<&[u8]>) -> Result<u8, DecodeError> {
    let mut buf = [0u8; 1];
    cur.read_exact(&mut buf)
        .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(buf[0])
}

fn read_i16(cur: &mut Cursor<&[u8]>) -> Result<i16, DecodeError> {
    let mut buf = [0u8; 2];
    cur.read_exact(&mut buf)
        .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(i16::from_be_bytes(buf))
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32, DecodeError> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)
        .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_i32(cur: &mut Cursor<&[u8]>) -> Result<i32, DecodeError> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)
        .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_u64(cur: &mut Cursor<&[u8]>) -> Result<u64, DecodeError> {
    let mut buf = [0u8; 8];
    cur.read_exact(&mut buf)
        .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(u64::from_be_bytes(buf))
}

/// Read a null-terminated C string from the cursor.
fn read_cstring(cur: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let mut bytes = Vec::new();
    loop {
        let b = read_u8(cur)?;
        if b == 0 {
            break;
        }
        bytes.push(b);
    }
    String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)
}

/// Decode a replica-identity byte into the typed enum.
fn decode_replica_identity(b: u8) -> Result<ReplicaIdentity, DecodeError> {
    match b {
        b'd' => Ok(ReplicaIdentity::Default),
        b'n' => Ok(ReplicaIdentity::Nothing),
        b'f' => Ok(ReplicaIdentity::Full),
        b'i' => Ok(ReplicaIdentity::Index),
        _ => Err(DecodeError::UnknownReplicaIdentity(b)),
    }
}

/// Read a `TupleData` block: i16 num-columns, then per column a kind byte and
/// optional payload.
fn read_tuple_data(cur: &mut Cursor<&[u8]>) -> Result<TupleData, DecodeError> {
    let num_cols = read_i16(cur)?;
    // Safety: pgoutput column counts are always non-negative.
    #[allow(clippy::cast_sign_loss)]
    let mut columns = Vec::with_capacity(num_cols as usize);

    for _ in 0..num_cols {
        let kind = read_u8(cur)?;
        let value = match kind {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::UnchangedToast,
            b't' => {
                let len = read_i32(cur)?;
                // Safety: pgoutput text lengths are always non-negative.
                #[allow(clippy::cast_sign_loss)]
                let mut data = vec![0u8; len as usize];
                cur.read_exact(&mut data)
                    .map_err(|_| DecodeError::UnexpectedEof)?;
                let text = String::from_utf8(data).map_err(|_| DecodeError::InvalidUtf8)?;
                ColumnValue::Text(text)
            }
            _ => return Err(DecodeError::UnknownColumnKind(kind)),
        };
        columns.push(value);
    }

    Ok(TupleData { columns })
}

// ---------------------------------------------------------------------------
// Top-level decoder
// ---------------------------------------------------------------------------

/// Decode a raw `pgoutput` binary message into a typed `PgOutputMessage`.
///
/// The first byte of `buf` is the message type identifier; the remainder is
/// the type-specific payload.
pub(crate) fn decode(buf: &[u8]) -> Result<PgOutputMessage, DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::EmptyMessage);
    }

    let msg_type = buf[0];
    let mut cur = Cursor::new(&buf[1..]);

    match msg_type {
        b'B' => decode_begin(&mut cur),
        b'C' => decode_commit(&mut cur),
        b'R' => decode_relation(&mut cur),
        b'I' => decode_insert(&mut cur),
        b'U' => decode_update(&mut cur),
        b'D' => decode_delete(&mut cur),
        b'T' => decode_truncate(&mut cur),
        b'O' => decode_origin(&mut cur),
        b'Y' => decode_type(&mut cur),
        b'M' => decode_message(&mut cur),
        _ => Err(DecodeError::UnknownMessageType(msg_type)),
    }
}

fn decode_begin(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let final_lsn = read_u64(cur)?;
    let commit_ts = read_u64(cur)?;
    let xid = read_u32(cur)?;
    Ok(PgOutputMessage::Begin {
        final_lsn,
        commit_ts,
        xid,
    })
}

fn decode_commit(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let flags = read_u8(cur)?;
    let commit_lsn = read_u64(cur)?;
    let end_lsn = read_u64(cur)?;
    let commit_ts = read_u64(cur)?;
    Ok(PgOutputMessage::Commit {
        flags,
        commit_lsn,
        end_lsn,
        commit_ts,
    })
}

fn decode_relation(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let oid = read_u32(cur)?;
    let namespace = read_cstring(cur)?;
    let name = read_cstring(cur)?;
    let ri_byte = read_u8(cur)?;
    let replica_identity = decode_replica_identity(ri_byte)?;
    let num_cols = read_i16(cur)?;

    // Safety: pgoutput column counts are always non-negative.
    #[allow(clippy::cast_sign_loss)]
    let mut columns = Vec::with_capacity(num_cols as usize);
    for _ in 0..num_cols {
        let flags = read_u8(cur)?;
        let col_name = read_cstring(cur)?;
        let type_oid = read_u32(cur)?;
        let type_modifier = read_i32(cur)?;
        columns.push(ColumnDef {
            flags,
            name: col_name,
            type_oid,
            type_modifier,
        });
    }

    Ok(PgOutputMessage::Relation {
        oid,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let relation_oid = read_u32(cur)?;
    let marker = read_u8(cur)?;
    if marker != b'N' {
        return Err(DecodeError::UnknownMessageType(marker));
    }
    let new_tuple = read_tuple_data(cur)?;
    Ok(PgOutputMessage::Insert {
        relation_oid,
        new_tuple,
    })
}

fn decode_update(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let relation_oid = read_u32(cur)?;
    let first = read_u8(cur)?;

    match first {
        b'K' => {
            let key_tuple = read_tuple_data(cur)?;
            let n_marker = read_u8(cur)?;
            if n_marker != b'N' {
                return Err(DecodeError::UnknownMessageType(n_marker));
            }
            let new_tuple = read_tuple_data(cur)?;
            Ok(PgOutputMessage::Update {
                relation_oid,
                key_tuple: Some(key_tuple),
                old_tuple: None,
                new_tuple,
            })
        }
        b'O' => {
            let old_tuple = read_tuple_data(cur)?;
            let n_marker = read_u8(cur)?;
            if n_marker != b'N' {
                return Err(DecodeError::UnknownMessageType(n_marker));
            }
            let new_tuple = read_tuple_data(cur)?;
            Ok(PgOutputMessage::Update {
                relation_oid,
                key_tuple: None,
                old_tuple: Some(old_tuple),
                new_tuple,
            })
        }
        b'N' => {
            let new_tuple = read_tuple_data(cur)?;
            Ok(PgOutputMessage::Update {
                relation_oid,
                key_tuple: None,
                old_tuple: None,
                new_tuple,
            })
        }
        _ => Err(DecodeError::UnknownMessageType(first)),
    }
}

fn decode_delete(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let relation_oid = read_u32(cur)?;
    let marker = read_u8(cur)?;

    match marker {
        b'K' => {
            let key_tuple = read_tuple_data(cur)?;
            Ok(PgOutputMessage::Delete {
                relation_oid,
                key_tuple: Some(key_tuple),
                old_tuple: None,
            })
        }
        b'O' => {
            let old_tuple = read_tuple_data(cur)?;
            Ok(PgOutputMessage::Delete {
                relation_oid,
                key_tuple: None,
                old_tuple: Some(old_tuple),
            })
        }
        _ => Err(DecodeError::UnknownMessageType(marker)),
    }
}

fn decode_truncate(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let num_relations = read_i32(cur)?;
    let options = read_u8(cur)?;

    // Safety: pgoutput relation counts are always non-negative.
    #[allow(clippy::cast_sign_loss)]
    let mut relation_oids = Vec::with_capacity(num_relations as usize);
    for _ in 0..num_relations {
        relation_oids.push(read_u32(cur)?);
    }

    Ok(PgOutputMessage::Truncate {
        num_relations,
        options,
        relation_oids,
    })
}

fn decode_origin(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let origin_lsn = read_u64(cur)?;
    let name = read_cstring(cur)?;
    Ok(PgOutputMessage::Origin { origin_lsn, name })
}

fn decode_type(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let type_oid = read_u32(cur)?;
    let namespace = read_cstring(cur)?;
    let name = read_cstring(cur)?;
    Ok(PgOutputMessage::Type {
        type_oid,
        namespace,
        name,
    })
}

fn decode_message(cur: &mut Cursor<&[u8]>) -> Result<PgOutputMessage, DecodeError> {
    let flags = read_u8(cur)?;
    let message_lsn = read_u64(cur)?;
    let prefix = read_cstring(cur)?;
    let content_len = read_u32(cur)?;
    let mut content = vec![0u8; content_len as usize];
    cur.read_exact(&mut content)
        .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(PgOutputMessage::Message {
        flags,
        message_lsn,
        prefix,
        content,
    })
}

// ---------------------------------------------------------------------------
// LSN utilities
// ---------------------------------------------------------------------------

/// Format a u64 LSN into the standard `PostgreSQL` `X/YYYYYYYY` notation.
pub(crate) fn lsn_to_string(lsn: u64) -> String {
    let high = lsn >> 32;
    let low = lsn & 0xFFFF_FFFF;
    format!("{high:X}/{low:08X}")
}

/// Parse an LSN string in `X/YYYYYYYY` notation into a u64.
pub(crate) fn parse_lsn(s: &str) -> Option<u64> {
    let (high_str, low_str) = s.split_once('/')?;
    let high = u64::from_str_radix(high_str, 16).ok()?;
    let low = u64::from_str_radix(low_str, 16).ok()?;
    Some((high << 32) | low)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Builder helpers for constructing binary messages -------------------

    fn push_u8(buf: &mut Vec<u8>, v: u8) {
        buf.push(v);
    }

    fn push_i16(buf: &mut Vec<u8>, v: i16) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_i32(buf: &mut Vec<u8>, v: i32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_u64(buf: &mut Vec<u8>, v: u64) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_cstring(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(s.as_bytes());
        buf.push(0);
    }

    /// Build a text column value inside a tuple: kind='t', len(i32), bytes.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn push_text_column(buf: &mut Vec<u8>, val: &str) {
        push_u8(buf, b't');
        push_i32(buf, val.len() as i32);
        buf.extend_from_slice(val.as_bytes());
    }

    // -- Message decoders ---------------------------------------------------

    #[test]
    fn decode_begin() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'B');
        push_u64(&mut buf, 0x0000_0001_0000_0100); // final_lsn
        push_u64(&mut buf, 700_000_000_000); // commit_ts (micros since PG epoch)
        push_u32(&mut buf, 42); // xid

        let msg = decode(&buf).unwrap();
        assert_eq!(
            msg,
            PgOutputMessage::Begin {
                final_lsn: 0x0000_0001_0000_0100,
                commit_ts: 700_000_000_000,
                xid: 42,
            }
        );
    }

    #[test]
    fn decode_commit() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'C');
        push_u8(&mut buf, 0); // flags
        push_u64(&mut buf, 0xAA); // commit_lsn
        push_u64(&mut buf, 0xBB); // end_lsn
        push_u64(&mut buf, 123_456); // commit_ts

        let msg = decode(&buf).unwrap();
        assert_eq!(
            msg,
            PgOutputMessage::Commit {
                flags: 0,
                commit_lsn: 0xAA,
                end_lsn: 0xBB,
                commit_ts: 123_456,
            }
        );
    }

    #[test]
    fn decode_relation_with_columns() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'R');
        push_u32(&mut buf, 16385); // oid
        push_cstring(&mut buf, "public"); // namespace
        push_cstring(&mut buf, "users"); // name
        push_u8(&mut buf, b'd'); // replica identity = default
        push_i16(&mut buf, 2); // 2 columns

        // Column 0: id integer
        push_u8(&mut buf, 1); // flags (part of key)
        push_cstring(&mut buf, "id");
        push_u32(&mut buf, 23); // int4 OID
        push_i32(&mut buf, -1); // type modifier

        // Column 1: name text
        push_u8(&mut buf, 0); // flags
        push_cstring(&mut buf, "name");
        push_u32(&mut buf, 25); // text OID
        push_i32(&mut buf, -1); // type modifier

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            } => {
                assert_eq!(oid, 16385);
                assert_eq!(namespace, "public");
                assert_eq!(name, "users");
                assert_eq!(replica_identity, ReplicaIdentity::Default);
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].flags, 1);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].type_oid, 23);
                assert_eq!(columns[0].type_modifier, -1);
                assert_eq!(columns[1].flags, 0);
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[1].type_oid, 25);
            }
            other => panic!("expected Relation, got {other:?}"),
        }
    }

    #[test]
    fn decode_insert() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'I');
        push_u32(&mut buf, 16385); // relation_oid
        push_u8(&mut buf, b'N'); // new tuple marker
        push_i16(&mut buf, 2); // 2 columns
        push_text_column(&mut buf, "1"); // id
        push_text_column(&mut buf, "Alice"); // name

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Insert {
                relation_oid,
                new_tuple,
            } => {
                assert_eq!(relation_oid, 16385);
                assert_eq!(new_tuple.columns.len(), 2);
                assert_eq!(new_tuple.columns[0], ColumnValue::Text("1".into()));
                assert_eq!(new_tuple.columns[1], ColumnValue::Text("Alice".into()));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn decode_update_simple() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'U');
        push_u32(&mut buf, 16385); // relation_oid
        push_u8(&mut buf, b'N'); // direct new-tuple marker
        push_i16(&mut buf, 2); // 2 columns
        push_text_column(&mut buf, "1");
        push_text_column(&mut buf, "Bob");

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Update {
                relation_oid,
                key_tuple,
                old_tuple,
                new_tuple,
            } => {
                assert_eq!(relation_oid, 16385);
                assert!(key_tuple.is_none());
                assert!(old_tuple.is_none());
                assert_eq!(new_tuple.columns[1], ColumnValue::Text("Bob".into()));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decode_update_with_old_tuple() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'U');
        push_u32(&mut buf, 16385); // relation_oid
                                   // Old tuple
        push_u8(&mut buf, b'O');
        push_i16(&mut buf, 1);
        push_text_column(&mut buf, "old_val");
        // New tuple
        push_u8(&mut buf, b'N');
        push_i16(&mut buf, 1);
        push_text_column(&mut buf, "new_val");

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Update {
                old_tuple,
                new_tuple,
                key_tuple,
                ..
            } => {
                assert!(key_tuple.is_none());
                let old = old_tuple.expect("should have old tuple");
                assert_eq!(old.columns[0], ColumnValue::Text("old_val".into()));
                assert_eq!(new_tuple.columns[0], ColumnValue::Text("new_val".into()));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decode_delete_with_key() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'D');
        push_u32(&mut buf, 16385);
        push_u8(&mut buf, b'K'); // key tuple marker
        push_i16(&mut buf, 1);
        push_text_column(&mut buf, "42");

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Delete {
                relation_oid,
                key_tuple,
                old_tuple,
            } => {
                assert_eq!(relation_oid, 16385);
                assert!(old_tuple.is_none());
                let key = key_tuple.expect("should have key tuple");
                assert_eq!(key.columns[0], ColumnValue::Text("42".into()));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn decode_truncate() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'T');
        push_i32(&mut buf, 2); // num_relations
        push_u8(&mut buf, 0); // options
        push_u32(&mut buf, 16385); // relation 1
        push_u32(&mut buf, 16386); // relation 2

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Truncate {
                num_relations,
                options,
                relation_oids,
            } => {
                assert_eq!(num_relations, 2);
                assert_eq!(options, 0);
                assert_eq!(relation_oids, vec![16385, 16386]);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn decode_unchanged_toast_in_tuple() {
        let mut buf = Vec::new();
        push_u8(&mut buf, b'I');
        push_u32(&mut buf, 100);
        push_u8(&mut buf, b'N');
        push_i16(&mut buf, 3);
        push_text_column(&mut buf, "1");
        push_u8(&mut buf, b'n'); // NULL
        push_u8(&mut buf, b'u'); // unchanged TOAST

        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Insert { new_tuple, .. } => {
                assert_eq!(new_tuple.columns.len(), 3);
                assert_eq!(new_tuple.columns[0], ColumnValue::Text("1".into()));
                assert_eq!(new_tuple.columns[1], ColumnValue::Null);
                assert_eq!(new_tuple.columns[2], ColumnValue::UnchangedToast);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    // -- Error cases -------------------------------------------------------

    #[test]
    fn decode_empty_message_errors() {
        let result = decode(&[]);
        assert_eq!(result.unwrap_err(), DecodeError::EmptyMessage);
    }

    #[test]
    fn decode_unknown_type_errors() {
        let result = decode(b"Z");
        assert_eq!(result.unwrap_err(), DecodeError::UnknownMessageType(b'Z'));
    }

    #[test]
    fn decode_truncated_begin_errors() {
        // Begin needs 20 bytes after the type byte; provide only 4.
        let mut buf = Vec::new();
        push_u8(&mut buf, b'B');
        push_u32(&mut buf, 1);

        let result = decode(&buf);
        assert_eq!(result.unwrap_err(), DecodeError::UnexpectedEof);
    }

    // -- LSN utilities -----------------------------------------------------

    #[test]
    fn lsn_roundtrip() {
        let lsn: u64 = 0x0000_0000_16B3_7480;
        let s = lsn_to_string(lsn);
        assert_eq!(s, "0/16B37480");
        let parsed = parse_lsn(&s).expect("should parse");
        assert_eq!(parsed, lsn);
    }

    #[test]
    fn lsn_high_part() {
        let lsn: u64 = 0x0000_0002_0000_0001;
        let s = lsn_to_string(lsn);
        assert_eq!(s, "2/00000001");
        assert_eq!(parse_lsn(&s), Some(lsn));
    }

    #[test]
    fn lsn_comparison() {
        let a = parse_lsn("1/00000000").unwrap();
        let b = parse_lsn("0/FFFFFFFF").unwrap();
        assert!(a > b);

        let c = parse_lsn("0/2").unwrap();
        let d = parse_lsn("0/1").unwrap();
        assert!(c > d);
    }

    #[test]
    fn cdc_op_as_str() {
        assert_eq!(CdcOp::Insert.as_str(), "insert");
        assert_eq!(CdcOp::Update.as_str(), "update");
        assert_eq!(CdcOp::Delete.as_str(), "delete");
    }
}
