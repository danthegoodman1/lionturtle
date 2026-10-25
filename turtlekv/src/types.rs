use bytes::Bytes;
use std::cmp::Ordering;

/// Compares keys lexicographically, taking the first eight bytes as one
/// integer when both keys have them.
#[inline]
pub fn cmp_keys(a: &[u8], b: &[u8]) -> Ordering {
    if a.len() >= 8 && b.len() >= 8 {
        let x = u64::from_be_bytes(a[..8].try_into().unwrap());
        let y = u64::from_be_bytes(b[..8].try_into().unwrap());
        if x != y {
            return x.cmp(&y);
        }
        let (a, b) = (&a[8..], &b[8..]);
        if a.is_empty() || b.is_empty() {
            return a.len().cmp(&b.len());
        }
        return a.cmp(b);
    }
    a.cmp(b)
}

/// The first eight bytes of `key` as a big-endian integer, zero-padded. Keys
/// with different prefixes order as their prefixes do.
#[inline]
pub fn key_prefix(key: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = key.len().min(8);
    b[..n].copy_from_slice(&key[..n]);
    u64::from_be_bytes(b)
}

/// The newest state of a key: a value or a tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Put(Bytes),
    Delete,
}

impl Value {
    pub fn as_put(&self) -> Option<&Bytes> {
        match self {
            Value::Put(v) => Some(v),
            Value::Delete => None,
        }
    }

    pub fn into_put(self) -> Option<Bytes> {
        match self {
            Value::Put(v) => Some(v),
            Value::Delete => None,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Value::Delete)
    }

    /// Bytes this value occupies in a page entry, excluding the entry header.
    pub fn encoded_len(&self) -> usize {
        self.as_put().map_or(0, |v| v.len())
    }
}

/// One mutation in a write batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOp {
    pub key: Bytes,
    pub value: Value,
}

impl WriteOp {
    pub fn put(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self { key: key.into(), value: Value::Put(value.into()) }
    }

    pub fn delete(key: impl Into<Bytes>) -> Self {
        Self { key: key.into(), value: Value::Delete }
    }
}

/// Largest key the store accepts. Keys are length-prefixed with a `u16`.
pub const MAX_KEY_LEN: usize = u16::MAX as usize;
/// Largest value the store accepts.
pub const MAX_VALUE_LEN: usize = 1 << 24;
