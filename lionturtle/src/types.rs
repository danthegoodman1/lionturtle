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

/// The smallest key above every key that starts with `prefix`, or `None`
/// when no key is (the prefix is empty or all `0xff` bytes).
pub fn prefix_end(prefix: &[u8]) -> Option<Bytes> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(Bytes::from(end));
        }
    }
    None
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
pub enum WriteOp {
    Put {
        key: Bytes,
        value: Bytes,
    },
    Delete {
        key: Bytes,
    },
    /// Deletes every key in `[start, end)`; `None` leaves the range
    /// unbounded above. An empty or inverted range deletes nothing.
    DeleteRange {
        start: Bytes,
        end: Option<Bytes>,
    },
}

impl WriteOp {
    pub fn put(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self::Put { key: key.into(), value: value.into() }
    }

    pub fn delete(key: impl Into<Bytes>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// Deletes every key in `[start, end)`.
    pub fn delete_range(start: impl Into<Bytes>, end: impl Into<Bytes>) -> Self {
        Self::DeleteRange { start: start.into(), end: Some(end.into()) }
    }

    /// Deletes `start` and every key after it.
    pub fn delete_from(start: impl Into<Bytes>) -> Self {
        Self::DeleteRange { start: start.into(), end: None }
    }

    /// Deletes every key that starts with `prefix`.
    pub fn delete_prefix(prefix: impl Into<Bytes>) -> Self {
        let start = prefix.into();
        let end = prefix_end(&start);
        Self::DeleteRange { start, end }
    }
}

/// A mutation over borrowed bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpRef<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
    /// Every key in `[start, end)`, or from `start` on for `None`.
    DeleteRange(&'a [u8], Option<&'a [u8]>),
}

/// One mutation as the write path reads it. The log and the memtable each
/// copy what they keep, so a batch can borrow its caller's bytes:
/// [`Db::write`] takes a slice of [`WriteOp`]s, of [`OpRef`]s, or of
/// `(key, value)` pairs of byte slices.
///
/// [`Db::write`]: crate::Db::write
pub trait Op {
    fn op(&self) -> OpRef<'_>;
}

impl Op for WriteOp {
    #[inline]
    fn op(&self) -> OpRef<'_> {
        match self {
            WriteOp::Put { key, value } => OpRef::Put(key, value),
            WriteOp::Delete { key } => OpRef::Delete(key),
            WriteOp::DeleteRange { start, end } => OpRef::DeleteRange(start, end.as_deref()),
        }
    }
}

impl Op for OpRef<'_> {
    #[inline]
    fn op(&self) -> OpRef<'_> {
        *self
    }
}

/// A borrowed key and the value to put, or `None` to delete the key.
impl Op for (&[u8], Option<&[u8]>) {
    #[inline]
    fn op(&self) -> OpRef<'_> {
        match self.1 {
            Some(v) => OpRef::Put(self.0, v),
            None => OpRef::Delete(self.0),
        }
    }
}

/// Largest key the store accepts. Keys are length-prefixed with a `u16`.
pub const MAX_KEY_LEN: usize = u16::MAX as usize;
/// Largest value the store accepts.
pub const MAX_VALUE_LEN: usize = 1 << 24;

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn prefix_end_skips_trailing_ff() {
        assert_eq!(prefix_end(b""), None);
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b"ab").as_deref(), Some(&b"ac"[..]));
        assert_eq!(prefix_end(b"a\xff\xff").as_deref(), Some(&b"b"[..]));
        assert_eq!(prefix_end(b"\x00\xff").as_deref(), Some(&b"\x01"[..]));
    }
}
