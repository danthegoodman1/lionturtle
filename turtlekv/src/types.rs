use bytes::Bytes;

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
