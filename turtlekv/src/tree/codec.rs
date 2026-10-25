//! Little-endian encoding helpers for node pages.

use bytes::Bytes;

pub struct Reader<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a Bytes) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let out = self.buf.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(out)
    }

    pub fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// A `u32`-length-prefixed byte string, sharing the buffer.
    pub fn bytes(&mut self) -> Option<Bytes> {
        let n = self.u32()? as usize;
        let start = self.pos;
        self.take(n)?;
        Some(self.buf.slice(start..start + n))
    }

    pub fn raw(&mut self, n: usize) -> Option<&'a [u8]> {
        self.take(n)
    }
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}
