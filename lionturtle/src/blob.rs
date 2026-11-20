//! Blobs: large values stored apart from the tree.
//!
//! A value of at least [`crate::Options::blob_threshold`] bytes takes a page
//! store slot of its own, and its tree entry holds a [`BlobPtr`] in place of
//! the value. Merges and flushes move the pointer, so the value is written
//! once however far its entry travels down the tree.
//!
//! ```text
//! blob = crc: u32 | kind: u32 | raw_len: u32 | payload_len: u32 | payload | padding
//! ```
//!
//! The checksum covers the blob's id and everything after the checksum up to
//! the payload's end. No id is used twice, across crashes too (see
//! [`crate::pagestore::Superblock::next_blob_id`]), so a pointer read against
//! a slot that holds any other blob, however alike, fails its checksum. The payload is the value, or
//! the value compressed with LZ4 when that saves at least an eighth of it.

use crate::error::{Result, corruption};
use crate::page::PageId;
use crate::pagestore::{CLASSES, PagePtr, slot_size};
use bytes::Bytes;

/// Bytes of a stored blob before its payload.
pub const HEADER: usize = 16;
const KIND_RAW: u32 = 0;
const KIND_LZ4: u32 = 1;

/// A blob compresses when its payload shrinks to at most this share of the
/// value: below an eighth saved, the smaller write does not pay for
/// decompressing every read.
const WORTH_NUM: usize = 7;
const WORTH_DEN: usize = 8;

/// Values longer than this are sampled before they are compressed whole.
const SAMPLE_OVER: usize = 1 << 20;
/// Bytes of each of the two samples, the value's start and its middle. LZ4
/// finds repeats up to 64 KiB back, so a sample twice that long shows
/// whether the value has any.
const SAMPLE: usize = 128 << 10;

/// Where a blob lives and how long its value is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobPtr {
    pub id: PageId,
    pub class: u8,
    pub slot: u32,
    /// Bytes stored: the header and payload, without padding.
    pub len: u32,
    /// Length of the value.
    pub raw_len: u32,
}

impl BlobPtr {
    pub const ENCODED_LEN: usize = 21;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..8].copy_from_slice(&self.id.to_le_bytes());
        out[8] = self.class;
        out[9..13].copy_from_slice(&self.slot.to_le_bytes());
        out[13..17].copy_from_slice(&self.len.to_le_bytes());
        out[17..21].copy_from_slice(&self.raw_len.to_le_bytes());
        out
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let bad = || corruption("malformed blob pointer");
        if b.len() != Self::ENCODED_LEN {
            return Err(bad());
        }
        let word = |at: usize| u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
        let ptr =
            Self { id: u64::from_le_bytes(b[0..8].try_into().unwrap()), class: b[8], slot: word(9), len: word(13), raw_len: word(17) };
        if ptr.class as usize >= CLASSES || (ptr.len as usize) < HEADER || ptr.len as u64 > slot_size(ptr.class) {
            return Err(bad());
        }
        Ok(ptr)
    }

    /// The blob's slot, as the page store names it.
    pub fn page_ptr(&self) -> PagePtr {
        PagePtr { id: self.id, class: self.class, slot: self.slot, len: self.len, meta_off: 0 }
    }
}

/// Bytes [`encode`] may need for a value of `len` bytes.
pub fn stored_bound(len: usize, compress: bool) -> usize {
    HEADER + if compress { lz4_flex::block::get_maximum_output_size(len).max(len) } else { len }
}

/// Whether compressing a value to `compressed` bytes is worth storing.
fn worth(compressed: usize, raw: usize) -> bool {
    compressed * WORTH_DEN <= raw * WORTH_NUM
}

/// Whether a long value looks compressible, judged from two samples.
/// Compressing an incompressible value whole only to store it raw would
/// cost a pass over it for nothing.
fn sample_compresses(value: &[u8], scratch: &mut [u8]) -> bool {
    if value.len() <= SAMPLE_OVER {
        return true;
    }
    let starts = [0, value.len() / 2];
    let compressed: usize =
        starts.iter().map(|&at| lz4_flex::block::compress_into(&value[at..at + SAMPLE], scratch).unwrap_or(SAMPLE)).sum();
    worth(compressed, starts.len() * SAMPLE)
}

/// Writes the stored form of `value` as blob `id` to the start of `out`,
/// which must hold [`stored_bound`] bytes, and returns its length.
pub fn encode(id: PageId, value: &[u8], compress: bool, out: &mut [u8]) -> usize {
    let (head, body) = out.split_at_mut(HEADER);
    let compressed = if compress && sample_compresses(value, body) {
        lz4_flex::block::compress_into(value, body).ok().filter(|&c| worth(c, value.len()))
    } else {
        None
    };
    let (kind, payload_len) = match compressed {
        Some(c) => (KIND_LZ4, c),
        None => {
            body[..value.len()].copy_from_slice(value);
            (KIND_RAW, value.len())
        }
    };
    head[4..8].copy_from_slice(&kind.to_le_bytes());
    head[8..12].copy_from_slice(&(value.len() as u32).to_le_bytes());
    head[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());
    let len = HEADER + payload_len;
    let crc = checksum(id, &out[4..len]);
    out[0..4].copy_from_slice(&crc.to_le_bytes());
    len
}

fn checksum(id: PageId, body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&id.to_le_bytes()), body)
}

/// The value of the blob at `ptr`, verified, from `stored`, its bytes as
/// read from its slot.
pub fn decode(ptr: &BlobPtr, stored: Bytes) -> Result<Bytes> {
    let word = |at: usize| u32::from_le_bytes(stored[at..at + 4].try_into().unwrap());
    if stored.len() != ptr.len as usize || stored.len() < HEADER {
        return Err(corruption(format!("blob {} has the wrong length", ptr.id)));
    }
    if word(0) != checksum(ptr.id, &stored[4..]) {
        return Err(corruption(format!("blob {} checksum mismatch", ptr.id)));
    }
    let (kind, raw_len, payload_len) = (word(4), word(8) as usize, word(12) as usize);
    if raw_len != ptr.raw_len as usize || HEADER + payload_len != stored.len() {
        return Err(corruption(format!("blob {} does not match its pointer", ptr.id)));
    }
    match kind {
        KIND_RAW if payload_len == raw_len => Ok(stored.slice(HEADER..)),
        KIND_LZ4 => match lz4_flex::block::decompress(&stored[HEADER..], raw_len) {
            Ok(raw) if raw.len() == raw_len => Ok(Bytes::from(raw)),
            _ => Err(corruption(format!("blob {} does not decompress", ptr.id))),
        },
        _ => Err(corruption(format!("blob {} is of an unknown kind", ptr.id))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(id: PageId, value: &[u8], compress: bool) -> (BlobPtr, Bytes) {
        let mut out = vec![0u8; stored_bound(value.len(), compress)];
        let len = encode(id, value, compress, &mut out);
        out.truncate(len);
        (BlobPtr { id, class: 12, slot: 3, len: len as u32, raw_len: value.len() as u32 }, Bytes::from(out))
    }

    fn noise(len: usize) -> Vec<u8> {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn round_trips_raw_and_compressed() {
        for len in [0, 1, 4095, 4096, 100_000, 1 << 20, (2 << 20) + 5] {
            for value in [vec![7u8; len], noise(len)] {
                for compress in [false, true] {
                    let (ptr, bytes) = stored(9, &value, compress);
                    assert_eq!(decode(&ptr, bytes).unwrap(), value, "len {len}, compress {compress}");
                }
            }
        }
    }

    #[test]
    fn compresses_only_when_worth_it() {
        let repetitive = vec![7u8; 1 << 20];
        assert!(stored(1, &repetitive, true).1.len() < repetitive.len() / 8);
        assert_eq!(stored(1, &repetitive, false).1.len(), HEADER + repetitive.len());
        let random = noise(1 << 20);
        assert_eq!(stored(1, &random, true).1.len(), HEADER + random.len());
        // A long value is judged by its samples first, then by how it
        // compresses whole: one whose samples compress and whose bulk does
        // not is stored as it is, as is one whose samples do not compress.
        let mut mixed = noise(4 << 20);
        for at in [0, mixed.len() / 2] {
            mixed[at..at + SAMPLE].fill(0);
        }
        assert_eq!(stored(1, &mixed, true).1.len(), HEADER + mixed.len());
        let mut unsampled = vec![0u8; 4 << 20];
        for at in [0, unsampled.len() / 2] {
            unsampled[at..at + SAMPLE].copy_from_slice(&noise(SAMPLE));
        }
        assert_eq!(stored(1, &unsampled, true).1.len(), HEADER + unsampled.len());
        // Halves that repeat within LZ4's reach compress, sampled or not.
        for len in [1 << 20, 4 << 20] {
            let mut halves = noise(len);
            for piece in halves.chunks_mut(32 << 10) {
                let half = piece.len() / 2;
                piece.copy_within(..half, half);
            }
            let stored_len = stored(1, &halves, true).1.len();
            assert!(stored_len < len * 6 / 10, "{stored_len} of {len}");
        }
    }

    #[test]
    fn rejects_damage_and_the_wrong_blob() {
        let value = noise(10_000);
        let (ptr, bytes) = stored(5, &value, true);
        let mut flipped = bytes.to_vec();
        flipped[HEADER + 100] ^= 1;
        assert!(decode(&ptr, Bytes::from(flipped)).is_err());
        // The slot holds a blob written under another id.
        assert!(decode(&BlobPtr { id: 6, ..ptr }, bytes.clone()).is_err());
        assert!(decode(&BlobPtr { raw_len: ptr.raw_len + 1, ..ptr }, bytes.clone()).is_err());
        assert!(decode(&ptr, bytes.slice(..bytes.len() - 1)).is_err());
    }

    #[test]
    fn pointer_round_trips_and_rejects_bad_fields() {
        let ptr = BlobPtr { id: 77, class: 9, slot: 12345, len: 70_000, raw_len: 123_456 };
        assert_eq!(BlobPtr::decode(&ptr.encode()).unwrap(), ptr);
        assert!(BlobPtr::decode(&ptr.encode()[..20]).is_err());
        assert!(BlobPtr::decode(&BlobPtr { class: CLASSES as u8, ..ptr }.encode()).is_err());
        assert!(BlobPtr::decode(&BlobPtr { len: 8, ..ptr }.encode()).is_err());
        assert!(BlobPtr::decode(&BlobPtr { class: 0, ..ptr }.encode()).is_err());
    }
}
