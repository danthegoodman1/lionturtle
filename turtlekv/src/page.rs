//! Data pages: immutable sorted runs of entries. Tree leaves and node-buffer
//! segments share this format, and a page's in-memory image is byte-for-byte
//! what goes to disk.
//!
//! ```text
//! block*  | index | keys | filter | footer
//! ```
//!
//! Entries are packed into blocks of [`BLOCK_SIZE`] bytes (a block holding one
//! entry larger than that spans several), so a point lookup reads one block.
//! Each block is checksummed on its own:
//!
//! ```text
//! block  = crc: u32 | count: u32 | offsets_pos: u32 | entry* | offset: u32 * count | padding
//! entry  = key_len: u16 | value_len: u32 (high bit marks a tombstone) | key | value
//! ```
//!
//! The metadata region (index, keys, filter, footer) is what stays in memory
//! while a page's blocks live on disk: the index holds each block's location,
//! first item ordinal, and first key, the keys area holds those keys plus the
//! page's last key, and the filter is a split-block Bloom filter over all keys.

use crate::bloom;
use crate::error::{Result, corruption};
use crate::types::cmp_keys;
use bytes::Bytes;
use std::cmp::Ordering;
use std::ops::Bound;

pub const BLOCK_SIZE: usize = 4096;
const BLOCK_HEADER: usize = 12;
const ENTRY_HEADER: usize = 6;
const TOMBSTONE: u32 = 1 << 31;
const INDEX_RECORD: usize = 20;
pub const FOOTER_LEN: usize = 64;
const MAGIC: u32 = 0x5054_4b56;

/// Identity of a page image. Ids are never reused, so they key caches safely.
pub type PageId = u64;

/// Bytes an entry contributes to a page's logical size.
pub fn entry_size(key_len: usize, value_len: usize) -> u64 {
    (ENTRY_HEADER + key_len + value_len) as u64
}

/// A borrowed entry: `value` is `None` for a tombstone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryRef<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}

impl EntryRef<'_> {
    pub fn size(&self) -> u64 {
        entry_size(self.key.len(), self.value.map_or(0, <[u8]>::len))
    }
}

fn block_crc(page_id: PageId, body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&page_id.to_le_bytes()), body)
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockInfo {
    pub offset: u32,
    pub len: u32,
    pub first_item: u32,
}

/// The metadata region of a page, decoded.
#[derive(Clone)]
pub struct PageMeta {
    raw: Bytes,
    pub id: PageId,
    pub n_items: u32,
    pub n_blocks: u32,
    pub data_bytes: u64,
    pub tombstones: u32,
    /// Offset of the metadata region within the page image.
    pub meta_off: u32,
    keys_off: u32,
    filter_off: u32,
    filter_len: u32,
    last_key_off: u32,
    last_key_len: u32,
}

impl std::fmt::Debug for PageMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageMeta")
            .field("id", &self.id)
            .field("n_items", &self.n_items)
            .field("data_bytes", &self.data_bytes)
            .finish()
    }
}

impl PageMeta {
    /// Decodes and verifies a metadata region, as read from `meta_off` to the
    /// end of the page.
    pub fn decode(raw: Bytes, expected_id: PageId) -> Result<Self> {
        if raw.len() < FOOTER_LEN {
            return Err(corruption("page metadata too short"));
        }
        let f = raw.len() - FOOTER_LEN;
        let footer = &raw[f..];
        if u32_at(footer, 0) != MAGIC {
            return Err(corruption("bad page magic"));
        }
        let crc = u32_at(footer, 4);
        let mut expected = crc32c::crc32c(&raw[..f + 4]);
        expected = crc32c::crc32c_append(expected, &footer[8..]);
        if crc != expected {
            return Err(corruption(format!("page {expected_id} metadata checksum mismatch")));
        }
        let meta = Self {
            id: u64_at(footer, 8),
            n_items: u32_at(footer, 16),
            n_blocks: u32_at(footer, 20),
            meta_off: u32_at(footer, 24),
            keys_off: u32_at(footer, 28),
            filter_off: u32_at(footer, 32),
            filter_len: u32_at(footer, 36),
            last_key_off: u32_at(footer, 40),
            last_key_len: u32_at(footer, 44),
            data_bytes: u64_at(footer, 48),
            tombstones: u32_at(footer, 56),
            raw,
        };
        if meta.id != expected_id {
            return Err(corruption(format!("expected page {expected_id}, found {}", meta.id)));
        }
        Ok(meta)
    }

    /// Copies the metadata out of the page image it may share, so holding it
    /// does not keep the whole image alive.
    pub fn detach(&mut self) {
        self.raw = Bytes::copy_from_slice(&self.raw);
    }

    /// Bytes of metadata, as stored at the end of the page.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    pub fn image_len(&self) -> u64 {
        self.meta_off as u64 + self.raw.len() as u64
    }

    pub fn block(&self, i: usize) -> BlockInfo {
        let at = i * INDEX_RECORD;
        BlockInfo {
            offset: u32_at(&self.raw, at),
            len: u32_at(&self.raw, at + 4),
            first_item: u32_at(&self.raw, at + 8),
        }
    }

    pub fn block_first_key(&self, i: usize) -> &[u8] {
        let at = i * INDEX_RECORD;
        let off = self.keys_off as usize + u32_at(&self.raw, at + 12) as usize;
        let len = u32_at(&self.raw, at + 16) as usize;
        &self.raw[off..off + len]
    }

    pub fn min_key(&self) -> &[u8] {
        self.block_first_key(0)
    }

    pub fn max_key(&self) -> &[u8] {
        let off = self.keys_off as usize + self.last_key_off as usize;
        &self.raw[off..off + self.last_key_len as usize]
    }

    pub fn may_contain(&self, hash: u64) -> bool {
        let off = self.filter_off as usize;
        bloom::may_contain(&self.raw[off..off + self.filter_len as usize], hash)
    }

    pub fn filter_bytes(&self) -> usize {
        self.filter_len as usize
    }

    /// The block that would hold `key`, or `None` if `key` is outside the
    /// page's key range.
    pub fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.n_blocks == 0 || key < self.min_key() || key > self.max_key() {
            return None;
        }
        Some(self.upper_block(key) - 1)
    }

    /// Number of blocks whose first key is below `key`.
    pub fn blocks_before(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0usize, self.n_blocks as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if cmp_keys(self.block_first_key(mid), key) == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Number of blocks whose first key is at most `key`.
    fn upper_block(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0usize, self.n_blocks as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if cmp_keys(self.block_first_key(mid), key) != Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Blocks that may hold keys in `[lo, hi)`; `hi = None` means unbounded.
    pub fn blocks_for_range(&self, lo: &[u8], hi: Option<&[u8]>) -> std::ops::Range<usize> {
        let n = self.n_blocks as usize;
        if n == 0 || hi.is_some_and(|hi| hi <= self.min_key()) || lo > self.max_key() {
            return 0..0;
        }
        let start = self.upper_block(lo).saturating_sub(1);
        let end = match hi {
            None => n,
            // Blocks whose first key is below `hi`.
            Some(hi) => {
                let (mut l, mut h) = (0usize, n);
                while l < h {
                    let mid = (l + h) / 2;
                    if cmp_keys(self.block_first_key(mid), hi) == Ordering::Less {
                        l = mid + 1;
                    } else {
                        h = mid;
                    }
                }
                l
            }
        };
        start..end.max(start)
    }

    /// Estimated logical bytes of entries in `[lo, hi)`, at block granularity.
    pub fn estimate_bytes(&self, lo: &[u8], hi: Option<&[u8]>) -> u64 {
        let blocks = self.blocks_for_range(lo, hi);
        if blocks.is_empty() || self.n_items == 0 {
            return 0;
        }
        let first = self.block(blocks.start).first_item as u64;
        let end = if blocks.end == self.n_blocks as usize {
            self.n_items as u64
        } else {
            self.block(blocks.end).first_item as u64
        };
        (end - first) * self.data_bytes / self.n_items as u64
    }

    /// Byte span of `blocks` within the page image.
    pub fn span(&self, blocks: std::ops::Range<usize>) -> (u64, u64) {
        if blocks.is_empty() {
            return (0, 0);
        }
        let first = self.block(blocks.start);
        let last = self.block(blocks.end - 1);
        (first.offset as u64, (last.offset + last.len - first.offset) as u64)
    }
}

/// A verified block.
#[derive(Clone)]
pub struct Block {
    bytes: Bytes,
    count: usize,
    offsets_pos: usize,
}

impl Block {
    /// Decodes a block read from disk, verifying its checksum.
    pub fn decode(bytes: Bytes, page_id: PageId) -> Result<Self> {
        if bytes.len() >= BLOCK_HEADER && u32_at(&bytes, 0) != block_crc(page_id, &bytes[4..]) {
            return Err(corruption(format!("page {page_id} block checksum mismatch")));
        }
        Self::decode_trusted(bytes)
    }

    /// Decodes a block of an image that never left memory.
    pub fn decode_trusted(bytes: Bytes) -> Result<Self> {
        if bytes.len() < BLOCK_HEADER {
            return Err(corruption("block too short"));
        }
        let count = u32_at(&bytes, 4) as usize;
        let offsets_pos = u32_at(&bytes, 8) as usize;
        if offsets_pos + count * 4 > bytes.len() {
            return Err(corruption("block offsets out of range"));
        }
        Ok(Self { bytes, count, offsets_pos })
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn entry_pos(&self, i: usize) -> usize {
        u32_at(&self.bytes, self.offsets_pos + i * 4) as usize
    }

    /// Byte offsets of entry `i` within the block: key start, key length,
    /// and value start and length (`None` for a tombstone).
    #[inline]
    pub fn entry_span(&self, i: usize) -> (usize, usize, Option<(usize, usize)>) {
        let pos = self.entry_pos(i);
        let b = &self.bytes;
        let klen = u16::from_le_bytes([b[pos], b[pos + 1]]) as usize;
        let vword = u32_at(b, pos + 2);
        let kstart = pos + ENTRY_HEADER;
        let value = (vword & TOMBSTONE == 0).then_some((kstart + klen, vword as usize));
        (kstart, klen, value)
    }

    pub fn raw(&self) -> &Bytes {
        &self.bytes
    }

    pub fn entry(&self, i: usize) -> EntryRef<'_> {
        let pos = self.entry_pos(i);
        let b = &self.bytes;
        let klen = u16::from_le_bytes([b[pos], b[pos + 1]]) as usize;
        let vword = u32_at(b, pos + 2);
        let kstart = pos + ENTRY_HEADER;
        let key = &b[kstart..kstart + klen];
        let value = if vword & TOMBSTONE != 0 {
            None
        } else {
            let vstart = kstart + klen;
            Some(&b[vstart..vstart + vword as usize])
        };
        EntryRef { key, value }
    }

    /// The value of entry `i` as a zero-copy slice; `None` for a tombstone.
    pub fn value_bytes(&self, i: usize) -> Option<Bytes> {
        let pos = self.entry_pos(i);
        let b = &self.bytes;
        let klen = u16::from_le_bytes([b[pos], b[pos + 1]]) as usize;
        let vword = u32_at(b, pos + 2);
        if vword & TOMBSTONE != 0 {
            return None;
        }
        let vstart = pos + ENTRY_HEADER + klen;
        Some(self.bytes.slice(vstart..vstart + vword as usize))
    }

    pub fn key_bytes(&self, i: usize) -> Bytes {
        let pos = self.entry_pos(i);
        let b = &self.bytes;
        let klen = u16::from_le_bytes([b[pos], b[pos + 1]]) as usize;
        self.bytes.slice(pos + ENTRY_HEADER..pos + ENTRY_HEADER + klen)
    }

    /// Binary search by key, as [`slice::binary_search`].
    pub fn search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match cmp_keys(self.entry(mid).key, key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }
}

/// A complete page: its image and decoded metadata.
#[derive(Clone)]
pub struct PageImage {
    pub bytes: Bytes,
    pub meta: PageMeta,
}

impl PageImage {
    /// Decodes and verifies the metadata of a full image read from disk.
    /// Blocks are verified as they are read.
    pub fn decode(bytes: Bytes, id: PageId, meta_off: u32) -> Result<Self> {
        if meta_off as usize > bytes.len() {
            return Err(corruption("page metadata offset out of range"));
        }
        let meta = PageMeta::decode(bytes.slice(meta_off as usize..), id)?;
        if meta.meta_off != meta_off {
            return Err(corruption("page metadata offset mismatch"));
        }
        Ok(Self { bytes, meta })
    }

    pub fn block(&self, i: usize) -> Result<Block> {
        let info = self.meta.block(i);
        let range = info.offset as usize..(info.offset + info.len) as usize;
        Block::decode(self.bytes.slice(range), self.meta.id)
    }
}

/// Builds a page from entries pushed in strictly increasing key order.
pub struct PageBuilder {
    id: PageId,
    buf: Vec<u8>,
    block_start: usize,
    block_entries: Vec<u32>,
    index: Vec<BlockInfo>,
    first_keys: Vec<(u32, u32)>,
    keys: Vec<u8>,
    hashes: Vec<u64>,
    n_items: u32,
    data_bytes: u64,
    tombstones: u32,
    last_key: Vec<u8>,
}

impl PageBuilder {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            buf: Vec::new(),
            block_start: 0,
            block_entries: Vec::new(),
            index: Vec::new(),
            first_keys: Vec::new(),
            keys: Vec::new(),
            hashes: Vec::new(),
            n_items: 0,
            data_bytes: 0,
            tombstones: 0,
            last_key: Vec::new(),
        }
    }

    pub fn with_capacity(id: PageId, bytes: usize) -> Self {
        let mut b = Self::new(id);
        b.buf.reserve(bytes + bytes / 32);
        b
    }

    pub fn is_empty(&self) -> bool {
        self.n_items == 0
    }

    pub fn len(&self) -> u32 {
        self.n_items
    }

    /// Logical bytes pushed so far.
    pub fn data_bytes(&self) -> u64 {
        self.data_bytes
    }

    pub fn last_key(&self) -> Option<&[u8]> {
        (self.n_items > 0).then_some(&self.last_key[..])
    }

    pub fn push(&mut self, e: EntryRef<'_>) {
        debug_assert!(self.n_items == 0 || e.key > &self.last_key[..], "keys out of order");
        let vlen = e.value.map_or(0, <[u8]>::len);
        let size = ENTRY_HEADER + e.key.len() + vlen;
        let used = self.buf.len() - self.block_start + 4 * self.block_entries.len();
        if !self.block_entries.is_empty() && used + size + 4 > BLOCK_SIZE {
            self.close_block();
        }
        if self.block_entries.is_empty() {
            self.block_start = self.buf.len();
            self.buf.extend_from_slice(&[0; BLOCK_HEADER]);
            self.first_keys.push((self.keys.len() as u32, e.key.len() as u32));
            self.keys.extend_from_slice(e.key);
            self.index.push(BlockInfo {
                offset: self.block_start as u32,
                len: 0,
                first_item: self.n_items,
            });
        }
        self.block_entries.push((self.buf.len() - self.block_start) as u32);
        self.buf.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
        let vword = match e.value {
            Some(v) => v.len() as u32,
            None => {
                self.tombstones += 1;
                TOMBSTONE
            }
        };
        self.buf.extend_from_slice(&vword.to_le_bytes());
        self.buf.extend_from_slice(e.key);
        if let Some(v) = e.value {
            self.buf.extend_from_slice(v);
        }
        self.hashes.push(bloom::hash_key(e.key));
        self.n_items += 1;
        self.data_bytes += size as u64;
        self.last_key.clear();
        self.last_key.extend_from_slice(e.key);
    }

    fn close_block(&mut self) {
        let offsets_pos = self.buf.len() - self.block_start;
        for off in self.block_entries.drain(..) {
            self.buf.extend_from_slice(&off.to_le_bytes());
        }
        let len = (self.buf.len() - self.block_start).next_multiple_of(BLOCK_SIZE);
        self.buf.resize(self.block_start + len, 0);
        let entries = self.n_items - self.index.last().unwrap().first_item;
        let block = &mut self.buf[self.block_start..];
        block[4..8].copy_from_slice(&entries.to_le_bytes());
        block[8..12].copy_from_slice(&(offsets_pos as u32).to_le_bytes());
        let crc = block_crc(self.id, &block[4..]);
        block[0..4].copy_from_slice(&crc.to_le_bytes());
        self.index.last_mut().unwrap().len = len as u32;
    }

    pub fn finish(mut self, bits_per_key: u32) -> PageImage {
        if !self.block_entries.is_empty() {
            self.close_block();
        }
        let meta_off = self.buf.len();
        let n_blocks = self.index.len();
        for (info, (key_off, key_len)) in self.index.iter().zip(&self.first_keys) {
            for word in [info.offset, info.len, info.first_item, *key_off, *key_len] {
                self.buf.extend_from_slice(&word.to_le_bytes());
            }
        }
        let keys_off = self.buf.len() - meta_off;
        let last_key_off = self.keys.len();
        self.buf.extend_from_slice(&self.keys);
        self.buf.extend_from_slice(&self.last_key);
        let filter_off = self.buf.len() - meta_off;
        let filter_len = bloom::filter_len(self.hashes.len(), bits_per_key);
        let filter_start = self.buf.len();
        self.buf.resize(filter_start + filter_len, 0);
        for &h in &self.hashes {
            bloom::insert(&mut self.buf[filter_start..], h);
        }
        let footer_start = self.buf.len();
        self.buf.extend_from_slice(&MAGIC.to_le_bytes());
        self.buf.extend_from_slice(&[0; 4]);
        self.buf.extend_from_slice(&self.id.to_le_bytes());
        for word in [
            self.n_items,
            n_blocks as u32,
            meta_off as u32,
            keys_off as u32,
            filter_off as u32,
            filter_len as u32,
            last_key_off as u32,
            self.last_key.len() as u32,
        ] {
            self.buf.extend_from_slice(&word.to_le_bytes());
        }
        self.buf.extend_from_slice(&self.data_bytes.to_le_bytes());
        self.buf.extend_from_slice(&self.tombstones.to_le_bytes());
        self.buf.resize(footer_start + FOOTER_LEN, 0);
        let mut crc = crc32c::crc32c(&self.buf[meta_off..footer_start + 4]);
        crc = crc32c::crc32c_append(crc, &self.buf[footer_start + 8..]);
        self.buf[footer_start + 4..footer_start + 8].copy_from_slice(&crc.to_le_bytes());
        // The image may stay pinned or cached for a long time, so it must not
        // keep much slack. `shrink_to_fit` cannot guarantee that: mimalloc
        // keeps a block in place when shrinking it by less than half, so a
        // 4.1 MB page that outgrew a 4 MiB buffer would hold 8 MiB. A copy
        // into an exact allocation can.
        let bytes = if self.buf.capacity() - self.buf.len() > self.buf.len() / 16 {
            Bytes::copy_from_slice(&self.buf)
        } else {
            Bytes::from(self.buf)
        };
        let meta = PageMeta::decode(bytes.slice(meta_off..), self.id).expect("fresh page decodes");
        PageImage { bytes, meta }
    }
}

/// Compares a key against an optional exclusive upper bound.
pub fn below(key: &[u8], hi: Option<&[u8]>) -> bool {
    hi.is_none_or(|hi| key < hi)
}

/// Whether `key` lies in `[lo, hi)`.
pub fn in_range(key: &[u8], lo: &[u8], hi: Option<&[u8]>) -> bool {
    key >= lo && below(key, hi)
}

pub fn bound_ref(b: &Bound<Bytes>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(&k[..]),
        Bound::Excluded(k) => Bound::Excluded(&k[..]),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(n: u32, vlen: usize) -> PageImage {
        let mut b = PageBuilder::new(42);
        for i in 0..n {
            let key = format!("key{i:08}");
            let value = vec![i as u8; vlen];
            let v = if i % 7 == 3 { None } else { Some(&value[..]) };
            b.push(EntryRef { key: key.as_bytes(), value: v });
        }
        b.finish(10)
    }

    #[test]
    fn roundtrip() {
        for &(n, vlen) in &[(1u32, 0usize), (100, 100), (5000, 37), (10, 9000)] {
            let page = build(n, vlen);
            let reread = PageImage::decode(page.bytes.clone(), 42, page.meta.meta_off).unwrap();
            assert_eq!(reread.meta.n_items, n);
            let mut seen = 0u32;
            for bi in 0..reread.meta.n_blocks as usize {
                let block = reread.block(bi).unwrap();
                assert_eq!(reread.meta.block(bi).first_item, seen);
                for i in 0..block.len() {
                    let e = block.entry(i);
                    assert_eq!(e.key, format!("key{seen:08}").as_bytes());
                    assert_eq!(e.value.is_none(), seen % 7 == 3);
                    seen += 1;
                }
            }
            assert_eq!(seen, n);
            let key = format!("key{:08}", n - 1);
            let b = reread.meta.find_block(key.as_bytes()).unwrap();
            assert!(reread.block(b).unwrap().search(key.as_bytes()).is_ok());
            assert!(reread.meta.may_contain(bloom::hash_key(key.as_bytes())));
        }
    }

    #[test]
    fn detects_corruption() {
        let page = build(500, 50);
        let mut raw = page.bytes.to_vec();
        raw[100] ^= 1;
        let bad = PageImage::decode(Bytes::from(raw), 42, page.meta.meta_off).unwrap();
        assert!(bad.block(0).is_err());
        let mut raw = page.bytes.to_vec();
        let at = page.meta.meta_off as usize + 3;
        raw[at] ^= 4;
        assert!(PageImage::decode(Bytes::from(raw), 42, page.meta.meta_off).is_err());
        assert!(PageImage::decode(page.bytes.clone(), 41, page.meta.meta_off).is_err());
    }
}
