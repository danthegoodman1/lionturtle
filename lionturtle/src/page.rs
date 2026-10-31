//! Data pages: immutable sorted runs of entries. Tree leaves and node-buffer
//! segments share this format. Without compression, a page's in-memory image
//! is what goes to disk:
//!
//! ```text
//! block*  | index | keys | filter | footer
//! ```
//!
//! Entries are packed into blocks of [`BLOCK_SIZE`] bytes, each padded to a
//! multiple of it (a block holding one entry larger than that spans several),
//! so a point lookup reads one aligned 4 KiB page. Each block is checksummed
//! on its own as its page is written, by [`seal_blocks`] or
//! [`compress_image`]; a page in memory is trusted and leaves the checksums
//! zero:
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
//!
//! With [`Compression::Lz4`], a page's blocks go unpadded in memory, each
//! holding about as many entries as compress into one 4 KiB page, and
//! [`compress_image`] builds the stored form that goes to disk. Each stored
//! block starts at a multiple of [`BLOCK_SIZE`] and is padded to one, so a
//! point lookup still reads one aligned page, and the metadata region gains
//! a map of where each stored block starts:
//!
//! ```text
//! stored page  = stored block* | index | keys | filter | map | footer
//! stored block = block
//!              | crc: u32 | COMPRESSED: u32 | raw_len: u32 | payload_len: u32 | payload | padding
//! map          = offset: u32 * n_blocks
//! ```
//!
//! A block is stored compressed with LZ4 when that takes fewer 4 KiB pages
//! than the block itself, and otherwise as it would be without compression.
//! `COMPRESSED` is a count no block can hold, which tells the two apart. The
//! index keeps describing the image, so pages in memory and in the cache stay
//! uncompressed, and only reads from disk decompress.

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
const STORED_HEADER: usize = 16;
/// Marks a stored block as compressed, in the place of a block's count.
const COMPRESSED: u32 = u32::MAX;
/// Largest block a compressed page holds in memory. Each stored block takes
/// at least one 4 KiB page, so this bounds compression to a quarter.
pub const MAX_COMPRESSED_BLOCK: usize = 4 * BLOCK_SIZE;
/// Payload room in one page of a stored compressed block.
pub const PAGE_PAYLOAD: usize = BLOCK_SIZE - STORED_HEADER;

/// How data pages are stored on disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    /// Blocks are stored as they sit in memory, one per 4 KiB.
    #[default]
    None,
    /// Each block is compressed on its own with LZ4.
    Lz4,
}

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
    /// Offset of the stored-block map within the region; 0 when blocks are
    /// stored as in the image.
    map_off: u32,
}

impl std::fmt::Debug for PageMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageMeta").field("id", &self.id).field("n_items", &self.n_items).field("data_bytes", &self.data_bytes).finish()
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
            map_off: u32_at(footer, 60),
            raw,
        };
        if meta.id != expected_id {
            return Err(corruption(format!("expected page {expected_id}, found {}", meta.id)));
        }
        if meta.map_off != 0 {
            let map_end = meta.map_off as usize + meta.n_blocks as usize * 4;
            if meta.map_off < meta.filter_off + meta.filter_len || map_end > f {
                return Err(corruption(format!("page {expected_id} block map out of range")));
            }
            // Stored blocks start on page boundaries, each at least a page
            // past the one before.
            let mut next = 0u64;
            for i in 0..meta.n_blocks as usize {
                let off = meta.stored_offset(i) as u64;
                if off < next || !off.is_multiple_of(BLOCK_SIZE as u64) {
                    return Err(corruption(format!("page {expected_id} block map out of order")));
                }
                next = off + BLOCK_SIZE as u64;
            }
            if next > meta.meta_off as u64 {
                return Err(corruption(format!("page {expected_id} block map past its blocks")));
            }
        }
        Ok(meta)
    }

    /// Whether this region was read from a page stored compressed.
    pub fn is_stored_compressed(&self) -> bool {
        self.map_off != 0
    }

    /// Offset of stored block `i` within the stored page.
    pub fn stored_offset(&self, i: usize) -> u32 {
        u32_at(&self.raw, self.map_off as usize + i * 4)
    }

    /// The metadata region of the stored form of this page: this region with
    /// `map` before the footer, and the footer pointing at it and at
    /// `meta_off`, where the region starts in the stored page.
    fn write_stored(&self, meta_off: u32, map: &[u32], out: &mut [u8]) -> usize {
        let body = self.raw.len() - FOOTER_LEN;
        out[..body].copy_from_slice(&self.raw[..body]);
        let mut at = body;
        for &off in map {
            out[at..at + 4].copy_from_slice(&off.to_le_bytes());
            at += 4;
        }
        let footer = at;
        out[footer..footer + FOOTER_LEN].copy_from_slice(&self.raw[body..]);
        out[footer + 24..footer + 28].copy_from_slice(&meta_off.to_le_bytes());
        out[footer + 60..footer + 64].copy_from_slice(&(body as u32).to_le_bytes());
        let mut crc = crc32c::crc32c(&out[..footer + 4]);
        crc = crc32c::crc32c_append(crc, &out[footer + 8..footer + FOOTER_LEN]);
        out[footer + 4..footer + 8].copy_from_slice(&crc.to_le_bytes());
        footer + FOOTER_LEN
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
        BlockInfo { offset: u32_at(&self.raw, at), len: u32_at(&self.raw, at + 4), first_item: u32_at(&self.raw, at + 8) }
    }

    pub fn block_first_key(&self, i: usize) -> &[u8] {
        let at = i * INDEX_RECORD;
        let off = self.keys_off as usize + u32_at(&self.raw, at + 12) as usize;
        let len = u32_at(&self.raw, at + 16) as usize;
        &self.raw[off..off + len]
    }

    /// Entries in block `i`.
    pub fn block_items(&self, i: usize) -> usize {
        let end = if i + 1 < self.n_blocks as usize { self.block(i + 1).first_item } else { self.n_items };
        (end - self.block(i).first_item) as usize
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
        let end = if blocks.end == self.n_blocks as usize { self.n_items as u64 } else { self.block(blocks.end).first_item as u64 };
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

    /// Decodes a block of a compressed page as read from disk: verifies its
    /// checksum, then decompresses it, or decodes it as it is if it was
    /// stored uncompressed.
    pub fn decode_stored(stored: Bytes, page_id: PageId) -> Result<Self> {
        let Some((payload, raw_len)) = compressed_payload(&stored, page_id)? else {
            return Self::decode(stored, page_id);
        };
        match lz4_flex::block::decompress(payload, raw_len) {
            Ok(raw) if raw.len() == raw_len => Self::decode_trusted(Bytes::from(raw)),
            _ => Err(corruption(format!("page {page_id} block does not decompress"))),
        }
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
    /// Bytes a block may hold before the next entry starts a new one.
    block_cap: usize,
    /// Whether blocks are padded to a multiple of [`BLOCK_SIZE`], as a page
    /// stored as it is in memory needs.
    pad: bool,
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
            block_cap: BLOCK_SIZE,
            pad: true,
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

    /// Lays blocks out for a page that will be stored compressed: up to
    /// `block_cap` bytes each, at most [`MAX_COMPRESSED_BLOCK`], without
    /// padding.
    pub fn for_compression(mut self, block_cap: usize) -> Self {
        self.block_cap = block_cap.clamp(BLOCK_SIZE, MAX_COMPRESSED_BLOCK);
        self.pad = false;
        self
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
        if !self.block_entries.is_empty() && used + size + 4 > self.block_cap {
            self.close_block();
        }
        if self.block_entries.is_empty() {
            self.block_start = self.buf.len();
            self.buf.extend_from_slice(&[0; BLOCK_HEADER]);
            self.first_keys.push((self.keys.len() as u32, e.key.len() as u32));
            self.keys.extend_from_slice(e.key);
            self.index.push(BlockInfo { offset: self.block_start as u32, len: 0, first_item: self.n_items });
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
        let mut len = self.buf.len() - self.block_start;
        if self.pad {
            len = len.next_multiple_of(BLOCK_SIZE);
            self.buf.resize(self.block_start + len, 0);
        }
        let entries = self.n_items - self.index.last().unwrap().first_item;
        let block = &mut self.buf[self.block_start..];
        block[4..8].copy_from_slice(&entries.to_le_bytes());
        block[8..12].copy_from_slice(&(offsets_pos as u32).to_le_bytes());
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

/// The payload and block length of a stored compressed block, verified, or
/// `None` for a block stored as it is, which carries its own checksum.
fn compressed_payload(stored: &[u8], page_id: PageId) -> Result<Option<(&[u8], usize)>> {
    if stored.len() < BLOCK_HEADER {
        return Err(corruption(format!("page {page_id} stored block too short")));
    }
    if u32_at(stored, 4) != COMPRESSED {
        return Ok(None);
    }
    let end = if stored.len() >= STORED_HEADER { STORED_HEADER + u32_at(stored, 12) as usize } else { usize::MAX };
    if end > stored.len() || u32_at(stored, 0) != block_crc(page_id, &stored[4..end]) {
        return Err(corruption(format!("page {page_id} block checksum mismatch")));
    }
    Ok(Some((&stored[STORED_HEADER..end], u32_at(stored, 8) as usize)))
}

/// Decompresses `payload` into all of `out`.
fn decompress(payload: &[u8], out: &mut [u8], page_id: PageId) -> Result<()> {
    match lz4_flex::block::decompress_into(payload, out) {
        Ok(n) if n == out.len() => Ok(()),
        _ => Err(corruption(format!("page {page_id} block does not decompress"))),
    }
}

/// Verifies stored block `stored` and writes the block it holds to the
/// start of `out`, the block's place in the image.
pub fn inflate_into(stored: &[u8], page_id: PageId, out: &mut [u8]) -> Result<()> {
    match compressed_payload(stored, page_id)? {
        Some((payload, raw_len)) => match out.get_mut(..raw_len) {
            Some(out) => decompress(payload, out, page_id),
            None => Err(corruption(format!("page {page_id} block larger than its place"))),
        },
        None => {
            if u32_at(stored, 0) != block_crc(page_id, &stored[4..]) {
                return Err(corruption(format!("page {page_id} block checksum mismatch")));
            }
            let Some(block) = stored.get(..out.len()) else {
                return Err(corruption(format!("page {page_id} stored block shorter than its block")));
            };
            out.copy_from_slice(block);
            Ok(())
        }
    }
}

/// Fills in the checksum of every block of `image`, a copy of a page's
/// image about to be written as it is.
pub fn seal_blocks(image: &mut [u8], meta: &PageMeta) {
    for i in 0..meta.n_blocks as usize {
        let info = meta.block(i);
        let block = &mut image[info.offset as usize..(info.offset + info.len) as usize];
        let crc = block_crc(meta.id, &block[4..]);
        block[0..4].copy_from_slice(&crc.to_le_bytes());
    }
}

/// Bytes [`compress_image`] may need for a page. A block is stored
/// compressed only in fewer pages than it takes as it is, so the stored
/// blocks fit in the pages the blocks would take uncompressed, past which
/// LZ4 needs room for its worst case while it compresses the last one.
pub fn compressed_bound(meta: &PageMeta) -> usize {
    let n = meta.n_blocks as usize;
    let (mut pages, mut scratch) = (0, 0);
    for i in 0..n {
        let len = meta.block(i).len as usize;
        pages += len.next_multiple_of(BLOCK_SIZE);
        scratch = scratch.max(lz4_flex::block::get_maximum_output_size(len));
    }
    pages + STORED_HEADER + scratch + meta.len() + 4 * n + BLOCK_SIZE
}

/// A page's stored form, as [`compress_image`] wrote it.
pub struct StoredPage {
    pub len: usize,
    /// Where the metadata region starts.
    pub meta_off: u32,
    /// Where each stored block starts.
    pub map: Box<[u32]>,
    /// The compression ratio, compressed over raw bytes, that all but about
    /// one in a hundred of the page's compressed blocks met or beat. The
    /// page's last block, which is usually short, counts only when it is
    /// the only one.
    pub fit_ratio: Option<f64>,
}

/// Writes the stored form of a page to `out`, which must hold
/// [`compressed_bound`] bytes.
pub fn compress_image(image: &[u8], meta: &PageMeta, out: &mut [u8]) -> StoredPage {
    let n = meta.n_blocks as usize;
    let mut map = Vec::with_capacity(n);
    let mut at = 0usize;
    let mut ratios: Vec<f32> = Vec::new();
    for i in 0..n {
        let info = meta.block(i);
        let block = &image[info.offset as usize..(info.offset + info.len) as usize];
        // The block without any padding: entries, then their offsets.
        let raw_len = u32_at(block, 8) as usize + 4 * u32_at(block, 4) as usize;
        let raw = &block[..raw_len];
        let raw_pages = raw_len.div_ceil(BLOCK_SIZE);
        // A block of one page cannot shrink by a page, so compressing it
        // only tells how well blocks compress; a sample tells enough.
        let compressed = if raw_pages > 1 || i % 8 == 0 {
            let dst = &mut out[at + STORED_HEADER..at + STORED_HEADER + lz4_flex::block::get_maximum_output_size(raw_len)];
            let c = lz4_flex::block::compress_into(raw, dst).ok();
            if i + 1 < n || ratios.is_empty() {
                ratios.push(c.map_or(1.0, |c| c as f32 / raw_len as f32));
            }
            c
        } else {
            None
        };
        let pages = match compressed {
            Some(c) if (STORED_HEADER + c).div_ceil(BLOCK_SIZE) < raw_pages => {
                out[at + 4..at + 8].copy_from_slice(&COMPRESSED.to_le_bytes());
                out[at + 8..at + 12].copy_from_slice(&(raw_len as u32).to_le_bytes());
                out[at + 12..at + 16].copy_from_slice(&(c as u32).to_le_bytes());
                let end = at + STORED_HEADER + c;
                let crc = block_crc(meta.id, &out[at + 4..end]);
                out[at..at + 4].copy_from_slice(&crc.to_le_bytes());
                let pages = (STORED_HEADER + c).div_ceil(BLOCK_SIZE);
                out[end..at + pages * BLOCK_SIZE].fill(0);
                pages
            }
            _ => {
                let end = at + raw_pages * BLOCK_SIZE;
                out[at..at + raw_len].copy_from_slice(raw);
                out[at + raw_len..end].fill(0);
                let crc = block_crc(meta.id, &out[at + 4..end]);
                out[at..at + 4].copy_from_slice(&crc.to_le_bytes());
                raw_pages
            }
        };
        map.push(at as u32);
        at += pages * BLOCK_SIZE;
    }
    let meta_off = at as u32;
    let len = at + meta.write_stored(meta_off, &map, &mut out[at..]);
    let fit_ratio = (!ratios.is_empty()).then(|| {
        let k = (ratios.len() - 1) * 99 / 100;
        *ratios.select_nth_unstable_by(k, f32::total_cmp).1 as f64
    });
    StoredPage { len, meta_off, map: map.into_boxed_slice(), fit_ratio }
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
            let page = sealed(build(n, vlen));
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

    /// `page` as written uncompressed, with its block checksums filled in.
    fn sealed(page: PageImage) -> PageImage {
        let mut bytes = page.bytes.to_vec();
        seal_blocks(&mut bytes, &page.meta);
        PageImage { bytes: Bytes::from(bytes), meta: page.meta }
    }

    /// Builds a page of `n` entries whose values repeat a pattern of
    /// `period` random bytes, or are random throughout when `period` is
    /// `vlen`, with blocks of up to `block_cap` bytes laid out for
    /// compression, or padded as for a page stored as it is.
    fn build_patterned(n: u32, vlen: usize, period: usize, block_cap: Option<usize>) -> PageImage {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut b = PageBuilder::new(7);
        if let Some(cap) = block_cap {
            b = b.for_compression(cap);
        }
        for i in 0..n {
            let mut value = vec![0u8; vlen];
            for byte in value.iter_mut().take(period) {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                *byte = seed as u8;
            }
            for j in period..vlen {
                value[j] = value[j - period];
            }
            let key = format!("key{i:08}");
            b.push(EntryRef { key: key.as_bytes(), value: (i % 11 != 5).then_some(&value[..]) });
        }
        b.finish(10)
    }

    /// The stored form of `page`, and its metadata as read back.
    fn store(page: &PageImage) -> (Vec<u8>, PageMeta) {
        let mut out = vec![0u8; compressed_bound(&page.meta)];
        let stored = compress_image(&page.bytes, &page.meta, &mut out);
        out.truncate(stored.len);
        let meta = PageMeta::decode(Bytes::copy_from_slice(&out[stored.meta_off as usize..]), page.meta.id).unwrap();
        assert!(meta.is_stored_compressed());
        assert_eq!(meta.meta_off, stored.meta_off);
        assert_eq!(meta.image_len(), stored.len as u64);
        for (i, &off) in stored.map.iter().enumerate() {
            assert_eq!(meta.stored_offset(i), off);
            assert_eq!(off as usize % BLOCK_SIZE, 0);
        }
        (out, meta)
    }

    /// Stored block `i`'s bytes.
    fn stored_block<'a>(stored: &'a [u8], meta: &PageMeta, i: usize) -> &'a [u8] {
        let end = if i + 1 < meta.n_blocks as usize { meta.stored_offset(i + 1) } else { meta.meta_off };
        &stored[meta.stored_offset(i) as usize..end as usize]
    }

    #[test]
    fn compressed_roundtrip() {
        // Patterned values compress; random ones, and the 9,000-byte values
        // that span several pages, stay as they are. A padded page stores
        // compressed too.
        let cases = [
            (2000u32, 120usize, 8usize, Some(MAX_COMPRESSED_BLOCK)),
            (2000, 120, 120, Some(BLOCK_SIZE)),
            (300, 1000, 50, Some(2 * BLOCK_SIZE)),
            (12, 9000, 9000, Some(BLOCK_SIZE)),
            (12, 9000, 3, Some(BLOCK_SIZE)),
            (2000, 120, 8, None),
            (1, 0, 0, Some(BLOCK_SIZE)),
        ];
        for (n, vlen, period, cap) in cases {
            let page = build_patterned(n, vlen, period, cap);
            let (stored, meta) = store(&page);
            // No block takes more pages than it would uncompressed, and a
            // compressible block takes one.
            let uncompressed: usize =
                (0..meta.n_blocks as usize).map(|i| (page.meta.block(i).len as usize).next_multiple_of(BLOCK_SIZE)).sum();
            let one_each = meta.n_blocks as usize * BLOCK_SIZE;
            assert!(meta.meta_off as usize <= uncompressed, "{n}x{vlen}/{period}");
            if period < vlen / 4 {
                assert_eq!(meta.meta_off as usize, one_each, "{n}x{vlen}/{period}");
                // Blocks larger than a page shrink.
                if cap.is_some_and(|c| c > BLOCK_SIZE) || vlen > BLOCK_SIZE {
                    assert!(one_each < uncompressed, "{n}x{vlen}/{period}");
                }
            }
            assert_eq!(meta.n_blocks, page.meta.n_blocks);
            let mut raw = vec![0u8; page.meta.meta_off as usize];
            for i in 0..meta.n_blocks as usize {
                let info = page.meta.block(i);
                let want = Block::decode_trusted(page.bytes.slice(info.offset as usize..(info.offset + info.len) as usize)).unwrap();
                let got = Block::decode_stored(Bytes::copy_from_slice(stored_block(&stored, &meta, i)), 7).unwrap();
                assert_eq!(got.len(), want.len());
                for e in 0..want.len() {
                    assert_eq!(got.entry(e), want.entry(e));
                }
                let slot = &mut raw[info.offset as usize..(info.offset + info.len) as usize];
                inflate_into(stored_block(&stored, &meta, i), 7, slot).unwrap();
                let back = Block::decode_trusted(Bytes::copy_from_slice(slot)).unwrap();
                assert_eq!(back.len(), want.len());
                assert_eq!(back.entry(back.len() - 1), want.entry(want.len() - 1));
            }
        }
    }

    #[test]
    fn compressed_detects_corruption() {
        let page = build_patterned(2000, 120, 8, Some(2 * BLOCK_SIZE));
        let (stored, meta) = store(&page);
        // Every byte up to the end of the payload is covered, header included.
        let block = stored_block(&stored, &meta, 1);
        assert_eq!(u32_at(block, 4), COMPRESSED);
        let payload_end = STORED_HEADER + u32_at(block, 12) as usize;
        for at in [0, 4, 7, 8, 12, 15, 16, payload_end / 2, payload_end - 1] {
            let mut bad = block.to_vec();
            bad[at] ^= 0x10;
            assert!(Block::decode_stored(Bytes::from(bad.clone()), 7).is_err(), "flip at {at}");
            assert!(inflate_into(&bad, 7, &mut [0u8; 2 * BLOCK_SIZE]).is_err(), "flip at {at}");
        }
        assert!(Block::decode_stored(Bytes::copy_from_slice(block), 8).is_err(), "wrong page id");
        assert!(Block::decode_stored(Bytes::copy_from_slice(&block[..4]), 7).is_err(), "truncated");
        assert!(Block::decode_stored(Bytes::copy_from_slice(&block[..payload_end - 1]), 7).is_err(), "truncated payload");
        // A block stored as it is keeps its own checksum.
        let page = build_patterned(200, 120, 120, Some(BLOCK_SIZE));
        let (stored, meta) = store(&page);
        let mut bad = stored_block(&stored, &meta, 1).to_vec();
        assert_ne!(u32_at(&bad, 4), COMPRESSED);
        bad[100] ^= 1;
        assert!(Block::decode_stored(Bytes::from(bad.clone()), 7).is_err());
        assert!(inflate_into(&bad, 7, &mut [0u8; BLOCK_SIZE]).is_err());
        // The map is part of the checksummed metadata.
        let mut bad = stored[meta.meta_off as usize..].to_vec();
        let map_at = bad.len() - FOOTER_LEN - 4;
        bad[map_at] ^= 1;
        assert!(PageMeta::decode(Bytes::from(bad), 7).is_err());
    }

    #[test]
    fn detects_corruption() {
        let page = sealed(build(500, 50));
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
