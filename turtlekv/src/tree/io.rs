//! Data page handles and the page cache.
//!
//! A [`DataPage`] is born in memory, pinned there by the tree that references
//! it, and written at the next checkpoint. Writing moves its image into the
//! cache; after that its blocks come from the cache or from disk.

use crate::error::Result;
use crate::page::{Block, PageId, PageImage, PageMeta};
use crate::pagestore::{PagePtr, PageStore};
use bytes::Bytes;
use parking_lot::Mutex;
use quick_cache::sync::Cache;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Range;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub struct DataPage {
    pub meta: PageMeta,
    ptr: OnceLock<PagePtr>,
    pinned: Mutex<Option<Bytes>>,
}

impl std::fmt::Debug for DataPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataPage({}, {} items, written={})", self.meta.id, self.meta.n_items, self.ptr.get().is_some())
    }
}

impl DataPage {
    pub fn unwritten(image: PageImage) -> Self {
        // The metadata outlives the pinned image, so it must not share the
        // image's allocation.
        let mut meta = image.meta;
        meta.detach();
        Self { meta, ptr: OnceLock::new(), pinned: Mutex::new(Some(image.bytes)) }
    }

    pub fn written(meta: PageMeta, ptr: PagePtr) -> Self {
        let lock = OnceLock::new();
        let _ = lock.set(ptr);
        Self { meta, ptr: lock, pinned: Mutex::new(None) }
    }

    pub fn id(&self) -> PageId {
        self.meta.id
    }

    pub fn ptr(&self) -> Option<PagePtr> {
        self.ptr.get().copied()
    }

    pub fn is_written(&self) -> bool {
        self.ptr.get().is_some()
    }

    /// The in-memory image of a page not yet written. A written page skips
    /// the lock: once written, its blocks come from the cache or disk.
    fn pinned(&self) -> Option<Bytes> {
        if self.is_written() {
            return None;
        }
        self.pinned.lock().clone()
    }

    /// Logical size of the entries.
    pub fn data_bytes(&self) -> u64 {
        self.meta.data_bytes
    }
}

#[derive(Clone)]
enum Cached {
    Image(Bytes),
    Block(Block),
}

#[derive(Clone)]
struct Weigh;

impl quick_cache::Weighter<(PageId, u32), Cached> for Weigh {
    fn weight(&self, _key: &(PageId, u32), val: &Cached) -> u64 {
        match val {
            Cached::Image(b) => b.len() as u64,
            Cached::Block(_) => crate::page::BLOCK_SIZE as u64,
        }
    }
}

const IMAGE: u32 = u32::MAX;

/// Hashes a cache key, a page id and a block number, by folding the words
/// together and finishing with splitmix64's avalanche. It has no random
/// seed, so simulated runs replay exactly.
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        let mut x = self.0;
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u64(b as u64);
        }
    }

    fn write_u32(&mut self, n: u32) {
        self.write_u64(n as u64);
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = self.0.rotate_left(32) ^ n;
    }
}

type BuildKeyHasher = BuildHasherDefault<KeyHasher>;

#[derive(Default, Debug)]
pub struct IoCounters {
    pub block_reads: AtomicU64,
    pub span_reads: AtomicU64,
    pub span_read_bytes: AtomicU64,
    pub pages_written: AtomicU64,
    pub page_bytes_written: AtomicU64,
}

/// Reads and writes data pages through a byte-weighted cache. The cache
/// hashes deterministically, so simulated runs replay exactly.
pub struct PageIo {
    pub store: PageStore,
    cache: Cache<(PageId, u32), Cached, Weigh, BuildKeyHasher>,
    pub counters: IoCounters,
}

impl PageIo {
    pub fn new(store: PageStore, cache_bytes: u64, max_image: u64) -> Self {
        // Every shard must hold a few whole page images.
        let shards = (cache_bytes / (max_image.max(1) * 4)).clamp(1, 64) as usize;
        let options = quick_cache::OptionsBuilder::new()
            .shards(shards)
            .estimated_items_capacity((cache_bytes / 4096).clamp(16, 1 << 24) as usize)
            .weight_capacity(cache_bytes.max(1))
            .build()
            .expect("valid cache options");
        Self {
            store,
            cache: Cache::with_options(options, Weigh, BuildKeyHasher::default(), Default::default()),
            counters: IoCounters::default(),
        }
    }

    /// Block `b` of `page`, verified.
    pub fn block(&self, page: &DataPage, b: usize) -> Result<Block> {
        let info = page.meta.block(b);
        let range = info.offset as usize..(info.offset + info.len) as usize;
        // Pinned and cached images were built in memory, so only blocks read
        // from disk need their checksums verified.
        if let Some(image) = page.pinned() {
            return Block::decode_trusted(image.slice(range));
        }
        let id = page.id();
        if let Some(Cached::Image(image)) = self.cache.get(&(id, IMAGE)) {
            return Block::decode_trusted(image.slice(range));
        }
        if let Some(Cached::Block(block)) = self.cache.get(&(id, b as u32)) {
            return Ok(block);
        }
        let ptr = page.ptr().expect("unpinned page is written");
        let bytes = self.store.read(&ptr, info.offset as u64, info.len as u64)?;
        self.counters.block_reads.fetch_add(1, Relaxed);
        let block = Block::decode(bytes, id)?;
        self.cache.insert((id, b as u32), Cached::Block(block.clone()));
        Ok(block)
    }

    /// The bytes of `blocks`, contiguous, for sequential reading, and whether
    /// they came from disk (so their checksums need verifying). The first
    /// returned byte is the start of `blocks.start`. Reads from disk bypass
    /// the cache, since merges consume what they read.
    pub fn span(&self, page: &DataPage, blocks: Range<usize>) -> Result<(Bytes, bool)> {
        let (off, len) = page.meta.span(blocks);
        let range = off as usize..(off + len) as usize;
        if let Some(image) = page.pinned() {
            return Ok((image.slice(range), false));
        }
        if let Some(Cached::Image(image)) = self.cache.get(&(page.id(), IMAGE)) {
            return Ok((image.slice(range), false));
        }
        let ptr = page.ptr().expect("unpinned page is written");
        self.counters.span_reads.fetch_add(1, Relaxed);
        self.counters.span_read_bytes.fetch_add(len, Relaxed);
        Ok((self.store.read(&ptr, off, len)?, true))
    }

    /// Writes an unwritten page to a fresh slot, then moves its image from the
    /// page into the cache. Returns true if an arena file was created.
    pub fn write(&self, page: &DataPage) -> Result<bool> {
        let image = page.pinned().expect("unwritten page is pinned");
        let ptr = self.store.allocate(page.id(), image.len() as u64, page.meta.meta_off)?;
        let created = self.store.write(&ptr, &image)?;
        self.counters.pages_written.fetch_add(1, Relaxed);
        self.counters.page_bytes_written.fetch_add(image.len() as u64, Relaxed);
        page.ptr.set(ptr).expect("page written once");
        *page.pinned.lock() = None;
        self.cache.insert((page.id(), IMAGE), Cached::Image(image));
        Ok(created)
    }

    /// Reads the metadata region of a written page.
    pub fn load_meta(&self, ptr: &PagePtr) -> Result<PageMeta> {
        let raw = self.store.read(ptr, ptr.meta_off as u64, (ptr.len - ptr.meta_off) as u64)?;
        let meta = PageMeta::decode(raw, ptr.id)?;
        if meta.meta_off != ptr.meta_off || meta.image_len() != ptr.len as u64 {
            return Err(crate::error::corruption(format!("page {} metadata does not match its pointer", ptr.id)));
        }
        Ok(meta)
    }

    pub fn cache_weight(&self) -> u64 {
        self.cache.weight()
    }

    pub fn cache_capacity(&self) -> u64 {
        self.cache.capacity()
    }

    /// Drops a garbage page's cached image. Its cached blocks age out, since
    /// page ids are never reused.
    pub fn evict(&self, page: PageId) {
        self.cache.remove(&(page, IMAGE));
    }
}
