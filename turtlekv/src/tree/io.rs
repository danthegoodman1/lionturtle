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
use std::cell::Cell;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Range;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

pub struct DataPage {
    pub meta: PageMeta,
    ptr: OnceLock<PagePtr>,
    pinned: Mutex<Option<Bytes>>,
    /// Whether the cache may hold the whole image. Writing caches it; once
    /// evicted it never returns, so a miss clears this and later reads skip
    /// the image lookup.
    image_cached: AtomicBool,
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
        Self { meta, ptr: OnceLock::new(), pinned: Mutex::new(Some(image.bytes)), image_cached: AtomicBool::new(true) }
    }

    pub fn written(meta: PageMeta, ptr: PagePtr) -> Self {
        let lock = OnceLock::new();
        let _ = lock.set(ptr);
        Self { meta, ptr: lock, pinned: Mutex::new(None), image_cached: AtomicBool::new(false) }
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

thread_local! {
    /// Range opens left before this thread stops prefetching, renewed by each
    /// block read that waited on the device. A thread whose reads hit a cache
    /// skips the prefetch and its extra index and cache lookups.
    static PREFETCH_BUDGET: Cell<u8> = const { Cell::new(0) };
}

/// Range opens a device read keeps prefetching enabled for.
const PREFETCH_RENEW: u8 = 8;
/// Reads slower than this went to the device rather than the page cache.
const DEVICE_READ: std::time::Duration = std::time::Duration::from_micros(20);

/// Enables prefetching for this thread's next range opens, as a device read
/// would. The simulator calls it, since its reads never wait on a device.
pub fn arm_prefetch() {
    PREFETCH_BUDGET.set(PREFETCH_RENEW);
}

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

    fn cached_image(&self, page: &DataPage) -> Option<Bytes> {
        if !page.image_cached.load(Relaxed) {
            return None;
        }
        match self.cache.get(&(page.id(), IMAGE)) {
            Some(Cached::Image(image)) => Some(image),
            _ => {
                page.image_cached.store(false, Relaxed);
                None
            }
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
        if let Some(image) = self.cached_image(page) {
            return Block::decode_trusted(image.slice(range));
        }
        if let Some(Cached::Block(block)) = self.cache.get(&(id, b as u32)) {
            return Ok(block);
        }
        let ptr = page.ptr().expect("unpinned page is written");
        let t = std::time::Instant::now();
        let bytes = self.store.read(&ptr, info.offset as u64, info.len as u64)?;
        if t.elapsed() > DEVICE_READ {
            PREFETCH_BUDGET.set(PREFETCH_RENEW);
        }
        self.counters.block_reads.fetch_add(1, Relaxed);
        let block = Block::decode(bytes, id)?;
        self.cache.insert((id, b as u32), Cached::Block(block.clone()));
        Ok(block)
    }

    /// Whether reading block `b` of `page` would go to disk.
    fn is_cold(&self, page: &DataPage, b: usize) -> bool {
        let id = page.id();
        page.is_written()
            && !(page.image_cached.load(Relaxed) && self.cache.contains_key(&(id, IMAGE)))
            && !self.cache.contains_key(&(id, b as u32))
    }

    /// Whether a range about to open should prefetch: true while this thread
    /// has read from disk recently.
    pub fn should_prefetch(&self) -> bool {
        let budget = PREFETCH_BUDGET.get();
        PREFETCH_BUDGET.set(budget.saturating_sub(1));
        budget > 0
    }

    /// Starts the disk reads of the cold blocks among `blocks` together, so
    /// their latencies overlap instead of adding up. A lone cold block is
    /// left to its synchronous read, which would gain nothing.
    pub fn prefetch(&self, blocks: &[(&DataPage, usize)]) {
        let mut cold = blocks.iter().filter(|(page, b)| self.is_cold(page, *b));
        let (Some(first), Some(second)) = (cold.next(), cold.next()) else {
            return;
        };
        for (page, b) in [first, second].into_iter().chain(cold) {
            if let Some(ptr) = page.ptr() {
                let info = page.meta.block(*b);
                self.store.prefetch(&ptr, info.offset as u64, info.len as u64);
            }
        }
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
        if let Some(image) = self.cached_image(page) {
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
        // Cache the image before publishing the pointer, so a reader that
        // sees the page written finds the image.
        self.cache.insert((page.id(), IMAGE), Cached::Image(image));
        page.ptr.set(ptr).expect("page written once");
        *page.pinned.lock() = None;
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
