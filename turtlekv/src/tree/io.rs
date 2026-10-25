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
use std::hash::BuildHasherDefault;
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
        Self { meta: image.meta, ptr: OnceLock::new(), pinned: Mutex::new(Some(image.bytes)) }
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

    fn pinned(&self) -> Option<Bytes> {
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

type Hasher = BuildHasherDefault<std::hash::DefaultHasher>;

#[derive(Default, Debug)]
pub struct IoCounters {
    pub block_reads: AtomicU64,
    pub span_reads: AtomicU64,
    pub span_read_bytes: AtomicU64,
    pub pages_written: AtomicU64,
    pub page_bytes_written: AtomicU64,
    pub cache_hits: AtomicU64,
}

/// Reads and writes data pages through a byte-weighted cache. The cache
/// hashes deterministically, so simulated runs replay exactly.
pub struct PageIo {
    pub store: PageStore,
    cache: Cache<(PageId, u32), Cached, Weigh, Hasher>,
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
            cache: Cache::with_options(options, Weigh, Hasher::default(), Default::default()),
            counters: IoCounters::default(),
        }
    }

    /// Block `b` of `page`, verified.
    pub fn block(&self, page: &DataPage, b: usize) -> Result<Block> {
        let info = page.meta.block(b);
        let range = info.offset as usize..(info.offset + info.len) as usize;
        if let Some(image) = page.pinned() {
            return Block::decode(image.slice(range), page.id());
        }
        let id = page.id();
        match self.cache.get(&(id, IMAGE)) {
            Some(Cached::Image(image)) => {
                self.counters.cache_hits.fetch_add(1, Relaxed);
                return Block::decode(image.slice(range), id);
            }
            _ => {}
        }
        if let Some(Cached::Block(block)) = self.cache.get(&(id, b as u32)) {
            self.counters.cache_hits.fetch_add(1, Relaxed);
            return Ok(block);
        }
        let ptr = page.ptr().expect("unpinned page is written");
        let bytes = self.store.read(&ptr, info.offset as u64, info.len as u64)?;
        self.counters.block_reads.fetch_add(1, Relaxed);
        let block = Block::decode(bytes, id)?;
        self.cache.insert((id, b as u32), Cached::Block(block.clone()));
        Ok(block)
    }

    /// The bytes of `blocks`, contiguous, for sequential reading. The first
    /// returned byte is the start of `blocks.start`. Reads from disk bypass
    /// the cache, since merges consume what they read.
    pub fn span(&self, page: &DataPage, blocks: Range<usize>) -> Result<Bytes> {
        let (off, len) = page.meta.span(blocks);
        let range = off as usize..(off + len) as usize;
        if let Some(image) = page.pinned() {
            return Ok(image.slice(range));
        }
        if let Some(Cached::Image(image)) = self.cache.get(&(page.id(), IMAGE)) {
            self.counters.cache_hits.fetch_add(1, Relaxed);
            return Ok(image.slice(range));
        }
        let ptr = page.ptr().expect("unpinned page is written");
        self.counters.span_reads.fetch_add(1, Relaxed);
        self.counters.span_read_bytes.fetch_add(len, Relaxed);
        self.store.read(&ptr, off, len)
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
