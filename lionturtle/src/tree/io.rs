//! Data page handles and the page cache.
//!
//! A [`DataPage`] is born in memory, pinned there by the tree that references
//! it, and written at the next checkpoint. Writing moves its image into the
//! cache; after that its blocks come from the cache or from disk.

use crate::error::Result;
use crate::page::{self, BLOCK_SIZE, Block, Compression, PageBuilder, PageId, PageImage, PageMeta};
use crate::pagestore::{PagePtr, PageStore};
use crate::storage::DIRECT_ALIGN;
use bytes::Bytes;
use parking_lot::Mutex;
use quick_cache::sync::Cache;
use std::cell::Cell;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Range;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

pub struct DataPage {
    pub meta: PageMeta,
    ptr: OnceLock<PagePtr>,
    /// Where each block starts in the stored page, for a page this process
    /// wrote compressed. A page read back from disk finds them in `meta`.
    stored_map: OnceLock<Box<[u32]>>,
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
        Self {
            meta,
            ptr: OnceLock::new(),
            stored_map: OnceLock::new(),
            pinned: Mutex::new(Some(image.bytes)),
            image_cached: AtomicBool::new(true),
        }
    }

    pub fn written(meta: PageMeta, ptr: PagePtr) -> Self {
        let lock = OnceLock::new();
        let _ = lock.set(ptr);
        Self { meta, ptr: lock, stored_map: OnceLock::new(), pinned: Mutex::new(None), image_cached: AtomicBool::new(false) }
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

    /// Whether the page was stored in the compressed layout, each block on
    /// pages of its own, rather than as its image.
    fn stored_compressed(&self) -> bool {
        self.stored_map.get().is_some() || self.meta.is_stored_compressed()
    }

    /// Where stored block `b` starts, or where the blocks end for
    /// `b == n_blocks`, in a page stored compressed.
    fn stored_offset(&self, ptr: &PagePtr, b: usize) -> u64 {
        if b == self.meta.n_blocks as usize {
            return ptr.meta_off as u64;
        }
        match self.stored_map.get() {
            Some(map) => map[b] as u64,
            None => self.meta.stored_offset(b) as u64,
        }
    }

    /// Byte span of `blocks` within the stored page.
    fn stored_span(&self, ptr: &PagePtr, blocks: Range<usize>) -> (u64, u64) {
        if !self.stored_compressed() {
            return self.meta.span(blocks);
        }
        if blocks.is_empty() {
            return (0, 0);
        }
        let start = self.stored_offset(ptr, blocks.start);
        (start, self.stored_offset(ptr, blocks.end) - start)
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
            Cached::Block(b) => b.raw().len() as u64,
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
const DEVICE_READ: Duration = Duration::from_micros(20);
/// Bytes per write of a page while its checkpoint is paced, so a client read
/// queues behind at most this much per writer thread. Unpaced, a page goes
/// out in one write, which finishes sooner: on a drive throttled to 200
/// MB/s, whole 4 MiB writes raised read p99 at 15k ops/s from 17 to 71 ms,
/// and on an EC2 instance-store drive they raised peak throughput by 7%.
const PACED_WRITE: usize = 256 << 10;

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

/// A checkpoint's I/O schedule: `expected` bytes of merge reads and page
/// writes spread over `target`, or sooner as `progress` (the share of the
/// active memtable filled, 0 to 1) reaches `fill`.
pub struct Pace {
    pub expected: u64,
    pub target: Duration,
    pub progress: Box<dyn Fn() -> f64 + Send + Sync>,
    pub fill: f64,
}

/// Spreads a checkpoint's I/O evenly over the time it has before writers
/// would wait on it, at the lowest steady rate that finishes on time. Time
/// the checkpoint spends idle raises the rate for the rest of it rather than
/// allowing a burst later.
#[derive(Default)]
pub struct Pacer {
    state: Mutex<Option<Schedule>>,
    /// Nanoseconds checkpoint I/O has waited on the schedule.
    pub waited_ns: AtomicU64,
}

struct Schedule {
    pace: Pace,
    started: Instant,
    done: u64,
    /// When the next I/O may start.
    next: Instant,
}

impl Schedule {
    /// Seconds left before the deadline and bytes left of the estimate, or
    /// none once either runs out.
    fn left(&self, now: Instant) -> Option<(f64, u64)> {
        let remaining = self.pace.expected.saturating_sub(self.done);
        let target = self.pace.target.as_secs_f64();
        let share = (now.duration_since(self.started).as_secs_f64() / target).max((self.pace.progress)() / self.pace.fill);
        let left = target * (1.0 - share);
        (remaining > 0 && left > 0.0).then_some((left, remaining))
    }

    /// Accounts for `bytes` of I/O asked for at `now` and returns when it
    /// may start.
    fn claim(&mut self, bytes: u64, now: Instant) -> Instant {
        let left = self.left(now);
        self.done += bytes;
        // Past the estimate or the deadline, go at full speed.
        let Some((left, remaining)) = left else { return now };
        // Each I/O claims its share of the time left at the rate that
        // finishes on time. Time spent idle raises the rate rather than
        // banking a burst.
        let start = self.next.max(now);
        self.next = start + Duration::from_secs_f64(left * bytes as f64 / remaining as f64);
        start
    }
}

impl Pacer {
    pub fn start(&self, pace: Pace) {
        let now = Instant::now();
        *self.state.lock() = Some(Schedule { pace, started: now, done: 0, next: now });
    }

    /// Lets the running checkpoint's I/O proceed at full speed.
    pub fn stop(&self) {
        *self.state.lock() = None;
    }

    /// Whether a checkpoint's I/O is on a schedule, so it favors client
    /// reads over its own speed. That holds past the schedule's deadline,
    /// until writers would wait and [`Pacer::stop`] lifts it: on a drive
    /// throttled to 200 MB/s, switching a late checkpoint to whole-page writes
    /// and read-ahead held reads up to 190 ms, against 33 ms.
    pub fn pacing(&self) -> bool {
        self.state.lock().is_some()
    }

    /// Accounts for `bytes` of checkpoint I/O about to start, first waiting
    /// for its turn on the schedule.
    pub fn take(&self, bytes: u64) {
        let start = match self.state.lock().as_mut() {
            Some(s) => s.claim(bytes, Instant::now()),
            None => return,
        };
        // Re-check at least every 10 ms, since a stop lifts the pace.
        while start > Instant::now() {
            let wait = (start - Instant::now()).min(Duration::from_millis(10));
            std::thread::sleep(wait);
            self.waited_ns.fetch_add(wait.as_nanos() as u64, Relaxed);
            if self.state.lock().is_none() {
                break;
            }
        }
    }
}

/// Reads and writes data pages through a byte-weighted cache. The cache
/// hashes deterministically, so simulated runs replay exactly.
pub struct PageIo {
    pub store: PageStore,
    cache: Cache<(PageId, u32), Cached, Weigh, BuildKeyHasher>,
    pub counters: IoCounters,
    pub pacer: Pacer,
    /// How pages written from here on are stored.
    compression: Compression,
    /// Buffers that pages are written from, kept so that each write reuses
    /// memory already faulted in rather than zeroing a fresh allocation.
    write_bufs: Mutex<Vec<Vec<u8>>>,
    /// Largest buffer worth keeping: one for a page of the largest usual size.
    max_write_buf: usize,
    /// Bytes each block of a page built for compression holds: about what
    /// compresses into one page, judged from recent pages.
    block_target: AtomicUsize,
}

/// Share of a page that a block compressing at the learned ratio fills,
/// leaving room for pages that compress worse than the ones before them.
const COMPRESSED_FILL: f64 = 0.97;

/// Smallest block target that pages are laid out for compression at. Below
/// it, each page on disk would hold under an eighth more entries than
/// without compression, too little to pay for compressing every block as it
/// is written and decompressing every block read from disk.
const MIN_COMPRESSED_BLOCK: usize = BLOCK_SIZE + BLOCK_SIZE / 8;

impl PageIo {
    pub fn new(store: PageStore, cache_bytes: u64, max_image: u64, compression: Compression) -> Self {
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
            pacer: Pacer::default(),
            compression,
            write_bufs: Mutex::new(Vec::new()),
            max_write_buf: (max_image + max_image / 8) as usize + (2 << 20),
            block_target: AtomicUsize::new(BLOCK_SIZE),
        }
    }

    /// A builder for a page this store will write, its blocks laid out for
    /// how the store compresses: for compression while blocks compress well
    /// enough to reach [`MIN_COMPRESSED_BLOCK`], and padded otherwise.
    pub fn builder(&self, id: PageId, capacity: usize) -> PageBuilder {
        let b = PageBuilder::with_capacity(id, capacity);
        let target = self.block_target.load(Relaxed);
        match self.compression {
            Compression::Lz4 if target >= MIN_COMPRESSED_BLOCK => b.for_compression(target),
            _ => b,
        }
    }

    /// Aims later blocks at a size whose compressed form fits one page,
    /// given that nearly every block of a page just written compressed by
    /// `ratio` or better. Aiming at the worst blocks rather than the
    /// average keeps a block from spilling into a second page, which costs
    /// more than the space the average leaves unused. The target starts at
    /// one page, where nothing can shrink by a page, and moves halfway to
    /// each page's estimate.
    fn learn_ratio(&self, ratio: Option<f64>) {
        let Some(ratio) = ratio else { return };
        let ratio = ratio.max(1e-3);
        let target = ((COMPRESSED_FILL * page::PAGE_PAYLOAD as f64 / ratio) as usize).clamp(BLOCK_SIZE, page::MAX_COMPRESSED_BLOCK);
        let old = self.block_target.load(Relaxed);
        self.block_target.store((old + target) / 2, Relaxed);
    }

    /// Runs `f` on a buffer of `len` bytes, rounded up to a multiple of
    /// [`DIRECT_ALIGN`], at an aligned address. Its contents are arbitrary.
    fn with_write_buf<R>(&self, len: usize, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let len = len.next_multiple_of(DIRECT_ALIGN);
        let need = len + DIRECT_ALIGN;
        let kept = {
            let mut bufs = self.write_bufs.lock();
            bufs.iter().position(|b| b.len() >= need).map(|i| bufs.swap_remove(i))
        };
        let mut buf = kept.unwrap_or_else(|| vec![0u8; need]);
        let start = buf.as_ptr().align_offset(DIRECT_ALIGN);
        let r = f(&mut buf[start..start + len]);
        if buf.len() <= self.max_write_buf {
            let mut bufs = self.write_bufs.lock();
            if bufs.len() < self.page_writers() {
                bufs.push(buf);
            }
        }
        r
    }

    /// The page's whole image, if the cache holds it. Only merges, which read
    /// whole pages, count as a use of the image. A point read or a scan only
    /// looks: under skewed reads nearly every page holds a hot key, and
    /// letting those reads keep whole images resident would fill the cache
    /// with cold blocks.
    fn cached_image(&self, page: &DataPage, merge: bool) -> Option<Bytes> {
        if !page.image_cached.load(Relaxed) {
            return None;
        }
        let key = (page.id(), IMAGE);
        let hit = if merge { self.cache.get(&key) } else { self.cache.peek(&key) };
        match hit {
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
        if let Some(image) = self.cached_image(page, false) {
            return Block::decode_trusted(image.slice(range));
        }
        if let Some(Cached::Block(block)) = self.cache.get(&(id, b as u32)) {
            return Ok(block);
        }
        let ptr = page.ptr().expect("unpinned page is written");
        let (off, len) = page.stored_span(&ptr, b..b + 1);
        let t = Instant::now();
        let bytes = self.store.read(&ptr, off, len)?;
        if t.elapsed() > DEVICE_READ {
            PREFETCH_BUDGET.set(PREFETCH_RENEW);
        }
        self.counters.block_reads.fetch_add(1, Relaxed);
        let block = if page.stored_compressed() { Block::decode_stored(bytes, id)? } else { Block::decode(bytes, id)? };
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
                let (off, len) = page.stored_span(&ptr, *b..*b + 1);
                self.store.prefetch(&ptr, off, len);
            }
        }
    }

    /// Starts reading `blocks` of a written page into the page cache without
    /// waiting, so a merge's later span read of them waits less. A paced
    /// checkpoint skips it: read-ahead reaches the drive outside the
    /// schedule, and many merge threads' read-ahead queues ahead of client
    /// reads.
    pub fn prefetch_span(&self, page: &DataPage, blocks: Range<usize>) {
        if blocks.is_empty() || page.image_cached.load(Relaxed) || self.pacer.pacing() {
            return;
        }
        if let Some(ptr) = page.ptr() {
            let (off, len) = page.stored_span(&ptr, blocks);
            self.store.prefetch(&ptr, off, len);
        }
    }

    /// The bytes of `blocks`, contiguous and laid out as in the image, for
    /// sequential reading, and whether they still need their checksums
    /// verified. The first returned byte is the start of `blocks.start`.
    /// Reads from disk bypass the cache, since merges consume what they read.
    pub fn span(&self, page: &DataPage, blocks: Range<usize>) -> Result<(Bytes, bool)> {
        let (off, len) = page.meta.span(blocks.clone());
        let range = off as usize..(off + len) as usize;
        if let Some(image) = page.pinned() {
            return Ok((image.slice(range), false));
        }
        if let Some(image) = self.cached_image(page, true) {
            return Ok((image.slice(range), false));
        }
        let ptr = page.ptr().expect("unpinned page is written");
        let (stored_off, stored_len) = page.stored_span(&ptr, blocks.clone());
        self.counters.span_reads.fetch_add(1, Relaxed);
        self.counters.span_read_bytes.fetch_add(stored_len, Relaxed);
        self.pacer.take(stored_len);
        let stored = self.store.read(&ptr, stored_off, stored_len)?;
        if !page.stored_compressed() {
            return Ok((stored, true));
        }
        // Decompress each block straight into its place in the image layout.
        // Each block's checksum is verified on the way, so the caller has
        // none left to check.
        let mut raw = vec![0u8; len as usize];
        for b in blocks {
            let s = (page.stored_offset(&ptr, b) - stored_off) as usize;
            let e = (page.stored_offset(&ptr, b + 1) - stored_off) as usize;
            let info = page.meta.block(b);
            let at = (info.offset as u64 - off) as usize;
            page::inflate_into(&stored[s..e], page.id(), &mut raw[at..at + info.len as usize])?;
        }
        Ok((Bytes::from(raw), false))
    }

    /// Threads that write a checkpoint's queued pages. Each writes one page
    /// at a time and waits for it to reach the device, so two keep the drive
    /// busy. With compression, each spends longer compressing a page than
    /// writing it: on a RAM-backed store loading 3M records a second, two
    /// left writers waiting on checkpoints for up to 0.4 s, and four never.
    pub fn page_writers(&self) -> usize {
        match self.compression {
            Compression::None => 2,
            Compression::Lz4 => 4,
        }
    }

    /// Bytes per write of a page: [`PACED_WRITE`] while the checkpoint is
    /// paced, otherwise the whole page.
    pub fn write_piece(&self) -> usize {
        if self.pacer.pacing() { PACED_WRITE } else { usize::MAX }
    }

    /// Writes an unwritten page to a fresh slot, then moves its image from the
    /// page into the cache. Returns true if an arena file was created.
    pub fn write(&self, page: &DataPage) -> Result<bool> {
        let image = page.pinned().expect("unwritten page is pinned");
        let pace = |n| self.pacer.take(n);
        // The stored page is built in an aligned buffer, checksummed as it
        // goes, and written from there. A page laid out for compression is
        // stored compressed, and any other as its image; while the store
        // compresses, the latter's blocks are sampled, so the store notices
        // when they begin to compress well.
        let compress = self.compression == Compression::Lz4;
        let (ptr, created) = if compress && !page.meta.blocks_fill_pages() {
            self.with_write_buf(page::compressed_bound(&page.meta), |out| {
                let stored = page::compress_image(&image, &page.meta, out);
                self.learn_ratio(stored.fit_ratio);
                let padded = stored.len.next_multiple_of(DIRECT_ALIGN);
                out[stored.len..padded].fill(0);
                let ptr = self.store.allocate(page.id(), stored.len as u64, stored.meta_off)?;
                let created = self.store.write_aligned(&ptr, &out[..padded], self.write_piece(), &pace)?;
                page.stored_map.set(stored.map).expect("page written once");
                Ok::<_, crate::Error>((ptr, created))
            })?
        } else {
            let len = image.len().next_multiple_of(DIRECT_ALIGN);
            let scratch = if compress { page::sample_bound(&page.meta) } else { 0 };
            self.with_write_buf(len + scratch, |buf| {
                let (out, scratch) = buf.split_at_mut(len);
                if compress {
                    self.learn_ratio(page::sample_ratio(&image, &page.meta, scratch));
                }
                out[..image.len()].copy_from_slice(&image);
                page::seal_blocks(&mut out[..image.len()], &page.meta);
                out[image.len()..].fill(0);
                let ptr = self.store.allocate(page.id(), image.len() as u64, page.meta.meta_off)?;
                Ok::<_, crate::Error>((ptr, self.store.write_aligned(&ptr, out, self.write_piece(), &pace)?))
            })?
        };
        self.counters.pages_written.fetch_add(1, Relaxed);
        self.counters.page_bytes_written.fetch_add(ptr.len as u64, Relaxed);
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

    /// Bytes of whole page images in the cache. It visits every entry.
    pub fn cache_image_bytes(&self) -> u64 {
        self.cache.iter().map(|(_, v)| if let Cached::Image(b) = v { b.len() as u64 } else { 0 }).sum()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    const MS: Duration = Duration::from_millis(1);

    fn pace(expected: u64, target_ms: u64, progress: f64) -> Pace {
        Pace { expected, target: Duration::from_millis(target_ms), progress: Box::new(move || progress), fill: 0.5 }
    }

    fn schedule(pace: Pace, started: Instant) -> Schedule {
        Schedule { pace, started, done: 0, next: started }
    }

    /// Asserts that `t` falls `ms` milliseconds after `t0`, to the microsecond.
    #[track_caller]
    fn assert_at(t0: Instant, t: Instant, ms: f64) {
        let at = (t - t0).as_nanos() as f64 / 1e6;
        assert!((at - ms).abs() < 1e-3, "at {at} ms, want {ms}");
    }

    #[test]
    fn spaces_io_evenly() {
        let t0 = Instant::now();
        let mut s = schedule(pace(1000, 200, 0.0), t0);
        // I/O asked for just as its turn comes goes out every 50 ms.
        for i in 0..4 {
            assert_at(t0, s.claim(250, t0 + 50 * i * MS), 50.0 * i as f64);
        }
        // I/O asked for early waits for its turn.
        let mut s = schedule(pace(1000, 200, 0.0), t0);
        assert_at(t0, s.claim(250, t0), 0.0);
        assert_at(t0, s.claim(250, t0 + 10 * MS), 50.0);
    }

    #[test]
    fn idle_time_raises_the_rate_without_a_burst() {
        let t0 = Instant::now();
        let mut s = schedule(pace(1000, 400, 0.0), t0);
        // Half the time is gone and all the bytes remain: 250 bytes now take
        // 50 ms. At the starting rate they took 100 ms, and a schedule that
        // banked the idle time would let 500 through at once.
        let at = t0 + 200 * MS;
        assert_at(t0, s.claim(250, at), 200.0);
        assert_at(t0, s.claim(250, at), 250.0);
    }

    #[test]
    fn filling_memtable_deadline_and_overrun_lift_the_pace() {
        let t0 = Instant::now();
        // The next memtable is already as full as the pace's fill share.
        let mut s = schedule(pace(1000, 60_000, 0.5), t0);
        assert_eq!(s.claim(500, t0), t0);
        assert_eq!(s.claim(500, t0), t0);
        // Past the deadline.
        let mut s = schedule(pace(1000, 200, 0.0), t0);
        assert_eq!(s.claim(500, t0 + 200 * MS), t0 + 200 * MS);
        assert_eq!(s.claim(500, t0 + 200 * MS), t0 + 200 * MS);
        // Past the estimate.
        let mut s = schedule(pace(100, 60_000, 0.0), t0);
        assert_eq!(s.claim(150, t0), t0);
        assert_eq!(s.claim(50, t0), t0);
    }

    // The tests below run on the real clock, so they check waits from one
    // side only: a sleep can overrun on a busy machine, but never ends early.

    #[test]
    fn take_waits_its_turn_until_stopped() {
        let pacer = Pacer::default();
        let waited = || pacer.waited_ns.load(Relaxed);
        pacer.take(1 << 20);
        assert_eq!(waited(), 0, "nothing to pace");
        let started = Instant::now();
        pacer.start(pace(1000, 200, 0.0));
        let t = Instant::now();
        pacer.take(250);
        let first = Instant::now();
        assert_eq!(waited(), 0, "the first I/O starts at once");
        pacer.take(250);
        // The second waits out the first's share: a quarter of the time left
        // when the first asked.
        let share = (200 * MS).saturating_sub(first - started) / 4;
        assert!(t.elapsed() >= share, "waited {:?}, want {share:?}", t.elapsed());
        pacer.stop();
        let before = waited();
        pacer.take(400);
        assert_eq!(waited(), before, "a stop lifts the pace");
    }

    #[test]
    fn stop_ends_a_wait() {
        let pacer = Pacer::default();
        pacer.start(pace(1000, 60_000, 0.0));
        pacer.take(500);
        let t = Instant::now();
        std::thread::scope(|s| {
            s.spawn(|| {
                std::thread::sleep(50 * MS);
                pacer.stop();
            });
            // Its turn comes in 30 s.
            pacer.take(500);
        });
        assert!(t.elapsed() < Duration::from_secs(10), "waited {:?}", t.elapsed());
    }

    #[test]
    fn compressed_blocks_learn_to_fill_their_pages() {
        use crate::page::EntryRef;
        use crate::sim::{FaultConfig, SimStorage};
        let store = PageStore::open(std::sync::Arc::new(SimStorage::new(1, FaultConfig::default())), &[]).unwrap();
        let io = PageIo::new(store, 1 << 20, 4 << 20, Compression::Lz4);
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut fill = 0.0;
        for id in 1..=6u64 {
            // 120-byte values whose last 56 bytes repeat their first.
            let mut b = io.builder(id, 2 << 20);
            for i in 0..15_000u64 {
                let mut value = [0u8; 120];
                for chunk in value[..64].chunks_mut(8) {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    chunk.copy_from_slice(&seed.to_le_bytes());
                }
                value.copy_within(..56, 64);
                b.push(EntryRef { key: &(id << 32 | i).to_be_bytes(), value: Some(&value) });
            }
            let image = b.finish(10);
            let (bytes, meta) = (image.bytes.clone(), image.meta.clone());
            io.write(&DataPage::unwritten(image)).unwrap();

            // Each block of the page, but for its last, fits one page.
            let mut out = vec![0u8; page::compressed_bound(&meta)];
            let stored = page::compress_image(&bytes, &meta, &mut out);
            let n = stored.map.len() - 1;
            assert_eq!(stored.map[n] as usize, n * BLOCK_SIZE, "page {id}: a block spilled into a second page");
            let header = |at: u32, field: usize| u32::from_le_bytes(out[at as usize + field..][..4].try_into().unwrap());
            let compressed = stored.map[..n].iter().filter(|&&at| header(at, 4) == u32::MAX);
            let used: usize = compressed.map(|&at| 16 + header(at, 12) as usize).sum();
            fill = used as f64 / (n * BLOCK_SIZE) as f64;
        }
        // The first pages, sized for one raw page each, teach the store how
        // well the values compress; by the last, nearly all of every page
        // holds compressed data.
        assert!(fill > 0.9, "compressed blocks fill {fill:.3} of their pages");
    }

    #[test]
    fn pages_store_as_images_while_blocks_compress_too_little() {
        use crate::page::EntryRef;
        use crate::sim::{FaultConfig, SimStorage};
        let store = PageStore::open(std::sync::Arc::new(SimStorage::new(1, FaultConfig::default())), &[]).unwrap();
        let io = PageIo::new(store, 1 << 20, 4 << 20, Compression::Lz4);
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next_id = 0u64;
        // Writes a page of 120-byte values, random throughout or with their
        // last 56 bytes repeating their first, and returns whether it was
        // laid out for compression and whether it was stored compressed.
        let mut write = |compressible: bool| {
            next_id += 1;
            let mut b = io.builder(next_id, 2 << 20);
            for i in 0..15_000u64 {
                let mut value = [0u8; 120];
                for chunk in value.chunks_mut(8) {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    chunk.copy_from_slice(&seed.to_le_bytes());
                }
                if compressible {
                    value.copy_within(..56, 64);
                }
                b.push(EntryRef { key: &(next_id << 32 | i).to_be_bytes(), value: Some(&value) });
            }
            let page = DataPage::unwritten(b.finish(10));
            let laid_out = !page.meta.blocks_fill_pages();
            io.write(&page).unwrap();
            (laid_out, page.stored_compressed())
        };
        // Random values never leave the padded layout.
        for _ in 0..4 {
            assert_eq!(write(false), (false, false));
        }
        // Once values compress, the sampled blocks show it within a page.
        assert_eq!(write(true), (false, false));
        for _ in 0..4 {
            assert_eq!(write(true), (true, true));
        }
        // Once they stop, the store returns to storing images.
        let back = (0..6).position(|_| write(false) == (false, false));
        assert!(back.is_some_and(|n| n <= 4), "still compressing after {back:?} pages");
    }
}
