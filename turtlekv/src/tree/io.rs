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
use std::time::{Duration, Instant};

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
        let mut state = self.state.lock();
        let Some(s) = state.as_mut() else { return };
        let now = Instant::now();
        let left = s.left(now);
        s.done += bytes;
        // Past the estimate or the deadline, go at full speed.
        let Some((left, remaining)) = left else { return };
        // Each I/O claims its share of the time left at the rate that
        // finishes on time. Time spent idle raises the rate rather than
        // banking a burst.
        let start = s.next.max(now);
        s.next = start + Duration::from_secs_f64(left * bytes as f64 / remaining as f64);
        drop(state);
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
            pacer: Pacer::default(),
        }
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
        let t = Instant::now();
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
            let (off, len) = page.meta.span(blocks);
            self.store.prefetch(&ptr, off, len);
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
        if let Some(image) = self.cached_image(page, true) {
            return Ok((image.slice(range), false));
        }
        let ptr = page.ptr().expect("unpinned page is written");
        self.counters.span_reads.fetch_add(1, Relaxed);
        self.counters.span_read_bytes.fetch_add(len, Relaxed);
        self.pacer.take(len);
        Ok((self.store.read(&ptr, off, len)?, true))
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
        let ptr = self.store.allocate(page.id(), image.len() as u64, page.meta.meta_off)?;
        let created = self.store.write(&ptr, &image, self.write_piece(), &|n| self.pacer.take(n))?;
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

    fn pace(expected: u64, target_ms: u64, progress: f64) -> Pace {
        Pace { expected, target: Duration::from_millis(target_ms), progress: Box::new(move || progress), fill: 0.5 }
    }

    #[test]
    fn spaces_io_until_stopped() {
        let pacer = Pacer::default();
        let t = Instant::now();
        pacer.take(1 << 20);
        assert!(t.elapsed() < Duration::from_millis(20), "nothing to pace");
        pacer.start(pace(1000, 200, 0.0));
        let t = Instant::now();
        pacer.take(250);
        assert!(t.elapsed() < Duration::from_millis(20), "the first I/O starts at once");
        pacer.take(250);
        assert!(t.elapsed() >= Duration::from_millis(40), "the next waits out the first's share; waited {:?}", t.elapsed());
        pacer.stop();
        let t = Instant::now();
        pacer.take(400);
        assert!(t.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn idle_time_raises_the_rate_without_a_burst() {
        let pacer = Pacer::default();
        pacer.start(pace(1000, 400, 0.0));
        std::thread::sleep(Duration::from_millis(200));
        // Half the time is gone and all the bytes remain: 250 bytes now take
        // 50 ms, where a schedule that banked the idle time would let 500
        // through at once.
        let t = Instant::now();
        pacer.take(250);
        pacer.take(250);
        let waited = t.elapsed();
        assert!(waited >= Duration::from_millis(40) && waited < Duration::from_millis(90), "waited {waited:?}");
    }

    #[test]
    fn stop_ends_a_wait() {
        let pacer = Pacer::default();
        pacer.start(pace(1000, 60_000, 0.0));
        pacer.take(500);
        let t = Instant::now();
        std::thread::scope(|s| {
            s.spawn(|| {
                std::thread::sleep(Duration::from_millis(50));
                pacer.stop();
            });
            // Its turn comes in 30 s.
            pacer.take(500);
        });
        assert!(t.elapsed() < Duration::from_millis(500), "waited {:?}", t.elapsed());
    }

    #[test]
    fn filling_memtable_and_overrun_lift_the_pace() {
        let pacer = Pacer::default();
        // The next memtable is already as full as the pace's fill share.
        pacer.start(pace(1000, 60_000, 0.5));
        let t = Instant::now();
        pacer.take(500);
        pacer.take(500);
        // Past the estimate, I/O no longer waits.
        pacer.start(pace(100, 60_000, 0.0));
        pacer.take(150);
        pacer.take(50);
        assert!(t.elapsed() < Duration::from_millis(50));
    }
}
