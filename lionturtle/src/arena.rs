//! Memory that memtables own.
//!
//! A memtable takes its memory from an [`Arena`], which bumps a pointer
//! through large chunks and frees them all at once when it drops. Freed
//! chunks go to a [`ChunkPool`], and the next memtable's arena takes them
//! from there, still resident. A memtable therefore drops in the time it
//! takes to move a list of chunks, and after the first few memtables its
//! inserts write memory that is already mapped.
//!
//! The general allocator serves a memtable poorly on both counts. Each
//! entry is a few small blocks, so a dropped memtable of two million entries
//! is several million frees, which took half a second on the thread that
//! dropped it. An allocator that returns freed memory to the kernel then
//! has the next memtable fault it back in, and may do the returning on a
//! writer's thread: with mimalloc's defaults, writes stalled for 4 to 40 ms
//! after each drop.

use parking_lot::Mutex;
use std::alloc::Layout;
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// One anonymous mapping, unmapped when dropped.
struct Chunk {
    ptr: NonNull<u8>,
    len: usize,
    /// Whether anything may have written to it since it was mapped as
    /// zeros.
    dirty: bool,
}

// SAFETY: a chunk owns its mapping, which any thread may use or unmap.
unsafe impl Send for Chunk {}

impl Chunk {
    /// Maps `len` zeroed bytes at a multiple of `align`. Both are multiples
    /// of the system's page size.
    fn map(len: usize, align: usize) -> Option<Self> {
        // An aligned address lies within the first `align` bytes of a
        // mapping that much longer; the rest of the excess is unmapped.
        let padded = len.checked_add(align)?;
        // SAFETY: an anonymous private mapping at an address of the
        // kernel's choosing touches no existing memory.
        let base = unsafe {
            libc::mmap(std::ptr::null_mut(), padded, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANON, -1, 0)
        };
        if base == libc::MAP_FAILED {
            return None;
        }
        let base = base as usize;
        let start = base.next_multiple_of(align);
        // SAFETY: both ranges lie within the mapping made above, outside
        // the part this chunk keeps.
        unsafe {
            if start > base {
                libc::munmap(base as *mut libc::c_void, start - base);
            }
            let (end, mapped_end) = (start + len, base + padded);
            if mapped_end > end {
                libc::munmap(end as *mut libc::c_void, mapped_end - end);
            }
        }
        Some(Self { ptr: NonNull::new(start as *mut u8)?, len, dirty: false })
    }

    fn end(&self) -> *mut u8 {
        // SAFETY: one past the end of the mapping.
        unsafe { self.ptr.as_ptr().add(self.len) }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: the chunk owns exactly this mapping, and nothing uses it
        // once its arena or pool lets it go.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

fn page_size() -> usize {
    // SAFETY: sysconf only reads its argument.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).unwrap_or(4096).max(4096)
}

/// Chunks that dropped arenas leave for new ones.
///
/// The pool keeps every chunk it is given until [`trim`](Self::trim) finds
/// that the chunk went unused for a whole [`TRIM_INTERVAL`]. While
/// memtables fill and drop faster than that, every chunk a new memtable
/// takes is one an old memtable left, however many memtables are alive at
/// once. A store that stops writing gives its idle chunks back to the
/// system within two intervals.
pub struct ChunkPool {
    chunk_len: usize,
    idle: Mutex<Idle>,
}

struct Idle {
    chunks: Vec<Chunk>,
    /// The fewest idle chunks since the last trim: chunks nothing needed
    /// in that time.
    fewest: usize,
    trimmed: Instant,
}

/// How long a chunk goes unused before the pool unmaps it. Longer than a
/// memtable takes to fill under any load whose latency matters, so that a
/// memtable's chunks are still there for the one after next.
pub const TRIM_INTERVAL: Duration = Duration::from_secs(30);

/// The largest chunk: the size of a huge page, so that a kernel set to use
/// them can back each chunk with one.
const MAX_CHUNK: usize = 2 << 20;
const MIN_CHUNK: usize = 64 << 10;

impl ChunkPool {
    /// A pool of chunks for memtables that hold about `memtable_bytes`
    /// each. A memtable spans at least 16 chunks, so the memory it leaves
    /// unused in its last chunk is a small share of it.
    pub fn new(memtable_bytes: u64) -> Arc<Self> {
        let chunk_len = (memtable_bytes / 16).clamp(MIN_CHUNK as u64, MAX_CHUNK as u64).next_power_of_two() as usize;
        let idle = Idle { chunks: Vec::new(), fewest: 0, trimmed: Instant::now() };
        Arc::new(Self { chunk_len: chunk_len.max(page_size()), idle: Mutex::new(idle) })
    }

    fn take(&self) -> Option<Chunk> {
        let mut idle = self.idle.lock();
        let chunk = idle.chunks.pop();
        idle.fewest = idle.fewest.min(idle.chunks.len());
        drop(idle);
        chunk.or_else(|| Chunk::map(self.chunk_len, self.chunk_len))
    }

    fn give(&self, chunks: Vec<Chunk>) {
        let returned = chunks.into_iter().map(|mut chunk| {
            chunk.dirty = true;
            chunk
        });
        self.idle.lock().chunks.extend(returned);
    }

    /// A block of `len` zeroed bytes from one of the pool's chunks, which
    /// returns to the pool when the block drops; `None` if a chunk is
    /// shorter than that, or the system has no memory to map.
    pub fn zeroed(self: &Arc<Self>, len: usize) -> Option<Zeroed> {
        if len > self.chunk_len {
            return None;
        }
        let chunk = self.take()?;
        if chunk.dirty {
            // SAFETY: the chunk spans at least `len` bytes.
            unsafe { chunk.ptr.as_ptr().write_bytes(0, len) };
        }
        Some(Zeroed { chunk: Some(chunk), pool: self.clone() })
    }

    /// Unmaps the chunks that sat idle since the last trim, if that was at
    /// least [`TRIM_INTERVAL`] ago. Returns how long until the next trim is
    /// due, or `None` if the pool holds no idle chunk to trim then.
    pub fn trim(&self) -> Option<Duration> {
        self.trim_after(TRIM_INTERVAL)
    }

    fn trim_after(&self, interval: Duration) -> Option<Duration> {
        let mut idle = self.idle.lock();
        let since = idle.trimmed.elapsed();
        if since < interval {
            return (!idle.chunks.is_empty()).then_some(interval - since);
        }
        let unused = idle.fewest.min(idle.chunks.len());
        let unmapped: Vec<Chunk> = idle.chunks.drain(..unused).collect();
        idle.fewest = idle.chunks.len();
        idle.trimmed = Instant::now();
        let next = (!idle.chunks.is_empty()).then_some(interval);
        drop(idle);
        // They unmap here, outside the lock.
        drop(unmapped);
        next
    }

    /// Bytes held in idle chunks.
    pub fn idle_bytes(&self) -> usize {
        self.idle.lock().chunks.len() * self.chunk_len
    }
}

/// A block of a pool's memory that began as zeros; see
/// [`ChunkPool::zeroed`].
pub struct Zeroed {
    chunk: Option<Chunk>,
    pool: Arc<ChunkPool>,
}

impl Zeroed {
    /// The block's first byte, at a multiple of the system's page size.
    pub fn as_ptr(&self) -> *mut u8 {
        self.chunk.as_ref().expect("held until dropped").ptr.as_ptr()
    }
}

impl Drop for Zeroed {
    fn drop(&mut self) {
        self.pool.give(self.chunk.take().into_iter().collect());
    }
}

/// Memory for one memtable: entries, allocated by any thread with
/// [`alloc`](Self::alloc), and the index's nodes, with
/// [`alloc_page`](Self::alloc_page). Nothing is freed until the arena
/// drops.
///
/// Entries fill each chunk from its start and pages from its end, so
/// neither leaves gaps for the other's alignment. Each thread carves its
/// entries from a block of its own, which it takes from the chunk under the
/// arena's lock, so writers contend for the lock once per block rather than
/// once per entry.
pub struct Arena {
    /// Distinct from every other arena's, past and present: a thread's
    /// block names its arena by it.
    id: u64,
    pool: Arc<ChunkPool>,
    state: Mutex<State>,
}

struct State {
    /// The unused part of the newest chunk.
    lo: *mut u8,
    hi: *mut u8,
    chunks: Vec<Chunk>,
    /// Mappings of their own for requests no chunk holds.
    oversized: Vec<Chunk>,
}

// SAFETY: the pointers lie in chunks the state owns.
unsafe impl Send for State {}

/// A thread's block of an arena.
#[derive(Clone, Copy)]
struct Block {
    arena: u64,
    cur: *mut u8,
    end: *mut u8,
    /// Bytes to ask for when this block runs out.
    next_len: usize,
}

impl Block {
    const NONE: Self = Self { arena: 0, cur: std::ptr::null_mut(), end: std::ptr::null_mut(), next_len: 0 };

    fn room(&self) -> usize {
        self.end as usize - self.cur as usize
    }
}

/// A thread's first block of an arena is this long, and each one after is
/// twice the last, up to [`MAX_BLOCK`]. A thread leaves at most its last
/// block unused when the memtable freezes, so one that writes little wastes
/// little, and a hundred busy writers waste a few percent of a 128 MiB
/// memtable.
const FIRST_BLOCK: usize = 256;
const MAX_BLOCK: usize = 64 << 10;
/// Arenas a thread holds blocks of at once: one for each store it writes
/// to in turn.
const BLOCKS: usize = 4;

thread_local! {
    /// This thread's blocks, most recently used first.
    static THREAD_BLOCKS: Cell<[Block; BLOCKS]> = const { Cell::new([Block::NONE; BLOCKS]) };
}

static NEXT_ARENA: AtomicU64 = AtomicU64::new(1);

impl Arena {
    pub fn new(pool: Arc<ChunkPool>) -> Self {
        let state = State { lo: std::ptr::null_mut(), hi: std::ptr::null_mut(), chunks: Vec::new(), oversized: Vec::new() };
        Self { id: NEXT_ARENA.fetch_add(1, Relaxed), pool, state: Mutex::new(state) }
    }

    /// `size` bytes aligned to 8, valid until the arena drops. Their
    /// contents are arbitrary.
    #[inline]
    pub fn alloc(&self, size: usize) -> NonNull<u8> {
        let size = size.next_multiple_of(8);
        THREAD_BLOCKS.with(|cell| {
            // SAFETY: only this thread reaches its blocks, and nothing
            // called below does.
            let blocks = unsafe { &mut *cell.as_ptr() };
            if blocks[0].arena != self.id {
                // Move this arena's block to the front, or start one there
                // in place of the block used longest ago.
                let at = blocks.iter().position(|b| b.arena == self.id).unwrap_or(BLOCKS - 1);
                if blocks[at].arena != self.id {
                    blocks[at] = Block { arena: self.id, next_len: FIRST_BLOCK, ..Block::NONE };
                }
                blocks[..=at].rotate_right(1);
            }
            let block = &mut blocks[0];
            if block.room() < size {
                let (start, len) = self.take_block(size, block.next_len);
                block.cur = start;
                // SAFETY: `take_block` returns `len` bytes at `start`.
                block.end = unsafe { start.add(len) };
                block.next_len = (block.next_len * 2).min(MAX_BLOCK);
            }
            let p = block.cur;
            // SAFETY: the block has room for `size` more bytes.
            block.cur = unsafe { p.add(size) };
            // SAFETY: blocks lie within mappings, which never start at null.
            unsafe { NonNull::new_unchecked(p) }
        })
    }

    /// Returns the last `size` bytes of this thread's latest allocation,
    /// which end at `end`, to its block. A no-op unless that allocation was
    /// the thread's last from this arena.
    #[inline]
    pub fn unalloc(&self, end: *mut u8, size: usize) {
        THREAD_BLOCKS.with(|cell| {
            // SAFETY: as in `alloc`.
            let block = unsafe { &mut (*cell.as_ptr())[0] };
            if block.arena == self.id && block.cur == end {
                // The allocation that ends at `end` spans at least `size`
                // bytes of the block.
                block.cur = end.wrapping_sub(size);
            }
        });
    }

    /// At least `size` bytes for a thread's next block, and `want` if the
    /// chunk has them.
    #[cold]
    fn take_block(&self, size: usize, want: usize) -> (*mut u8, usize) {
        let mut s = self.state.lock();
        if (s.hi as usize - s.lo as usize) < size {
            if size > self.pool.chunk_len {
                let Some(chunk) = Chunk::map(size.next_multiple_of(page_size()), page_size()) else { out_of_memory(size) };
                let start = chunk.ptr.as_ptr();
                s.oversized.push(chunk);
                return (start, size);
            }
            // What the last chunk has left is too short for this request,
            // so it stays unused.
            let Some(chunk) = self.pool.take() else { out_of_memory(size) };
            (s.lo, s.hi) = (chunk.ptr.as_ptr(), chunk.end());
            s.chunks.push(chunk);
        }
        let len = want.max(size).min(s.hi as usize - s.lo as usize);
        let start = s.lo;
        // SAFETY: the chunk has `len` unused bytes at `lo`.
        s.lo = unsafe { start.add(len) };
        (start, len)
    }

    /// A zeroed block for `layout`, valid until the arena drops, or `None`
    /// if the system has no memory to map.
    pub fn alloc_page(&self, layout: Layout) -> Option<NonNull<u8>> {
        let (size, align) = (layout.size(), layout.align());
        let page = {
            let mut s = self.state.lock();
            loop {
                let (lo, hi) = (s.lo as usize, s.hi as usize);
                if hi - lo >= size {
                    let start = (hi - size) & !(align - 1);
                    if start >= lo {
                        s.hi = start as *mut u8;
                        break s.hi;
                    }
                }
                if size > self.pool.chunk_len / 2 || align > page_size() {
                    let chunk = Chunk::map(size.next_multiple_of(page_size()), align.max(page_size()))?;
                    let start = chunk.ptr.as_ptr();
                    s.oversized.push(chunk);
                    break start;
                }
                let chunk = self.pool.take()?;
                (s.lo, s.hi) = (chunk.ptr.as_ptr(), chunk.end());
                s.chunks.push(chunk);
            }
        };
        // A chunk from the pool holds an old memtable's bytes.
        // SAFETY: the arena just set these `size` bytes aside.
        unsafe { page.write_bytes(0, size) };
        NonNull::new(page)
    }

    /// Bytes of chunks the arena holds.
    pub fn reserved(&self) -> usize {
        let s = self.state.lock();
        s.chunks.len() * self.pool.chunk_len + s.oversized.iter().map(|c| c.len).sum::<usize>()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        self.pool.give(std::mem::take(&mut self.state.get_mut().chunks));
    }
}

fn out_of_memory(size: usize) -> ! {
    std::alloc::handle_alloc_error(Layout::from_size_align(size, 8).unwrap_or(Layout::new::<u8>()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocations_are_distinct_and_aligned() {
        let pool = ChunkPool::new(1 << 20);
        let arena = Arena::new(pool.clone());
        let mut seen: Vec<(usize, usize)> = Vec::new();
        for i in 0..20_000usize {
            let size = 1 + i % 300;
            let p = arena.alloc(size).as_ptr();
            assert_eq!(p as usize % 8, 0);
            // SAFETY: the arena set aside `size` bytes at `p`.
            unsafe { p.write_bytes(i as u8, size) };
            seen.push((p as usize, size));
            if i % 50 == 0 {
                let layout = Layout::from_size_align(1024, 1024).unwrap();
                let page = arena.alloc_page(layout).unwrap().as_ptr();
                assert_eq!(page as usize % 1024, 0);
                // SAFETY: the arena set aside 1024 bytes at `page`.
                assert!(unsafe { std::slice::from_raw_parts(page, 1024) }.iter().all(|&b| b == 0));
                unsafe { page.write_bytes(0xff, 1024) };
                seen.push((page as usize, 1024));
            }
        }
        seen.sort_unstable();
        for pair in seen.windows(2) {
            assert!(pair[0].0 + pair[0].1 <= pair[1].0, "allocations overlap");
        }
        assert!(arena.reserved() >= seen.iter().map(|&(_, len)| len).sum::<usize>());
    }

    #[test]
    fn a_request_larger_than_a_chunk_gets_its_own_mapping() {
        let pool = ChunkPool::new(0);
        let arena = Arena::new(pool.clone());
        let small = arena.alloc(100).as_ptr();
        let big = arena.alloc(MIN_CHUNK * 3).as_ptr();
        // SAFETY: the arena set aside these bytes.
        unsafe {
            small.write_bytes(1, 100);
            big.write_bytes(2, MIN_CHUNK * 3);
            assert_eq!(*small, 1);
        }
        let page = arena.alloc_page(Layout::from_size_align(MIN_CHUNK, 1 << 16).unwrap()).unwrap().as_ptr();
        assert_eq!(page as usize % (1 << 16), 0);
        drop(arena);
        // Only whole chunks return to the pool.
        assert_eq!(pool.idle_bytes(), MIN_CHUNK);
    }

    #[test]
    fn the_last_allocation_can_shrink() {
        let arena = Arena::new(ChunkPool::new(0));
        let a = arena.alloc(64).as_ptr();
        // SAFETY: within the 64 bytes at `a`.
        arena.unalloc(unsafe { a.add(64) }, 40);
        let b = arena.alloc(16).as_ptr();
        assert_eq!(b as usize, a as usize + 24);
        // An allocation that is no longer the last stays whole.
        arena.unalloc(unsafe { a.add(24) }, 8);
        assert_eq!(arena.alloc(8).as_ptr() as usize, b as usize + 16);
    }

    #[test]
    fn dropped_arenas_feed_new_ones() {
        let pool = ChunkPool::new(1 << 20);
        let fill = |arena: &Arena| {
            for _ in 0..3000 {
                arena.alloc(200);
            }
            arena.reserved()
        };
        let first = Arena::new(pool.clone());
        let reserved = fill(&first);
        assert!(reserved >= 3000 * 200);
        drop(first);
        assert_eq!(pool.idle_bytes(), reserved);
        // Two arenas alive at once take the idle chunks, then map more, and
        // the pool keeps what both return.
        let (a, b) = (Arena::new(pool.clone()), Arena::new(pool.clone()));
        fill(&a);
        assert_eq!(pool.idle_bytes(), 0);
        fill(&b);
        drop(a);
        drop(b);
        assert_eq!(pool.idle_bytes(), 2 * reserved);
    }

    #[test]
    fn zeroed_blocks_are_zero_whatever_their_chunk_held() {
        let pool = ChunkPool::new(0);
        let arena = Arena::new(pool.clone());
        // SAFETY: the arena set aside these bytes.
        unsafe { arena.alloc(MIN_CHUNK).as_ptr().write_bytes(0xff, MIN_CHUNK) };
        drop(arena);
        for _ in 0..2 {
            let block = pool.zeroed(MIN_CHUNK).expect("a chunk");
            // SAFETY: the block spans `MIN_CHUNK` bytes.
            let bytes = unsafe { std::slice::from_raw_parts_mut(block.as_ptr(), MIN_CHUNK) };
            assert!(bytes.iter().all(|&b| b == 0));
            bytes.fill(0xee);
            assert_eq!(pool.idle_bytes(), 0);
        }
        assert_eq!(pool.idle_bytes(), MIN_CHUNK);
        assert!(pool.zeroed(MIN_CHUNK + 1).is_none());
    }

    #[test]
    fn a_trim_unmaps_the_chunks_nothing_took() {
        let pool = ChunkPool::new(1 << 20);
        let now = Duration::ZERO;
        let fill = |chunks: usize| {
            let arena = Arena::new(pool.clone());
            for _ in 0..chunks {
                arena.alloc(MIN_CHUNK);
            }
            arena
        };
        assert_eq!(pool.trim_after(now), None, "an empty pool has nothing to trim");
        drop(fill(6));
        assert_eq!(pool.idle_bytes(), 6 * MIN_CHUNK);
        // The chunks arrived since the last trim, so this one keeps them.
        assert_eq!(pool.trim_after(now), Some(now));
        assert_eq!(pool.idle_bytes(), 6 * MIN_CHUNK);
        // An arena takes four and returns them: two sat idle throughout.
        drop(fill(4));
        assert_eq!(pool.trim_after(now), Some(now));
        assert_eq!(pool.idle_bytes(), 4 * MIN_CHUNK);
        // A trim before the interval has passed waits for it.
        let wait = pool.trim_after(Duration::from_secs(3600)).expect("idle chunks remain");
        assert!(wait > Duration::from_secs(3500));
        assert_eq!(pool.idle_bytes(), 4 * MIN_CHUNK);
        // With nothing taken, the next trim unmaps the rest.
        assert_eq!(pool.trim_after(now), None);
        assert_eq!(pool.idle_bytes(), 0);
    }

    #[test]
    fn threads_allocate_from_one_arena() {
        let arena = Arena::new(ChunkPool::new(1 << 20));
        let all: Vec<(usize, usize)> = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8usize)
                .map(|t| {
                    let arena = &arena;
                    scope.spawn(move || {
                        (0..5000usize)
                            .map(|i| {
                                let size = 8 + (i * 7 + t) % 120;
                                let p = arena.alloc(size).as_ptr();
                                // SAFETY: the arena set aside `size` bytes.
                                unsafe { p.write_bytes(t as u8, size) };
                                (p as usize, size)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            threads.into_iter().flat_map(|t| t.join().unwrap()).collect()
        });
        let mut sorted = all.clone();
        sorted.sort_unstable();
        for pair in sorted.windows(2) {
            assert!(pair[0].0 + pair[0].1 <= pair[1].0, "allocations overlap");
        }
    }
}
