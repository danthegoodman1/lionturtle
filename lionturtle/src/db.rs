//! The key-value store.
//!
//! Writes go to the write-ahead log and the active memtable. When the active
//! memtable reaches the checkpoint distance (χ leaf-sized batches), it is
//! frozen and a new one takes its place. The checkpointer drains frozen
//! memtables oldest first: it cuts each into leaf-sized sorted batches,
//! applies them to an in-memory copy of the TurtleTree, writes the new pages,
//! syncs, and commits a superblock naming the new root. Only then does the
//! frozen memtable leave the read path and its log segment get deleted.
//!
//! Readers see an immutable [`Version`]: the active memtable, the frozen
//! memtables, and the last committed tree. Pages the new tree no longer
//! references return to the allocator once every reader of an older version
//! is gone (see [`Epoch`]).

use crate::arena::ChunkPool;
use crate::blob::BlobPtr;
use crate::bloom::{self, SharedFilter};
use crate::error::{Error, Result};
use crate::memtable::{Entry, MemTable, MemTableKind, View, new_memtable};
use crate::page::{Compression, EntryRef, PageBuilder, entry_size};
use crate::pagestore::{PagePtr, PageStore, Superblock};
use crate::rangedel::{FrozenDeletes, Overlay, RangeDelete, RangeDeletes};
use crate::storage::Storage;
use crate::tree::io::{DataPage, Pace, PageIo};
use crate::tree::{self, Ctx, Found, Subtree, TreeConfig, TreeScan};
use crate::types::{MAX_KEY_LEN, MAX_VALUE_LEN, Op, OpRef, Value, WriteOp, cmp_keys, prefix_end};
use crate::wal::{self, WalWriter};
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;

#[derive(Clone, Debug)]
pub struct Options {
    pub tree: TreeConfig,
    /// Leaf-sized batches per memtable (χ). Each frozen memtable becomes one
    /// checkpoint, so this is the checkpoint distance.
    pub checkpoint_distance: u64,
    pub cache_bytes: u64,
    pub memtable: MemTableKind,
    /// Write buffered log records to the file once this many bytes accumulate.
    pub wal_buffer_bytes: usize,
    /// Frozen memtables allowed to await checkpointing before writers block.
    /// A second one lets a checkpoint run past the next memtable's filling,
    /// as when the drive slows for a few seconds, without stopping writers.
    pub max_frozen: usize,
    /// Checkpoint on a background thread. Without it, callers drive
    /// checkpoints through [`Db::checkpoint_step`], and a writer that finds no
    /// room for another frozen memtable checkpoints inline.
    pub background: bool,
    /// Threads in the store's own pool for parallel merges and page writes;
    /// 0 runs them on the caller's rayon pool. Merges spend most of their
    /// time waiting on reads, so each thread mostly adds reads in flight: on
    /// an EC2 instance-store drive, 8 threads raised peak throughput on 1 KB
    /// records by 13% over 4, and 16 by 18%. Each thread costs memory, since
    /// an allocator keeps what other threads free from a worker's buffers
    /// resident in that worker's heap until the worker allocates again: 16
    /// threads held about 440 MB more than 4, and under 128 durable writers
    /// outgrew a 3 GB memory cap within six minutes, where 8 stayed under it.
    pub merge_threads: usize,
    /// Spread each background checkpoint's I/O over most of the time the
    /// next memtable takes to fill, rather than run it at full speed, so
    /// client reads queue behind less of it. See [`PACE_SHARE`].
    pub pace_checkpoints: bool,
    /// Write whole log segments of zeros ahead of the memtables that use
    /// them, so that a durable write overwrites allocated space instead of
    /// extending the file, and its sync has no file size or block
    /// allocation to journal. On ext4 over NVMe, that halved the cost of
    /// each sync. A segment written over a retired one already gets this,
    /// so the option speeds up only segments with none to reuse: a new
    /// store's first three, each written twice, once as zeros.
    ///
    /// Opening a store with no segment to reuse writes the first one before
    /// it returns. After that a background thread writes one whenever none
    /// is kept, spread over [`PREPARE_LOG_SECS`] or half the time the
    /// active memtable takes to fill, whichever is shorter, in a file the
    /// log is not writing. A memtable that fills before its segment is
    /// ready gets a new, empty one, which grows as it is written.
    pub preallocate_log: bool,
    /// How data pages written from now on are stored. With
    /// [`Compression::Lz4`], the default, each block holds about as many
    /// entries as compress into one 4 KiB page, judged from the pages
    /// written before, and is compressed on its own as its page is written.
    /// Fewer bytes reach the drive and the files shrink, while a point read
    /// still reads one aligned page. Blocks in memory and in the block cache
    /// stay uncompressed, so only a read from disk decompresses, about 1 µs
    /// for a block that compressed by half. While blocks would gain less
    /// than an eighth more entries per page, pages are stored as they would
    /// be without compression, and one block in eight is compressed only to
    /// tell when that changes. A store may switch either way between opens:
    /// each page records how it was stored. lionturtle 0.4.0 and later read
    /// compressed pages; earlier versions report them as corrupt.
    pub compression: Compression,
    /// Values of at least this many bytes are stored apart from the tree,
    /// each in a slot of its own, a blob. A writer writes the blob before it
    /// logs the put, and the log and the tree hold a pointer of 21 bytes in
    /// the value's place. The value thus reaches the drive once, where the
    /// tree rewrites a value it holds at each buffer level the value passes,
    /// and writers of large values write in parallel instead of through the
    /// log. A read of such a value reads its tree entry and then its blob.
    ///
    /// Each blob takes about 60 bytes of memory while the tree references
    /// it, and its slot is padded to 4 KiB, so both costs double with each
    /// halving of the threshold. Values of 8 KiB and over loaded, updated,
    /// read and scanned faster as blobs than in the tree; the default of
    /// 32 KiB keeps the memory near 2 GB per TB of the smallest blobs and
    /// the padding near an eighth of them.
    ///
    /// With [`Compression::Lz4`], a blob is compressed whole when that saves
    /// at least an eighth of it. A store may change the threshold between
    /// opens: each entry records whether its value is a blob. `usize::MAX`
    /// keeps every value in the tree. A store that has written a blob opens
    /// only with a version of lionturtle that has blobs.
    pub blob_threshold: usize,
}

/// A paced checkpoint aims to finish its I/O within this share of the time
/// its memtable took to fill, or by the time the next memtable is this full.
pub const PACE_SHARE: f64 = 0.75;

/// Seconds a background thread takes to write a log segment of zeros while
/// writers are idle; see [`Options::preallocate_log`]. One memtable's log
/// over two seconds is a few percent of an NVMe drive's write rate.
pub const PREPARE_LOG_SECS: f64 = 2.0;

/// Checkpoint I/O per memtable byte that pacing expects before any
/// checkpoint has run. Checkpoints of uniform 1 KB updates read and wrote 9
/// to 15 times their memtable, more as the tree grows.
const FIRST_CHECKPOINT_IO: u64 = 12;

impl Default for Options {
    fn default() -> Self {
        Self {
            tree: TreeConfig::default(),
            checkpoint_distance: 32,
            cache_bytes: 1 << 30,
            memtable: MemTableKind::BBTree,
            wal_buffer_bytes: 1 << 20,
            max_frozen: 2,
            background: true,
            merge_threads: 8,
            pace_checkpoints: true,
            preallocate_log: false,
            compression: Compression::Lz4,
            blob_threshold: 32 << 10,
        }
    }
}

impl Options {
    fn memtable_limit(&self) -> u64 {
        self.checkpoint_distance.max(1) * self.tree.leaf_size
    }

    /// Bytes of log one memtable is expected to fill: its entries, and an
    /// eighth more for the log's record headers.
    fn log_segment_len(&self) -> u64 {
        self.memtable_limit() + self.memtable_limit() / 8
    }
}

/// Memtable bytes per bit of its filter: 16 bits per key for 128-byte
/// entries.
const MEM_BYTES_PER_FILTER_BIT: u64 = 8;

/// A memtable and the log segment that shares its id.
pub struct Mem {
    pub id: u64,
    created: std::time::Instant,
    /// When the memtable filled; unset if a caller froze it early.
    filled: std::sync::OnceLock<std::time::Instant>,
    /// Whether its writers waited for the previous checkpoint before it
    /// could freeze: checkpoints are falling behind.
    waited: AtomicBool,
    table: Box<dyn MemTable>,
    /// The keys inserted, so a get of a key the memtable lacks skips the
    /// search: in a large memtable, a cache miss at every level.
    filter: SharedFilter,
    /// Range deletes, which the filter does not cover.
    ranges: RangeDeletes,
    counts: MemCounts,
    /// Writers assigned to this memtable that have not finished inserting.
    inflight: Inflight,
    /// The blob each large put wrote before it was logged, by the put's
    /// sequence number. The memtable keeps the value too, for reads; its
    /// checkpoint gives the tree the pointer.
    blobs: Mutex<Vec<(u64, BlobPtr)>>,
}

/// Counters the writer lock's holder updates for every batch. They sit on a
/// cache line of their own: every get and insert loads `Mem::table`, and a
/// shared line would cost each of them a cache miss after every batch.
#[derive(Default)]
#[repr(align(128))]
struct MemCounts {
    /// Entry bytes assigned to the memtable.
    bytes: AtomicU64,
    /// Sequence number of the newest op assigned to the memtable.
    last_seq: AtomicU64,
}

/// A value on a cache line of its own, for one that many threads write.
#[repr(align(128))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

const INFLIGHT_SHARDS: usize = 16;

#[derive(Default)]
#[repr(align(128))]
struct PaddedCount(AtomicUsize);

/// A count of writers split across cache lines, so that writers on
/// different threads update different lines. It only needs to reach zero
/// once no writer can join.
#[derive(Default)]
struct Inflight([PaddedCount; INFLIGHT_SHARDS]);

impl Inflight {
    /// The calling thread's shard.
    fn shard() -> usize {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        thread_local! {
            static SHARD: usize = NEXT.fetch_add(1, Relaxed) % INFLIGHT_SHARDS;
        }
        SHARD.with(|s| *s)
    }

    /// Counts a writer in on `shard`.
    fn enter(&self, shard: usize) {
        self.0[shard].0.fetch_add(1, SeqCst);
    }

    fn exit(&self, shard: usize) {
        self.0[shard].0.fetch_sub(1, SeqCst);
    }

    fn is_zero(&self) -> bool {
        self.0.iter().all(|c| c.0.load(SeqCst) == 0)
    }
}

/// Batches between taking their sequence numbers and becoming visible.
///
/// A writer registers its batch under the writer lock, inserts its ops into
/// the memtable, and commits the batch; a get takes the newest version
/// whose batch has committed. A batch thus shows whole or not at all, and no
/// writer waits for another's inserts. `visible` trails behind as the prefix
/// of committed batches: every version at or below it is committed, and an
/// iterator reads that prefix as its snapshot.
///
/// Every commit writes `folded` and takes `fold_lock`, so they keep to
/// their own cache line, away from the writer lock.
#[repr(align(128))]
struct Commits {
    /// Every batch ending at or below this sequence number has committed.
    visible: AtomicU64,
    /// The last sequence number of the newest registered batch.
    assigned: AtomicU64,
    /// Each in-flight batch, by ticket modulo the ring size.
    slots: Box<[CommitSlot]>,
    /// The oldest ticket not yet folded into `visible`.
    folded: AtomicU64,
    /// Held while folding committed batches into `visible`.
    fold_lock: Mutex<()>,
}

#[repr(align(128))]
#[derive(Default)]
struct CommitSlot {
    /// `ticket << 1`, plus one once the batch commits.
    state: AtomicU64,
    /// The batch's last sequence number.
    last: AtomicU64,
}

/// Batches that may be in flight at once. Each writer thread has at most
/// one, and a turn at the writer lock registers at most [`TURN_LIMIT`].
const COMMIT_RING: u64 = 1024;

impl Commits {
    /// Tickets start at 1, so an empty slot names no batch.
    fn new(visible: u64) -> Self {
        Self {
            visible: AtomicU64::new(visible),
            assigned: AtomicU64::new(visible),
            slots: (0..COMMIT_RING).map(|_| CommitSlot::default()).collect(),
            folded: AtomicU64::new(1),
            fold_lock: Mutex::new(()),
        }
    }

    fn slot(&self, ticket: u64) -> &CommitSlot {
        &self.slots[(ticket % COMMIT_RING) as usize]
    }

    /// Registers batch `ticket`, ending at sequence number `last`. Called
    /// under the writer lock, in ticket order.
    fn register(&self, ticket: u64, last: u64) {
        // A slot is reused once the batch a ring earlier has folded.
        let mut spins = 0;
        while self.folded.load(Acquire) + COMMIT_RING <= ticket {
            self.fold();
            backoff(&mut spins);
        }
        let slot = self.slot(ticket);
        slot.last.store(last, Relaxed);
        slot.state.store(ticket << 1, Release);
        self.assigned.store(last, Release);
    }

    /// Makes batch `ticket` visible.
    fn commit(&self, ticket: u64) {
        self.slot(ticket).state.store(ticket << 1 | 1, SeqCst);
        std::sync::atomic::fence(SeqCst);
        self.fold();
    }

    /// Advances `visible` over the committed batches at the front.
    fn fold(&self) {
        loop {
            let Some(guard) = self.fold_lock.try_lock() else { return };
            let mut t = self.folded.load(Relaxed);
            loop {
                let slot = self.slot(t);
                if slot.state.load(Acquire) != t << 1 | 1 {
                    break;
                }
                self.visible.store(slot.last.load(Relaxed), Release);
                t += 1;
            }
            self.folded.store(t, Release);
            drop(guard);
            // A batch that committed while the lock was held, and found it
            // taken, leaves its folding to this thread.
            std::sync::atomic::fence(SeqCst);
            if self.slot(t).state.load(SeqCst) != t << 1 | 1 {
                return;
            }
        }
    }

    /// Whether a get that loaded `visible` sees the version at `seq`
    /// written by batch `ticket`.
    fn sees(&self, visible: u64, seq: u64, ticket: u64) -> bool {
        if seq <= visible {
            return true;
        }
        let state = self.slot(ticket).state.load(Acquire);
        // A later ticket in the slot means this batch folded long ago.
        state == ticket << 1 | 1 || state >> 1 > ticket
    }

    /// A sequence number at or below which every batch has committed and
    /// above which none had registered when this was called: a snapshot
    /// holding every batch written before the call.
    fn prefix(&self) -> u64 {
        let upto = self.assigned.load(Acquire);
        let mut spins = 0;
        while self.visible.load(Acquire) < upto {
            self.fold();
            backoff(&mut spins);
        }
        upto
    }
}

fn backoff(spins: &mut u32) {
    if *spins < 64 {
        std::hint::spin_loop();
        *spins += 1;
    } else {
        std::thread::yield_now();
    }
}

impl Mem {
    fn new(id: u64, opts: &Options, last_seq: u64, pool: &Arc<ChunkPool>) -> Self {
        Self {
            id,
            created: std::time::Instant::now(),
            filled: std::sync::OnceLock::new(),
            waited: AtomicBool::new(false),
            table: new_memtable(opts.memtable, pool),
            filter: SharedFilter::new_in(pool, (opts.memtable_limit() / MEM_BYTES_PER_FILTER_BIT / 8) as usize),
            ranges: RangeDeletes::default(),
            counts: MemCounts { bytes: AtomicU64::new(0), last_seq: AtomicU64::new(last_seq) },
            inflight: Inflight::default(),
            blobs: Mutex::new(Vec::new()),
        }
    }

    /// Adds a logged op, with the blob its record pointed to, if any.
    fn replay(&self, op: &WriteOp, seq: u64, blob: Option<BlobPtr>) {
        self.insert_op(op.op(), seq, 0);
        self.counts.bytes.fetch_add(op_entry_bytes(op.op()), Relaxed);
        if let Some(blob) = blob {
            self.blobs.lock().push((seq, blob));
        }
    }

    /// Adds the version of `key` written at `seq` by batch `ticket`. The
    /// filter takes the key first, so a get that can see the version finds
    /// the key in the filter.
    fn insert(&self, key: &[u8], value: Option<&[u8]>, seq: u64, ticket: u64) {
        self.filter.insert(bloom::hash_key(key));
        self.table.insert(key, value, seq, ticket);
    }

    fn insert_op(&self, op: OpRef<'_>, seq: u64, ticket: u64) {
        match op {
            OpRef::Put(key, value) => self.insert(key, Some(value), seq, ticket),
            OpRef::Delete(key) => self.insert(key, None, seq, ticket),
            OpRef::DeleteRange(start, end) => {
                self.ranges.insert(RangeDelete { start: Bytes::copy_from_slice(start), end: end.map(Bytes::copy_from_slice), seq, ticket })
            }
        }
    }

    /// Adds a version of each of `ops`, at sequence numbers counting up
    /// from `seq`, a group at a time so the table can look a group's keys
    /// up together. A batch holding a range delete goes one op at a time.
    fn insert_all<O: Op>(&self, ops: &[O], mut seq: u64, ticket: u64, ranges: bool) {
        if ranges {
            for (i, op) in ops.iter().enumerate() {
                self.insert_op(op.op(), seq + i as u64, ticket);
            }
            return;
        }
        const GROUP: usize = 64;
        for group in ops.chunks(GROUP) {
            let mut entries: [Entry<'_>; GROUP] = [(&[], None); GROUP];
            for (entry, op) in entries.iter_mut().zip(group) {
                *entry = match op.op() {
                    OpRef::Put(key, value) => (key, Some(value)),
                    OpRef::Delete(key) => (key, None),
                    OpRef::DeleteRange(..) => unreachable!("a batch with range deletes inserts one op at a time"),
                };
                self.filter.insert(bloom::hash_key(entry.0));
            }
            self.table.insert_many(&entries[..group.len()], seq, ticket);
            seq += group.len() as u64;
        }
    }

    /// Whether the memtable holds no write.
    fn is_empty(&self) -> bool {
        self.table.is_empty() && self.ranges.is_empty()
    }

    /// The newest value of the key hashing to `hash` that `view` sees.
    fn get(&self, key: &[u8], hash: u64, view: View) -> Option<Value> {
        if !self.filter.may_contain(hash) {
            return None;
        }
        self.table.get(key, view)
    }

    pub fn bytes(&self) -> u64 {
        self.counts.bytes.load(Relaxed)
    }
}

/// Bytes `op` adds to its memtable. A range delete counts as an entry
/// holding both of its bounds.
fn op_entry_bytes(op: OpRef<'_>) -> u64 {
    match op {
        OpRef::Put(key, value) => entry_size(key.len(), value.len()),
        OpRef::Delete(key) => entry_size(key.len(), 0),
        OpRef::DeleteRange(start, end) => entry_size(start.len(), end.map_or(0, <[u8]>::len)),
    }
}

/// Holds the pages that became garbage when the checkpoint after this epoch
/// committed. They return to the allocator when this epoch drops, which
/// happens only after every older epoch has dropped, because each epoch
/// keeps the next one alive.
struct Epoch {
    io: Arc<PageIo>,
    garbage: Mutex<Garbage>,
    next: Mutex<Option<Arc<Epoch>>>,
}

/// Slots no durable tree references: pages, and blobs.
#[derive(Default)]
struct Garbage {
    pages: Vec<PagePtr>,
    blobs: Vec<PagePtr>,
}

impl Epoch {
    fn new(io: Arc<PageIo>) -> Self {
        Self { io, garbage: Mutex::new(Garbage::default()), next: Mutex::new(None) }
    }

    fn retire(&self, garbage: Garbage, next: Arc<Epoch>) {
        *self.garbage.lock() = garbage;
        *self.next.lock() = Some(next);
    }
}

impl Drop for Epoch {
    fn drop(&mut self) {
        // Free iteratively: dropping a long chain recursively could overflow
        // the stack.
        let mut garbage = std::mem::take(self.garbage.get_mut());
        let mut next = self.next.get_mut().take();
        loop {
            for ptr in &garbage.pages {
                self.io.evict(ptr.id);
            }
            self.io.store.free(garbage.pages);
            self.io.blobs.free(garbage.blobs);
            let Some(arc) = next else { break };
            match Arc::try_unwrap(arc) {
                Ok(mut e) => {
                    garbage = std::mem::take(e.garbage.get_mut());
                    next = e.next.get_mut().take();
                }
                Err(_) => break,
            }
        }
    }
}

/// An immutable view of the store.
pub struct Version {
    active: Arc<Mem>,
    /// Newest first.
    frozen: Vec<Arc<Mem>>,
    tree: Option<Subtree>,
    _epoch: Arc<Epoch>,
    /// Declared after `_epoch`, so a version counts as live until its
    /// epoch has released what it could.
    _live: LiveVersion,
}

/// Counts a version in [`Inner::live_versions`] while it exists.
struct LiveVersion(Arc<AtomicUsize>);

impl LiveVersion {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, SeqCst);
        Self(count.clone())
    }
}

impl Drop for LiveVersion {
    fn drop(&mut self) {
        self.0.fetch_sub(1, SeqCst);
    }
}

struct Writer {
    wal: WalWriter,
    /// Log bytes some writer has been told to flush; see [`Inner::assign`].
    flush_claimed: u64,
    /// Requests a turn left unserved, oldest first, for the next turn; see
    /// [`Inner::serve`].
    carried: VecDeque<usize>,
    next_seq: u64,
    /// Commit ticket of the next batch; see [`Commits`].
    next_ticket: u64,
    active: Arc<Mem>,
    user_bytes: u64,
    wal_bytes: u64,
}

/// A batch waiting for its sequence numbers. Writers push requests onto
/// [`Inner::queue`], and whichever writer holds the writer lock serves every
/// queued request in one turn, so many writers cost one lock handoff. It
/// lives on its writer's stack, which waits until `done` before reading
/// `out` or returning.
struct Request {
    body: *const wal::Body,
    ops: u64,
    entry_bytes: u64,
    user_bytes: u64,
    sync: bool,
    shard: usize,
    next: AtomicPtr<Request>,
    out: UnsafeCell<Option<Result<Assigned>>>,
    done: AtomicBool,
}

/// What a batch learns from the writer lock.
struct Assigned {
    /// The memtable the batch goes into. The batch's writer is counted in
    /// its `inflight`, and a memtable leaves the store only once that count
    /// is zero, so it outlives the writer's use without a reference count.
    mem: *const Mem,
    seq: u64,
    ticket: u64,
    /// The log and the end of the batch's record, for a synchronous batch.
    sync_to: Option<(Arc<wal::Log>, u64)>,
    /// A log whose buffer this batch should flush.
    flush: Option<Arc<wal::Log>>,
}

/// Requests one turn at the writer lock serves at most. A turn registers
/// their tickets while its own batch waits to commit, so it must leave the
/// commit ring room for them.
const TURN_LIMIT: usize = COMMIT_RING as usize / 2;
/// Spins before a waiting writer parks.
const REQUEST_SPINS: u32 = 200;
/// How long a parked writer sleeps before checking whether it should take
/// the writer lock itself. Releasing the lock wakes the writer next in line
/// (see [`WriterGuard`]), so this is a backstop: a short one would have
/// every parked writer polling, and with many more writers than cores those
/// wake-ups starve the turn they wait on.
const REQUEST_PARK: std::time::Duration = std::time::Duration::from_millis(50);

/// The writer lock. Releasing it wakes the writer whose request is next in
/// line to take a turn, so a writer that parks waiting for the lock sleeps
/// until its request is served or it is that writer.
struct WriterGuard<'a> {
    inner: &'a Inner,
    w: Option<parking_lot::MutexGuard<'a, Writer>>,
}

impl std::ops::Deref for WriterGuard<'_> {
    type Target = Writer;
    fn deref(&self) -> &Writer {
        self.w.as_ref().expect("held until dropped")
    }
}

impl std::ops::DerefMut for WriterGuard<'_> {
    fn deref_mut(&mut self) -> &mut Writer {
        self.w.as_mut().expect("held until dropped")
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        let w = self.w.take().expect("held until dropped");
        // The oldest request a turn left goes first, or else the newest
        // queued one, read once the lock is free.
        let carried = w.carried.front().copied();
        drop(w);
        // Pairs with the fence in `Inner::await_request`: a writer that
        // queued a request and then saw the lock held is parked or about to
        // park, and this load sees its request or a later one.
        std::sync::atomic::fence(SeqCst);
        let next = carried.or_else(|| {
            let head = self.inner.queue.load(Acquire);
            (!head.is_null()).then_some(head as usize)
        });
        if let Some(key) = next {
            Inner::wake(&[key]);
        }
    }
}

struct Durable {
    sb: Superblock,
    live: HashSet<PagePtr>,
    blobs: BlobRefs,
    epoch: Arc<Epoch>,
}

/// The blobs the durable tree references. A blob stays allocated while any
/// page of the tree lists it. A flush copies an entry to a child and leaves
/// the original in its segment's page, so two pages can list one blob.
#[derive(Default)]
struct BlobRefs {
    /// The tree's pages that list blobs.
    pages: HashMap<PagePtr, Arc<DataPage>>,
    /// How many of those pages list each blob.
    counts: HashMap<BlobPtr, u32>,
    /// Bytes the blobs occupy, as stored.
    bytes: u64,
}

impl BlobRefs {
    /// Counts the blobs `page` lists, unless they are counted already.
    fn add(&mut self, page: Arc<DataPage>) {
        let ptr = page.ptr().expect("a durable page is written");
        if self.pages.contains_key(&ptr) {
            return;
        }
        for blob in page.meta.blobs() {
            let count = self.counts.entry(blob).or_insert(0);
            if *count == 0 {
                self.bytes += blob.len as u64;
            }
            *count += 1;
        }
        self.pages.insert(ptr, page);
    }

    /// Stops counting the blobs that the page at `ptr` lists, and adds the
    /// slot of each blob no page lists any more to `dead`.
    fn remove(&mut self, ptr: &PagePtr, dead: &mut Vec<PagePtr>) {
        let Some(page) = self.pages.remove(ptr) else { return };
        for blob in page.meta.blobs() {
            let count = self.counts.get_mut(&blob).expect("a listed blob is counted");
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&blob);
                self.bytes -= blob.len as u64;
                dead.push(blob.page_ptr());
            }
        }
    }

    /// Counts the blobs of every page in `pages`.
    fn of(pages: Vec<Arc<DataPage>>) -> Self {
        let mut refs = Self::default();
        pages.into_iter().for_each(|p| refs.add(p));
        refs
    }
}

/// The applier's view: the newest tree, which may be ahead of the committed
/// one by checkpoints still waiting to commit.
struct Applier {
    root: Option<Subtree>,
    last_applied: u64,
}

/// A checkpoint whose pages are written but not yet synced and committed.
struct Applied {
    mem: Arc<Mem>,
    root: Option<Subtree>,
    created_files: bool,
    /// Pages written while the checkpoint applied. Any that a later batch
    /// superseded are garbage no committed tree references.
    written: Vec<PagePtr>,
    /// Every blob written for the memtable: by its writers before they
    /// logged, and by the checkpoint for large values that arrived without
    /// one. Those the new tree leaves unreferenced are garbage: their puts
    /// were overwritten or deleted within the memtable.
    blobs: Vec<BlobPtr>,
    /// Whether the checkpoint wrote any of them.
    wrote_blobs: bool,
}

/// An applied checkpoint's tree, whether it created an arena file, and the
/// pages it wrote while applying.
type Checkpointed = (Option<Subtree>, bool, Vec<PagePtr>);

/// The blobs written while cutting a checkpoint's batches.
type Built = Vec<BlobPtr>;

#[derive(Debug, Default, Clone)]
pub struct DbStats {
    pub checkpoints: u64,
    pub user_bytes: u64,
    pub wal_bytes: u64,
    pub page_bytes_written: u64,
    pub pages_written: u64,
    pub block_reads: u64,
    pub span_reads: u64,
    pub span_read_bytes: u64,
    pub cache_bytes: u64,
    /// Cache bytes held as whole page images rather than single blocks.
    pub cache_image_bytes: u64,
    pub memtable_bytes: u64,
    pub frozen_memtables: usize,
    /// Range deletes in memtables, which reads check until their
    /// checkpoints apply them to the tree.
    pub range_deletes: usize,
    pub tree: tree::TreeStats,
    /// Blobs the durable tree references, and the bytes they occupy as
    /// stored.
    pub blobs: u64,
    pub blob_bytes: u64,
    /// Blobs written and read since the store opened, and the bytes written
    /// as stored.
    pub blobs_written: u64,
    pub blob_bytes_written: u64,
    pub blob_reads: u64,
    pub arena_bytes: u64,
    /// Seconds in each checkpoint stage: build, apply, write, sync, commit;
    /// then seconds writers stalled, and seconds checkpoint I/O waited on
    /// its pace.
    pub stage_secs: [f64; 7],
}

struct Inner {
    storage: Arc<dyn Storage>,
    opts: Options,
    io: Arc<PageIo>,
    next_page_id: AtomicU64,
    version: ArcSwap<Version>,
    /// Versions in existence: the current one, and older ones that readers
    /// or background threads still hold. Garbage pages wait for every
    /// version older than the current one to drop.
    live_versions: Arc<AtomicUsize>,
    /// Serializes version replacements.
    version_lock: Mutex<()>,
    /// The writer lock and the queue of requests waiting for it, newest
    /// first, which every writer updates. Each keeps to a cache line of its
    /// own, as [`Commits`] does: Rust orders these fields as it likes, and
    /// when the writer lock came to share a line with the commit counters, a
    /// load of 10M inserts by 16 writers lost 9% of its throughput.
    writer: CachePadded<Mutex<Writer>>,
    queue: CachePadded<AtomicPtr<Request>>,
    /// Paired with `signal`: wakes the checkpointer when a memtable freezes
    /// and blocked writers when a checkpoint commits.
    signal_lock: Mutex<()>,
    signal: Condvar,
    durable: Mutex<Durable>,
    applier: Mutex<Applier>,
    pending: Mutex<VecDeque<Applied>>,
    poisoned: AtomicBool,
    shutdown: AtomicBool,
    checkpoints: AtomicU64,
    commits: Commits,
    timing: Timing,
    merge_pool: Option<rayon::ThreadPool>,
    /// Bytes the last few checkpoints read for merges and wrote as pages.
    checkpoint_io: Mutex<VecDeque<u64>>,
    recycler: wal::Recycler,
    /// The memory of dropped memtables, for new ones.
    mem_pool: Arc<ChunkPool>,
    /// Whether the superblock marks the store for range deletes; see
    /// [`Superblock::range_deletes`].
    range_deletes: AtomicBool,
    /// Ids for new blobs, none of them used before.
    blob_ids: BlobIds,
    /// Makes the blobs written so far durable. Each log sync runs it first,
    /// so no record becomes durable ahead of a blob it points to.
    sync_blobs: wal::PreSync,
    /// The buffers of a log segment that has written its last record, for
    /// the next memtable rotation's segment.
    log_buffers: Arc<Mutex<Option<wal::Buffers>>>,
    /// The newest log segment, for the committer, which must not take the
    /// writer lock: a writer holds that lock while it waits for a commit.
    newest_log: Mutex<Arc<wal::Log>>,
}

/// Where blob ids come from. No id is used twice, across crashes too: the
/// superblock reserves a range durably before the store uses an id in it
/// (see [`Superblock::next_blob_id`]), and each open starts past the last
/// reservation, leaving the rest of it unused.
struct BlobIds {
    next: AtomicU64,
    /// The end of the reserved range.
    reserved: AtomicU64,
}

/// Freed blob slots keep their disk space for reuse up to this many
/// memtables' worth: what a few checkpoints of overwrites free and take
/// again. See [`PageStore::open_blobs`].
const FREE_BLOB_MEMTABLES: u64 = 4;

/// Blob ids reserved at a time. Each reservation writes the superblock, and
/// the writer whose blob needs it waits for that write.
const BLOB_ID_BATCH: u64 = 1 << 24;

/// Cumulative nanoseconds spent in each checkpoint stage, and by writers
/// waiting for a frozen-memtable slot.
#[derive(Default, Debug)]
pub struct Timing {
    pub build_ns: AtomicU64,
    pub apply_ns: AtomicU64,
    pub write_ns: AtomicU64,
    pub sync_ns: AtomicU64,
    pub commit_ns: AtomicU64,
    pub stall_ns: AtomicU64,
}

fn since(t: std::time::Instant) -> u64 {
    t.elapsed().as_nanos() as u64
}

pub struct Db {
    inner: Arc<Inner>,
    workers: Vec<JoinHandle<()>>,
}

impl Db {
    /// Opens the store in `storage`, creating it if empty, and recovers the
    /// last checkpoint plus every logged write after it.
    pub fn open(storage: Arc<dyn Storage>, opts: Options) -> Result<Self> {
        let sb = match Superblock::load(&*storage) {
            Ok(Some(sb)) => sb,
            // Creating the superblock is the first durable step of a new
            // store. If nothing else exists, a crash interrupted creation.
            Ok(None) | Err(Error::Corruption(_)) if storage.list()?.iter().all(|n| n == "superblock") => {
                let sb = Superblock { next_page_id: 1, ..Default::default() };
                sb.create(&*storage)?;
                sb
            }
            Ok(None) => return Err(crate::error::corruption("store files exist without a superblock")),
            Err(e) => return Err(e),
        };
        let max_image = opts.tree.leaf_size * 2 + (1 << 20);
        // Load the tree and replay the log through provisional stores, then
        // reopen the arenas knowing which slots the tree and the log use.
        let probe = PageIo::open(storage.clone(), 0, max_image, Compression::None)?;
        let root = sb.root.map(|r| tree::load_tree(&probe, r)).transpose()?;
        let mut live = HashSet::new();
        let mut blob_pages = Vec::new();
        if let Some(r) = &root {
            tree::reachable(r, &mut live, &mut blob_pages);
        }
        let blobs = BlobRefs::of(blob_pages);

        // Replay the log into one memtable. A put that points to a blob
        // takes its value from the blob, which shows the blob is whole.
        let mem_pool = ChunkPool::new(opts.memtable_limit());
        let mut last_seq = sb.last_seq;
        let mut max_id = sb.last_memtable_id;
        let replayed = Mem::new(0, &opts, last_seq, &mem_pool);
        let segments = wal::list_segments(&*storage)?;
        let mut gap = false;
        for &id in &segments {
            max_id = max_id.max(id);
            if id <= sb.last_memtable_id || gap {
                // Covered by the checkpoint, or written after the crash point
                // that a gap revealed.
                storage.remove(&wal::segment_name(id))?;
                continue;
            }
            let before = last_seq;
            let blobs_before = replayed.blobs.lock().len();
            let read = |ptr: &BlobPtr| match probe.read_blob(ptr) {
                Ok(value) => Ok(Some(value)),
                // The blob's write was lost or torn, or its arena never
                // grew to hold it.
                Err(Error::Corruption(_)) => Ok(None),
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
                Err(e) => Err(e),
            };
            let r = wal::replay_blobs(&*storage, id, last_seq, read, |seq, op, blob| replayed.replay(&op, seq, blob))?;
            // Replay may have read records that only reached the OS cache
            // (after a process crash). New writes build on them, so they must
            // be durable before the store accepts any: the blobs they point
            // to first, as in any sync.
            if replayed.blobs.lock().len() > blobs_before {
                probe.blobs.sync()?;
                storage.sync_dir()?;
            }
            storage.open(&wal::segment_name(id), false)?.sync_data()?;
            if std::env::var_os("TURTLEKV_DEBUG").is_some() {
                eprintln!("recovery: sb {sb:?}, segment {id}: {} -> {} {:?}", last_seq, r.last_seq, r.end);
            }
            last_seq = r.last_seq;
            // A gap after replayed records ends this segment's history like a
            // torn record: the segment is the last one written before the
            // crash, and replay always stops at the same place. A gap before
            // any record means this segment and every later one were written
            // after the crash point.
            if r.end == wal::End::Gap && r.last_seq == before {
                gap = true;
                storage.remove(&wal::segment_name(id))?;
            }
        }
        storage.sync_dir()?;
        drop(probe);
        // The slots in use are the tree's and those of the blobs that
        // replayed puts point to.
        let logged: Vec<BlobPtr> = replayed.blobs.lock().iter().map(|&(_, blob)| blob).collect();
        let live_vec: Vec<PagePtr> = live.iter().copied().collect();
        let live_blobs: Vec<PagePtr> = blobs.counts.keys().chain(&logged).map(BlobPtr::page_ptr).collect();
        let io = Arc::new(PageIo::new(
            PageStore::open(storage.clone(), &live_vec)?,
            PageStore::open_blobs(storage.clone(), &live_blobs, FREE_BLOB_MEMTABLES * opts.memtable_limit())?,
            opts.cache_bytes,
            max_image,
            opts.compression,
        ));
        io.store.punch_free();
        io.blobs.punch_free();
        io.counters.blobs_live.store(blobs.counts.len() as u64, Relaxed);
        io.counters.blob_bytes_live.store(blobs.bytes, Relaxed);
        let sync_blobs: wal::PreSync = {
            let io = io.clone();
            Arc::new(move || io.blobs.sync_new())
        };
        let mut frozen = Vec::new();
        if !replayed.is_empty() {
            replayed.counts.last_seq.store(last_seq, Relaxed);
            let mem = Mem { id: max_id, ..replayed };
            frozen.push(Arc::new(mem));
        }
        let active_id = max_id + 1;
        let recycler = wal::Recycler::open(&*storage, active_id)?;
        if opts.preallocate_log && recycler.is_empty() {
            recycler.prepare(&*storage, recycler.segment_len(opts.log_segment_len()), |_| true);
        }
        let wal = WalWriter::create_after(&*storage, active_id, recycler.take(), wal::Buffers::default(), Some(sync_blobs.clone()))?;
        let newest_log = Mutex::new(wal.log());
        let active = Arc::new(Mem::new(active_id, &opts, last_seq, &mem_pool));
        let epoch = Arc::new(Epoch::new(io.clone()));
        let applier = Applier { root: root.clone(), last_applied: sb.last_memtable_id };
        let live_versions = Arc::new(AtomicUsize::new(0));
        let version =
            Version { active: active.clone(), frozen, tree: root, _epoch: epoch.clone(), _live: LiveVersion::new(&live_versions) };
        let merge_pool = (opts.tree.parallel && opts.merge_threads > 0).then(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(opts.merge_threads)
                .thread_name(|i| format!("lionturtle-merge-{i}"))
                .build()
                .expect("build merge pool")
        });
        let inner = Arc::new(Inner {
            storage,
            next_page_id: AtomicU64::new(sb.next_page_id),
            io,
            version: ArcSwap::from_pointee(version),
            live_versions,
            version_lock: Mutex::new(()),
            writer: CachePadded(Mutex::new(Writer {
                wal,
                flush_claimed: 0,
                carried: VecDeque::new(),
                next_seq: last_seq + 1,
                next_ticket: 1,
                active,
                user_bytes: 0,
                wal_bytes: 0,
            })),
            queue: CachePadded(AtomicPtr::new(std::ptr::null_mut())),
            signal_lock: Mutex::new(()),
            signal: Condvar::new(),
            range_deletes: AtomicBool::new(sb.range_deletes),
            blob_ids: BlobIds { next: AtomicU64::new(sb.next_blob_id), reserved: AtomicU64::new(sb.next_blob_id) },
            sync_blobs,
            log_buffers: Arc::default(),
            newest_log,
            durable: Mutex::new(Durable { sb, live, blobs, epoch }),
            applier: Mutex::new(applier),
            pending: Mutex::new(VecDeque::new()),
            poisoned: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            checkpoints: AtomicU64::new(0),
            commits: Commits::new(last_seq),
            timing: Timing::default(),
            merge_pool,
            checkpoint_io: Mutex::new(VecDeque::new()),
            recycler,
            mem_pool,
            opts,
        });
        // Two pipeline stages, as in the paper: one thread applies the next
        // frozen memtable while the other syncs and commits the last one.
        let mut workers = Vec::new();
        if inner.opts.background {
            let prepare = inner.opts.preallocate_log.then_some(("lionturtle-log", Stage::PrepareLog));
            for (name, stage) in [("lionturtle-apply", Stage::Apply), ("lionturtle-commit", Stage::Commit)].into_iter().chain(prepare) {
                let inner = inner.clone();
                workers.push(
                    std::thread::Builder::new().name(name.into()).spawn(move || inner.stage_loop(stage)).expect("spawn checkpoint thread"),
                );
            }
        }
        Ok(Self { inner, workers })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.write(&[(key, Some(value))], false)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.write(&[(key, None)], false)
    }

    /// Deletes every key in `[start, end)`; `None` leaves the range
    /// unbounded above. The cost does not depend on how many keys the range
    /// holds: until the next checkpoint, reads check the range, and the
    /// checkpoint drops the subtrees inside it without reading them. An empty
    /// or inverted range deletes nothing.
    ///
    /// A store that has taken a range delete opens only with lionturtle
    /// 0.7.0 or later.
    pub fn delete_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<()> {
        self.inner.write(&[OpRef::DeleteRange(start, end)], false)
    }

    /// Deletes every key that starts with `prefix`; see [`Db::delete_range`].
    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<()> {
        let end = prefix_end(prefix);
        self.inner.write(&[OpRef::DeleteRange(prefix, end.as_deref())], false)
    }

    /// Applies `ops` atomically. With `sync`, returns once they are durable.
    ///
    /// The ops are [`WriteOp`]s, or `(key, value)` pairs of borrowed bytes
    /// with `None` for a delete. The store copies the keys and values into
    /// its log and its memtable, and keeps none of the caller's buffers, so
    /// a caller that holds its keys and values as slices need not allocate
    /// to write them.
    pub fn write<O: Op>(&self, ops: &[O], sync: bool) -> Result<()> {
        self.inner.write(ops, sync)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    /// Up to `limit` live entries with keys in `[start, end)`, in order, as
    /// of one snapshot.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        self.snapshot().scan(start, end, limit)
    }

    /// A consistent view of the store for reads that must agree with each
    /// other; see [`Snapshot`].
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.inner.clone())
    }

    /// An iterator over live entries with keys in `[start, end)`, in key
    /// order, as of a snapshot taken by this call: it sees every batch
    /// written before the call and none after.
    pub fn iter(&self, start: &[u8], end: Option<&[u8]>) -> DbIter {
        self.snapshot().iter(start, end)
    }

    /// [`Db::iter`] in descending key order.
    pub fn iter_rev(&self, start: &[u8], end: Option<&[u8]>) -> DbIter {
        self.snapshot().iter_rev(start, end)
    }

    /// An iterator over live entries whose keys start with `prefix`, in key
    /// order, as of a snapshot taken by this call.
    pub fn prefix(&self, prefix: &[u8]) -> DbIter {
        self.snapshot().prefix(prefix)
    }

    /// [`Db::prefix`] in descending key order.
    pub fn prefix_rev(&self, prefix: &[u8]) -> DbIter {
        self.snapshot().prefix_rev(prefix)
    }

    /// Makes every completed write durable.
    pub fn sync(&self) -> Result<()> {
        self.inner.check_poisoned()?;
        let (log, end) = {
            let w = self.inner.lock_writer();
            (w.wal.log(), w.wal.len())
        };
        let r = log.sync_to(end);
        self.inner.poison_on_err(r)
    }

    /// Freezes the active memtable (if it holds anything) and waits until
    /// every frozen memtable is checkpointed.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.check_poisoned()?;
        {
            let mut w = self.inner.lock_writer();
            if !w.active.is_empty() {
                let r = self.inner.rotate(&mut w);
                self.inner.poison_on_err(r)?;
            }
        }
        // The caller waits on any checkpoint under way.
        self.inner.io.pacer.stop();
        loop {
            if self.inner.version.load().frozen.is_empty() {
                return Ok(());
            }
            self.inner.check_poisoned()?;
            if self.inner.opts.background {
                let mut g = self.inner.signal_lock.lock();
                if !self.inner.version.load().frozen.is_empty() && !self.inner.poisoned.load(SeqCst) {
                    self.inner.signal.wait_for(&mut g, std::time::Duration::from_millis(50));
                }
            } else {
                self.inner.checkpoint_step()?;
            }
        }
    }

    /// Advances the checkpoint pipeline by one stage: commits an applied
    /// checkpoint if one is waiting, or else applies the oldest unapplied
    /// frozen memtable. Returns false if there was nothing to do. For callers
    /// that run without background threads.
    pub fn checkpoint_step(&self) -> Result<bool> {
        self.inner.checkpoint_step()
    }

    /// Freezes the active memtable if it holds anything, without waiting.
    pub fn freeze(&self) -> Result<()> {
        let mut w = self.inner.lock_writer();
        if !w.active.is_empty() {
            let r = self.inner.rotate(&mut w);
            self.inner.poison_on_err(r)?;
        }
        Ok(())
    }

    /// Sequence number of the newest write, durable or not.
    pub fn last_seq(&self) -> u64 {
        self.inner.lock_writer().next_seq - 1
    }

    pub fn is_poisoned(&self) -> bool {
        self.inner.poisoned.load(SeqCst)
    }

    pub fn stats(&self) -> DbStats {
        let inner = &self.inner;
        let (user_bytes, wal_bytes) = {
            let w = inner.lock_writer();
            (w.user_bytes, w.wal_bytes)
        };
        let v = inner.version.load();
        let c = &inner.io.counters;
        DbStats {
            checkpoints: inner.checkpoints.load(Relaxed),
            user_bytes,
            wal_bytes,
            page_bytes_written: c.page_bytes_written.load(Relaxed),
            pages_written: c.pages_written.load(Relaxed),
            block_reads: c.block_reads.load(Relaxed),
            span_reads: c.span_reads.load(Relaxed),
            span_read_bytes: c.span_read_bytes.load(Relaxed),
            cache_bytes: inner.io.cache_weight(),
            cache_image_bytes: inner.io.cache_image_bytes(),
            memtable_bytes: v.active.bytes() + v.frozen.iter().map(|m| m.bytes()).sum::<u64>(),
            frozen_memtables: v.frozen.len(),
            range_deletes: std::iter::once(&v.active).chain(&v.frozen).map(|m| m.ranges.len()).sum(),
            tree: v.tree.as_ref().map(tree::stats).unwrap_or_default(),
            blobs: c.blobs_live.load(Relaxed),
            blob_bytes: c.blob_bytes_live.load(Relaxed),
            blobs_written: c.blobs_written.load(Relaxed),
            blob_bytes_written: c.blob_bytes_written.load(Relaxed),
            blob_reads: c.blob_reads.load(Relaxed),
            arena_bytes: inner.io.store.arena_bytes() + inner.io.blobs.arena_bytes(),
            stage_secs: [
                &inner.timing.build_ns,
                &inner.timing.apply_ns,
                &inner.timing.write_ns,
                &inner.timing.sync_ns,
                &inner.timing.commit_ns,
                &inner.timing.stall_ns,
                &inner.io.pacer.waited_ns,
            ]
            .map(|c| c.load(Relaxed) as f64 / 1e9),
        }
    }

    /// Checks the tree's structure and that the allocators hold exactly the
    /// pages and blobs that the durable tree, the checkpoints under way and
    /// the memtables reference, plus garbage still pinned by readers. Reads
    /// every page; for tests and simulation, while no write is in flight.
    pub fn check_invariants(&self) -> Result<()> {
        let inner = &self.inner;
        // A commit holds `durable` from replacing the version until it lets
        // go of the old one, so no commit is half done while this runs.
        let durable = inner.durable.lock();
        let v = inner.version.load_full();
        if let Some(t) = &v.tree {
            tree::check_invariants(&inner.io, &inner.opts.tree, t)?;
        }
        // The durable tree's blob counts match a count from scratch.
        let (mut pages, mut blob_pages) = (HashSet::new(), Vec::new());
        if let Some(t) = &v.tree {
            tree::reachable(t, &mut pages, &mut blob_pages);
        }
        let recount = BlobRefs::of(blob_pages);
        assert!(recount.counts == durable.blobs.counts && recount.bytes == durable.blobs.bytes, "blob counts drifted from the tree");
        let mut expected = durable.live.clone();
        let mut blobs: HashSet<PagePtr> = durable.blobs.counts.keys().map(BlobPtr::page_ptr).collect();
        for a in inner.pending.lock().iter() {
            let mut blob_pages = Vec::new();
            if let Some(r) = &a.root {
                tree::reachable(r, &mut expected, &mut blob_pages);
            }
            // Its superseded pages stay allocated until it commits, as do
            // the blobs written for its memtable.
            expected.extend(a.written.iter().copied());
            blobs.extend(blob_pages.iter().flat_map(|p| p.meta.blobs()).chain(a.blobs.iter().copied()).map(|b| b.page_ptr()));
        }
        // A memtable's blobs stay allocated until its checkpoint commits.
        for m in std::iter::once(&v.active).chain(&v.frozen) {
            blobs.extend(m.blobs.lock().iter().map(|(_, b)| b.page_ptr()));
        }
        for (what, store, expected) in [("page", &inner.io.store, &expected), ("blob", &inner.io.blobs, &blobs)] {
            let allocated: HashSet<(u8, u32)> = store.allocated().into_iter().collect();
            let live: HashSet<(u8, u32)> = expected.iter().map(|p| (p.class, p.slot)).collect();
            assert!(live.is_subset(&allocated), "a live {what} is not allocated");
            // With no older version left, no garbage awaits release. A
            // background thread can hold an older version for a moment
            // after a commit, so a check after `checkpoint` may find one.
            if inner.live_versions.load(SeqCst) == 1 {
                let leaked: Vec<_> = allocated.difference(&live).collect();
                assert!(leaked.is_empty(), "leaked {what} slots: {leaked:?}");
            }
        }
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, SeqCst);
        {
            let _g = self.inner.signal_lock.lock();
            self.inner.signal.notify_all();
        }
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
        if !self.inner.poisoned.load(SeqCst) {
            let _ = self.inner.lock_writer().wal.sync();
        }
    }
}

impl Inner {
    /// Takes the writer lock for a caller that serves no requests; see
    /// [`WriterGuard`].
    fn lock_writer(&self) -> WriterGuard<'_> {
        WriterGuard { inner: self, w: Some(self.writer.lock()) }
    }

    fn check_poisoned(&self) -> Result<()> {
        if self.poisoned.load(SeqCst) { Err(Error::Poisoned) } else { Ok(()) }
    }

    /// A failed write leaves the log or arenas in an unknown state, so the
    /// store stops accepting writes until it is reopened.
    fn poison_on_err<T>(&self, r: Result<T>) -> Result<T> {
        if r.is_err() {
            self.poisoned.store(true, SeqCst);
            let _g = self.signal_lock.lock();
            self.signal.notify_all();
        }
        r
    }

    fn notify(&self) {
        let _g = self.signal_lock.lock();
        self.signal.notify_all();
    }

    /// Marks the superblock for range deletes before the first one reaches
    /// the log.
    fn mark_range_deletes(&self) -> Result<()> {
        let mut durable = self.durable.lock();
        if !durable.sb.range_deletes {
            durable.sb.mark_range_deletes(&*self.storage)?;
        }
        self.range_deletes.store(true, Release);
        Ok(())
    }

    /// An id for a new blob, reserved durably before it is returned. The
    /// first reservation also marks the superblock for blobs.
    fn new_blob_id(&self) -> Result<u64> {
        let id = self.blob_ids.next.fetch_add(1, Relaxed);
        if id >= self.blob_ids.reserved.load(Acquire) {
            let mut durable = self.durable.lock();
            if id >= self.blob_ids.reserved.load(Acquire) {
                // Past every id handed out so far, this one included.
                let upto = self.blob_ids.next.load(Relaxed).max(id + 1) + BLOB_ID_BATCH;
                durable.sb.reserve_blob_ids(upto, &*self.storage)?;
                self.blob_ids.reserved.store(upto, Release);
            }
        }
        Ok(id)
    }

    /// Writes `value` to a blob of its own and returns the blob's pointer.
    /// A checkpoint's write is `paced`; see [`PageIo::write_blob`].
    fn write_blob(&self, value: &[u8], paced: bool) -> Result<BlobPtr> {
        self.io.write_blob(self.new_blob_id()?, value, paced)
    }

    /// Logs `ops` and inserts them in the memtable, each of which copies
    /// the bytes it keeps.
    ///
    /// A put of at least [`Options::blob_threshold`] bytes first writes its
    /// value to a blob, on this thread and outside every lock, and its
    /// record carries the blob's pointer. Writers thus write their large
    /// values in parallel, and the value reaches the drive once. The
    /// memtable keeps the value for reads and the pointer for the
    /// checkpoint.
    fn write<O: Op>(&self, ops: &[O], sync: bool) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let threshold = self.opts.blob_threshold;
        let (mut entry_bytes, mut user_bytes, mut ranges, mut large) = (0, 0, false, false);
        for op in ops {
            let op = op.op();
            let (key, value) = match op {
                OpRef::Put(key, value) => {
                    large |= value.len() >= threshold;
                    (key.len(), value.len())
                }
                OpRef::Delete(key) => (key.len(), 0),
                OpRef::DeleteRange(start, end) => {
                    ranges = true;
                    if end.is_some_and(|e| e.len() > MAX_KEY_LEN) {
                        return Err(Error::InvalidArgument("range end too large".into()));
                    }
                    (start.len(), end.map_or(0, <[u8]>::len))
                }
            };
            if key > MAX_KEY_LEN || value > MAX_VALUE_LEN {
                return Err(Error::InvalidArgument("key or value too large".into()));
            }
            entry_bytes += op_entry_bytes(op);
            user_bytes += (key + value) as u64;
        }
        self.check_poisoned()?;
        if ranges && !self.range_deletes.load(Acquire) {
            let r = self.mark_range_deletes();
            self.poison_on_err(r)?;
        }
        let blobs: Vec<Option<BlobPtr>> = match large {
            true => {
                let written = ops.iter().map(|op| match op.op() {
                    OpRef::Put(_, value) if value.len() >= threshold => self.write_blob(value, false).map(Some),
                    _ => Ok(None),
                });
                let written = written.collect();
                self.poison_on_err(written)?
            }
            false => Vec::new(),
        };
        thread_local! {
            static BODY: std::cell::RefCell<wal::Body> = Default::default();
        }
        BODY.with_borrow_mut(|body| {
            // Everything but the sequence number is encoded and checksummed
            // before queueing, which keeps the writer lock's turns short.
            body.encode(ops, &blobs);
            let request = Request {
                body,
                ops: ops.len() as u64,
                entry_bytes,
                user_bytes,
                sync,
                shard: Inflight::shard(),
                next: AtomicPtr::new(std::ptr::null_mut()),
                out: UnsafeCell::new(None),
                done: AtomicBool::new(false),
            };
            self.enqueue(&request);
            self.await_request(&request);
            // SAFETY: `done` is set, so the writer that served the request
            // has stored `out` and no longer touches it.
            let a = unsafe { (*request.out.get()).take() }.expect("served request")?;
            // A synchronous batch becomes visible once it is durable.
            let flushed = a.flush.map_or(Ok(()), |log| log.flush()).and_then(|()| a.sync_to.map_or(Ok(()), |(log, end)| log.sync_to(end)));
            // SAFETY: see `Assigned::mem`; this writer stays counted in
            // until the `exit` below, its last use of the memtable.
            let mem = unsafe { &*a.mem };
            if large {
                let logged = blobs.iter().enumerate().filter_map(|(i, blob)| blob.map(|blob| (a.seq + i as u64, blob)));
                mem.blobs.lock().extend(logged);
            }
            mem.insert_all(ops, a.seq, a.ticket, ranges);
            self.commits.commit(a.ticket);
            mem.inflight.exit(request.shard);
            self.poison_on_err(flushed)
        })
    }

    fn enqueue(&self, request: &Request) {
        let p = request as *const Request as *mut Request;
        let mut head = self.queue.load(Relaxed);
        loop {
            request.next.store(head, Relaxed);
            match self.queue.compare_exchange_weak(head, p, Release, Relaxed) {
                Ok(_) => return,
                Err(h) => head = h,
            }
        }
    }

    /// Returns once `request` is served: by this thread, if it takes the
    /// writer lock, or by whichever writer holds it.
    fn await_request(&self, request: &Request) {
        let mut spins = 0;
        loop {
            if request.done.load(Acquire) {
                return;
            }
            if let Some(w) = self.writer.try_lock() {
                thread_local! {
                    /// The requests a turn served, kept between turns so a
                    /// turn allocates nothing.
                    static SERVED: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
                }
                SERVED.with_borrow_mut(|served| {
                    let mut w = WriterGuard { inner: self, w: Some(w) };
                    served.clear();
                    self.serve(&mut w, served);
                    // Releasing the lock wakes a writer to take the next turn.
                    drop(w);
                    Self::wake(served);
                });
                continue;
            }
            if spins < REQUEST_SPINS {
                spins += 1;
                std::hint::spin_loop();
                continue;
            }
            let key = request as *const Request as usize;
            let deadline = std::time::Instant::now() + REQUEST_PARK;
            // Park only while the lock is held. The fence pairs with the one
            // in `WriterGuard::drop`: either this check sees the lock free,
            // or the writer releasing it sees this request queued and wakes
            // a writer to serve it.
            let waiting = || {
                std::sync::atomic::fence(SeqCst);
                !request.done.load(Acquire) && self.writer.is_locked()
            };
            // SAFETY: the callbacks neither panic nor call into the parking
            // lot.
            unsafe {
                parking_lot_core::park(key, waiting, || {}, |_, _| {}, parking_lot_core::DEFAULT_PARK_TOKEN, Some(deadline));
            }
        }
    }

    /// One turn at the writer lock: serves the requests the last turn left,
    /// then those queued now, oldest first, and adds their keys to `served`
    /// for [`Self::wake`] once the lock is released.
    ///
    /// A turn takes the queue once and serves at most [`TURN_LIMIT`]
    /// requests, so it ends and lets its own batch commit. A turn rotates the
    /// memtable only before serving anything: its writer's own batch, once
    /// served, sits in the memtable a rotation may have to wait to
    /// checkpoint. A request needing a rotation after that waits for the next
    /// turn.
    fn serve(&self, w: &mut Writer, served: &mut Vec<usize>) {
        let mut p = self.queue.swap(std::ptr::null_mut(), Acquire);
        // The queue runs newest first: its requests join the carried ones
        // in that order, then turn around in place.
        let carried = w.carried.len();
        while !p.is_null() {
            w.carried.push_back(p as usize);
            // SAFETY: a queued request stays alive until it is done.
            p = unsafe { (*p).next.load(Relaxed) };
        }
        w.carried.make_contiguous()[carried..].reverse();
        while let Some(&key) = w.carried.front() {
            if served.len() == TURN_LIMIT || !served.is_empty() && w.active.bytes() >= self.opts.memtable_limit() {
                break;
            }
            w.carried.pop_front();
            // SAFETY: as above; its writer reads `out` only after `done`.
            let request = unsafe { &*(key as *const Request) };
            let out = self.assign(w, request);
            unsafe { *request.out.get() = Some(out) };
            request.done.store(true, Release);
            served.push(key);
        }
    }

    /// Wakes the writers of served requests that parked. Only the keys are
    /// used: the requests themselves may be gone.
    fn wake(keys: &[usize]) {
        for &key in keys {
            // SAFETY: the callback neither panics nor calls into the
            // parking lot.
            unsafe { parking_lot_core::unpark_one(key, |_| parking_lot_core::DEFAULT_UNPARK_TOKEN) };
        }
    }

    /// Gives one batch its sequence numbers, logs it, and registers its
    /// ticket.
    fn assign(&self, w: &mut Writer, r: &Request) -> Result<Assigned> {
        self.check_poisoned()?;
        if w.active.bytes() >= self.opts.memtable_limit() {
            let _ = w.active.filled.set(std::time::Instant::now());
            let res = self.rotate(w);
            self.poison_on_err(res)?;
        }
        // SAFETY: the request's writer waits, keeping its body alive.
        let body = unsafe { &*r.body };
        let seq = w.next_seq;
        w.next_seq += r.ops;
        w.wal.append_body(body, seq);
        w.user_bytes += r.user_bytes;
        w.wal_bytes += body.record_len() as u64;
        // A synchronous batch syncs after the lock is released, sharing a
        // write and fsync with batches that arrive meanwhile. Otherwise one
        // batch per `wal_buffer_bytes` of log flushes it.
        let len = w.wal.len();
        let mut sync_to = None;
        let mut flush = None;
        if r.sync {
            sync_to = Some((w.wal.log(), len));
        } else if len - w.flush_claimed >= self.opts.wal_buffer_bytes as u64 {
            w.flush_claimed = len;
            flush = Some(w.wal.log());
        }
        w.active.counts.last_seq.store(seq + r.ops - 1, Relaxed);
        w.active.counts.bytes.fetch_add(r.entry_bytes, Relaxed);
        w.active.inflight.enter(r.shard);
        let ticket = w.next_ticket;
        w.next_ticket += 1;
        self.commits.register(ticket, seq + r.ops - 1);
        Ok(Assigned { mem: Arc::as_ptr(&w.active), seq, ticket, sync_to, flush })
    }

    /// Freezes the active memtable and starts a new one with a new log
    /// segment. Waits (or checkpoints inline) until a frozen slot is free.
    fn rotate(&self, w: &mut Writer) -> Result<()> {
        let stall = std::time::Instant::now();
        loop {
            if self.version.load().frozen.len() < self.opts.max_frozen {
                break;
            }
            // Writers wait on the checkpoint: let it run at full speed.
            self.io.pacer.stop();
            w.active.waited.store(true, Relaxed);
            self.check_poisoned()?;
            if self.opts.background {
                let mut g = self.signal_lock.lock();
                if self.version.load().frozen.len() >= self.opts.max_frozen && !self.poisoned.load(SeqCst) {
                    self.signal.wait_for(&mut g, std::time::Duration::from_millis(50));
                }
            } else {
                self.checkpoint_step()?;
            }
        }
        self.timing.stall_ns.fetch_add(since(stall), Relaxed);
        let id = w.active.id + 1;
        if self.opts.preallocate_log && !self.opts.background && self.recycler.is_empty() {
            // No thread prepares segments ahead, so this one is prepared
            // now, and a simulated run stays deterministic.
            self.recycler.prepare(&*self.storage, self.recycler.segment_len(self.opts.log_segment_len()), |_| true);
        }
        // The old segment must be durable before the new one's file takes a
        // record, so that recovery always replays a prefix of history. This
        // thread holds the writer lock, so it leaves that sync, and the sync
        // of the new segment's directory entry, to the new segment's first
        // write: the new segment buffers its records until then, and its
        // first flush or sync, outside the writer lock, runs this first.
        // The old segment's buffers then wait here for the next rotation.
        let settle = {
            let (old, storage, spare) = (w.wal.log(), self.storage.clone(), self.log_buffers.clone());
            move || -> Result<()> {
                old.sync_to(old.len())?;
                storage.sync_dir()?;
                *spare.lock() = Some(old.take_buffers());
                Ok(())
            }
        };
        let buffers = self.log_buffers.lock().take().unwrap_or_default();
        let sync_blobs = Some(self.sync_blobs.clone());
        let wal = WalWriter::create_deferred(&*self.storage, id, self.recycler.take(), buffers, sync_blobs, Box::new(settle))?;
        let mem = Arc::new(Mem::new(id, &self.opts, w.next_seq - 1, &self.mem_pool));
        {
            let _g = self.version_lock.lock();
            let v = self.version.load();
            let mut frozen = vec![w.active.clone()];
            frozen.extend(v.frozen.iter().cloned());
            if frozen.len() > 1 {
                // A frozen memtable now waits behind the checkpoint under way.
                self.io.pacer.stop();
            }
            let epoch = v._epoch.clone();
            self.version.store(Arc::new(Version {
                active: mem.clone(),
                frozen,
                tree: v.tree.clone(),
                _epoch: epoch,
                _live: LiveVersion::new(&self.live_versions),
            }));
        }
        *self.newest_log.lock() = wal.log();
        w.wal = wal;
        w.flush_claimed = 0;
        w.active = mem;
        self.notify();
        Ok(())
    }

    /// The newest committed value of `key`.
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let v = self.version.load();
        let visible = self.commits.visible.load(Acquire);
        self.get_in(&v, key, &|seq, ticket| self.commits.sees(visible, seq, ticket))
    }

    /// The value of `key` in version `v`, reading the memtables through
    /// `view`. The tree holds only committed batches.
    ///
    /// A get reads the range deletes after the versions it returns. A
    /// batch's range deletes are in their memtable before it commits, so a
    /// get that saw a version from a committed batch then sees that batch's
    /// range deletes too, and never half of a batch that both writes and
    /// range-deletes a key. A get that finds a range delete covering the key
    /// looks for a version newer than the delete, and reads the deletes
    /// again after finding one, since its batch may have committed since.
    fn get_in(&self, v: &Version, key: &[u8], view: View) -> Result<Option<Bytes>> {
        let hash = bloom::hash_key(key);
        let mems = || std::iter::once(&v.active).chain(&v.frozen);
        let covering = || mems().map(|m| m.ranges.covering(key, view)).max().unwrap_or(0);
        let found = mems().find_map(|m| m.get(key, hash, view));
        let mut covered = covering();
        if covered == 0 {
            return match (found, &v.tree) {
                (Some(val), _) => Ok(val.into_put()),
                (None, Some(t)) => match tree::get(&self.io, t, key, hash)? {
                    Some(found) => found.resolve(&self.io),
                    None => Ok(None),
                },
                (None, None) => Ok(None),
            };
        }
        loop {
            // Memtables run newest first, and each holds newer versions than
            // the next.
            let newer = move |seq, ticket| seq > covered && view(seq, ticket);
            let Some(val) = mems().take_while(|m| m.counts.last_seq.load(Relaxed) > covered).find_map(|m| m.get(key, hash, &newer)) else {
                return Ok(None);
            };
            match covering() {
                again if again == covered => return Ok(val.into_put()),
                again => covered = again,
            }
        }
    }

    /// Runs one pipeline stage until shutdown or failure.
    fn stage_loop(self: Arc<Self>, stage: Stage) {
        loop {
            // The committer also wakes to give memtable memory that has
            // gone unused back to the system.
            let trim_in = matches!(stage, Stage::Commit).then(|| self.mem_pool.trim().unwrap_or(crate::arena::TRIM_INTERVAL));
            let mut timed_out = false;
            {
                let mut g = self.signal_lock.lock();
                while !timed_out && !self.stage_ready(stage) && !self.shutdown.load(SeqCst) && !self.poisoned.load(SeqCst) {
                    match trim_in {
                        Some(wait) => timed_out = self.signal.wait_for(&mut g, wait).timed_out(),
                        None => self.signal.wait(&mut g),
                    }
                }
            }
            if self.shutdown.load(SeqCst) || self.poisoned.load(SeqCst) {
                return;
            }
            if timed_out {
                continue;
            }
            let r = match stage {
                Stage::Apply => self.apply_step(),
                Stage::Commit => self.commit_step(),
                // After a failed write, segments grow as they are written.
                Stage::PrepareLog if self.prepare_log() => Ok(true),
                Stage::PrepareLog => return,
            };
            if self.poison_on_err(r).is_err() {
                return;
            }
            self.notify();
        }
    }

    fn stage_ready(&self, stage: Stage) -> bool {
        match stage {
            // Apply ahead of the committer by at most one checkpoint.
            Stage::Apply => {
                let idle = self.pending.lock().is_empty();
                idle && self.next_to_apply().is_some()
            }
            Stage::Commit => !self.pending.lock().is_empty(),
            Stage::PrepareLog => self.recycler.is_empty(),
        }
    }

    /// Writes a log segment of zeros for a coming memtable to take, paced
    /// to stay ahead of the active memtable's filling at twice its pace,
    /// and to finish within [`PREPARE_LOG_SECS`] regardless. Returns
    /// whether it wrote one.
    fn prepare_log(&self) -> bool {
        let len = self.recycler.segment_len(self.opts.log_segment_len());
        let limit = self.opts.memtable_limit() as f64;
        let began = std::time::Instant::now();
        self.recycler.prepare(&*self.storage, len, |written| {
            loop {
                if self.shutdown.load(SeqCst) || self.poisoned.load(SeqCst) {
                    return false;
                }
                let filled = self.version.load().active.bytes() as f64 / limit;
                let share = (began.elapsed().as_secs_f64() / PREPARE_LOG_SECS).max(filled * 2.0);
                if written as f64 <= share * len as f64 {
                    return true;
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    }

    /// The oldest frozen memtable not yet applied.
    fn next_to_apply(&self) -> Option<Arc<Mem>> {
        let last = self.applier.lock().last_applied;
        self.version.load().frozen.iter().rev().find(|m| m.id > last).cloned()
    }

    /// Cuts a frozen memtable into sorted batches of about one leaf each and
    /// hands each to `send`, stopping early if it returns false.
    ///
    /// With the memtable's range deletes, the batches leave out the entries
    /// a newer range delete covers, and carry a tombstone for each key of
    /// `tombstones`, the tree's keys the range deletes left, unless the
    /// memtable has a newer entry for it.
    ///
    /// An entry whose value is at least [`Options::blob_threshold`] bytes
    /// carries a pointer to its blob: the one its writer wrote, or one
    /// written as the entry is cut, for a value that arrived without one,
    /// as when the threshold was higher when its put was logged. Returns
    /// the blobs written here.
    fn build_batches(
        &self,
        mem: &Mem,
        deletes: Option<&FrozenDeletes>,
        tombstones: &[Bytes],
        send: &mut dyn FnMut(Arc<DataPage>) -> bool,
    ) -> Result<Built> {
        let mut b = Batcher::new(self, mem.blobs.lock().iter().copied().collect(), send);
        let Some(deletes) = deletes else {
            mem.table.for_each(&mut |key, value, seq| b.push(key, value, seq));
            return b.finish();
        };
        let mut cover = deletes.cursor();
        let mut t = 0;
        mem.table.for_each(&mut |key, value, seq| {
            while t < tombstones.len() && tombstones[t][..] < *key {
                b.push(&tombstones[t], None, 0);
                t += 1;
            }
            let hidden = cover.newest(key) > seq;
            let in_tree = t < tombstones.len() && tombstones[t][..] == *key;
            t += in_tree as usize;
            match (hidden, in_tree) {
                (false, _) => b.push(key, value, seq),
                (true, true) => b.push(key, None, 0),
                (true, false) => {}
            }
        });
        for key in &tombstones[t..] {
            b.push(key, None, 0);
        }
        b.finish()
    }

    /// Runs `f` on the store's merge pool, if it has one.
    fn in_pool<T: Send>(&self, f: impl FnOnce() -> T + Send) -> T {
        match &self.merge_pool {
            Some(pool) => pool.install(f),
            None => f(),
        }
    }

    fn checkpoint_step(&self) -> Result<bool> {
        let r = if !self.pending.lock().is_empty() { self.commit_step() } else { self.apply_step() };
        let r = self.poison_on_err(r);
        self.notify();
        self.mem_pool.trim();
        r
    }

    /// Applies the oldest unapplied frozen memtable to the newest tree and
    /// writes the resulting pages, leaving the checkpoint for the committer.
    fn apply_step(&self) -> Result<bool> {
        self.check_poisoned()?;
        let mut applier = self.applier.lock();
        let Some(mem) = self.version.load().frozen.iter().rev().find(|m| m.id > applier.last_applied).cloned() else {
            return Ok(false);
        };
        while !mem.inflight.is_zero() {
            std::thread::yield_now();
        }
        let ctx = Ctx { cfg: &self.opts.tree, io: &self.io, next_page_id: &self.next_page_id };
        // Pace the checkpoint of a memtable that filled, if no other frozen
        // memtable waits behind it, expecting as much I/O as the largest of
        // the last few checkpoints, or FIRST_CHECKPOINT_IO times its memtable
        // after open. One a caller froze early runs at full speed, as does
        // one whose writers waited on the checkpoint before it: when writers
        // outrun checkpoints, every checkpoint is late.
        let expected = self.checkpoint_io.lock().iter().copied().max().unwrap_or_else(|| mem.bytes() * FIRST_CHECKPOINT_IO);
        let version = self.version.load();
        if self.opts.pace_checkpoints
            && self.opts.background
            && expected > 0
            && version.frozen.len() == 1
            && !mem.waited.load(Relaxed)
            && let Some(filled) = mem.filled.get()
        {
            let active = version.active.clone();
            let limit = self.opts.memtable_limit() as f64;
            self.io.pacer.start(Pace {
                expected,
                target: filled.duration_since(mem.created).mul_f64(PACE_SHARE),
                progress: Box::new(move || active.bytes() as f64 / limit),
                fill: PACE_SHARE,
            });
        }
        drop(version);
        let io_bytes = || {
            let c = &self.io.counters;
            c.span_read_bytes.load(Relaxed) + c.page_bytes_written.load(Relaxed) + c.checkpoint_blob_bytes.load(Relaxed)
        };
        let io_before = io_bytes();
        // The memtable's range deletes go first: everything in the tree is
        // older than they are.
        let deletes = mem.ranges.load().map(|i| FrozenDeletes::new(i.all().cloned().collect()));
        let (root, tombstones) = match &deletes {
            None => (applier.root.clone(), Vec::new()),
            Some(d) => {
                let t = std::time::Instant::now();
                let (ranges, base) = (d.ranges(), applier.root.clone());
                let deleted = self.in_pool(|| -> Result<_> {
                    let root = tree::delete_ranges(&ctx, base.as_ref(), &ranges)?;
                    let left = tree::live_keys(&self.io, root.as_ref(), &ranges, self.opts.tree.parallel)?;
                    Ok((root, left))
                });
                self.timing.apply_ns.fetch_add(since(t), Relaxed);
                match deleted {
                    Ok(out) => out,
                    Err(e) => {
                        self.io.pacer.stop();
                        return Err(e);
                    }
                }
            }
        };
        let apply = |batches| {
            let root = root.clone();
            if self.opts.background { self.apply_writing(&ctx, root, batches) } else { self.apply_then_write(&ctx, root, batches) }
        };
        let run = |rx| self.in_pool(|| apply(rx));
        let (applied, built) = if self.opts.background {
            // Another thread cuts the batches while this one applies them,
            // one batch ahead, so the checkpoint never holds a copy of the
            // memtable.
            std::thread::scope(|scope| {
                let (tx, rx) = std::sync::mpsc::sync_channel(1);
                let frozen = &*mem;
                let builder = scope.spawn(move || {
                    let t = std::time::Instant::now();
                    let mut blocked = 0;
                    let built = self.build_batches(frozen, deletes.as_ref(), &tombstones, &mut |batch| {
                        let t = std::time::Instant::now();
                        let open = tx.send(batch).is_ok();
                        blocked += since(t);
                        open
                    });
                    self.timing.build_ns.fetch_add(since(t) - blocked, Relaxed);
                    built
                });
                let applied = run(rx);
                (applied, builder.join().expect("batch builder panicked"))
            })
        } else {
            // Cut every batch first, so page ids, and with them simulated
            // runs, stay deterministic.
            let (tx, rx) = std::sync::mpsc::channel();
            let t = std::time::Instant::now();
            let built = self.build_batches(&mem, deletes.as_ref(), &tombstones, &mut |batch| tx.send(batch).is_ok());
            self.timing.build_ns.fetch_add(since(t), Relaxed);
            drop(tx);
            (run(rx), built)
        };
        self.io.pacer.stop();
        let mut blobs = built?;
        let wrote_blobs = !blobs.is_empty();
        let (root, created_files, written) = applied?;
        blobs.extend(mem.blobs.lock().iter().map(|&(_, blob)| blob));
        let mut recent = self.checkpoint_io.lock();
        if recent.len() == 3 {
            recent.pop_front();
        }
        recent.push_back(io_bytes() - io_before);
        drop(recent);
        applier.root = root.clone();
        applier.last_applied = mem.id;
        self.pending.lock().push_back(Applied { mem, root, created_files, written, blobs, wrote_blobs });
        Ok(true)
    }

    /// Applies a checkpoint's batches while writer threads write the pages
    /// each leaves below the root, so the checkpoint's writes overlap its
    /// merges and the pacer can spread them over the whole checkpoint. The
    /// newest quarter of the cache in queued pages waits, so a page a later
    /// batch supersedes goes unwritten. Queued pages stay within half the
    /// cache: past that, applying waits.
    fn apply_writing(&self, ctx: &Ctx, mut root: Option<Subtree>, batches: Receiver<Arc<DataPage>>) -> Result<Checkpointed> {
        std::thread::scope(|scope| {
            let mut writer = tree::PageWriter::spawn(scope, ctx.io, self.opts.cache_bytes / 4, &self.timing.write_ns);
            let mut applied = Ok(());
            for batch in batches {
                let t = std::time::Instant::now();
                match tree::apply_batch(ctx, root.as_ref(), batch) {
                    Ok(r) => root = r,
                    Err(e) => {
                        applied = Err(e);
                        break;
                    }
                }
                self.timing.apply_ns.fetch_add(since(t), Relaxed);
                if let Some(r) = &root {
                    writer.queue(tree::unwritten_below_root(r));
                }
                if !writer.wait_below(self.opts.cache_bytes / 2) {
                    break;
                }
            }
            if applied.is_ok()
                && let Some(r) = &root
            {
                writer.queue(tree::unwritten(r));
            }
            let written = writer.finish();
            applied?;
            let (mut created_files, written) = written?;
            if let Some(r) = &root {
                created_files |= tree::write_nodes(ctx, r)?;
            }
            Ok((root, created_files, written))
        })
    }

    /// Applies a checkpoint's batches and writes its pages on this thread,
    /// in an order that depends only on the batches.
    fn apply_then_write(&self, ctx: &Ctx, mut root: Option<Subtree>, batches: Receiver<Arc<DataPage>>) -> Result<Checkpointed> {
        let mut created_files = false;
        let mut early = Vec::new();
        for batch in batches {
            let t = std::time::Instant::now();
            root = tree::apply_batch(ctx, root.as_ref(), batch)?;
            self.timing.apply_ns.fetch_add(since(t), Relaxed);
            // Pages a checkpoint has yet to write count against half the
            // cache: past that, write the new pages below the root now
            // rather than hold them all until the checkpoint ends. A later
            // batch may still supersede one, wasting its write, so a large
            // cache keeps them all in memory.
            if let Some(r) = &root {
                let pages = tree::unwritten_below_root(r);
                if pages.iter().map(|p| p.meta.image_len()).sum::<u64>() > self.opts.cache_bytes / 2 {
                    let t = std::time::Instant::now();
                    created_files |= tree::write_pages(ctx, &pages)?;
                    early.extend(pages.iter().filter_map(|p| p.ptr()));
                    self.timing.write_ns.fetch_add(since(t), Relaxed);
                }
            }
        }
        let t = std::time::Instant::now();
        if let Some(r) = &root {
            created_files |= tree::write_tree(ctx, r)?;
        }
        self.timing.write_ns.fetch_add(since(t), Relaxed);
        Ok((root, created_files, early))
    }

    /// Makes the oldest applied checkpoint durable, commits it, and swaps it
    /// into the read path in place of its memtable.
    fn commit_step(&self) -> Result<bool> {
        self.check_poisoned()?;
        let mut durable = self.durable.lock();
        let Some(Applied { mem, root, created_files, written, blobs, wrote_blobs }) = self.pending.lock().pop_front() else {
            return Ok(false);
        };
        let t = std::time::Instant::now();
        // The memtable's writers wrote its blobs before they logged, and a
        // segment's sync makes durable the blobs its records point to. So
        // settling the newest segment, which syncs every segment before it
        // that has yet to sync, the memtable's among them, leaves those
        // blobs durable. It also leaves the memtable's segment with nothing
        // more to write, and its file is reused once it retires. Only blobs
        // the checkpoint wrote itself are left to sync, and of the page
        // arenas, those it wrote to: each sync has the drive flush its
        // cache, which holds up the writes then in flight.
        let newest = self.newest_log.lock().clone();
        newest.settle()?;
        self.io.store.sync_new()?;
        if wrote_blobs {
            self.io.blobs.sync_new()?;
        }
        if created_files {
            self.storage.sync_dir()?;
        }
        self.timing.sync_ns.fetch_add(since(t), Relaxed);
        let t = std::time::Instant::now();
        let sb = Superblock {
            checkpoint_id: durable.sb.checkpoint_id + 1,
            last_seq: mem.counts.last_seq.load(SeqCst),
            last_memtable_id: mem.id,
            next_page_id: self.next_page_id.load(SeqCst),
            root: root.as_ref().and_then(tree::root_ptr),
            range_deletes: durable.sb.range_deletes,
            blobs: durable.sb.blobs,
            next_blob_id: durable.sb.next_blob_id,
        };
        sb.store(&*self.storage)?;
        if std::env::var_os("TURTLEKV_DEBUG").is_some() {
            eprintln!("commit {sb:?}");
        }

        let mut live = HashSet::new();
        let mut blob_pages = Vec::new();
        if let Some(r) = &root {
            tree::reachable(r, &mut live, &mut blob_pages);
        }
        let mut garbage: Vec<PagePtr> = durable.live.difference(&live).copied().collect();
        // The new tree's pages count their blobs before the old tree's
        // pages stop counting theirs, so a blob whose entry moved from one
        // page to another stays counted throughout. A blob no page lists
        // after that is garbage, as is one written for the memtable whose
        // put never reached the tree.
        blob_pages.into_iter().for_each(|p| durable.blobs.add(p));
        let mut dead = Vec::new();
        for ptr in &garbage {
            durable.blobs.remove(ptr, &mut dead);
        }
        dead.extend(blobs.iter().filter(|b| !durable.blobs.counts.contains_key(b)).map(BlobPtr::page_ptr));
        self.io.counters.blobs_live.store(durable.blobs.counts.len() as u64, Relaxed);
        self.io.counters.blob_bytes_live.store(durable.blobs.bytes, Relaxed);
        garbage.extend(written.into_iter().filter(|p| !live.contains(p)));
        let garbage = Garbage { pages: garbage, blobs: dead };
        let epoch = Arc::new(Epoch::new(self.io.clone()));
        let old_epoch = std::mem::replace(&mut durable.epoch, epoch.clone());
        durable.live = live;
        durable.sb = sb;
        {
            let _g = self.version_lock.lock();
            let cur = self.version.load();
            let frozen = cur.frozen.iter().filter(|m| m.id != mem.id).cloned().collect();
            old_epoch.retire(garbage, epoch.clone());
            self.version.store(Arc::new(Version {
                active: cur.active.clone(),
                frozen,
                tree: root,
                _epoch: epoch,
                _live: LiveVersion::new(&self.live_versions),
            }));
        }
        drop(old_epoch);
        // Reclaiming returns freed slots to the free pool and takes slots
        // left free out of it while it punches their holes; holding `durable`
        // meanwhile keeps a leak check from finding a slot in neither.
        self.io.store.reclaim();
        self.io.blobs.reclaim();
        drop(durable);
        self.timing.commit_ns.fetch_add(since(t), Relaxed);
        self.checkpoints.fetch_add(1, Relaxed);
        for id in wal::list_segments(&*self.storage)? {
            if id <= mem.id {
                self.recycler.retire(&*self.storage, id)?;
            }
        }
        Ok(true)
    }
}

/// Cuts sorted entries into batches of about one leaf each, giving each
/// large value's entry a pointer to its blob; see [`Inner::build_batches`].
struct Batcher<'a> {
    inner: &'a Inner,
    send: &'a mut dyn FnMut(Arc<DataPage>) -> bool,
    b: PageBuilder,
    open: bool,
    limit: u64,
    bits: u32,
    /// The blobs the memtable's writers wrote, by sequence number.
    logged: HashMap<u64, BlobPtr>,
    /// Blobs written here so far.
    written: Vec<BlobPtr>,
    /// The failed blob write that closed the batcher.
    failed: Option<Error>,
}

impl<'a> Batcher<'a> {
    fn new(inner: &'a Inner, logged: HashMap<u64, BlobPtr>, send: &'a mut dyn FnMut(Arc<DataPage>) -> bool) -> Self {
        let (limit, bits) = (inner.opts.tree.leaf_size, inner.opts.tree.filter_bits);
        Self { b: Self::builder(inner, limit), inner, send, open: true, limit, bits, logged, written: Vec::new(), failed: None }
    }

    fn builder(inner: &Inner, limit: u64) -> PageBuilder {
        inner.io.builder(inner.next_page_id.fetch_add(1, Relaxed), (limit + limit / 16) as usize + 4096)
    }

    /// Adds `key` with `value`, written at `seq`, or a tombstone for `None`.
    #[inline]
    fn push(&mut self, key: &[u8], value: Option<&[u8]>, seq: u64) {
        if !self.open {
            return;
        }
        match value {
            Some(v) if v.len() >= self.inner.opts.blob_threshold => self.push_blob(key, v, seq),
            _ => self.push_entry(EntryRef::new(key, value)),
        }
    }

    /// Adds `key` with a pointer to the blob that holds `value`: the one
    /// the put at `seq` wrote, or else one written now.
    #[cold]
    fn push_blob(&mut self, key: &[u8], value: &[u8], seq: u64) {
        let ptr = match self.logged.get(&seq) {
            Some(ptr) => *ptr,
            None => match self.inner.write_blob(value, true) {
                Ok(ptr) => {
                    self.written.push(ptr);
                    ptr
                }
                Err(e) => {
                    self.failed = Some(e);
                    self.open = false;
                    return;
                }
            },
        };
        self.push_entry(EntryRef { key, value: Some(&ptr.encode()), blob: true });
    }

    fn push_entry(&mut self, e: EntryRef<'_>) {
        if !self.b.is_empty() && self.b.data_bytes() + e.size() > self.limit {
            let full = std::mem::replace(&mut self.b, Self::builder(self.inner, self.limit));
            self.open = (self.send)(Arc::new(DataPage::unwritten(full.finish(self.bits))));
        }
        self.b.push(e);
    }

    fn finish(self) -> Result<Built> {
        if let Some(e) = self.failed {
            return Err(e);
        }
        if self.open && !self.b.is_empty() {
            (self.send)(Arc::new(DataPage::unwritten(self.b.finish(self.bits))));
        }
        Ok(self.written)
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Apply,
    Commit,
    /// Keeps a log segment ready; see [`Options::preallocate_log`].
    PrepareLog,
}

/// A consistent view of the store: every batch committed when it was
/// taken, and none after. Reads through one snapshot agree with each other
/// however long they run. Holding one keeps its memtables in memory and its
/// tree's pages on disk. Drop snapshots and iterators before reopening the
/// store, which may reuse the pages they read.
#[derive(Clone)]
pub struct Snapshot {
    inner: Arc<Inner>,
    version: Arc<Version>,
    seq: u64,
}

impl Snapshot {
    fn new(inner: Arc<Inner>) -> Self {
        let version = inner.version.load_full();
        // Taken after loading the version, so it covers every batch the
        // version's tree holds.
        let seq = inner.commits.prefix();
        Self { inner, version, seq }
    }

    /// The sequence number of the last write the snapshot holds.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// The range deletes in the snapshot's memtables, if any.
    fn overlay(&self) -> Option<Overlay> {
        let v = &self.version;
        Overlay::new(std::iter::once(&v.active).chain(&v.frozen).filter_map(|m| m.ranges.load()).collect(), self.seq)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let seq = self.seq;
        self.inner.get_in(&self.version, key, &|s, _| s <= seq)
    }

    /// Up to `limit` live entries with keys in `[start, end)`, in order.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        collect_scan(Forward::new(self, start, end), limit)
    }

    /// An iterator over live entries with keys in `[start, end)`, in key
    /// order.
    pub fn iter(&self, start: &[u8], end: Option<&[u8]>) -> DbIter {
        DbIter(Walk::Forward(Forward::new(self, start, end)))
    }

    /// [`Snapshot::iter`] in descending key order.
    pub fn iter_rev(&self, start: &[u8], end: Option<&[u8]>) -> DbIter {
        DbIter(Walk::Reverse(Reverse::new(self, start, end)))
    }

    /// An iterator over live entries whose keys start with `prefix`.
    pub fn prefix(&self, prefix: &[u8]) -> DbIter {
        self.iter(prefix, prefix_end(prefix).as_deref())
    }

    /// [`Snapshot::prefix`] in descending key order.
    pub fn prefix_rev(&self, prefix: &[u8]) -> DbIter {
        self.iter_rev(prefix, prefix_end(prefix).as_deref())
    }
}

/// Up to `limit` entries from `it`.
///
/// Small entries are copied into one buffer per call. Sharing them would
/// make every result touch the reference count of a cached block, and
/// threads scanning the same hot blocks contend on those counts.
///
/// Values that live in blobs are read once the scan has found its entries,
/// each read started a few blobs ahead of the one being returned, so their
/// device reads overlap.
fn collect_scan(mut it: Forward, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
    it.tree.expect(limit);
    enum Part {
        Copied(usize, usize, usize),
        Shared(Bytes, Bytes),
        Blob(Bytes, BlobPtr),
    }
    const COPY_LIMIT: usize = 512;
    let mut buf = Vec::with_capacity(4096);
    let mut parts = Vec::with_capacity(limit.min(1024));
    while parts.len() < limit {
        let part = it.step(|e| {
            let (k, v) = (e.key(), e.value());
            if e.is_blob() || k.len() + v.len() > COPY_LIMIT {
                return e.to_found().map(|(k, found)| match found {
                    Found::Blob(ptr) => Part::Blob(k, ptr),
                    Found::Value(v) => Part::Shared(k, v),
                    Found::Delete => unreachable!("a scan steps over tombstones"),
                });
            }
            let at = buf.len();
            buf.extend_from_slice(k);
            buf.extend_from_slice(v);
            Ok(Part::Copied(at, k.len(), v.len()))
        })?;
        match part {
            Some(p) => parts.push(p?),
            None => break,
        }
    }
    let io = &it.snap.inner.io;
    let blobs: Vec<BlobPtr> = parts.iter().filter_map(|p| if let Part::Blob(_, ptr) = p { Some(*ptr) } else { None }).collect();
    let mut ahead = BlobReadAhead::new(io, &blobs);
    let buf = Bytes::from(buf);
    parts
        .into_iter()
        .map(|p| match p {
            Part::Copied(at, kl, vl) => Ok((buf.slice(at..at + kl), buf.slice(at + kl..at + kl + vl))),
            Part::Shared(k, v) => Ok((k, v)),
            Part::Blob(k, _) => Ok((k, ahead.next()?)),
        })
        .collect()
}

/// Reads a run of blobs in order, keeping the reads of the next few started
/// ahead of the one being returned.
struct BlobReadAhead<'a> {
    io: &'a PageIo,
    blobs: &'a [BlobPtr],
    /// The next blob to return, and the first not yet started.
    at: usize,
    started: usize,
    /// Stored bytes of the blobs started and not yet returned.
    in_flight: u64,
}

/// Stored bytes of blobs a scan keeps reading ahead of the one it returns.
const BLOB_READ_AHEAD: u64 = 4 << 20;

impl<'a> BlobReadAhead<'a> {
    fn new(io: &'a PageIo, blobs: &'a [BlobPtr]) -> Self {
        Self { io, blobs, at: 0, started: 0, in_flight: 0 }
    }

    /// The value of the next blob.
    fn next(&mut self) -> Result<Bytes> {
        // The blob being returned is read by this thread; the ones after it
        // start now.
        self.started = self.started.max(self.at + 1);
        while self.started < self.blobs.len() && self.in_flight < BLOB_READ_AHEAD {
            self.io.prefetch_blob(&self.blobs[self.started]);
            self.in_flight += self.blobs[self.started].len as u64;
            self.started += 1;
        }
        let ptr = &self.blobs[self.at];
        self.at += 1;
        if self.at < self.started {
            self.in_flight = self.in_flight.saturating_sub(self.blobs[self.at].len as u64);
        }
        self.io.read_blob(ptr)
    }
}

/// An iterator over one snapshot's live entries, in either key order.
pub struct DbIter(Walk);

enum Walk {
    Forward(Forward),
    Reverse(Reverse),
}

impl DbIter {
    /// The next entry, or `None` past the end. A read can fail, so this
    /// returns a `Result` and `DbIter` has no `Iterator` impl.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        match &mut self.0 {
            Walk::Forward(f) => f.next(),
            Walk::Reverse(r) => r.next(),
        }
    }
}

/// Reads `[start, hi)` backward one window at a time. Each window goes
/// through the forward merge and comes back reversed, so both directions
/// share one merge. Windows start at one leaf block and double, so short
/// reverse scans read little and long ones amortize each window's seeks.
struct Reverse {
    snap: Snapshot,
    /// The range deletes the snapshot sees, if any.
    overlay: Option<Overlay>,
    start: Bytes,
    /// Exclusive upper bound of what remains; `None` for the end of the key
    /// space.
    hi: Option<Bytes>,
    /// The current window's entries in key order, taken from the back. A
    /// value that lives in a blob is read as its entry is returned.
    window: Vec<(Bytes, Found)>,
    /// Leaf blocks the next window spans.
    blocks: usize,
    done: bool,
}

/// Entries of each memtable a reverse window holds at most, per leaf block
/// it spans.
const MEM_WINDOW: usize = 64;
const MAX_WINDOW_BLOCKS: usize = 32;

impl Reverse {
    fn new(snap: &Snapshot, start: &[u8], end: Option<&[u8]>) -> Self {
        Self {
            snap: snap.clone(),
            overlay: snap.overlay(),
            start: Bytes::copy_from_slice(start),
            hi: end.map(Bytes::copy_from_slice),
            window: Vec::new(),
            blocks: 1,
            done: false,
        }
    }

    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if let Some((key, found)) = self.window.pop() {
                return Ok(Some((key, live_value(found, &self.snap.inner.io)?)));
            }
            if self.done || self.hi.as_deref().is_some_and(|h| h <= &self.start[..]) {
                self.done = true;
                return Ok(None);
            }
            let lo = self.window_start();
            let mut forward = Forward::new(&self.snap, &lo, self.hi.as_deref());
            while let Some(e) = forward.next_found()? {
                self.window.push(e);
            }
            self.done = lo == self.start;
            self.hi = Some(lo);
            self.blocks = (self.blocks * 2).min(MAX_WINDOW_BLOCKS);
        }
    }

    /// Where the next window begins: at or after `start`, below `hi`, and
    /// late enough that the window spans about `blocks` leaf blocks of the
    /// tree and at most `blocks` × [`MEM_WINDOW`] entries of each memtable.
    fn window_start(&self) -> Bytes {
        let hi = self.hi.as_deref();
        let mut lo = self.start.clone();
        if let Some(t) = &self.snap.version.tree {
            // The tree holds nothing the snapshot sees in keys a range
            // delete covers, so its share of the window starts below them.
            let covered = self.overlay.as_ref().and_then(|o| o.covered_below(hi));
            lo = lo.max(tree::window_start(t, covered.as_deref().or(hi), self.blocks));
        }
        // Each memtable is read only within the window so far, which narrows
        // as it goes.
        let seq = self.snap.seq;
        let limit = MEM_WINDOW * self.blocks;
        let mut out = Vec::new();
        for m in std::iter::once(&self.snap.version.active).chain(&self.snap.version.frozen) {
            out.clear();
            m.table.range_rev(&lo, hi, &|s, _| s <= seq, limit, &mut out);
            if let Some((k, _)) = out.get(limit - 1) {
                lo = k.clone();
            }
        }
        lo
    }
}

/// A merged iterator over the memtables and the tree of one snapshot.
///
/// With range deletes in the snapshot's memtables, the merge walks the key
/// space in spans over which the newest delete the snapshot sees is the
/// same. In a span under a delete, each source shows only versions newer
/// than it: the tree skips to the span's end, as does each memtable older
/// than the delete, and newer memtables hide the versions it covers.
struct Forward {
    snap: Snapshot,
    /// Newest first; the tree comes last.
    mems: Vec<MemSource>,
    tree: TreeScan,
    end: Option<Bytes>,
    overlay: Option<Overlay>,
    /// The newest delete covering the current span (0 for none), and the
    /// span's exclusive end (`None` for the end of the key space).
    span_seq: u64,
    span_end: Option<Bytes>,
    /// Whether the tree has yet to skip past the current span.
    skip_tree: bool,
}

struct MemSource {
    mem: Arc<Mem>,
    snapshot: u64,
    /// Versions at or below this are hidden by a range delete.
    hidden_below: u64,
    buf: VecDeque<(Bytes, Value)>,
    start: Bound<Bytes>,
    /// The end of what the next refills read: the scan's end, or the
    /// current span's.
    end: Option<Bytes>,
    /// Entries the next refill copies. It starts small and doubles, since
    /// memtables hold a small share of the data and most scans are short.
    chunk: usize,
    /// Whether the memtable holds nothing more to read before `end`.
    exhausted: bool,
}

const MEM_CHUNK_MIN: usize = 8;
const MEM_CHUNK_MAX: usize = 512;

impl MemSource {
    /// Refills the buffer if it is empty and the memtable may hold more.
    fn fill(&mut self) {
        if !self.buf.is_empty() || self.exhausted {
            return;
        }
        let mut out = Vec::with_capacity(self.chunk);
        let start = match &self.start {
            Bound::Included(k) => Bound::Included(&k[..]),
            Bound::Excluded(k) => Bound::Excluded(&k[..]),
            Bound::Unbounded => Bound::Unbounded,
        };
        let (snapshot, hidden) = (self.snapshot, self.hidden_below);
        self.mem.table.range(start, self.end.as_deref(), &|seq, _| seq <= snapshot && seq > hidden, self.chunk, &mut out);
        self.exhausted = out.len() < self.chunk;
        self.chunk = (self.chunk * 2).min(MEM_CHUNK_MAX);
        if let Some((k, _)) = out.last() {
            self.start = Bound::Excluded(k.clone());
        }
        self.buf.extend(out);
    }

    fn key(&self) -> Option<&[u8]> {
        self.buf.front().map(|(k, _)| &k[..])
    }
}

/// The value of a live entry, read from its blob if it lives in one.
fn live_value(found: Found, io: &PageIo) -> Result<Bytes> {
    Ok(found.resolve(io)?.expect("a live entry holds a value"))
}

/// A live entry borrowed from the source that holds it.
enum EntryView<'a> {
    Mem(&'a Bytes, &'a Bytes),
    Tree(&'a TreeScan),
}

impl EntryView<'_> {
    fn key(&self) -> &[u8] {
        match self {
            EntryView::Mem(k, _) => k,
            EntryView::Tree(t) => t.peek().expect("settled").0,
        }
    }

    /// The value, or the encoded pointer of a value that lives in a blob.
    fn value(&self) -> &[u8] {
        match self {
            EntryView::Mem(_, v) => v,
            EntryView::Tree(t) => t.peek().expect("settled").1,
        }
    }

    /// Whether the value lives in a blob.
    fn is_blob(&self) -> bool {
        matches!(self, EntryView::Tree(t) if t.is_blob())
    }

    /// The key and what its source holds for it, as handles sharing the
    /// source's buffers.
    fn to_found(&self) -> Result<(Bytes, Found)> {
        match self {
            EntryView::Mem(k, v) => Ok(((*k).clone(), Found::Value((*v).clone()))),
            EntryView::Tree(t) => Ok(t.peek_found()?.expect("settled")),
        }
    }
}

impl Forward {
    fn new(snap: &Snapshot, start: &[u8], end: Option<&[u8]>) -> Self {
        let (version, snapshot) = (&snap.version, snap.seq);
        let end_b = end.map(Bytes::copy_from_slice);
        let start_b = Bytes::copy_from_slice(start);
        let mems = std::iter::once(&version.active)
            .chain(&version.frozen)
            .map(|m| MemSource {
                mem: m.clone(),
                snapshot,
                hidden_below: 0,
                buf: VecDeque::new(),
                start: Bound::Included(start_b.clone()),
                end: end_b.clone(),
                chunk: MEM_CHUNK_MIN,
                exhausted: false,
            })
            .collect();
        let tree = TreeScan::new(version.tree.clone(), start, end_b.clone());
        let mut f =
            Self { snap: snap.clone(), mems, tree, end: end_b, overlay: snap.overlay(), span_seq: 0, span_end: None, skip_tree: false };
        if let Some(overlay) = &f.overlay {
            (f.span_seq, f.span_end) = enter_span(overlay, &mut f.mems, f.end.as_ref(), start_b);
            f.skip_tree = f.span_seq > 0;
        }
        f
    }

    /// Whether `key` lies before the end of the current span.
    fn in_span(&self, key: &[u8]) -> bool {
        self.span_end.as_ref().is_none_or(|e| key < &e[..])
    }

    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        match self.next_found()? {
            Some((key, found)) => Ok(Some((key, live_value(found, &self.snap.inner.io)?))),
            None => Ok(None),
        }
    }

    /// The next live entry, with a value that lives in a blob left unread.
    fn next_found(&mut self) -> Result<Option<(Bytes, Found)>> {
        self.step(|e| e.to_found())?.transpose()
    }

    /// Finds the next live entry, passes it to `f`, and moves past it.
    fn step<T>(&mut self, f: impl FnOnce(EntryView<'_>) -> T) -> Result<Option<T>> {
        let io = &self.snap.inner.io;
        loop {
            if self.skip_tree {
                self.skip_tree = false;
                self.tree.skip_to(io, self.span_end.as_deref())?;
            }
            self.tree.settle(io)?;
            for m in &mut self.mems {
                m.fill();
            }
            // The smallest key wins; on a tie the newest source holds it:
            // memtables newest first, then the tree.
            let n = self.mems.len();
            let mut best: Option<(usize, &[u8])> = None;
            for (i, m) in self.mems.iter().enumerate() {
                if let Some(k) = m.key()
                    && best.is_none_or(|(_, b)| cmp_keys(k, b).is_lt())
                {
                    best = Some((i, k));
                }
            }
            if let Some((k, _)) = self.tree.peek()
                && best.is_none_or(|(_, b)| cmp_keys(k, b).is_lt())
                && self.span_seq == 0
                && self.in_span(k)
            {
                best = Some((n, k));
            }
            let Some((w, _)) = best else {
                // The span is done; the next one starts at its end.
                match (&self.overlay, self.span_end.clone()) {
                    (Some(overlay), Some(next)) if self.end.as_ref().is_none_or(|e| next < *e) => {
                        (self.span_seq, self.span_end) = enter_span(overlay, &mut self.mems, self.end.as_ref(), next);
                        self.skip_tree = self.span_seq > 0;
                        continue;
                    }
                    _ => return Ok(None),
                }
            };
            if w == n {
                let out = f(EntryView::Tree(&self.tree));
                self.tree.advance(io)?;
                return Ok(Some(out));
            }
            if let Some((k, Value::Put(v))) = self.mems[w].buf.front() {
                let out = f(EntryView::Mem(k, v));
                consume_mem(&mut self.mems, &mut self.tree, io, w)?;
                return Ok(Some(out));
            }
            consume_mem(&mut self.mems, &mut self.tree, io, w)?;
        }
    }
}

/// Starts the memtables on the span of `overlay` that begins at `at`, within
/// the scan's `end`. Returns the newest delete covering the span and the
/// span's end.
fn enter_span(overlay: &Overlay, mems: &mut [MemSource], end: Option<&Bytes>, at: Bytes) -> (u64, Option<Bytes>) {
    let (seq, until) = overlay.span(&at);
    let fill_end = match (end, &until) {
        (None, u) => u.clone(),
        (e, None) => e.cloned(),
        (Some(e), Some(u)) => Some(if e <= u { e.clone() } else { u.clone() }),
    };
    for m in mems {
        debug_assert!(m.buf.is_empty());
        m.start = Bound::Included(at.clone());
        m.end = fill_end.clone();
        m.hidden_below = seq;
        // A memtable whose versions all predate the delete shows none.
        m.exhausted = m.mem.counts.last_seq.load(Relaxed) <= seq;
    }
    (seq, until)
}

/// Moves past memtable `w`'s front entry and the older entries for the same
/// key that it shadows.
fn consume_mem(mems: &mut [MemSource], tree: &mut TreeScan, io: &PageIo, w: usize) -> Result<()> {
    for j in w + 1..mems.len() {
        let (l, r) = mems.split_at_mut(j);
        if r[0].key() == l[w].key() {
            r[0].buf.pop_front();
        }
    }
    if tree.peek().is_some_and(|(k, _)| Some(k) == mems[w].key()) {
        tree.advance(io)?;
    }
    mems[w].buf.pop_front();
    Ok(())
}
