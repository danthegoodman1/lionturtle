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

use crate::bloom::{self, SharedFilter};
use crate::error::{Error, Result};
use crate::memtable::{MemTable, MemTableKind, View, new_memtable};
use crate::page::{EntryRef, PageBuilder, entry_size};
use crate::pagestore::{PagePtr, PageStore, Superblock};
use crate::storage::Storage;
use crate::tree::io::{DataPage, Pace, PageIo};
use crate::tree::{self, Ctx, Subtree, TreeConfig, TreeScan};
use crate::types::{MAX_KEY_LEN, MAX_VALUE_LEN, Value, WriteOp, cmp_keys, prefix_end};
use crate::wal::{self, WalWriter};
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering::*};
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
    pub max_frozen: usize,
    /// Checkpoint on a background thread. Without it, callers drive
    /// checkpoints through [`Db::checkpoint_step`], and a writer that finds no
    /// room for another frozen memtable checkpoints inline.
    pub background: bool,
    /// Threads in the store's own pool for parallel merges and page writes;
    /// 0 runs them on the caller's rayon pool. Merges wait on I/O more than
    /// CPU, so a few threads keep up, and each extra thread costs memory: an
    /// allocator keeps what other threads free from a worker's buffers
    /// resident in that worker's heap until the worker allocates again.
    pub merge_threads: usize,
    /// Spread each background checkpoint's I/O over most of the time the
    /// next memtable takes to fill, rather than run it at full speed, so
    /// client reads queue behind less of it. See [`PACE_SHARE`].
    pub pace_checkpoints: bool,
}

/// A paced checkpoint aims to finish its I/O within this share of the time
/// its memtable took to fill, or by the time the next memtable is this full.
pub const PACE_SHARE: f64 = 0.75;

impl Default for Options {
    fn default() -> Self {
        Self {
            tree: TreeConfig::default(),
            checkpoint_distance: 32,
            cache_bytes: 1 << 30,
            memtable: MemTableKind::BBTree,
            wal_buffer_bytes: 1 << 20,
            max_frozen: 1,
            background: true,
            merge_threads: 4,
            pace_checkpoints: true,
        }
    }
}

impl Options {
    fn memtable_limit(&self) -> u64 {
        self.checkpoint_distance.max(1) * self.tree.leaf_size
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
    counts: MemCounts,
    /// Writers assigned to this memtable that have not finished inserting.
    inflight: Inflight,
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
    fn new(id: u64, opts: &Options, last_seq: u64) -> Self {
        Self {
            id,
            created: std::time::Instant::now(),
            filled: std::sync::OnceLock::new(),
            waited: AtomicBool::new(false),
            table: new_memtable(opts.memtable),
            filter: SharedFilter::new((opts.memtable_limit() / MEM_BYTES_PER_FILTER_BIT / 8) as usize),
            counts: MemCounts { bytes: AtomicU64::new(0), last_seq: AtomicU64::new(last_seq) },
            inflight: Inflight::default(),
        }
    }

    fn replay(&self, op: &WriteOp, seq: u64) {
        self.insert(&op.key, op.value.clone(), seq, 0);
        self.counts.bytes.fetch_add(entry_size(op.key.len(), op.value.encoded_len()), Relaxed);
    }

    /// Adds the version of `key` written at `seq` by batch `ticket`. The
    /// filter takes the key first, so a get that can see the version finds
    /// the key in the filter.
    fn insert(&self, key: &[u8], value: Value, seq: u64, ticket: u64) {
        self.filter.insert(bloom::hash_key(key));
        self.table.insert(key, value, seq, ticket);
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

/// Holds the pages that became garbage when the checkpoint after this epoch
/// committed. They return to the allocator when this epoch drops, which
/// happens only after every older epoch has dropped, because each epoch
/// keeps the next one alive.
struct Epoch {
    io: Arc<PageIo>,
    garbage: Mutex<Vec<PagePtr>>,
    next: Mutex<Option<Arc<Epoch>>>,
}

impl Epoch {
    fn new(io: Arc<PageIo>) -> Self {
        Self { io, garbage: Mutex::new(Vec::new()), next: Mutex::new(None) }
    }

    fn retire(&self, garbage: Vec<PagePtr>, next: Arc<Epoch>) {
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
            for ptr in &garbage {
                self.io.evict(ptr.id);
            }
            self.io.store.free(garbage);
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
/// the writer lock itself. A request queued while a caller other than a
/// writer holds the lock waits at most this long.
const REQUEST_PARK: std::time::Duration = std::time::Duration::from_micros(200);

struct Durable {
    sb: Superblock,
    live: HashSet<PagePtr>,
    epoch: Arc<Epoch>,
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
    /// Pages written before the checkpoint finished applying. Any that a
    /// later batch superseded are garbage no committed tree references.
    early: Vec<PagePtr>,
}

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
    pub tree: tree::TreeStats,
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
    /// Serializes version replacements.
    version_lock: Mutex<()>,
    writer: Mutex<Writer>,
    /// Requests waiting for the writer lock, newest first.
    queue: AtomicPtr<Request>,
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
}

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
        // Load the tree through a provisional store, then reopen the arenas
        // knowing which slots the tree uses.
        let probe = PageIo::new(PageStore::open(storage.clone(), &[])?, 0, max_image);
        let root = sb.root.map(|r| tree::load_tree(&probe, r)).transpose()?;
        let mut live = HashSet::new();
        if let Some(r) = &root {
            tree::reachable(r, &mut live);
        }
        let live_vec: Vec<PagePtr> = live.iter().copied().collect();
        let io = Arc::new(PageIo::new(PageStore::open(storage.clone(), &live_vec)?, opts.cache_bytes, max_image));
        io.store.punch_free();

        // Replay the log into one memtable.
        let mut last_seq = sb.last_seq;
        let mut max_id = sb.last_memtable_id;
        let replayed = Mem::new(0, &opts, last_seq);
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
            let r = wal::replay(&*storage, id, last_seq, |seq, op| replayed.replay(&op, seq))?;
            // Replay may have read records that only reached the OS cache
            // (after a process crash). New writes build on them, so they must
            // be durable before the store accepts any.
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
        let mut frozen = Vec::new();
        if replayed.table.len() > 0 {
            replayed.counts.last_seq.store(last_seq, Relaxed);
            let mem = Mem { id: max_id, ..replayed };
            frozen.push(Arc::new(mem));
        }
        let active_id = max_id + 1;
        let recycler = wal::Recycler::open(&*storage, active_id)?;
        let wal = WalWriter::create(&*storage, active_id, recycler.take())?;
        let active = Arc::new(Mem::new(active_id, &opts, last_seq));
        let epoch = Arc::new(Epoch::new(io.clone()));
        let applier = Applier { root: root.clone(), last_applied: sb.last_memtable_id };
        let version = Version { active: active.clone(), frozen, tree: root, _epoch: epoch.clone() };
        let merge_pool = (opts.tree.parallel && opts.merge_threads > 0).then(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(opts.merge_threads)
                .thread_name(|i| format!("turtlekv-merge-{i}"))
                .build()
                .expect("build merge pool")
        });
        let inner = Arc::new(Inner {
            storage,
            next_page_id: AtomicU64::new(sb.next_page_id),
            io,
            version: ArcSwap::from_pointee(version),
            version_lock: Mutex::new(()),
            writer: Mutex::new(Writer { wal, flush_claimed: 0, carried: VecDeque::new(), next_seq: last_seq + 1, next_ticket: 1, active, user_bytes: 0, wal_bytes: 0 }),
            queue: AtomicPtr::new(std::ptr::null_mut()),
            signal_lock: Mutex::new(()),
            signal: Condvar::new(),
            durable: Mutex::new(Durable { sb, live, epoch }),
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
            opts,
        });
        // Two pipeline stages, as in the paper: one thread applies the next
        // frozen memtable while the other syncs and commits the last one.
        let mut workers = Vec::new();
        if inner.opts.background {
            for (name, stage) in [("turtlekv-apply", Stage::Apply), ("turtlekv-commit", Stage::Commit)] {
                let inner = inner.clone();
                workers.push(
                    std::thread::Builder::new()
                        .name(name.into())
                        .spawn(move || inner.stage_loop(stage))
                        .expect("spawn checkpoint thread"),
                );
            }
        }
        Ok(Self { inner, workers })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.write(vec![WriteOp::put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))], false)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.write(vec![WriteOp::delete(Bytes::copy_from_slice(key))], false)
    }

    /// Applies `ops` atomically. With `sync`, returns once they are durable.
    pub fn write(&self, ops: &[WriteOp], sync: bool) -> Result<()> {
        self.inner.write(ops.to_vec(), sync)
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
            let w = self.inner.writer.lock();
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
            let mut w = self.inner.writer.lock();
            if w.active.table.len() > 0 {
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
        let mut w = self.inner.writer.lock();
        if w.active.table.len() > 0 {
            let r = self.inner.rotate(&mut w);
            self.inner.poison_on_err(r)?;
        }
        Ok(())
    }

    /// Sequence number of the newest write, durable or not.
    pub fn last_seq(&self) -> u64 {
        self.inner.writer.lock().next_seq - 1
    }

    pub fn is_poisoned(&self) -> bool {
        self.inner.poisoned.load(SeqCst)
    }

    pub fn stats(&self) -> DbStats {
        let inner = &self.inner;
        let (user_bytes, wal_bytes) = {
            let w = inner.writer.lock();
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
            tree: v.tree.as_ref().map(tree::stats).unwrap_or_default(),
            arena_bytes: inner.io.store.arena_bytes(),
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

    /// Checks the tree's structure and that the allocator holds exactly the
    /// pages the durable tree references, plus garbage still pinned by
    /// readers. Reads every page; for tests and simulation.
    pub fn check_invariants(&self) -> Result<()> {
        let inner = &self.inner;
        let v = inner.version.load_full();
        if let Some(t) = &v.tree {
            tree::check_invariants(&inner.io, &inner.opts.tree, t)?;
        }
        let durable = inner.durable.lock();
        let allocated: HashSet<(u8, u32)> = inner.io.store.allocated().into_iter().collect();
        let mut expected = durable.live.clone();
        for a in inner.pending.lock().iter() {
            if let Some(r) = &a.root {
                tree::reachable(r, &mut expected);
            }
            // Its superseded early pages stay allocated until it commits.
            expected.extend(a.early.iter().copied());
        }
        let live: HashSet<(u8, u32)> = expected.iter().map(|p| (p.class, p.slot)).collect();
        assert!(live.is_subset(&allocated), "a live page is not allocated");
        if Arc::strong_count(&v) <= 2 {
            // No reader pins an older version, so no garbage awaits release.
            let leaked: Vec<_> = allocated.difference(&live).collect();
            assert!(leaked.is_empty(), "leaked page slots: {leaked:?}");
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
            let _ = self.inner.writer.lock().wal.sync();
        }
    }
}

impl Inner {
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

    /// Takes the ops by value so the memtable keeps their buffers as they
    /// are; a clone of a `Bytes` would add a shared header to every value.
    fn write(&self, ops: Vec<WriteOp>, sync: bool) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        for op in &ops {
            if op.key.len() > MAX_KEY_LEN || op.value.encoded_len() > MAX_VALUE_LEN {
                return Err(Error::InvalidArgument("key or value too large".into()));
            }
        }
        self.check_poisoned()?;
        let entry_bytes: u64 = ops.iter().map(|op| entry_size(op.key.len(), op.value.encoded_len())).sum();
        let user_bytes: u64 = ops.iter().map(|op| (op.key.len() + op.value.encoded_len()) as u64).sum();
        thread_local! {
            static BODY: std::cell::RefCell<wal::Body> = Default::default();
        }
        BODY.with_borrow_mut(|body| {
            // Everything but the sequence number is encoded and checksummed
            // before queueing, which keeps the writer lock's turns short.
            body.encode(&ops);
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
            let flushed = a
                .flush
                .map_or(Ok(()), |log| log.start_flush().write())
                .and_then(|()| a.sync_to.map_or(Ok(()), |(log, end)| log.sync_to(end)));
            // SAFETY: see `Assigned::mem`; this writer stays counted in
            // until the `exit` below, its last use of the memtable.
            let mem = unsafe { &*a.mem };
            for (i, op) in ops.into_iter().enumerate() {
                mem.insert(&op.key, op.value, a.seq + i as u64, a.ticket);
            }
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
            if let Some(mut w) = self.writer.try_lock() {
                let woken = self.serve(&mut w);
                let next = w.carried.front().copied().or_else(|| {
                    let head = self.queue.load(Acquire);
                    (!head.is_null()).then_some(head as usize)
                });
                drop(w);
                Self::wake(woken);
                // A request left for the next turn may belong to a parked
                // writer: wake one to take the turn.
                if let Some(key) = next {
                    Self::wake(vec![key]);
                }
                continue;
            }
            if spins < REQUEST_SPINS {
                spins += 1;
                std::hint::spin_loop();
                continue;
            }
            let key = request as *const Request as usize;
            let deadline = std::time::Instant::now() + REQUEST_PARK;
            // Park only while the lock is held: whoever releases it then
            // wakes a waiting writer, after this check runs.
            let waiting = || !request.done.load(Acquire) && self.writer.is_locked();
            // SAFETY: the callbacks neither panic nor call into the parking
            // lot.
            unsafe {
                parking_lot_core::park(key, waiting, || {}, |_, _| {}, parking_lot_core::DEFAULT_PARK_TOKEN, Some(deadline));
            }
        }
    }

    /// One turn at the writer lock: serves the requests the last turn left,
    /// then those queued now, oldest first, and returns their keys for
    /// [`Self::wake`] once the lock is released.
    ///
    /// A turn takes the queue once and serves at most [`TURN_LIMIT`]
    /// requests, so it ends and lets its own batch commit. A turn rotates the
    /// memtable only before serving anything: its writer's own batch, once
    /// served, sits in the memtable a rotation may have to wait to
    /// checkpoint. A request needing a rotation after that waits for the next
    /// turn.
    fn serve(&self, w: &mut Writer) -> Vec<usize> {
        let mut p = self.queue.swap(std::ptr::null_mut(), Acquire);
        let mut queued = Vec::new();
        while !p.is_null() {
            queued.push(p as usize);
            // SAFETY: a queued request stays alive until it is done.
            p = unsafe { (*p).next.load(Relaxed) };
        }
        w.carried.extend(queued.into_iter().rev());
        let mut served = Vec::new();
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
        served
    }

    /// Wakes the writers of served requests that parked. Only the keys are
    /// used: the requests themselves may be gone.
    fn wake(keys: Vec<usize>) {
        for key in keys {
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
        // The old segment must be durable before the new one takes writes,
        // so that recovery always replays a prefix of history.
        w.wal.sync()?;
        let id = w.active.id + 1;
        let wal = WalWriter::create(&*self.storage, id, self.recycler.take())?;
        let mem = Arc::new(Mem::new(id, &self.opts, w.next_seq - 1));
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
            self.version.store(Arc::new(Version { active: mem.clone(), frozen, tree: v.tree.clone(), _epoch: epoch }));
        }
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
    fn get_in(&self, v: &Version, key: &[u8], view: View) -> Result<Option<Bytes>> {
        let hash = bloom::hash_key(key);
        if let Some(val) = v.active.get(key, hash, view) {
            return Ok(val.into_put());
        }
        for m in &v.frozen {
            if let Some(val) = m.get(key, hash, view) {
                return Ok(val.into_put());
            }
        }
        match &v.tree {
            Some(t) => Ok(tree::get(&self.io, t, key, hash)?.and_then(Value::into_put)),
            None => Ok(None),
        }
    }

    /// Runs one pipeline stage until shutdown or failure.
    fn stage_loop(self: Arc<Self>, stage: Stage) {
        loop {
            {
                let mut g = self.signal_lock.lock();
                while !self.stage_ready(stage) && !self.shutdown.load(SeqCst) && !self.poisoned.load(SeqCst) {
                    self.signal.wait(&mut g);
                }
            }
            if self.shutdown.load(SeqCst) || self.poisoned.load(SeqCst) {
                return;
            }
            let r = match stage {
                Stage::Apply => self.apply_step(),
                Stage::Commit => self.commit_step(),
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
        }
    }

    /// The oldest frozen memtable not yet applied.
    fn next_to_apply(&self) -> Option<Arc<Mem>> {
        let last = self.applier.lock().last_applied;
        self.version.load().frozen.iter().rev().find(|m| m.id > last).cloned()
    }

    /// Cuts a frozen memtable into sorted batches of about one leaf each and
    /// hands each to `send`, stopping early if it returns false.
    fn build_batches(&self, mem: &Mem, send: &mut dyn FnMut(Arc<DataPage>) -> bool) {
        let limit = self.opts.tree.leaf_size;
        let bits = self.opts.tree.filter_bits;
        let capacity = (limit + limit / 16) as usize + 4096;
        let mut b = PageBuilder::with_capacity(self.next_page_id.fetch_add(1, Relaxed), capacity);
        let mut open = true;
        mem.table.for_each(&mut |key, value| {
            if !open {
                return;
            }
            let e = EntryRef { key, value: value.as_put().map(|v| &v[..]) };
            if !b.is_empty() && b.data_bytes() + e.size() > limit {
                let next = PageBuilder::with_capacity(self.next_page_id.fetch_add(1, Relaxed), capacity);
                let full = std::mem::replace(&mut b, next);
                open = send(Arc::new(DataPage::unwritten(full.finish(bits))));
            }
            b.push(e);
        });
        if open && !b.is_empty() {
            send(Arc::new(DataPage::unwritten(b.finish(bits))));
        }
    }

    fn checkpoint_step(&self) -> Result<bool> {
        let r = if !self.pending.lock().is_empty() { self.commit_step() } else { self.apply_step() };
        let r = self.poison_on_err(r);
        self.notify();
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
        // the last few checkpoints. One after open has no estimate and runs
        // at full speed, as does one a caller froze early, and one whose
        // writers waited on the checkpoint before it: when writers outrun
        // checkpoints, every checkpoint is late.
        let expected = self.checkpoint_io.lock().iter().copied().max().unwrap_or(0);
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
        let io_bytes = || self.io.counters.span_read_bytes.load(Relaxed) + self.io.counters.page_bytes_written.load(Relaxed);
        let io_before = io_bytes();
        let apply = |batches: std::sync::mpsc::Receiver<Arc<DataPage>>| -> Result<(Option<Subtree>, bool, Vec<PagePtr>)> {
            let mut root = applier.root.clone();
            let mut created_files = false;
            let mut early = Vec::new();
            for batch in batches {
                let t = std::time::Instant::now();
                root = tree::apply_batch(&ctx, root.as_ref(), batch)?;
                self.timing.apply_ns.fetch_add(since(t), Relaxed);
                // Pages a checkpoint has yet to write count against half the
                // cache: past that, write the new pages below the root now
                // rather than hold them all until the checkpoint ends. A
                // later batch may still supersede one, wasting its write,
                // so a large cache keeps them all in memory.
                if let Some(r) = &root {
                    let pages = tree::unwritten_below_root(r);
                    if pages.iter().map(|p| p.meta.image_len()).sum::<u64>() > self.opts.cache_bytes / 2 {
                        let t = std::time::Instant::now();
                        created_files |= tree::write_pages(&ctx, &pages)?;
                        early.extend(pages.iter().filter_map(|p| p.ptr()));
                        self.timing.write_ns.fetch_add(since(t), Relaxed);
                    }
                }
            }
            let t = std::time::Instant::now();
            if let Some(r) = &root {
                created_files |= tree::write_tree(&ctx, r)?;
            }
            self.timing.write_ns.fetch_add(since(t), Relaxed);
            Ok((root, created_files, early))
        };
        let run = |rx| match &self.merge_pool {
            Some(pool) => pool.install(|| apply(rx)),
            None => apply(rx),
        };
        let applied = if self.opts.background {
            // Another thread cuts the batches while this one applies them,
            // one batch ahead, so the checkpoint never holds a copy of the
            // memtable.
            std::thread::scope(|scope| {
                let (tx, rx) = std::sync::mpsc::sync_channel(1);
                let frozen = &*mem;
                scope.spawn(move || {
                    let t = std::time::Instant::now();
                    let mut blocked = 0;
                    self.build_batches(frozen, &mut |batch| {
                        let t = std::time::Instant::now();
                        let open = tx.send(batch).is_ok();
                        blocked += since(t);
                        open
                    });
                    self.timing.build_ns.fetch_add(since(t) - blocked, Relaxed);
                });
                run(rx)
            })
        } else {
            // Cut every batch first, so page ids, and with them simulated
            // runs, stay deterministic.
            let (tx, rx) = std::sync::mpsc::channel();
            let t = std::time::Instant::now();
            self.build_batches(&mem, &mut |batch| tx.send(batch).is_ok());
            self.timing.build_ns.fetch_add(since(t), Relaxed);
            drop(tx);
            run(rx)
        };
        self.io.pacer.stop();
        let (root, created_files, early) = applied?;
        let mut recent = self.checkpoint_io.lock();
        if recent.len() == 3 {
            recent.pop_front();
        }
        recent.push_back(io_bytes() - io_before);
        drop(recent);
        applier.root = root.clone();
        applier.last_applied = mem.id;
        self.pending.lock().push_back(Applied { mem, root, created_files, early });
        Ok(true)
    }

    /// Makes the oldest applied checkpoint durable, commits it, and swaps it
    /// into the read path in place of its memtable.
    fn commit_step(&self) -> Result<bool> {
        self.check_poisoned()?;
        let mut durable = self.durable.lock();
        let Some(Applied { mem, root, created_files, early }) = self.pending.lock().pop_front() else {
            return Ok(false);
        };
        let t = std::time::Instant::now();
        self.io.store.sync()?;
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
        };
        sb.store(&*self.storage)?;
        if std::env::var_os("TURTLEKV_DEBUG").is_some() {
            eprintln!("commit {sb:?}");
        }

        let mut live = HashSet::new();
        if let Some(r) = &root {
            tree::reachable(r, &mut live);
        }
        let mut garbage: Vec<PagePtr> = durable.live.difference(&live).copied().collect();
        garbage.extend(early.into_iter().filter(|p| !live.contains(p)));
        let epoch = Arc::new(Epoch::new(self.io.clone()));
        let old_epoch = std::mem::replace(&mut durable.epoch, epoch.clone());
        durable.live = live;
        durable.sb = sb;
        {
            let _g = self.version_lock.lock();
            let cur = self.version.load();
            let frozen = cur.frozen.iter().filter(|m| m.id != mem.id).cloned().collect();
            old_epoch.retire(garbage, epoch.clone());
            self.version.store(Arc::new(Version { active: cur.active.clone(), frozen, tree: root, _epoch: epoch }));
        }
        drop(old_epoch);
        drop(durable);
        self.io.store.reclaim();
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

#[derive(Clone, Copy)]
enum Stage {
    Apply,
    Commit,
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
fn collect_scan(mut it: Forward, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
    it.tree.expect(limit);
    enum Part {
        Copied(usize, usize, usize),
        Shared(Bytes, Bytes),
    }
    const COPY_LIMIT: usize = 512;
    let mut buf = Vec::with_capacity(4096);
    let mut parts = Vec::with_capacity(limit.min(1024));
    while parts.len() < limit {
        let part = it.step(|e| {
            let (k, v) = (e.key(), e.value());
            if k.len() + v.len() <= COPY_LIMIT {
                let at = buf.len();
                buf.extend_from_slice(k);
                buf.extend_from_slice(v);
                Part::Copied(at, k.len(), v.len())
            } else {
                let (k, v) = e.to_owned();
                Part::Shared(k, v)
            }
        })?;
        match part {
            Some(p) => parts.push(p),
            None => break,
        }
    }
    let buf = Bytes::from(buf);
    Ok(parts
        .into_iter()
        .map(|p| match p {
            Part::Copied(at, kl, vl) => (buf.slice(at..at + kl), buf.slice(at + kl..at + kl + vl)),
            Part::Shared(k, v) => (k, v),
        })
        .collect())
}

/// An iterator over one snapshot's live entries, in either key order.
pub struct DbIter(Walk);

enum Walk {
    Forward(Forward),
    Reverse(Reverse),
}

impl DbIter {
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
    start: Bytes,
    /// Exclusive upper bound of what remains; `None` for the end of the key
    /// space.
    hi: Option<Bytes>,
    /// The current window's entries in key order, taken from the back.
    window: Vec<(Bytes, Bytes)>,
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
            start: Bytes::copy_from_slice(start),
            hi: end.map(Bytes::copy_from_slice),
            window: Vec::new(),
            blocks: 1,
            done: false,
        }
    }

    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if let Some(e) = self.window.pop() {
                return Ok(Some(e));
            }
            if self.done || self.hi.as_deref().is_some_and(|h| h <= &self.start[..]) {
                self.done = true;
                return Ok(None);
            }
            let lo = self.window_start();
            let mut forward = Forward::new(&self.snap, &lo, self.hi.as_deref());
            while let Some(e) = forward.next()? {
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
            lo = lo.max(tree::window_start(t, hi, self.blocks));
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
struct Forward {
    snap: Snapshot,
    /// Newest first; the tree comes last.
    mems: Vec<MemSource>,
    tree: TreeScan,
}

struct MemSource {
    mem: Arc<Mem>,
    snapshot: u64,
    buf: VecDeque<(Bytes, Value)>,
    start: Bound<Bytes>,
    end: Option<Bytes>,
    /// Entries the next refill copies. It starts small and doubles, since
    /// memtables hold a small share of the data and most scans are short.
    chunk: usize,
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
        let snapshot = self.snapshot;
        self.mem.table.range(start, self.end.as_deref(), &|seq, _| seq <= snapshot, self.chunk, &mut out);
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

    fn value(&self) -> &[u8] {
        match self {
            EntryView::Mem(_, v) => v,
            EntryView::Tree(t) => t.peek().expect("settled").1,
        }
    }

    /// Handles sharing the source's buffers.
    fn to_owned(&self) -> (Bytes, Bytes) {
        match self {
            EntryView::Mem(k, v) => ((*k).clone(), (*v).clone()),
            EntryView::Tree(t) => t.peek_owned().expect("settled"),
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
                buf: VecDeque::new(),
                start: Bound::Included(start_b.clone()),
                end: end_b.clone(),
                chunk: MEM_CHUNK_MIN,
                exhausted: false,
            })
            .collect();
        let tree = TreeScan::new(version.tree.clone(), start, end_b);
        Self { snap: snap.clone(), mems, tree }
    }

    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        self.step(|e| e.to_owned())
    }

    /// Finds the next live entry, passes it to `f`, and moves past it.
    fn step<T>(&mut self, f: impl FnOnce(EntryView<'_>) -> T) -> Result<Option<T>> {
        let io = &self.snap.inner.io;
        loop {
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
            {
                best = Some((n, k));
            }
            let Some((w, _)) = best else { return Ok(None) };
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
