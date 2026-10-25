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

use crate::error::{Error, Result};
use crate::memtable::{MemTable, MemTableKind, new_memtable};
use crate::page::{EntryRef, PageBuilder, entry_size};
use crate::pagestore::{PagePtr, PageStore, Superblock};
use crate::storage::Storage;
use crate::tree::io::{DataPage, PageIo};
use crate::tree::{self, Ctx, Subtree, TreeConfig, TreeScan};
use crate::types::{MAX_KEY_LEN, MAX_VALUE_LEN, Value, WriteOp, cmp_keys};
use crate::wal::{self, WalWriter};
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::*};
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
}

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
        }
    }
}

impl Options {
    fn memtable_limit(&self) -> u64 {
        self.checkpoint_distance.max(1) * self.tree.leaf_size
    }
}

/// A memtable and the log segment that shares its id.
pub struct Mem {
    pub id: u64,
    table: Box<dyn MemTable>,
    /// Entry bytes assigned to this memtable. Writers add to it under the
    /// writer lock.
    bytes: AtomicU64,
    /// Sequence number of the newest op assigned to this memtable.
    last_seq: AtomicU64,
    /// Writers assigned to this memtable that have not finished inserting.
    inflight: Inflight,
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
    /// Counts a writer in and returns its shard.
    fn enter(&self) -> usize {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        thread_local! {
            static SHARD: usize = NEXT.fetch_add(1, Relaxed) % INFLIGHT_SHARDS;
        }
        let shard = SHARD.with(|s| *s);
        self.0[shard].0.fetch_add(1, SeqCst);
        shard
    }

    fn exit(&self, shard: usize) {
        self.0[shard].0.fetch_sub(1, SeqCst);
    }

    fn is_zero(&self) -> bool {
        self.0.iter().all(|c| c.0.load(SeqCst) == 0)
    }
}

impl Mem {
    fn new(id: u64, kind: MemTableKind, last_seq: u64) -> Self {
        Self {
            id,
            table: new_memtable(kind),
            bytes: AtomicU64::new(0),
            last_seq: AtomicU64::new(last_seq),
            inflight: Inflight::default(),
        }
    }

    fn insert(&self, op: &WriteOp, seq: u64) {
        self.table.insert(&op.key, op.value.clone(), seq);
        self.bytes.fetch_add(entry_size(op.key.len(), op.value.encoded_len()), Relaxed);
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Relaxed)
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
    next_seq: u64,
    active: Arc<Mem>,
    user_bytes: u64,
    wal_bytes: u64,
}

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
    pub memtable_bytes: u64,
    pub frozen_memtables: usize,
    pub tree: tree::TreeStats,
    pub arena_bytes: u64,
    /// Seconds in each checkpoint stage: build, apply, write, sync, commit;
    /// then seconds writers stalled.
    pub stage_secs: [f64; 6],
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
    timing: Timing,
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
        let replayed = Mem::new(0, opts.memtable, last_seq);
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
            let r = wal::replay(&*storage, id, last_seq, |seq, op| replayed.insert(&op, seq))?;
            // Replay may have read records that only reached the OS cache
            // (after a process crash). New writes build on them, so they must
            // be durable before the store accepts any.
            storage.open(&wal::segment_name(id), false)?.sync_data()?;
            if std::env::var_os("TURTLEKV_DEBUG").is_some() {
                eprintln!("recovery: sb {sb:?}, segment {id}: {} -> {} {:?}", last_seq, r.last_seq, r.end);
            }
            last_seq = r.last_seq;
            if r.end == wal::End::Gap {
                gap = true;
                storage.remove(&wal::segment_name(id))?;
            }
        }
        storage.sync_dir()?;
        let mut frozen = Vec::new();
        if replayed.table.len() > 0 {
            let mem = Mem { id: max_id, last_seq: AtomicU64::new(last_seq), ..replayed };
            frozen.push(Arc::new(mem));
        }
        let active_id = max_id + 1;
        let wal = WalWriter::create(&*storage, active_id)?;
        let active = Arc::new(Mem::new(active_id, opts.memtable, last_seq));
        let epoch = Arc::new(Epoch::new(io.clone()));
        let applier = Applier { root: root.clone(), last_applied: sb.last_memtable_id };
        let version = Version { active: active.clone(), frozen, tree: root, _epoch: epoch.clone() };
        let inner = Arc::new(Inner {
            storage,
            next_page_id: AtomicU64::new(sb.next_page_id),
            io,
            version: ArcSwap::from_pointee(version),
            version_lock: Mutex::new(()),
            writer: Mutex::new(Writer { wal, next_seq: last_seq + 1, active, user_bytes: 0, wal_bytes: 0 }),
            signal_lock: Mutex::new(()),
            signal: Condvar::new(),
            durable: Mutex::new(Durable { sb, live, epoch }),
            applier: Mutex::new(applier),
            pending: Mutex::new(VecDeque::new()),
            poisoned: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            checkpoints: AtomicU64::new(0),
            timing: Timing::default(),
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

    /// Up to `limit` live entries with keys in `[start, end)`, in order.
    ///
    /// Small entries are copied into one buffer per call. Sharing them would
    /// make every result touch the reference count of a cached block, and
    /// threads scanning the same hot blocks contend on those counts.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        enum Part {
            Copied(usize, usize, usize),
            Shared(Bytes, Bytes),
        }
        const COPY_LIMIT: usize = 512;
        let mut it = self.iter(start, end);
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

    /// An iterator over live entries with keys in `[start, end)`. It reads
    /// the memtables lazily, so it sees some writes made after it was created.
    pub fn iter(&self, start: &[u8], end: Option<&[u8]>) -> DbIter {
        DbIter::new(self.inner.clone(), start, end)
    }

    /// Makes every completed write durable.
    pub fn sync(&self) -> Result<()> {
        self.inner.check_poisoned()?;
        let mut w = self.inner.writer.lock();
        let r = w.wal.sync();
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
            // before taking the lock, which keeps the critical section short.
            body.encode(&ops);
            let (mem, seq, shard, flush) = {
                let mut w = self.writer.lock();
                self.check_poisoned()?;
                if w.active.bytes() >= self.opts.memtable_limit() {
                    let r = self.rotate(&mut w);
                    self.poison_on_err(r)?;
                }
                let seq = w.next_seq;
                w.next_seq += ops.len() as u64;
                w.wal.append_body(body, seq);
                w.user_bytes += user_bytes;
                w.wal_bytes += body.record_len() as u64;
                let mut flush = None;
                if sync {
                    let r = w.wal.sync();
                    self.poison_on_err(r)?;
                } else if w.wal.buffered() >= self.opts.wal_buffer_bytes {
                    flush = Some(w.wal.start_flush());
                }
                w.active.last_seq.store(seq + ops.len() as u64 - 1, Relaxed);
                w.active.bytes.fetch_add(entry_bytes, Relaxed);
                let shard = w.active.inflight.enter();
                (w.active.clone(), seq, shard, flush)
            };
            let flushed = flush.map_or(Ok(()), |f| f.write());
            for (i, op) in ops.into_iter().enumerate() {
                mem.table.insert(&op.key, op.value, seq + i as u64);
            }
            mem.inflight.exit(shard);
            self.poison_on_err(flushed)
        })
    }

    /// Freezes the active memtable and starts a new one with a new log
    /// segment. Waits (or checkpoints inline) until a frozen slot is free.
    fn rotate(&self, w: &mut Writer) -> Result<()> {
        let stall = std::time::Instant::now();
        loop {
            if self.version.load().frozen.len() < self.opts.max_frozen {
                break;
            }
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
        let wal = WalWriter::create(&*self.storage, id)?;
        let mem = Arc::new(Mem::new(id, self.opts.memtable, w.next_seq - 1));
        {
            let _g = self.version_lock.lock();
            let v = self.version.load();
            let mut frozen = vec![w.active.clone()];
            frozen.extend(v.frozen.iter().cloned());
            let epoch = v._epoch.clone();
            self.version.store(Arc::new(Version { active: mem.clone(), frozen, tree: v.tree.clone(), _epoch: epoch }));
        }
        w.wal = wal;
        w.active = mem;
        self.notify();
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let v = self.version.load();
        if let Some(val) = v.active.table.get(key) {
            return Ok(val.into_put());
        }
        for m in &v.frozen {
            if let Some(val) = m.table.get(key) {
                return Ok(val.into_put());
            }
        }
        match &v.tree {
            Some(t) => Ok(tree::get(&self.io, t, key)?.and_then(Value::into_put)),
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

    /// Cuts a frozen memtable into sorted batches of about one leaf each.
    fn build_batches(&self, mem: &Mem) -> Vec<Arc<DataPage>> {
        let limit = self.opts.tree.leaf_size;
        let bits = self.opts.tree.filter_bits;
        let mut batches = Vec::new();
        let mut b = PageBuilder::new(self.next_page_id.fetch_add(1, Relaxed));
        mem.table.for_each(&mut |key, value| {
            let e = EntryRef { key, value: value.as_put().map(|v| &v[..]) };
            if !b.is_empty() && b.data_bytes() + e.size() > limit {
                let full = std::mem::replace(&mut b, PageBuilder::new(self.next_page_id.fetch_add(1, Relaxed)));
                batches.push(Arc::new(DataPage::unwritten(full.finish(bits))));
            }
            b.push(e);
        });
        if !b.is_empty() {
            batches.push(Arc::new(DataPage::unwritten(b.finish(bits))));
        }
        batches
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
        let mut root = applier.root.clone();
        let t = std::time::Instant::now();
        let batches = self.build_batches(&mem);
        self.timing.build_ns.fetch_add(since(t), Relaxed);
        let t = std::time::Instant::now();
        for batch in batches {
            root = tree::apply_batch(&ctx, root.as_ref(), batch)?;
        }
        self.timing.apply_ns.fetch_add(since(t), Relaxed);
        let t = std::time::Instant::now();
        let created_files = match &root {
            Some(r) => tree::write_tree(&ctx, r)?,
            None => false,
        };
        self.timing.write_ns.fetch_add(since(t), Relaxed);
        applier.root = root.clone();
        applier.last_applied = mem.id;
        self.pending.lock().push_back(Applied { mem, root, created_files });
        Ok(true)
    }

    /// Makes the oldest applied checkpoint durable, commits it, and swaps it
    /// into the read path in place of its memtable.
    fn commit_step(&self) -> Result<bool> {
        self.check_poisoned()?;
        let mut durable = self.durable.lock();
        let Some(Applied { mem, root, created_files }) = self.pending.lock().pop_front() else {
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
            last_seq: mem.last_seq.load(SeqCst),
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
        let garbage: Vec<PagePtr> = durable.live.difference(&live).copied().collect();
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
                self.storage.remove(&wal::segment_name(id))?;
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

/// A merged iterator over the memtables and the tree of one version.
pub struct DbIter {
    inner: Arc<Inner>,
    /// Newest first; the tree comes last.
    mems: Vec<MemSource>,
    tree: TreeScan,
    _version: Arc<Version>,
}

struct MemSource {
    mem: Arc<Mem>,
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
        self.mem.table.range(start, self.end.as_deref(), self.chunk, &mut out);
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

impl DbIter {
    fn new(inner: Arc<Inner>, start: &[u8], end: Option<&[u8]>) -> Self {
        let version = inner.version.load_full();
        let end_b = end.map(Bytes::copy_from_slice);
        let start_b = Bytes::copy_from_slice(start);
        let mems = std::iter::once(&version.active)
            .chain(&version.frozen)
            .map(|m| MemSource {
                mem: m.clone(),
                buf: VecDeque::new(),
                start: Bound::Included(start_b.clone()),
                end: end_b.clone(),
                chunk: MEM_CHUNK_MIN,
                exhausted: false,
            })
            .collect();
        let tree = TreeScan::new(version.tree.clone(), start, end_b);
        Self { inner, mems, tree, _version: version }
    }

    pub fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        self.step(|e| e.to_owned())
    }

    /// Finds the next live entry, passes it to `f`, and moves past it.
    fn step<T>(&mut self, f: impl FnOnce(EntryView<'_>) -> T) -> Result<Option<T>> {
        let io = &self.inner.io;
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
