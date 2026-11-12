//! The write-ahead log.
//!
//! Each memtable owns one log segment, `wal-<id>.log`, which is retired as
//! soon as the checkpoint that absorbs its memtable is durable. A retired
//! segment is kept for reuse (see [`Recycler`]) or deleted. A segment is a
//! sequence of records, one per write batch:
//!
//! ```text
//! crc32c: u32 | len: u32 | count: u32 | op* | first_seq: u64
//! op = kind: u8 | key_len: u32 | key | (value_len: u32 | value)?   (value only for puts)
//! ```
//!
//! The checksum covers everything after itself. The sequence number comes
//! last so that writers can encode and checksum the rest of a record before
//! taking the lock that assigns sequence numbers.
//!
//! Records are written in whole 4 KiB blocks, the last one zero-filled past
//! the final record. Replay stops at the first record that is truncated,
//! zeroed, fails its checksum, or breaks the sequence-number chain: that
//! record and everything after it are the zero fill or the torn tail of the
//! last unsynced write. A reused segment still holds its old records past the
//! new ones; their sequence numbers are all covered by a checkpoint, so
//! replay skips them.

use crate::error::Result;
use crate::storage::{DIRECT_ALIGN, File, Storage};
use crate::types::{Op, Value, WriteOp};
use bytes::Bytes;
use parking_lot::{Mutex, lock_api::ArcMutexGuard};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const HEADER_LEN: usize = 8;
const KIND_PUT: u8 = 0;
const KIND_DELETE: u8 = 1;

pub fn segment_name(id: u64) -> String {
    format!("wal-{id:016x}.log")
}

fn recycled_name(n: u64) -> String {
    format!("recycled-{n:016x}.log")
}

/// Log segments kept for the memtables to come: retired ones, and ones
/// written ahead as zeros (see [`Recycler::prepare`]).
///
/// A new segment written over one of these keeps the file's length and its
/// allocated blocks, so fdatasync has no file size or block allocation to
/// journal, only data to write. On ext4 that halves the cost of a
/// synchronous write.
///
/// Of retired segments, only those created since the store opened are kept.
/// Once retired, every record in one has a sequence number its checkpoint
/// covers. A segment replayed at recovery can hold records past a crash
/// point, which are not covered, so it is deleted instead.
pub struct Recycler {
    pool: Mutex<Pool>,
    /// The first segment created since the store opened.
    first_new: u64,
    /// Bytes in the longest segment kept so far.
    longest: AtomicU64,
}

struct Pool {
    names: Vec<String>,
    next: u64,
}

/// Segments kept at most. Each memtable rotation takes one, and each
/// checkpoint returns one.
const MAX_RECYCLED: usize = 2;

/// Bytes of zeros written at a time while preparing a segment.
const PREPARE_STEP: u64 = 1 << 20;

fn preparing_name(n: u64) -> String {
    format!("preparing-{n:016x}.log")
}

impl Recycler {
    /// Adopts the kept segments left in storage, and removes any left half
    /// prepared. A retired segment enters the pool only after its
    /// checkpoint is durable, and a prepared one holds zeros, so none holds
    /// a record recovery would apply. `first_new` is the id of the first
    /// segment this store creates.
    pub fn open(storage: &dyn Storage, first_new: u64) -> Result<Self> {
        let mut found = Vec::new();
        for name in storage.list()? {
            if name.starts_with("preparing-") && name.ends_with(".log") {
                storage.remove(&name)?;
            } else if let Some(hex) = name.strip_prefix("recycled-").and_then(|n| n.strip_suffix(".log"))
                && let Ok(n) = u64::from_str_radix(hex, 16)
            {
                found.push((n, name));
            }
        }
        found.sort_unstable();
        let next = found.last().map_or(0, |(n, _)| n + 1);
        let keep = found.len().min(MAX_RECYCLED);
        for (_, name) in found.drain(keep..) {
            storage.remove(&name)?;
        }
        let mut longest = 0;
        for (_, name) in &found {
            longest = longest.max(storage.open(name, false)?.len()?);
        }
        let names = found.into_iter().map(|(_, name)| name).collect();
        Ok(Self { pool: Mutex::new(Pool { names, next }), first_new, longest: AtomicU64::new(longest) })
    }

    /// Retires segment `id`, whose checkpoint is durable: keeps it for reuse
    /// if it qualifies and the pool has room, and deletes it otherwise.
    pub fn retire(&self, storage: &dyn Storage, id: u64) -> Result<()> {
        let mut pool = self.pool.lock();
        if id >= self.first_new && pool.names.len() < MAX_RECYCLED {
            let name = recycled_name(pool.next);
            pool.next += 1;
            storage.rename(&segment_name(id), &name)?;
            self.longest.fetch_max(storage.open(&name, false)?.len()?, Ordering::Relaxed);
            pool.names.push(name);
        } else {
            storage.remove(&segment_name(id))?;
        }
        Ok(())
    }

    /// A kept segment to write the next one over, if any.
    pub fn take(&self) -> Option<String> {
        self.pool.lock().names.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.pool.lock().names.is_empty()
    }

    /// Bytes a prepared segment should span to hold one memtable's log:
    /// `expected`, or the longest segment kept so far if that was longer.
    pub fn segment_len(&self, expected: u64) -> u64 {
        expected.max(self.longest.load(Ordering::Relaxed)).next_multiple_of(DIRECT_ALIGN as u64)
    }

    /// Writes a segment of `len` zeroed bytes and adds it to the pool, so
    /// that the memtable rotation that takes it finds every block
    /// allocated. Returns whether it did: a failed write only leaves the
    /// pool as it was, and the next segment then grows as it is written.
    ///
    /// The zeros go to a file of their own, under a name recovery removes,
    /// and the file joins the pool once they are durable. Zeroing the file
    /// that takes the log's writes would hold up each write that overlaps a
    /// fill: on XFS, a durable 20 KB write took 219 us at the median while
    /// 16 MiB of its segment was being zeroed, against 46 us otherwise.
    ///
    /// `pace` is called with the bytes written so far, after each
    /// [`PREPARE_STEP`]. It returns when the next step may go ahead, or
    /// false to abandon the segment.
    pub fn prepare(&self, storage: &dyn Storage, len: u64, mut pace: impl FnMut(u64) -> bool) -> bool {
        let n = {
            let mut pool = self.pool.lock();
            pool.next += 1;
            pool.next - 1
        };
        let (tmp, name) = (preparing_name(n), recycled_name(n));
        let mut fill = || -> std::io::Result<bool> {
            let file = storage.open_direct(&tmp, true)?;
            let mut zeros = Vec::new();
            let mut written = 0;
            while written < len {
                let step = PREPARE_STEP.min(len - written);
                write_zeros(&*file, written, step, &mut zeros)?;
                written += step;
                if !pace(written) {
                    return Ok(false);
                }
            }
            file.sync_data()?;
            storage.rename(&tmp, &name)?;
            Ok(true)
        };
        if !matches!(fill(), Ok(true)) {
            let _ = storage.remove(&tmp);
            return false;
        }
        let mut pool = self.pool.lock();
        if pool.names.len() < MAX_RECYCLED {
            self.longest.fetch_max(len, Ordering::Relaxed);
            pool.names.push(name);
        } else {
            // Checkpoints filled the pool meanwhile.
            let _ = storage.remove(&name);
        }
        true
    }
}

/// Zeroes `[start, start + len)` of `file` as written blocks, without making
/// them durable. Both ends must be multiples of [`DIRECT_ALIGN`]. `zeros`
/// keeps the buffer of zeros between calls.
fn write_zeros(file: &dyn File, start: u64, len: u64, zeros: &mut Vec<Block>) -> std::io::Result<()> {
    match file.write_zeroes(start, len) {
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {}
        r => return r,
    }
    if zeros.is_empty() {
        zeros.resize((len as usize).div_ceil(DIRECT_ALIGN).min((PREPARE_STEP as usize) / DIRECT_ALIGN), Block([0; DIRECT_ALIGN]));
    }
    // SAFETY: a `Block` is a byte array without padding, as in
    // `Staging::write`.
    let bytes = unsafe { std::slice::from_raw_parts(zeros.as_ptr().cast::<u8>(), zeros.len() * DIRECT_ALIGN) };
    let mut offset = start;
    while offset < start + len {
        let n = bytes.len().min((start + len - offset) as usize);
        file.write_all_at(&bytes[..n], offset)?;
        offset += n as u64;
    }
    Ok(())
}

/// Ids of all log segments in storage, ascending.
pub fn list_segments(storage: &dyn Storage) -> Result<Vec<u64>> {
    let mut ids: Vec<u64> = storage
        .list()?
        .iter()
        .filter_map(|name| name.strip_prefix("wal-")?.strip_suffix(".log"))
        .filter_map(|hex| u64::from_str_radix(hex, 16).ok())
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// Bytes a batch occupies in the log.
pub fn encoded_len(ops: &[WriteOp]) -> usize {
    HEADER_LEN + 12 + ops.iter().map(|op| 1 + 4 + op.key.len() + op.value.as_put().map_or(0, |v| 4 + v.len())).sum::<usize>()
}

/// A record without its sequence number: the bytes between the header and
/// the sequence number, and the checksum of everything before the sequence
/// number.
#[derive(Default)]
pub struct Body {
    bytes: Vec<u8>,
    crc: u32,
}

impl Body {
    /// Encodes `ops`, reusing this body's buffer.
    pub fn encode<O: Op>(&mut self, ops: &[O]) {
        let buf = &mut self.bytes;
        buf.clear();
        buf.extend_from_slice(&(ops.len() as u32).to_le_bytes());
        encode_ops(buf, ops);
        let len = (buf.len() + 8) as u32;
        self.crc = crc32c::crc32c_append(crc32c::crc32c(&len.to_le_bytes()), buf);
    }

    /// Bytes the full record occupies in the log.
    pub fn record_len(&self) -> usize {
        HEADER_LEN + self.bytes.len() + 8
    }

    fn append_to(&self, buf: &mut Vec<u8>, first_seq: u64) {
        let seq = first_seq.to_le_bytes();
        let crc = crc32c::crc32c_append(self.crc, &seq);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&((self.bytes.len() + 8) as u32).to_le_bytes());
        buf.extend_from_slice(&self.bytes);
        buf.extend_from_slice(&seq);
    }
}

pub fn encode(buf: &mut Vec<u8>, first_seq: u64, ops: &[WriteOp]) {
    let mut body = Body::default();
    body.encode(ops);
    body.append_to(buf, first_seq);
}

fn encode_ops<O: Op>(buf: &mut Vec<u8>, ops: &[O]) {
    for op in ops {
        let key = op.key();
        match op.value() {
            Some(v) => {
                buf.push(KIND_PUT);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => {
                buf.push(KIND_DELETE);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
        }
    }
}

/// Appends records to one segment. Records accumulate in memory until
/// [`flush`](Self::flush) writes them or [`sync`](Self::sync) makes them durable.
pub struct WalWriter {
    id: u64,
    log: Arc<Log>,
}

/// One segment's buffered records, writes and fsyncs. It has its own locks,
/// so synchronous writers sync it after releasing the writer's lock.
///
/// Writes bypass the page cache. A buffered write of a few bytes into a page
/// cached as a large folio dirties the whole folio, so each fsync would write
/// back up to megabytes; a direct write of whole blocks writes only those.
///
/// Synchronous writers share fsyncs. One at a time leads: it writes every
/// buffered record, its own and the others', in one write, then fsyncs. The
/// rest wait for that fsync to end and return if it covered them. A group of
/// writers costs one write and one fsync, and none waits behind an fsync it
/// does not need. Waiters park without a lock and wake to check atomics, so
/// a large group wakes at once rather than taking turns at a mutex.
pub struct Log {
    file: Arc<dyn File>,
    pending: Mutex<Pending>,
    /// Held while writing to the file, so writes complete in offset order.
    io: Arc<Mutex<Staging>>,
    /// Bytes whose writes have completed, contiguous from the start of the
    /// segment.
    completed: AtomicU64,
    /// Bytes known durable.
    synced: AtomicU64,
    /// Whether a leader is writing and fsyncing.
    syncing: AtomicBool,
    /// A failed write leaves a gap no fsync can fill, and after a failed
    /// fsync, a later one can succeed without the data being durable, so
    /// after either, no sync of this segment succeeds.
    failed: AtomicBool,
}

struct Pending {
    buf: Vec<u8>,
    /// File offset of `buf[0]`: the bytes handed to writes so far.
    offset: u64,
    /// A flushed buffer kept for the next flush. Appends then fill memory
    /// that is already resident: in a fresh buffer each new page faults,
    /// and under a memory limit one fault can wait milliseconds in reclaim
    /// while the writer lock is held.
    spare: Vec<u8>,
}

/// The memory a segment's records pass through on their way to its file,
/// handed from each segment to the next. A new segment then appends, and
/// composes its writes, in memory that is resident, and a memtable rotation
/// asks the allocator for none: an allocation can wait milliseconds while
/// the allocator returns other threads' freed memory to the system.
#[derive(Default)]
pub struct Buffers {
    pending: Vec<u8>,
    spare: Vec<u8>,
    tail: Vec<u8>,
    blocks: Vec<Block>,
}

/// One aligned block of a direct write.
#[derive(Clone, Copy)]
#[repr(C, align(4096))]
struct Block([u8; DIRECT_ALIGN]);

/// What a direct write needs from the one before it: the segment's last,
/// partly written block, which the next write rewrites, and an aligned
/// buffer to compose writes in.
struct Staging {
    /// File offset of the first block not yet completely written.
    base: u64,
    /// The bytes written so far of the block at `base`.
    tail: Vec<u8>,
    blocks: Vec<Block>,
    /// After a failed write, the file no longer matches `tail`, so every
    /// later write fails too.
    failed: bool,
}

impl Staging {
    /// Writes `data`, which continues the tail, as whole blocks from `base`,
    /// zero-filling the last, and keeps the new partial block as the tail.
    /// With `durable`, the write is durable when this returns.
    fn write(&mut self, file: &dyn File, data: &[u8], durable: bool) -> Result<()> {
        if self.failed {
            return Err(std::io::Error::other("an earlier log write failed").into());
        }
        let len = self.tail.len() + data.len();
        let nblocks = len.div_ceil(DIRECT_ALIGN);
        if self.blocks.len() < nblocks {
            self.blocks.resize(nblocks, Block([0; DIRECT_ALIGN]));
        }
        // SAFETY: a `Block` is a byte array without padding, so `nblocks`
        // blocks are that many times `DIRECT_ALIGN` initialized bytes.
        let bytes = unsafe { std::slice::from_raw_parts_mut(self.blocks.as_mut_ptr().cast::<u8>(), nblocks * DIRECT_ALIGN) };
        bytes[..self.tail.len()].copy_from_slice(&self.tail);
        bytes[self.tail.len()..len].copy_from_slice(data);
        bytes[len..].fill(0);
        let written = if durable { file.write_all_at_durable(bytes, self.base) } else { file.write_all_at(bytes, self.base) };
        if let Err(e) = written {
            self.failed = true;
            return Err(e.into());
        }
        let full = len / DIRECT_ALIGN * DIRECT_ALIGN;
        self.tail.clear();
        self.tail.extend_from_slice(&bytes[full..len]);
        self.base += full as u64;
        Ok(())
    }
}

impl Log {
    /// Total bytes appended, written or not.
    pub fn len(&self) -> u64 {
        let p = self.pending.lock();
        p.offset + p.buf.len() as u64
    }

    /// Takes the buffered records for the caller to write after releasing
    /// its locks. Call [`Flush::write`] promptly: later writes of this
    /// segment wait for it.
    pub fn start_flush(self: &Arc<Self>) -> Flush {
        let mut p = self.pending.lock();
        let io = self.io.lock_arc();
        let offset = p.offset;
        // The records go to the write in their buffer, and the buffer the
        // last write returned takes its place, so a flush allocates only
        // until the segment has two.
        let buf = if p.buf.is_empty() {
            Vec::new()
        } else {
            let next = match std::mem::take(&mut p.spare) {
                spare if spare.capacity() > 0 => spare,
                _ => Vec::with_capacity(1 << 20),
            };
            std::mem::replace(&mut p.buf, next)
        };
        p.offset += buf.len() as u64;
        Flush { buf, offset, log: self.clone(), io }
    }

    /// Returns once the first `end` bytes of the segment are durable, leading
    /// a write and fsync if none is running that covers them.
    pub fn sync_to(self: &Arc<Self>, end: u64) -> Result<()> {
        // Waiters park on the log's address.
        let key = Arc::as_ptr(self) as usize;
        loop {
            if self.failed.load(Ordering::Acquire) {
                return Err(std::io::Error::other("an earlier log write or fsync failed").into());
            }
            if self.synced.load(Ordering::Acquire) >= end {
                return Ok(());
            }
            if self.syncing.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                // A leader that finished meanwhile may have covered `end`.
                if self.synced.load(Ordering::Acquire) < end {
                    match self.write_and_sync() {
                        // Everything appended before the call is written by
                        // now, unless an earlier write failed.
                        Ok(target) if target >= end => {
                            self.synced.fetch_max(target, Ordering::Release);
                        }
                        _ => self.failed.store(true, Ordering::Release),
                    }
                }
                self.syncing.store(false, Ordering::Release);
                // SAFETY: no callback panics or calls into parking_lot.
                unsafe { parking_lot_core::unpark_all(key, parking_lot_core::DEFAULT_UNPARK_TOKEN) };
                continue;
            }
            // Sleep until the running fsync ends. The check runs under the
            // parking lot's own lock, after which the leader's `unpark_all`
            // cannot be missed.
            let still_syncing = || self.syncing.load(Ordering::Acquire) && self.synced.load(Ordering::Acquire) < end;
            // SAFETY: no callback panics or calls into parking_lot.
            unsafe {
                parking_lot_core::park(key, still_syncing, || {}, |_, _| {}, parking_lot_core::DEFAULT_PARK_TOKEN, None);
            }
        }
    }

    /// Writes every buffered record and makes everything written durable.
    /// Returns the bytes now durable.
    ///
    /// When every earlier write is already durable, one durable write
    /// (RWF_DSYNC) of the rest does it, without an fsync: on Linux that
    /// saves a system call and, on RAID, a flush sent to every member drive.
    /// Otherwise a plain write and an fsync cover the earlier writes too.
    fn write_and_sync(self: &Arc<Self>) -> Result<u64> {
        let flush = self.start_flush();
        // The flush holds the I/O lock, so no write is in flight and
        // `completed` counts every write so far.
        let completed = self.completed.load(Ordering::Acquire);
        if !flush.buf.is_empty() && self.synced.load(Ordering::Acquire) == completed {
            let end = flush.offset + flush.buf.len() as u64;
            flush.write_durable()?;
            return Ok(end);
        }
        flush.write()?;
        drop(self.io.lock());
        let target = self.completed.load(Ordering::Acquire);
        self.file.sync_data()?;
        Ok(target)
    }

    /// Records a completed write at `offset`. Writes complete in offset
    /// order, so after a failed one, later ones leave `completed` below them.
    fn complete(&self, offset: u64, len: usize) {
        let _ = self.completed.compare_exchange(offset, offset + len as u64, Ordering::Release, Ordering::Relaxed);
    }
}

/// Buffered records taken from a [`Log`] to write without holding the locks
/// that guard appends. It holds the segment's I/O lock, so writes that come
/// after it wait for it.
pub struct Flush {
    buf: Vec<u8>,
    offset: u64,
    log: Arc<Log>,
    io: ArcMutexGuard<parking_lot::RawMutex, Staging>,
}

impl Flush {
    pub fn write(self) -> Result<()> {
        self.write_as(false)
    }

    /// Writes the records durably; see [`File::write_all_at_durable`].
    fn write_durable(self) -> Result<()> {
        self.write_as(true)
    }

    fn write_as(self, durable: bool) -> Result<()> {
        let Flush { mut buf, offset, log, mut io } = self;
        if !buf.is_empty() {
            // A failed write leaves `base` short of later offsets, and
            // `Staging::write` returns that failure.
            debug_assert!(io.failed || io.base + io.tail.len() as u64 == offset);
            io.write(&*log.file, &buf, durable)?;
            log.complete(offset, buf.len());
        }
        // Appends take `pending` before `io`, so `io` goes first.
        drop(io);
        if buf.capacity() > 0 {
            buf.clear();
            let mut p = log.pending.lock();
            if p.spare.capacity() < buf.capacity() {
                p.spare = buf;
            }
        }
        Ok(())
    }
}

impl WalWriter {
    /// Creates segment `id`, over the kept segment `reuse` if given, and
    /// makes its directory entry durable. Its records pass through
    /// `buffers`: the last segment's, or `Buffers::default()` for new ones.
    pub fn create(storage: &dyn Storage, id: u64, reuse: Option<String>, buffers: Buffers) -> Result<Self> {
        let file = match reuse {
            Some(old) => {
                storage.rename(&old, &segment_name(id))?;
                storage.open_direct(&segment_name(id), false)?
            }
            None => storage.open_direct(&segment_name(id), true)?,
        };
        storage.sync_dir()?;
        let Buffers { mut pending, spare, mut tail, blocks } = buffers;
        pending.reserve(1 << 20);
        tail.reserve(DIRECT_ALIGN);
        let log = Arc::new(Log {
            file,
            pending: Mutex::new(Pending { buf: pending, offset: 0, spare }),
            io: Arc::new(Mutex::new(Staging { base: 0, tail, blocks, failed: false })),
            completed: AtomicU64::new(0),
            synced: AtomicU64::new(0),
            syncing: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        });
        Ok(Self { id, log })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Takes the segment's buffers for the next segment. Call once every
    /// record is durable: the segment takes no more.
    pub fn take_buffers(&mut self) -> Buffers {
        let mut p = self.log.pending.lock();
        let mut io = self.log.io.lock();
        debug_assert!(p.buf.is_empty(), "records await a write");
        io.tail.clear();
        Buffers {
            pending: std::mem::take(&mut p.buf),
            spare: std::mem::take(&mut p.spare),
            tail: std::mem::take(&mut io.tail),
            blocks: std::mem::take(&mut io.blocks),
        }
    }

    pub fn append(&mut self, first_seq: u64, ops: &[WriteOp]) {
        encode(&mut self.log.pending.lock().buf, first_seq, ops);
    }

    /// Appends a record encoded ahead of time.
    pub fn append_body(&mut self, body: &Body, first_seq: u64) {
        body.append_to(&mut self.log.pending.lock().buf, first_seq);
    }

    pub fn buffered(&self) -> usize {
        self.log.pending.lock().buf.len()
    }

    /// Total bytes appended, written or not.
    pub fn len(&self) -> u64 {
        self.log.len()
    }

    /// The segment's log, for syncing without the caller's lock.
    pub fn log(&self) -> Arc<Log> {
        self.log.clone()
    }

    pub fn start_flush(&mut self) -> Flush {
        self.log.start_flush()
    }

    pub fn flush(&mut self) -> Result<()> {
        self.log.start_flush().write()
    }

    /// Makes every appended record durable.
    pub fn sync(&mut self) -> Result<()> {
        self.log.sync_to(self.log.len())
    }
}

/// How replay of a segment ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// Every record was valid, to the end of the file.
    Clean,
    /// A truncated, corrupt or zeroed record: the zero fill after the last
    /// record written, or the torn tail of an unsynced write.
    Torn,
    /// A valid record that skips sequence numbers: a record before it was
    /// lost, so it and everything after it come from after a crash point,
    /// which recovery must discard.
    Gap,
}

/// The result of replaying one segment.
pub struct Replay {
    /// Sequence number of the last applied op, or `after_seq` if none.
    pub last_seq: u64,
    pub end: End,
}

/// Replays segment `id`, calling `apply` for every op with a sequence number
/// above `after_seq`. Records must continue the sequence chain from
/// `after_seq`; replay stops at the first record that does not.
///
/// A segment can end torn, or at a gap, and still be followed by valid
/// segments: recovery leaves such a segment in place and logs new writes to
/// the next one, whose sequence numbers continue from the last valid record.
pub fn replay(storage: &dyn Storage, id: u64, after_seq: u64, mut apply: impl FnMut(u64, WriteOp)) -> Result<Replay> {
    let file = storage.open(&segment_name(id), false)?;
    let len = file.len()? as usize;
    let mut data = vec![0u8; len];
    file.read_exact_at(&mut data, 0)?;
    let data = Bytes::from(data);

    let mut pos = 0usize;
    let mut last_seq = after_seq;
    while pos < data.len() {
        let Some((first_seq, ops, next)) = decode_record(&data, pos) else {
            return Ok(Replay { last_seq, end: End::Torn });
        };
        let count = ops.len() as u64;
        if first_seq + count <= last_seq + 1 {
            // A record the checkpoint already covers.
            pos = next;
            continue;
        }
        if first_seq != last_seq + 1 {
            return Ok(Replay { last_seq, end: End::Gap });
        }
        for (i, op) in ops.into_iter().enumerate() {
            apply(first_seq + i as u64, op);
        }
        last_seq = first_seq + count - 1;
        pos = next;
    }
    Ok(Replay { last_seq, end: End::Clean })
}

fn decode_record(data: &Bytes, pos: usize) -> Option<(u64, Vec<WriteOp>, usize)> {
    let header = data.get(pos..pos + HEADER_LEN)?;
    let crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let body_start = pos + HEADER_LEN;
    let body = data.get(body_start..body_start + len)?;
    let expected = crc32c::crc32c_append(crc32c::crc32c(&header[4..8]), body);
    if crc != expected || len < 12 {
        return None;
    }
    let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let first_seq = u64::from_le_bytes(body[len - 8..].try_into().unwrap());
    let mut ops = Vec::with_capacity(count.min(1 << 16));
    let mut p = body_start + 4;
    let end = body_start + len - 8;
    let take = |p: &mut usize, n: usize| -> Option<Bytes> {
        if *p + n > end {
            return None;
        }
        let out = data.slice(*p..*p + n);
        *p += n;
        Some(out)
    };
    let take_u32 = |p: &mut usize| -> Option<usize> {
        let b = take(p, 4)?;
        Some(u32::from_le_bytes(b[..].try_into().unwrap()) as usize)
    };
    for _ in 0..count {
        let kind = take(&mut p, 1)?[0];
        let klen = take_u32(&mut p)?;
        let key = take(&mut p, klen)?;
        let value = match kind {
            KIND_PUT => {
                let vlen = take_u32(&mut p)?;
                Value::Put(take(&mut p, vlen)?)
            }
            KIND_DELETE => Value::Delete,
            _ => return None,
        };
        ops.push(WriteOp { key, value });
    }
    if p != end || first_seq == 0 {
        return None;
    }
    Some((first_seq, ops, end + 8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{FaultConfig, SimStorage};

    #[test]
    fn flushes_reuse_their_buffers() {
        let disk = SimStorage::new(4, FaultConfig::default());
        let mut w = WalWriter::create(&disk, 1, None, Buffers::default()).unwrap();
        let big = [WriteOp::put("k", "v".repeat(100 << 10))];
        let first = w.log.pending.lock().buf.as_ptr();
        w.append(1, &big);
        w.flush().unwrap();
        w.append(2, &big);
        w.flush().unwrap();
        // Each flush returns its buffer, so appends alternate between two
        // buffers and are back on the first.
        assert_eq!(w.log.pending.lock().buf.as_ptr(), first);
        w.append(3, &big);
        w.sync().unwrap();
        // The next segment takes both buffers with it.
        let second = w.log.pending.lock().buf.as_ptr();
        let mut next = WalWriter::create(&disk, 2, None, w.take_buffers()).unwrap();
        assert_eq!(next.log.pending.lock().buf.as_ptr(), second);
        next.append(4, &[WriteOp::put("k", "v")]);
        next.sync().unwrap();
        assert_eq!(next.log.pending.lock().buf.as_ptr(), first);
        let mut seen = Vec::new();
        replay(&disk, 2, 3, |seq, _| seen.push(seq)).unwrap();
        assert_eq!(seen, [4]);
        let mut seen = Vec::new();
        replay(&disk, 1, 0, |seq, _| seen.push(seq)).unwrap();
        assert_eq!(seen, [1, 2, 3]);
    }

    #[test]
    fn roundtrip_and_torn_tail() {
        let disk = SimStorage::new(3, FaultConfig::default());
        let mut w = WalWriter::create(&disk, 1, None, Buffers::default()).unwrap();
        w.append(1, &[WriteOp::put("a", "1"), WriteOp::delete("b")]);
        w.append(3, &[WriteOp::put("c", "3")]);
        w.sync().unwrap();
        w.append(4, &[WriteOp::put("d", "4")]);
        w.flush().unwrap();
        let mut seen = Vec::new();
        let r = replay(&disk, 1, 0, |seq, op| seen.push((seq, op))).unwrap();
        assert_eq!(r.last_seq, 4);
        assert_eq!(seen.len(), 4);
        assert_eq!(seen[1], (2, WriteOp::delete("b")));

        disk.crash();
        let mut seen = Vec::new();
        let r = replay(&disk, 1, 0, |seq, op| seen.push((seq, op))).unwrap();
        assert!(r.last_seq == 3 || r.last_seq == 4);
        let r = replay(&disk, 1, 2, |_, _| {}).unwrap();
        assert!(r.last_seq >= 3);
        let r = replay(&disk, 1, 7, |_, _| {}).unwrap();
        assert_eq!((r.last_seq, r.end), (7, End::Torn), "covered records are skipped up to the zero fill");

        let mut w = WalWriter::create(&disk, 2, None, Buffers::default()).unwrap();
        w.append(10, &[WriteOp::put("e", "5")]);
        w.sync().unwrap();
        let r = replay(&disk, 2, 5, |_, _| {}).unwrap();
        assert_eq!((r.last_seq, r.end), (5, End::Gap));
    }

    #[test]
    fn reused_segment_skips_old_records() {
        let disk = SimStorage::new(2, FaultConfig::default());
        let recycler = Recycler::open(&disk, 1).unwrap();
        let mut w = WalWriter::create(&disk, 1, recycler.take(), Buffers::default()).unwrap();
        for seq in 1..=20 {
            w.append(seq, &[WriteOp::put(format!("key{seq}"), "old value")]);
        }
        w.sync().unwrap();
        // Segment 1's checkpoint covers sequence numbers up to 20.
        recycler.retire(&disk, 1).unwrap();
        assert!(!disk.list().unwrap().contains(&segment_name(1)));

        let mut w = WalWriter::create(&disk, 2, recycler.take(), Buffers::default()).unwrap();
        w.append(21, &[WriteOp::put("a", "1")]);
        w.append(22, &[WriteOp::put("b", "2")]);
        w.sync().unwrap();
        let mut seen = Vec::new();
        let r = replay(&disk, 2, 20, |seq, op| seen.push((seq, op))).unwrap();
        assert_eq!(r.last_seq, 22);
        assert_eq!(seen, vec![(21, WriteOp::put("a", "1")), (22, WriteOp::put("b", "2"))]);

        // A segment from before the store opened is deleted, not kept.
        let recycler = Recycler::open(&disk, 3).unwrap();
        recycler.retire(&disk, 2).unwrap();
        assert!(recycler.take().is_none());
        assert!(!disk.list().unwrap().contains(&segment_name(2)));
    }

    /// A prepared segment is zeros for its whole length, joins the pool, and
    /// takes a log's records like any kept segment.
    #[test]
    fn prepared_segment_takes_records() {
        let disk = SimStorage::new(4, FaultConfig::default());
        let recycler = Recycler::open(&disk, 1).unwrap();
        let len = recycler.segment_len(3 * PREPARE_STEP / 2 + 5);
        assert_eq!(len % DIRECT_ALIGN as u64, 0);
        let mut steps = Vec::new();
        assert!(recycler.prepare(&disk, len, |written| {
            steps.push(written);
            true
        }));
        assert_eq!(steps, [PREPARE_STEP, len]);
        let name = recycler.take().expect("a prepared segment");
        let file = disk.open(&name, false).unwrap();
        assert_eq!(file.len().unwrap(), len);
        let mut data = vec![1u8; len as usize];
        file.read_exact_at(&mut data, 0).unwrap();
        assert!(data.iter().all(|&b| b == 0));

        let mut w = WalWriter::create(&disk, 1, Some(name), Buffers::default()).unwrap();
        for seq in 1..=300 {
            w.append(seq, &[WriteOp::put(format!("key{seq}"), vec![b'v'; 100])]);
            w.sync().unwrap();
        }
        assert_eq!(disk.open(&segment_name(1), false).unwrap().len().unwrap(), len, "writes stay within the prepared length");
        let mut seen = 0;
        let r = replay(&disk, 1, 0, |seq, _| {
            seen += 1;
            assert_eq!(seq, seen);
        })
        .unwrap();
        assert_eq!((r.last_seq, seen, r.end), (300, 300, End::Torn));
        // A longer segment kept raises the length of those prepared later.
        assert_eq!(recycler.segment_len(DIRECT_ALIGN as u64), len);
    }

    /// An abandoned preparation leaves nothing behind, and one cut short by
    /// a crash is removed when the store next opens.
    #[test]
    fn half_prepared_segments_are_removed() {
        let disk = SimStorage::new(5, FaultConfig::default());
        let recycler = Recycler::open(&disk, 1).unwrap();
        assert!(!recycler.prepare(&disk, 4 * PREPARE_STEP, |written| written < 2 * PREPARE_STEP));
        assert!(recycler.take().is_none());
        assert!(disk.list().unwrap().is_empty());

        disk.open(&preparing_name(7), true).unwrap().write_all_at(&[0; DIRECT_ALIGN], 0).unwrap();
        disk.sync_dir().unwrap();
        let recycler = Recycler::open(&disk, 1).unwrap();
        assert!(recycler.take().is_none());
        assert!(disk.list().unwrap().is_empty());
    }

    /// The pool holds what checkpoints retire first: a segment prepared
    /// once it is full is dropped.
    #[test]
    fn a_full_pool_drops_a_prepared_segment() {
        let disk = SimStorage::new(6, FaultConfig::default());
        let recycler = Recycler::open(&disk, 1).unwrap();
        for id in 1..=MAX_RECYCLED as u64 {
            let mut w = WalWriter::create(&disk, id, None, Buffers::default()).unwrap();
            w.append(id, &[WriteOp::put("k", "v")]);
            w.sync().unwrap();
            recycler.retire(&disk, id).unwrap();
        }
        assert!(recycler.prepare(&disk, PREPARE_STEP, |_| true));
        assert_eq!(disk.list().unwrap().len(), MAX_RECYCLED);
    }
}
