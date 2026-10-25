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
//! taking the lock that assigns sequence numbers. Replay stops at the first
//! record that is truncated, fails its checksum, or breaks the sequence-number
//! chain: that record and everything after it are the torn tail of the last
//! unsynced flush. A reused segment still holds its old records past the new
//! ones; their sequence numbers are all covered by a checkpoint, so replay
//! skips them.

use crate::error::Result;
use crate::storage::{File, Storage};
use crate::types::{Value, WriteOp};
use bytes::Bytes;
use parking_lot::{Condvar, Mutex, MutexGuard, lock_api::ArcMutexGuard};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const HEADER_LEN: usize = 8;
const KIND_PUT: u8 = 0;
const KIND_DELETE: u8 = 1;

pub fn segment_name(id: u64) -> String {
    format!("wal-{id:016x}.log")
}

fn recycled_name(n: u64) -> String {
    format!("recycled-{n:016x}.log")
}

/// Retired log segments kept for reuse.
///
/// A new segment written over a retired one keeps the file's length, so
/// fdatasync has no file size to journal, only data to write. On ext4 that
/// halves the cost of a synchronous write.
///
/// Only segments created since the store opened are kept. Once retired,
/// every record in one has a sequence number its checkpoint covers. A
/// segment replayed at recovery can hold records past a crash point, which
/// are not covered, so it is deleted instead.
pub struct Recycler {
    pool: Mutex<Pool>,
    /// The first segment created since the store opened.
    first_new: u64,
}

struct Pool {
    names: Vec<String>,
    next: u64,
}

/// Retired segments kept at most. Each memtable rotation takes one, and each
/// checkpoint returns one.
const MAX_RECYCLED: usize = 2;

impl Recycler {
    /// Adopts retired segments left in storage. A segment enters the pool
    /// only after its checkpoint is durable, so these hold covered records
    /// only. `first_new` is the id of the first segment this store creates.
    pub fn open(storage: &dyn Storage, first_new: u64) -> Result<Self> {
        let mut found: Vec<(u64, String)> = storage
            .list()?
            .into_iter()
            .filter_map(|name| {
                let n = u64::from_str_radix(name.strip_prefix("recycled-")?.strip_suffix(".log")?, 16).ok()?;
                Some((n, name))
            })
            .collect();
        found.sort_unstable();
        let next = found.last().map_or(0, |(n, _)| n + 1);
        let keep = found.len().min(MAX_RECYCLED);
        for (_, name) in found.drain(keep..) {
            storage.remove(&name)?;
        }
        Ok(Self { pool: Mutex::new(Pool { names: found.into_iter().map(|(_, name)| name).collect(), next }), first_new })
    }

    /// Retires segment `id`, whose checkpoint is durable: keeps it for reuse
    /// if it qualifies and the pool has room, and deletes it otherwise.
    pub fn retire(&self, storage: &dyn Storage, id: u64) -> Result<()> {
        let mut pool = self.pool.lock();
        if id >= self.first_new && pool.names.len() < MAX_RECYCLED {
            let name = recycled_name(pool.next);
            pool.next += 1;
            storage.rename(&segment_name(id), &name)?;
            pool.names.push(name);
        } else {
            storage.remove(&segment_name(id))?;
        }
        Ok(())
    }

    /// A retired segment to reuse, if any.
    pub fn take(&self) -> Option<String> {
        self.pool.lock().names.pop()
    }
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
    HEADER_LEN
        + 12
        + ops
            .iter()
            .map(|op| 1 + 4 + op.key.len() + op.value.as_put().map_or(0, |v| 4 + v.len()))
            .sum::<usize>()
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
    pub fn encode(&mut self, ops: &[WriteOp]) {
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

fn encode_ops(buf: &mut Vec<u8>, ops: &[WriteOp]) {
    for op in ops {
        match &op.value {
            Value::Put(v) => {
                buf.push(KIND_PUT);
                buf.extend_from_slice(&(op.key.len() as u32).to_le_bytes());
                buf.extend_from_slice(&op.key);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            Value::Delete => {
                buf.push(KIND_DELETE);
                buf.extend_from_slice(&(op.key.len() as u32).to_le_bytes());
                buf.extend_from_slice(&op.key);
            }
        }
    }
}

/// Appends records to one segment. Records accumulate in memory until
/// [`flush`](Self::flush) writes them or [`sync`](Self::sync) makes them durable.
pub struct WalWriter {
    id: u64,
    buf: Vec<u8>,
    /// Bytes handed to the file, written or in flight.
    written: u64,
    /// Held while writing to the file, so writes complete in offset order and
    /// a [`Flush`] made under the caller's lock completes before any later
    /// write.
    io: Arc<Mutex<()>>,
    log_sync: Arc<LogSync>,
}

/// How much of a segment is written and how much is durable. Synchronous
/// writers share it: one fsync covers every record written before it began,
/// so writers that arrive during an fsync share the next one.
pub struct LogSync {
    file: Arc<dyn File>,
    /// Bytes whose writes have completed, all of them contiguous from the
    /// start of the segment.
    completed: AtomicU64,
    synced: Mutex<SyncState>,
    /// Signaled when an fsync ends.
    synced_cv: Condvar,
}

#[derive(Default)]
struct SyncState {
    /// Bytes known durable.
    len: u64,
    /// Whether an fsync is running.
    syncing: bool,
    /// A failed fsync may have dropped dirty pages that a later fsync would
    /// not report, so after one, no sync of this segment succeeds.
    failed: bool,
}

impl LogSync {
    /// Returns once the first `end` bytes of the segment are durable. The
    /// caller's own write of those bytes must have completed.
    ///
    /// One caller at a time runs an fsync; the rest wait for it to end, then
    /// return if it covered them. A waiter checks after every fsync, so none
    /// waits behind an fsync it does not need.
    pub fn sync_to(&self, end: u64) -> Result<()> {
        let mut state = self.synced.lock();
        loop {
            if state.failed {
                return Err(std::io::Error::other("an earlier log fsync failed").into());
            }
            if state.len >= end {
                return Ok(());
            }
            if state.syncing {
                self.synced_cv.wait(&mut state);
                continue;
            }
            let target = self.completed.load(Ordering::Acquire);
            if target < end {
                // An earlier write failed, leaving a gap before these bytes.
                return Err(std::io::Error::other("log write failed").into());
            }
            state.syncing = true;
            let r = MutexGuard::unlocked(&mut state, || self.file.sync_data());
            state.syncing = false;
            match r {
                Ok(()) => state.len = state.len.max(target),
                Err(_) => state.failed = true,
            }
            self.synced_cv.notify_all();
            r?;
        }
    }

    /// Records a completed write at `offset`. Writes complete in offset
    /// order, so after a failed one, later ones leave `completed` below them.
    fn complete(&self, offset: u64, len: usize) {
        let _ = self.completed.compare_exchange(offset, offset + len as u64, Ordering::Release, Ordering::Relaxed);
    }
}

/// Buffered records taken from a [`WalWriter`] to write without holding the
/// lock that guards appends. It holds the segment's I/O lock, so writes that
/// come after it wait for it.
pub struct Flush {
    buf: Vec<u8>,
    offset: u64,
    log_sync: Arc<LogSync>,
    _io: ArcMutexGuard<parking_lot::RawMutex, ()>,
}

impl Flush {
    pub fn write(self) -> Result<()> {
        self.log_sync.file.write_all_at(&self.buf, self.offset)?;
        self.log_sync.complete(self.offset, self.buf.len());
        Ok(())
    }
}

impl WalWriter {
    /// Creates segment `id`, reusing the retired segment `reuse` if given,
    /// and makes its directory entry durable.
    pub fn create(storage: &dyn Storage, id: u64, reuse: Option<String>) -> Result<Self> {
        let file = match reuse {
            Some(old) => {
                storage.rename(&old, &segment_name(id))?;
                storage.open(&segment_name(id), false)?
            }
            None => storage.open(&segment_name(id), true)?,
        };
        storage.sync_dir()?;
        let log_sync = Arc::new(LogSync { file, completed: AtomicU64::new(0), synced: Mutex::default(), synced_cv: Condvar::new() });
        Ok(Self { id, buf: Vec::with_capacity(1 << 20), written: 0, io: Arc::new(Mutex::new(())), log_sync })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn append(&mut self, first_seq: u64, ops: &[WriteOp]) {
        encode(&mut self.buf, first_seq, ops);
    }

    /// Appends a record encoded ahead of time.
    pub fn append_body(&mut self, body: &Body, first_seq: u64) {
        body.append_to(&mut self.buf, first_seq);
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Total bytes appended, flushed or not.
    pub fn len(&self) -> u64 {
        self.written + self.buf.len() as u64
    }

    /// The segment's durability, for syncing without the caller's lock.
    pub fn log_sync(&self) -> Arc<LogSync> {
        self.log_sync.clone()
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.start_flush().write()?;
        }
        Ok(())
    }

    /// Makes every appended record durable.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        // Taking the I/O lock waits out flushes still in flight.
        drop(self.io.lock());
        self.log_sync.sync_to(self.written)
    }

    /// Takes the buffered records for the caller to write after releasing
    /// its lock. Call [`Flush::write`] promptly: later writes of this
    /// segment wait for it.
    pub fn start_flush(&mut self) -> Flush {
        let io = self.io.lock_arc();
        let offset = self.written;
        // A synchronous writer flushes little; copying that keeps the
        // buffer's capacity.
        let buf = if self.buf.len() < 64 << 10 {
            let small = self.buf.clone();
            self.buf.clear();
            small
        } else {
            std::mem::replace(&mut self.buf, Vec::with_capacity(1 << 20))
        };
        self.written += buf.len() as u64;
        Flush { buf, offset, log_sync: self.log_sync.clone(), _io: io }
    }
}

/// How replay of a segment ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// Every record was valid.
    Clean,
    /// A truncated or corrupt record: the torn tail of an unsynced write.
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
pub fn replay(
    storage: &dyn Storage,
    id: u64,
    after_seq: u64,
    mut apply: impl FnMut(u64, WriteOp),
) -> Result<Replay> {
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
    fn roundtrip_and_torn_tail() {
        let disk = SimStorage::new(3, FaultConfig::default());
        let mut w = WalWriter::create(&disk, 1, None).unwrap();
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
        assert_eq!((r.last_seq, r.end), (7, End::Clean), "covered records are skipped");

        let mut w = WalWriter::create(&disk, 2, None).unwrap();
        w.append(10, &[WriteOp::put("e", "5")]);
        w.sync().unwrap();
        let r = replay(&disk, 2, 5, |_, _| {}).unwrap();
        assert_eq!((r.last_seq, r.end), (5, End::Gap));
    }

    #[test]
    fn reused_segment_skips_old_records() {
        let disk = SimStorage::new(2, FaultConfig::default());
        let recycler = Recycler::open(&disk, 1).unwrap();
        let mut w = WalWriter::create(&disk, 1, recycler.take()).unwrap();
        for seq in 1..=20 {
            w.append(seq, &[WriteOp::put(format!("key{seq}"), "old value")]);
        }
        w.sync().unwrap();
        // Segment 1's checkpoint covers sequence numbers up to 20.
        recycler.retire(&disk, 1).unwrap();
        assert!(!disk.list().unwrap().contains(&segment_name(1)));

        let mut w = WalWriter::create(&disk, 2, recycler.take()).unwrap();
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
}
