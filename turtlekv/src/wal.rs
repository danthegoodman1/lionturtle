//! The write-ahead log.
//!
//! Each memtable owns one log segment, `wal-<id>.log`, so a segment is deleted
//! as soon as the checkpoint that absorbs its memtable is durable. A segment is
//! a sequence of records, one per write batch:
//!
//! ```text
//! crc32c: u32 | len: u32 | first_seq: u64 | count: u32 | op*
//! op = kind: u8 | key_len: u32 | key | (value_len: u32 | value)?   (value only for puts)
//! ```
//!
//! The checksum covers everything after itself. Replay stops at the first
//! record that is truncated, fails its checksum, or breaks the sequence-number
//! chain: that record and everything after it are the torn tail of the last
//! unsynced flush.

use crate::error::Result;
use crate::storage::{File, Storage};
use crate::types::{Value, WriteOp};
use bytes::Bytes;
use std::sync::Arc;

const HEADER_LEN: usize = 8;
const KIND_PUT: u8 = 0;
const KIND_DELETE: u8 = 1;

pub fn segment_name(id: u64) -> String {
    format!("wal-{id:016x}.log")
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

pub fn encode(buf: &mut Vec<u8>, first_seq: u64, ops: &[WriteOp]) {
    let start = buf.len();
    buf.extend_from_slice(&[0; HEADER_LEN]);
    buf.extend_from_slice(&first_seq.to_le_bytes());
    buf.extend_from_slice(&(ops.len() as u32).to_le_bytes());
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
    let len = (buf.len() - start - HEADER_LEN) as u32;
    let crc = crc32c::crc32c_append(crc32c::crc32c(&len.to_le_bytes()), &buf[start + HEADER_LEN..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
    buf[start + 4..start + 8].copy_from_slice(&len.to_le_bytes());
}

/// Appends records to one segment. Records accumulate in memory until
/// [`flush`](Self::flush) writes them or [`sync`](Self::sync) makes them durable.
pub struct WalWriter {
    id: u64,
    file: Arc<dyn File>,
    buf: Vec<u8>,
    /// Bytes written to the file.
    written: u64,
    /// Bytes known durable.
    synced: u64,
}

impl WalWriter {
    /// Creates segment `id` and makes its directory entry durable.
    pub fn create(storage: &dyn Storage, id: u64) -> Result<Self> {
        let file = storage.open(&segment_name(id), true)?;
        storage.sync_dir()?;
        Ok(Self { id, file, buf: Vec::with_capacity(1 << 20), written: 0, synced: 0 })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn append(&mut self, first_seq: u64, ops: &[WriteOp]) {
        encode(&mut self.buf, first_seq, ops);
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Total bytes appended, flushed or not.
    pub fn len(&self) -> u64 {
        self.written + self.buf.len() as u64
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all_at(&self.buf, self.written)?;
            self.written += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        if self.synced < self.written {
            self.file.sync_data()?;
            self.synced = self.written;
        }
        Ok(())
    }

    /// Takes the buffered records, leaving the buffer empty, so a caller can
    /// write them without holding the lock that guards appends.
    pub fn take_buffer(&mut self) -> (Vec<u8>, u64) {
        let offset = self.written;
        let buf = std::mem::replace(&mut self.buf, Vec::with_capacity(1 << 20));
        self.written += buf.len() as u64;
        (buf, offset)
    }

    pub fn file(&self) -> &Arc<dyn File> {
        &self.file
    }

    pub fn mark_synced(&mut self, upto: u64) {
        self.synced = self.synced.max(upto);
    }

    pub fn written(&self) -> u64 {
        self.written
    }
}

/// How replay of a segment ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// Every record was valid.
    Clean,
    /// A truncated or corrupt record: the torn tail of an unsynced write.
    Torn,
    /// A valid record that skips sequence numbers: the segment holds writes
    /// from after an earlier crash point, which recovery must discard.
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
/// A segment can end torn and still be followed by valid segments: recovery
/// leaves a torn segment in place and logs new writes to the next one, whose
/// sequence numbers continue from the last valid record.
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
    let first_seq = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let count = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    let mut ops = Vec::with_capacity(count.min(1 << 16));
    let mut p = body_start + 12;
    let end = body_start + len;
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
    Some((first_seq, ops, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{FaultConfig, SimStorage};

    #[test]
    fn roundtrip_and_torn_tail() {
        let disk = SimStorage::new(3, FaultConfig::default());
        let mut w = WalWriter::create(&disk, 1).unwrap();
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

        let mut w = WalWriter::create(&disk, 2).unwrap();
        w.append(10, &[WriteOp::put("e", "5")]);
        w.sync().unwrap();
        let r = replay(&disk, 2, 5, |_, _| {}).unwrap();
        assert_eq!((r.last_seq, r.end), (5, End::Gap));
    }
}
