//! An in-memory disk with crash and fault semantics.
//!
//! Every write is recorded with the bytes it replaced. [`SimStorage::crash`]
//! then decides, sector by sector and newest write first, which unsynced writes
//! reached the media, producing any outcome a real device could: lost writes,
//! torn writes, and lost directory operations. Reads and writes can also fail,
//! and reads can return flipped bits, with configurable probabilities.

use super::rng::Rng;
use crate::storage::{File, Storage};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::io;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// Probability that a read fails.
    pub read_error: f64,
    /// Probability that a read returns one flipped bit.
    pub read_corruption: f64,
    /// File classes whose reads may be corrupted. Recovery tells a torn log
    /// tail or superblock from a valid one by checksum, so corrupting those
    /// reads would fabricate data loss no real device produces.
    pub corrupt_classes: Vec<String>,
    /// Probability that a write fails without writing.
    pub write_error: f64,
    /// Probability that a sync fails.
    pub sync_error: f64,
    /// Granularity at which a crash can tear a write.
    pub sector_size: u64,
    /// Probability that an unsynced sector write survives a crash.
    pub crash_keep_sector: f64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            read_error: 0.0,
            read_corruption: 0.0,
            corrupt_classes: vec!["pages".to_string()],
            write_error: 0.0,
            sync_error: 0.0,
            sector_size: 512,
            crash_keep_sector: 0.5,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClassStats {
    pub reads: u64,
    pub read_bytes: u64,
    pub writes: u64,
    pub write_bytes: u64,
    pub syncs: u64,
}

/// I/O totals keyed by file class: the file name up to its first `-` or `.`.
#[derive(Clone, Debug, Default)]
pub struct IoStats {
    pub by_class: BTreeMap<String, ClassStats>,
}

impl IoStats {
    pub fn class(&self, class: &str) -> ClassStats {
        self.by_class.get(class).cloned().unwrap_or_default()
    }

    pub fn total_write_bytes(&self) -> u64 {
        self.by_class.values().map(|c| c.write_bytes).sum()
    }

    pub fn total_read_bytes(&self) -> u64 {
        self.by_class.values().map(|c| c.read_bytes).sum()
    }
}

fn class_of(name: &str) -> String {
    name.split(['-', '.']).next().unwrap_or(name).to_string()
}

struct Undo {
    offset: u64,
    len: u64,
    /// The bytes this write replaced, up to the file length before it.
    old: Vec<u8>,
}

#[derive(Default)]
struct Inode {
    data: Vec<u8>,
    durable_len: u64,
    undo: Vec<Undo>,
}

enum DirOp {
    Create { name: String },
    Remove { name: String, ino: u64 },
    Rename { from: String, to: String, ino: u64, replaced: Option<u64> },
}

struct State {
    rng: Rng,
    faults: FaultConfig,
    names: BTreeMap<String, u64>,
    inodes: BTreeMap<u64, Inode>,
    next_ino: u64,
    dir_log: Vec<DirOp>,
    /// Bumped by every crash; handles from an older generation stop working.
    generation: u64,
    /// Mutating operations left before the disk goes down.
    crash_countdown: Option<u64>,
    down: bool,
    stats: IoStats,
}

impl State {
    fn check_up(&self, generation: u64) -> io::Result<()> {
        if self.down || generation != self.generation {
            return Err(io::Error::other("sim: disk is down"));
        }
        Ok(())
    }

    /// Counts a mutating operation against the crash countdown.
    fn tick_mutation(&mut self) -> io::Result<()> {
        if let Some(left) = self.crash_countdown.as_mut() {
            if *left == 0 {
                self.down = true;
                return Err(io::Error::other("sim: crashed"));
            }
            *left -= 1;
        }
        Ok(())
    }
}

/// A simulated disk. Clones share the same disk.
#[derive(Clone)]
pub struct SimStorage {
    state: Arc<Mutex<State>>,
}

impl SimStorage {
    pub fn new(seed: u64, faults: FaultConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                rng: Rng::new(seed),
                faults,
                names: BTreeMap::new(),
                inodes: BTreeMap::new(),
                next_ino: 1,
                dir_log: Vec::new(),
                generation: 0,
                crash_countdown: None,
                down: false,
                stats: IoStats::default(),
            })),
        }
    }

    pub fn set_faults(&self, faults: FaultConfig) {
        self.state.lock().faults = faults;
    }

    pub fn faults(&self) -> FaultConfig {
        self.state.lock().faults.clone()
    }

    /// Takes the disk down after `ops` more mutating operations.
    pub fn arm_crash(&self, ops: u64) {
        self.state.lock().crash_countdown = Some(ops);
    }

    pub fn disarm_crash(&self) {
        self.state.lock().crash_countdown = None;
    }

    pub fn is_down(&self) -> bool {
        self.state.lock().down
    }

    pub fn stats(&self) -> IoStats {
        self.state.lock().stats.clone()
    }

    pub fn reset_stats(&self) {
        self.state.lock().stats = IoStats::default();
    }

    /// Bytes stored across all files.
    /// Bytes a sparse file system would allocate: 4 KiB blocks holding any
    /// nonzero byte.
    pub fn disk_usage(&self) -> u64 {
        let state = self.state.lock();
        state
            .names
            .values()
            .map(|ino| state.inodes[ino].data.chunks(4096).filter(|c| c.iter().any(|&b| b != 0)).count() as u64 * 4096)
            .sum()
    }

    /// Loses power: discards a random subset of unsynced writes (tearing them
    /// at sector granularity) and a random suffix of unsynced directory
    /// operations, then brings the disk back up. Handles opened before the
    /// crash stop working.
    pub fn crash(&self) {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let keep_ops = state.rng.range(0, state.dir_log.len() as u64) as usize;
        for op in state.dir_log.drain(keep_ops..).rev() {
            match op {
                DirOp::Create { name } => {
                    state.names.remove(&name);
                }
                DirOp::Remove { name, ino } => {
                    state.names.insert(name, ino);
                }
                DirOp::Rename { from, to, ino, replaced } => {
                    match replaced {
                        Some(r) => state.names.insert(to, r),
                        None => state.names.remove(&to),
                    };
                    state.names.insert(from, ino);
                }
            }
        }
        state.dir_log.clear();
        let live: HashSet<u64> = state.names.values().copied().collect();
        state.inodes.retain(|ino, _| live.contains(ino));

        let sector = state.faults.sector_size;
        let keep_p = state.faults.crash_keep_sector;
        for inode in state.inodes.values_mut() {
            let mut kept: HashSet<u64> = HashSet::new();
            let mut new_len = inode.durable_len;
            for undo in inode.undo.drain(..).rev() {
                let end = undo.offset + undo.len;
                let mut s = undo.offset / sector;
                while s * sector < end {
                    let lo = (s * sector).max(undo.offset);
                    let hi = ((s + 1) * sector).min(end);
                    if !kept.contains(&s) {
                        if state.rng.chance(keep_p) {
                            kept.insert(s);
                            new_len = new_len.max(hi);
                        } else {
                            for pos in lo..hi {
                                let i = (pos - undo.offset) as usize;
                                inode.data[pos as usize] = undo.old.get(i).copied().unwrap_or(0);
                            }
                        }
                    }
                    s += 1;
                }
            }
            inode.data.resize(new_len as usize, 0);
            inode.durable_len = new_len;
        }
        state.generation += 1;
        state.down = false;
        state.crash_countdown = None;
    }
}

impl Storage for SimStorage {
    fn open(&self, name: &str, create: bool) -> io::Result<Arc<dyn File>> {
        let mut state = self.state.lock();
        let generation = state.generation;
        state.check_up(generation)?;
        let ino = match state.names.get(name) {
            Some(&ino) => ino,
            None if create => {
                state.tick_mutation()?;
                let ino = state.next_ino;
                state.next_ino += 1;
                state.inodes.insert(ino, Inode::default());
                state.names.insert(name.to_string(), ino);
                state.dir_log.push(DirOp::Create { name: name.to_string() });
                ino
            }
            None => return Err(io::Error::new(io::ErrorKind::NotFound, name.to_string())),
        };
        Ok(Arc::new(SimFile {
            state: self.state.clone(),
            ino,
            generation,
            class: class_of(name),
        }))
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        let mut state = self.state.lock();
        let generation = state.generation;
        state.check_up(generation)?;
        state.tick_mutation()?;
        match state.names.remove(name) {
            Some(ino) => {
                state.dir_log.push(DirOp::Remove { name: name.to_string(), ino });
                Ok(())
            }
            None => Err(io::Error::new(io::ErrorKind::NotFound, name.to_string())),
        }
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut state = self.state.lock();
        let generation = state.generation;
        state.check_up(generation)?;
        state.tick_mutation()?;
        let ino = state.names.remove(from).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, from.to_string()))?;
        let replaced = state.names.insert(to.to_string(), ino);
        state.dir_log.push(DirOp::Rename { from: from.to_string(), to: to.to_string(), ino, replaced });
        Ok(())
    }

    fn list(&self) -> io::Result<Vec<String>> {
        let state = self.state.lock();
        state.check_up(state.generation)?;
        Ok(state.names.keys().cloned().collect())
    }

    fn sync_dir(&self) -> io::Result<()> {
        let mut state = self.state.lock();
        let generation = state.generation;
        state.check_up(generation)?;
        state.tick_mutation()?;
        state.dir_log.clear();
        let live: HashSet<u64> = state.names.values().copied().collect();
        state.inodes.retain(|ino, _| live.contains(ino));
        Ok(())
    }
}

struct SimFile {
    state: Arc<Mutex<State>>,
    ino: u64,
    generation: u64,
    class: String,
}

impl File for SimFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        state.check_up(self.generation)?;
        if state.rng.chance(state.faults.read_error) {
            return Err(io::Error::other("sim: read error"));
        }
        let inode = state
            .inodes
            .get(&self.ino)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim: file removed"))?;
        let end = offset as usize + buf.len();
        if end > inode.data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "sim: read past end"));
        }
        buf.copy_from_slice(&inode.data[offset as usize..end]);
        if !buf.is_empty()
            && state.faults.corrupt_classes.contains(&self.class)
            && state.rng.chance(state.faults.read_corruption)
        {
            let bit = state.rng.below(buf.len() as u64 * 8);
            buf[(bit / 8) as usize] ^= 1 << (bit % 8);
        }
        let stats = state.stats.by_class.entry(self.class.clone()).or_default();
        stats.reads += 1;
        stats.read_bytes += buf.len() as u64;
        Ok(())
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        state.check_up(self.generation)?;
        state.tick_mutation()?;
        if state.rng.chance(state.faults.write_error) {
            return Err(io::Error::other("sim: write error"));
        }
        let inode = state
            .inodes
            .get_mut(&self.ino)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim: file removed"))?;
        let start = offset as usize;
        let end = start + buf.len();
        let old_end = end.min(inode.data.len()).max(start.min(inode.data.len()));
        let old = inode.data[start.min(inode.data.len())..old_end].to_vec();
        inode.undo.push(Undo { offset, len: buf.len() as u64, old });
        if end > inode.data.len() {
            inode.data.resize(end, 0);
        }
        inode.data[start..end].copy_from_slice(buf);
        let stats = state.stats.by_class.entry(self.class.clone()).or_default();
        stats.writes += 1;
        stats.write_bytes += buf.len() as u64;
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        state.check_up(self.generation)?;
        state.tick_mutation()?;
        if state.rng.chance(state.faults.sync_error) {
            return Err(io::Error::other("sim: sync error"));
        }
        if let Some(inode) = state.inodes.get_mut(&self.ino) {
            inode.undo.clear();
            inode.durable_len = inode.data.len() as u64;
        }
        state.stats.by_class.entry(self.class.clone()).or_default().syncs += 1;
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        let state = self.state.lock();
        state.check_up(self.generation)?;
        Ok(state.inodes.get(&self.ino).map_or(0, |inode| inode.data.len() as u64))
    }

    /// Zeroes the range like an unsynced write, so a crash may undo it.
    fn punch_hole(&self, offset: u64, len: u64) -> io::Result<()> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        state.check_up(self.generation)?;
        state.tick_mutation()?;
        if state.rng.chance(state.faults.write_error) {
            return Err(io::Error::other("sim: punch error"));
        }
        let inode = state
            .inodes
            .get_mut(&self.ino)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim: file removed"))?;
        let start = (offset as usize).min(inode.data.len());
        let end = (offset as usize).saturating_add(len as usize).min(inode.data.len());
        let old = inode.data[start..end].to_vec();
        inode.undo.push(Undo { offset: start as u64, len: (end - start) as u64, old });
        inode.data[start..end].fill(0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synced_data_survives_crash() {
        let disk = SimStorage::new(7, FaultConfig::default());
        let f = disk.open("a", true).unwrap();
        disk.sync_dir().unwrap();
        f.write_all_at(b"hello", 0).unwrap();
        f.sync_data().unwrap();
        f.write_all_at(b"world", 5).unwrap();
        disk.crash();
        let f = disk.open("a", false).unwrap();
        let mut buf = [0u8; 5];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
        assert!(f.len().unwrap() == 5 || f.len().unwrap() == 10);
    }

    #[test]
    fn unsynced_create_may_vanish() {
        let mut vanished = 0;
        for seed in 0..64 {
            let disk = SimStorage::new(seed, FaultConfig::default());
            disk.open("a", true).unwrap();
            disk.crash();
            if disk.open("a", false).is_err() {
                vanished += 1;
            }
        }
        assert!(vanished > 0 && vanished < 64);
    }

    #[test]
    fn stale_handles_fail_after_crash() {
        let disk = SimStorage::new(1, FaultConfig::default());
        let f = disk.open("a", true).unwrap();
        disk.sync_dir().unwrap();
        disk.crash();
        assert!(f.write_all_at(b"x", 0).is_err());
    }
}
