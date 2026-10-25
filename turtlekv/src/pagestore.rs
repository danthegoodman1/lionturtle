//! Page arenas and the superblock.
//!
//! Pages live in one file per power-of-two size class, `pages-<class>.dat`,
//! where slot `i` of class `c` starts at `i * (4 KiB << c)`. A page occupies
//! the smallest class that fits it; the unwritten tail of its slot stays a
//! hole in the sparse file.
//!
//! Free space is never persisted. At open, every slot that the recovered
//! checkpoint does not reference is free; afterwards a slot is freed only once
//! a newer checkpoint no longer references it and no reader can reach it.
//!
//! The superblock file holds two slots written alternately, so a torn write
//! of one leaves the other intact; open picks the valid slot with the highest
//! checkpoint id.

use crate::error::{Result, corruption};
use crate::page::PageId;
use crate::storage::{File, Storage};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

pub const MIN_SLOT: u64 = 4096;
pub const CLASSES: usize = 20;

/// Where a page image lives on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PagePtr {
    pub id: PageId,
    pub class: u8,
    pub slot: u32,
    /// Length of the page image.
    pub len: u32,
    /// Offset of the metadata region within the image.
    pub meta_off: u32,
}

impl PagePtr {
    pub const ENCODED_LEN: usize = 21;

    pub fn offset(&self) -> u64 {
        self.slot as u64 * slot_size(self.class)
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.to_le_bytes());
        out.push(self.class);
        out.extend_from_slice(&self.slot.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.meta_off.to_le_bytes());
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < Self::ENCODED_LEN {
            return None;
        }
        let ptr = Self {
            id: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            class: b[8],
            slot: u32::from_le_bytes(b[9..13].try_into().unwrap()),
            len: u32::from_le_bytes(b[13..17].try_into().unwrap()),
            meta_off: u32::from_le_bytes(b[17..21].try_into().unwrap()),
        };
        (ptr.class < CLASSES as u8
            && ptr.meta_off <= ptr.len
            && ptr.len as u64 <= slot_size(ptr.class))
        .then_some(ptr)
    }
}

pub fn slot_size(class: u8) -> u64 {
    MIN_SLOT << class
}

pub fn class_for(len: u64) -> Option<u8> {
    (0..CLASSES as u8).find(|&c| slot_size(c) >= len)
}

fn arena_name(class: u8) -> String {
    format!("pages-{class:02}.dat")
}

#[derive(Default)]
struct ClassAlloc {
    /// One past the highest slot ever handed out.
    next: u32,
    free: BTreeSet<u32>,
}

pub struct PageStore {
    storage: Arc<dyn Storage>,
    /// Arena files by size class. Reads find them without locking.
    files: Vec<OnceLock<Arc<dyn File>>>,
    /// Serializes arena creation.
    create: Mutex<()>,
    alloc: Mutex<Vec<ClassAlloc>>,
    /// Freed slots whose disk space has not been released yet. They become
    /// allocatable in [`PageStore::reclaim`].
    freed: Mutex<Vec<PagePtr>>,
}

impl PageStore {
    /// Opens the arenas, treating every slot outside `live` as free.
    pub fn open(storage: Arc<dyn Storage>, live: &[PagePtr]) -> Result<Self> {
        let files: Vec<OnceLock<Arc<dyn File>>> = (0..CLASSES).map(|_| OnceLock::new()).collect();
        let mut alloc: Vec<ClassAlloc> = (0..CLASSES).map(|_| ClassAlloc::default()).collect();
        let names = storage.list()?;
        for class in 0..CLASSES as u8 {
            if names.contains(&arena_name(class)) {
                let file = storage.open(&arena_name(class), false)?;
                file.advise_random();
                let slots = file.len()?.div_ceil(slot_size(class));
                alloc[class as usize].next = slots as u32;
                let _ = files[class as usize].set(file);
            }
        }
        let mut used: Vec<BTreeSet<u32>> = vec![BTreeSet::new(); CLASSES];
        for ptr in live {
            let a = &mut alloc[ptr.class as usize];
            a.next = a.next.max(ptr.slot + 1);
            if !used[ptr.class as usize].insert(ptr.slot) {
                return Err(corruption(format!("slot {}:{} referenced twice", ptr.class, ptr.slot)));
            }
        }
        for (class, a) in alloc.iter_mut().enumerate() {
            a.free = (0..a.next).filter(|s| !used[class].contains(s)).collect();
        }
        Ok(Self { storage, files, create: Mutex::new(()), alloc: Mutex::new(alloc), freed: Mutex::new(Vec::new()) })
    }

    /// Reserves a slot for a page image of `len` bytes.
    pub fn allocate(&self, id: PageId, len: u64, meta_off: u32) -> Result<PagePtr> {
        let class = class_for(len)
            .ok_or_else(|| crate::Error::InvalidArgument(format!("page of {len} bytes too large")))?;
        let mut alloc = self.alloc.lock();
        let a = &mut alloc[class as usize];
        let slot = match a.free.pop_first() {
            Some(slot) => slot,
            None => {
                a.next += 1;
                a.next - 1
            }
        };
        if std::env::var_os("TURTLEKV_DEBUG").is_some() {
            eprintln!("alloc page {id} -> {class}:{slot}");
        }
        Ok(PagePtr { id, class, slot, len: len as u32, meta_off })
    }

    /// Returns slots to the free pool.
    /// Releases the disk space of every free slot, which may still hold
    /// pages from before a restart. Call only on a store opened with the
    /// complete set of live pages.
    pub fn punch_free(&self) {
        let alloc = self.alloc.lock();
        for (class, a) in alloc.iter().enumerate() {
            let Some(file) = self.files[class].get() else { continue };
            let size = slot_size(class as u8);
            let mut slots = a.free.iter().copied().peekable();
            while let Some(first) = slots.next() {
                let mut last = first;
                while slots.next_if(|&s| s == last + 1).is_some() {
                    last += 1;
                }
                let _ = file.punch_hole(first as u64 * size, (last - first + 1) as u64 * size);
            }
        }
    }

    /// Queues slots for release. This is cheap, so any thread may call it;
    /// [`PageStore::reclaim`] does the I/O.
    pub fn free(&self, ptrs: impl IntoIterator<Item = PagePtr>) {
        self.freed.lock().extend(ptrs);
    }

    /// Releases the disk space of freed slots, then makes them allocatable.
    /// Punching before the slot is reusable keeps a hole from landing on a
    /// newer page. A failed punch only leaves the space allocated.
    pub fn reclaim(&self) {
        let freed = std::mem::take(&mut *self.freed.lock());
        if freed.is_empty() {
            return;
        }
        for ptr in &freed {
            if let Ok((file, _)) = self.file(ptr.class) {
                let _ = file.punch_hole(ptr.offset(), slot_size(ptr.class));
            }
        }
        let mut alloc = self.alloc.lock();
        for ptr in freed {
            if std::env::var_os("TURTLEKV_DEBUG").is_some() {
                eprintln!("free page {} at {}:{}", ptr.id, ptr.class, ptr.slot);
            }
            let inserted = alloc[ptr.class as usize].free.insert(ptr.slot);
            debug_assert!(inserted, "double free of {ptr:?}");
        }
    }

    /// Opens or creates the arena for `class`. Returns the file and whether it
    /// was created, in which case the directory needs a sync before a
    /// checkpoint may reference it.
    fn file(&self, class: u8) -> Result<(&Arc<dyn File>, bool)> {
        let slot = &self.files[class as usize];
        if let Some(f) = slot.get() {
            return Ok((f, false));
        }
        let _g = self.create.lock();
        if let Some(f) = slot.get() {
            return Ok((f, false));
        }
        let f = self.storage.open(&arena_name(class), true)?;
        f.advise_random();
        Ok((slot.get_or_init(|| f), true))
    }

    /// Writes a page image to its slot. Returns true if this created an arena
    /// file.
    pub fn write(&self, ptr: &PagePtr, image: &[u8]) -> Result<bool> {
        debug_assert_eq!(image.len(), ptr.len as usize);
        let (file, created) = self.file(ptr.class)?;
        file.write_all_at(image, ptr.offset())?;
        file.start_writeback(ptr.offset(), image.len() as u64);
        Ok(created)
    }

    /// Reads `len` bytes at `offset` within the page.
    pub fn read(&self, ptr: &PagePtr, offset: u64, len: u64) -> Result<Bytes> {
        if offset + len > ptr.len as u64 {
            return Err(corruption("read past end of page"));
        }
        let (file, _) = self.file(ptr.class)?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact_at(&mut buf, ptr.offset() + offset)?;
        Ok(Bytes::from(buf))
    }

    /// Starts reading `len` bytes at `offset` within the page into the page
    /// cache; see [`File::prefetch`].
    pub fn prefetch(&self, ptr: &PagePtr, offset: u64, len: u64) {
        if let Some(file) = self.files[ptr.class as usize].get() {
            file.prefetch(ptr.offset() + offset, len);
        }
    }

    /// Syncs every arena file.
    pub fn sync(&self) -> Result<()> {
        for f in self.files.iter().filter_map(OnceLock::get) {
            f.sync_data()?;
        }
        Ok(())
    }

    pub fn sync_dir(&self) -> Result<()> {
        Ok(self.storage.sync_dir()?)
    }

    /// Slots in use per class, for leak checks.
    pub fn allocated(&self) -> Vec<(u8, u32)> {
        let freed: BTreeSet<(u8, u32)> = self.freed.lock().iter().map(|p| (p.class, p.slot)).collect();
        let alloc = self.alloc.lock();
        let mut out = Vec::new();
        for (class, a) in alloc.iter().enumerate() {
            for slot in 0..a.next {
                if !a.free.contains(&slot) && !freed.contains(&(class as u8, slot)) {
                    out.push((class as u8, slot));
                }
            }
        }
        out
    }

    /// Bytes of address space the arenas span.
    pub fn arena_bytes(&self) -> u64 {
        let alloc = self.alloc.lock();
        alloc.iter().enumerate().map(|(c, a)| a.next as u64 * slot_size(c as u8)).sum()
    }
}

const SUPERBLOCK: &str = "superblock";
const SUPER_SLOT: u64 = 4096;
const SUPER_MAGIC: u64 = 0x544b_565f_5355_5052;

/// The durable root of the store: which checkpoint to load and where the log
/// picks up.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Superblock {
    pub checkpoint_id: u64,
    /// Every op with a sequence number up to this is in the checkpoint.
    pub last_seq: u64,
    /// Every memtable with an id up to this is in the checkpoint.
    pub last_memtable_id: u64,
    pub next_page_id: PageId,
    /// The root page and its height (0 for a leaf), or `None` for an empty tree.
    pub root: Option<(PagePtr, u32)>,
}

impl Superblock {
    fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        for v in [self.checkpoint_id, self.last_seq, self.last_memtable_id, self.next_page_id] {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        match &self.root {
            None => payload.push(0),
            Some((ptr, height)) => {
                payload.push(1);
                payload.extend_from_slice(&height.to_le_bytes());
                ptr.encode(&mut payload);
            }
        }
        let mut out = Vec::with_capacity(SUPER_SLOT as usize);
        out.extend_from_slice(&SUPER_MAGIC.to_le_bytes());
        out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        out.resize(SUPER_SLOT as usize, 0);
        out
    }

    fn decode(b: &[u8]) -> Option<Self> {
        if u64::from_le_bytes(b[0..8].try_into().unwrap()) != SUPER_MAGIC {
            return None;
        }
        let crc = u32::from_le_bytes(b[8..12].try_into().unwrap());
        let len = u32::from_le_bytes(b[12..16].try_into().unwrap()) as usize;
        let payload = b.get(16..16 + len)?;
        if crc32c::crc32c(payload) != crc || len < 33 {
            return None;
        }
        let u = |i: usize| u64::from_le_bytes(payload[i * 8..i * 8 + 8].try_into().unwrap());
        let root = match payload[32] {
            0 => None,
            1 => {
                let height = u32::from_le_bytes(payload.get(33..37)?.try_into().unwrap());
                Some((PagePtr::decode(payload.get(37..)?)?, height))
            }
            _ => return None,
        };
        Some(Self {
            checkpoint_id: u(0),
            last_seq: u(1),
            last_memtable_id: u(2),
            next_page_id: u(3),
            root,
        })
    }

    /// Reads the newest valid superblock, or `None` if the store is new.
    pub fn load(storage: &dyn Storage) -> Result<Option<Self>> {
        if !storage.list()?.iter().any(|n| n == SUPERBLOCK) {
            return Ok(None);
        }
        let file = storage.open(SUPERBLOCK, false)?;
        let len = file.len()?;
        let mut best: Option<Self> = None;
        for slot in 0..2 {
            if len < (slot + 1) * SUPER_SLOT {
                continue;
            }
            let mut buf = vec![0u8; SUPER_SLOT as usize];
            file.read_exact_at(&mut buf, slot * SUPER_SLOT)?;
            if let Some(sb) = Self::decode(&buf)
                && best.as_ref().is_none_or(|b| sb.checkpoint_id > b.checkpoint_id)
            {
                best = Some(sb);
            }
        }
        match best {
            Some(sb) => Ok(Some(sb)),
            None => Err(corruption("no valid superblock")),
        }
    }

    /// Writes this superblock durably into the slot its checkpoint id selects.
    pub fn store(&self, storage: &dyn Storage) -> Result<()> {
        let file = storage.open(SUPERBLOCK, true)?;
        file.write_all_at(&self.encode(), (self.checkpoint_id % 2) * SUPER_SLOT)?;
        file.sync_data()?;
        Ok(())
    }

    /// Creates the superblock file for a new store, durably.
    pub fn create(&self, storage: &dyn Storage) -> Result<()> {
        let file = storage.open(SUPERBLOCK, true)?;
        let encoded = self.encode();
        file.write_all_at(&encoded, 0)?;
        file.write_all_at(&encoded, SUPER_SLOT)?;
        file.sync_data()?;
        storage.sync_dir()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{FaultConfig, SimStorage};

    #[test]
    fn superblock_survives_torn_update() {
        for seed in 0..32 {
            let disk = SimStorage::new(seed, FaultConfig::default());
            let mut sb = Superblock { next_page_id: 1, ..Default::default() };
            sb.create(&disk).unwrap();
            sb.checkpoint_id = 1;
            sb.last_seq = 10;
            sb.root = Some((PagePtr { id: 5, class: 2, slot: 3, len: 100, meta_off: 50 }, 1));
            sb.store(&disk).unwrap();
            sb.checkpoint_id = 2;
            sb.last_seq = 20;
            let file = disk.open(SUPERBLOCK, false).unwrap();
            file.write_all_at(&sb.encode(), 0).unwrap();
            disk.crash();
            let loaded = Superblock::load(&disk).unwrap().unwrap();
            assert!(loaded.last_seq == 10 || loaded.last_seq == 20);
            assert!(loaded.root.is_some());
        }
    }
}
