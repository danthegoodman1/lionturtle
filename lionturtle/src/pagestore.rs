//! Page arenas and the superblock.
//!
//! Pages live in one file per power-of-two size class, `pages-<class>.dat`,
//! where slot `i` of class `c` starts at `i * (4 KiB << c)`. A page occupies
//! the smallest class that fits it, and the arenas are sparse files.
//!
//! Each page is written whole in one write that bypasses the page cache. On
//! a workstation's consumer NVMe drive, 4 MiB direct writes beside 4 KiB
//! reads held read p99.99 near 6 ms, where the same bandwidth written back
//! in smaller pieces, by the kernel or by `sync_file_range`, left it near
//! 50 ms. On an EC2 instance-store drive both held reads under 1 ms, and
//! whole-page writes raised peak throughput on 1 KB records by 7% over
//! 256 KiB pieces, since each writer thread finishes a page sooner.
//!
//! Free space is never persisted. At open, every slot that the recovered
//! checkpoint does not reference is free, and its space is released; afterwards
//! a slot is freed only once a newer checkpoint no longer references it and no
//! reader can reach it.
//!
//! A freed slot keeps its disk space while the next checkpoints may reuse it;
//! only a slot left free for [`PUNCH_AFTER`] reclaims is hole-punched. On XFS
//! a punch locks the file against reads, and punching every freed slot, about
//! 6,000 punches over 25 checkpoints of YCSB A at 400M records, raised read
//! p99.9 from 270 to 470 µs. A freed slot is dropped from the page cache
//! instead: without that, a dozen gets per run of A stalled for about 0.4 s.
//!
//! A reused slot keeps the blocks its last page wrote, so a page takes the free
//! slot whose last page reached furthest without passing its own end, and
//! overwrites every block the slot holds. Each arena grows ahead of its
//! writes, so that every write lands inside the file: XFS allocates a direct
//! write past the end of a file up to the next stripe boundary, and on 16
//! drives in RAID 0, slots reused with that excess held 4.6 GB more than their
//! pages at 400M records. Open takes each live slot to hold no more than its
//! page.
//!
//! The superblock file holds two slots written alternately, so a torn write
//! of one leaves the other intact; open picks the valid slot with the highest
//! checkpoint id.

use crate::error::{Result, corruption};
use crate::page::PageId;
use crate::storage::{DIRECT_ALIGN, File, Storage};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering::Acquire, Ordering::Relaxed, Ordering::Release};
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
        (ptr.class < CLASSES as u8 && ptr.meta_off <= ptr.len && ptr.len as u64 <= slot_size(ptr.class)).then_some(ptr)
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

/// Reclaims a free slot waits through before its disk space is released.
pub const PUNCH_AFTER: u64 = 2;

/// Bytes an arena grows past the slot that outgrew it.
const GROWTH: u64 = 64 << 20;

/// Blocks of [`DIRECT_ALIGN`] bytes that `len` bytes span.
fn blocks(len: u64) -> u32 {
    len.div_ceil(DIRECT_ALIGN as u64) as u32
}

#[derive(Default)]
struct ClassAlloc {
    /// One past the highest slot ever handed out.
    next: u32,
    /// Free slots by reach, the blocks from the slot's start that may hold
    /// disk space, then slot.
    free: BTreeSet<(u32, u32)>,
    /// Free slots that still hold disk space, with their reach and the
    /// reclaim that freed each.
    unpunched: BTreeMap<u32, (u32, u64)>,
}

pub struct PageStore {
    storage: Arc<dyn Storage>,
    /// Arena files by size class. Reads find them without locking.
    files: Vec<OnceLock<Arc<dyn File>>>,
    /// The same arenas opened for direct writes.
    direct: Vec<OnceLock<Arc<dyn File>>>,
    /// Serializes arena creation and growth.
    create: Mutex<()>,
    /// Each arena's length.
    lens: Vec<AtomicU64>,
    alloc: Mutex<Vec<ClassAlloc>>,
    /// Freed slots not yet returned to the free pool. They become
    /// allocatable in [`PageStore::reclaim`].
    freed: Mutex<Vec<PagePtr>>,
    /// Reclaims so far.
    reclaims: AtomicU64,
}

impl PageStore {
    /// Opens the arenas, treating every slot outside `live` as free.
    pub fn open(storage: Arc<dyn Storage>, live: &[PagePtr]) -> Result<Self> {
        let files: Vec<OnceLock<Arc<dyn File>>> = (0..CLASSES).map(|_| OnceLock::new()).collect();
        let mut alloc: Vec<ClassAlloc> = (0..CLASSES).map(|_| ClassAlloc::default()).collect();
        let lens: Vec<AtomicU64> = (0..CLASSES).map(|_| AtomicU64::new(0)).collect();
        let names = storage.list()?;
        for class in 0..CLASSES as u8 {
            if names.contains(&arena_name(class)) {
                let file = storage.open(&arena_name(class), false)?;
                file.advise_random();
                let len = file.len()?;
                lens[class as usize].store(len, Relaxed);
                let slots = len.div_ceil(slot_size(class));
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
        // A free slot may still hold a page from before the restart.
        for (class, a) in alloc.iter_mut().enumerate() {
            let full = blocks(slot_size(class as u8));
            a.free = (0..a.next).filter(|s| !used[class].contains(s)).map(|s| (full, s)).collect();
        }
        let direct = (0..CLASSES).map(|_| OnceLock::new()).collect();
        Ok(Self {
            storage,
            files,
            direct,
            create: Mutex::new(()),
            lens,
            alloc: Mutex::new(alloc),
            freed: Mutex::new(Vec::new()),
            reclaims: AtomicU64::new(0),
        })
    }

    /// Reserves a slot for a page image of `len` bytes.
    pub fn allocate(&self, id: PageId, len: u64, meta_off: u32) -> Result<PagePtr> {
        let class = class_for(len).ok_or_else(|| crate::Error::InvalidArgument(format!("page of {len} bytes too large")))?;
        let need = blocks(len);
        let mut alloc = self.alloc.lock();
        let a = &mut alloc[class as usize];
        let slot = match a.free.range(..=(need, u32::MAX)).next_back().copied() {
            Some(entry) => {
                a.free.remove(&entry);
                a.unpunched.remove(&entry.1);
                entry.1
            }
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

    /// Releases the disk space of every free slot, which may still hold
    /// pages from before a restart. Call only on a store opened with the
    /// complete set of live pages.
    pub fn punch_free(&self) {
        let mut alloc = self.alloc.lock();
        for (class, a) in alloc.iter_mut().enumerate() {
            let mut slots: Vec<u32> = a.free.iter().map(|&(_, s)| s).collect();
            slots.sort_unstable();
            if let Some(file) = self.files[class].get() {
                let size = slot_size(class as u8);
                let mut runs = slots.iter().copied().peekable();
                while let Some(first) = runs.next() {
                    let mut last = first;
                    while runs.next_if(|&s| s == last + 1).is_some() {
                        last += 1;
                    }
                    let _ = file.punch_hole(first as u64 * size, (last - first + 1) as u64 * size);
                }
            }
            a.free = slots.into_iter().map(|s| (0, s)).collect();
        }
    }

    /// Queues slots for release. This is cheap, so any thread may call it;
    /// [`PageStore::reclaim`] does the I/O.
    pub fn free(&self, ptrs: impl IntoIterator<Item = PagePtr>) {
        self.freed.lock().extend(ptrs);
    }

    /// Makes freed slots allocatable, dropping their pages from the page
    /// cache first, and releases the disk space of slots that have stayed
    /// free for [`PUNCH_AFTER`] reclaims. Those leave the free pool while
    /// their holes are punched, which keeps a hole from landing on a newer
    /// page. A failed punch only leaves the space allocated.
    pub fn reclaim(&self) {
        let freed = std::mem::take(&mut *self.freed.lock());
        for ptr in &freed {
            if let Ok((file, _)) = self.file(ptr.class) {
                file.drop_cache(ptr.offset(), slot_size(ptr.class));
            }
        }
        let round = self.reclaims.fetch_add(1, Relaxed) + 1;
        let mut idle: Vec<(u8, u32)> = Vec::new();
        {
            let mut alloc = self.alloc.lock();
            for ptr in freed {
                if std::env::var_os("TURTLEKV_DEBUG").is_some() {
                    eprintln!("free page {} at {}:{}", ptr.id, ptr.class, ptr.slot);
                }
                let a = &mut alloc[ptr.class as usize];
                let reach = blocks(ptr.len as u64);
                let inserted = a.free.insert((reach, ptr.slot));
                debug_assert!(inserted, "double free of {ptr:?}");
                a.unpunched.insert(ptr.slot, (reach, round));
            }
            for (class, a) in alloc.iter_mut().enumerate() {
                let ClassAlloc { free, unpunched, .. } = a;
                unpunched.retain(|&slot, &mut (reach, freed_at)| {
                    let keep = freed_at + PUNCH_AFTER > round;
                    if !keep {
                        free.remove(&(reach, slot));
                        idle.push((class as u8, slot));
                    }
                    keep
                });
            }
        }
        if idle.is_empty() {
            return;
        }
        idle.sort_unstable();
        let mut runs = idle.iter().copied().peekable();
        while let Some((class, first)) = runs.next() {
            let mut last = first;
            while runs.next_if(|&(c, s)| c == class && s == last + 1).is_some() {
                last += 1;
            }
            if let Ok((file, _)) = self.file(class) {
                let size = slot_size(class);
                let _ = file.punch_hole(first as u64 * size, (last - first + 1) as u64 * size);
            }
        }
        let mut alloc = self.alloc.lock();
        for (class, slot) in idle {
            alloc[class as usize].free.insert((0, slot));
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

    /// The arena for `class` opened for direct writes, and whether this
    /// created the arena. The buffered handle is opened first, so syncing
    /// the buffered handles covers direct writes too.
    fn direct_file(&self, class: u8) -> Result<(&Arc<dyn File>, bool)> {
        let (_, created) = self.file(class)?;
        let slot = &self.direct[class as usize];
        if let Some(f) = slot.get() {
            return Ok((f, created));
        }
        let _g = self.create.lock();
        if let Some(f) = slot.get() {
            return Ok((f, created));
        }
        let f = self.storage.open_direct(&arena_name(class), false)?;
        Ok((slot.get_or_init(|| f), created))
    }

    /// Writes a page image to its slot, padded to [`DIRECT_ALIGN`], in direct
    /// writes of at most `piece` bytes, calling `pace` with each write's
    /// length before it. Returns true if this created an arena file.
    pub fn write(&self, ptr: &PagePtr, image: &[u8], piece: usize, pace: &dyn Fn(u64)) -> Result<bool> {
        debug_assert_eq!(image.len(), ptr.len as usize);
        let len = image.len().next_multiple_of(DIRECT_ALIGN);
        let mut buf = vec![0u8; len + DIRECT_ALIGN];
        let start = buf.as_ptr().align_offset(DIRECT_ALIGN);
        let buf = &mut buf[start..start + len];
        buf[..image.len()].copy_from_slice(image);
        self.write_aligned(ptr, buf, piece, pace)
    }

    /// Writes a page to its slot from `buf`, whose address and length are
    /// multiples of [`DIRECT_ALIGN`], as [`PageStore::write`] does.
    pub fn write_aligned(&self, ptr: &PagePtr, buf: &[u8], piece: usize, pace: &dyn Fn(u64)) -> Result<bool> {
        debug_assert_eq!(buf.len(), (ptr.len as usize).next_multiple_of(DIRECT_ALIGN));
        debug_assert_eq!(buf.as_ptr().align_offset(DIRECT_ALIGN), 0);
        let (file, created) = self.direct_file(ptr.class)?;
        self.grow(ptr.class, &**file, ptr.offset() + buf.len() as u64)?;
        let piece = piece.min(buf.len()).max(DIRECT_ALIGN).next_multiple_of(DIRECT_ALIGN);
        for (i, part) in buf.chunks(piece).enumerate() {
            pace(part.len() as u64);
            file.write_all_at(part, ptr.offset() + (i * piece) as u64)?;
        }
        Ok(created)
    }

    /// Extends the arena for `class` past `end`, by [`GROWTH`] at a time.
    fn grow(&self, class: u8, file: &dyn File, end: u64) -> Result<()> {
        let len = &self.lens[class as usize];
        if end <= len.load(Acquire) {
            return Ok(());
        }
        let _g = self.create.lock();
        if end <= len.load(Acquire) {
            return Ok(());
        }
        let to = (end + GROWTH).next_multiple_of(slot_size(class));
        file.extend(to)?;
        len.store(to, Release);
        Ok(())
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
            let free: BTreeSet<u32> = a.free.iter().map(|&(_, s)| s).collect();
            for slot in 0..a.next {
                if !free.contains(&slot) && !freed.contains(&(class as u8, slot)) {
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
        Some(Self { checkpoint_id: u(0), last_seq: u(1), last_memtable_id: u(2), next_page_id: u(3), root })
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

    #[test]
    fn page_written_in_pieces_reads_back_whole() {
        let disk: Arc<dyn Storage> = Arc::new(SimStorage::new(1, FaultConfig::default()));
        let store = PageStore::open(disk, &[]).unwrap();
        let image: Vec<u8> = (0..3 * DIRECT_ALIGN as u32 + 100).map(|i| (i % 251) as u8).collect();
        for piece in [1, DIRECT_ALIGN, 2 * DIRECT_ALIGN, usize::MAX] {
            let ptr = store.allocate(7, image.len() as u64, 0).unwrap();
            let paced = std::cell::RefCell::new(Vec::new());
            store.write(&ptr, &image, piece, &|n| paced.borrow_mut().push(n)).unwrap();
            assert_eq!(&store.read(&ptr, 0, image.len() as u64).unwrap()[..], &image[..]);
            let paced = paced.into_inner();
            assert_eq!(paced.iter().sum::<u64>(), image.len().next_multiple_of(DIRECT_ALIGN) as u64);
            assert_eq!(paced.len(), if piece == usize::MAX { 1 } else { 4usize.div_ceil(piece.max(DIRECT_ALIGN) / DIRECT_ALIGN) });
        }
    }

    #[test]
    fn freed_slots_keep_their_space_until_left_free() {
        let disk: Arc<dyn Storage> = Arc::new(SimStorage::new(1, FaultConfig::default()));
        let store = PageStore::open(disk, &[]).unwrap();
        let image = vec![7u8; DIRECT_ALIGN];
        let len = image.len() as u64;
        let write = |id| {
            let ptr = store.allocate(id, len, 0).unwrap();
            store.write(&ptr, &image, usize::MAX, &|_| {}).unwrap();
            ptr
        };
        let (a, b) = (write(1), write(2));
        store.free([a, b]);
        store.reclaim();
        // A reused slot still held its page.
        assert_eq!(&store.read(&a, 0, len).unwrap()[..], &image[..]);
        let reused = write(3).slot;
        let other = if reused == a.slot { b } else { a };
        // The other keeps its space through the next reclaim.
        store.reclaim();
        assert_eq!(&store.read(&other, 0, len).unwrap()[..], &image[..]);
        // Left free for PUNCH_AFTER reclaims, it is punched, and stays allocatable.
        store.reclaim();
        assert!(store.read(&other, 0, len).unwrap().iter().all(|&x| x == 0));
        assert_eq!(write(4).slot, other.slot);
    }

    #[test]
    fn page_takes_the_free_slot_that_fits_it() {
        let disk: Arc<dyn Storage> = Arc::new(SimStorage::new(1, FaultConfig::default()));
        let store = PageStore::open(disk, &[]).unwrap();
        let kib = |n: u64| n << 10;
        let write = |id, len: u64| {
            let ptr = store.allocate(id, len, 0).unwrap();
            store.write(&ptr, &vec![7u8; len as usize], usize::MAX, &|_| {}).unwrap();
            ptr
        };
        // Both land in the 64 KiB class, reaching 64 and 40 KiB.
        let (long, short) = (write(1, kib(64)), write(2, kib(40)));
        assert_eq!((long.class, short.class), (4, 4));
        store.free([long, short]);
        store.reclaim();
        // A page shorter than both would leave blocks past its end, so it
        // takes a new slot.
        assert_eq!(store.allocate(3, kib(36), 0).unwrap().slot, 2);
        // Others take the slot that reached furthest within them.
        assert_eq!(store.allocate(4, kib(48), 0).unwrap().slot, short.slot);
        assert_eq!(store.allocate(5, kib(64), 0).unwrap().slot, long.slot);
    }
}
