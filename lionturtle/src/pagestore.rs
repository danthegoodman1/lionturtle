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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{
    AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::AcqRel, Ordering::Acquire, Ordering::Relaxed, Ordering::Release,
    Ordering::SeqCst,
};
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

/// The file name prefix of the arenas that hold the tree's pages.
pub const PAGES: &str = "pages";
/// The file name prefix of the arenas that hold blobs; see [`crate::blob`].
pub const BLOBS: &str = "blobs";

pub(crate) fn arena_name(prefix: &str, class: u8) -> String {
    format!("{prefix}-{class:02}.dat")
}

/// Reclaims a free slot waits through before its disk space is released.
pub const PUNCH_AFTER: u64 = 2;

/// A blob may take a free slot that held one up to this share longer: an
/// eighth. The blocks past its end stay allocated until the slot is next
/// released, so the slot wastes at most that share of the blob, and the
/// write lands on allocated blocks.
const REUSE_SLACK: u32 = 8;

/// Bytes an arena grows past the slot that outgrew it.
const GROWTH: u64 = 64 << 20;
/// Threads that issue background prefetches. Each prefetch may wait to
/// reclaim memory, and several let one wait without holding up the rest.
const PREFETCH_THREADS: usize = 8;

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
    /// The file name prefix of this store's arenas.
    prefix: &'static str,
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
    /// Runs [`PageStore::prefetch_background`] where the storage asks for
    /// background prefetches.
    prefetcher: Option<rayon::ThreadPool>,
    /// A bit per class whose arena has taken a write since
    /// [`PageStore::sync_new`] last ran.
    unsynced: AtomicU32,
    /// Whether an arena file was created since [`PageStore::sync_new`] last
    /// ran.
    created: AtomicBool,
    /// Held by [`PageStore::sync_new`] while it syncs, with whether a call
    /// has failed.
    syncing: Mutex<bool>,
    /// Bytes of freed slots that keep their disk space for reuse however
    /// long they stay free; see [`PageStore::open_blobs`].
    keep_free: u64,
    /// Whether a slot may be reused by an image somewhat shorter than the
    /// one it held; see [`REUSE_SLACK`].
    reuse_shorter: bool,
    /// Slots that backups have yet to copy; see [`PageStore::pin`].
    pins: Mutex<HashMap<(u8, u32), Pin>>,
    /// How many slots are pinned, so a free takes the pin lock only while a
    /// backup runs.
    pinned: AtomicUsize,
}

/// A slot some backup has yet to copy.
#[derive(Default)]
struct Pin {
    /// Backups that hold the slot.
    holders: u32,
    /// The slot's page, once the tree has dropped it. It is released when
    /// the last holder lets go.
    freed: Option<PagePtr>,
}

impl PageStore {
    /// Opens the arenas of the tree's pages, treating every slot outside
    /// `live` as free.
    pub fn open(storage: Arc<dyn Storage>, live: &[PagePtr]) -> Result<Self> {
        Self::open_named(storage, PAGES, live)
    }

    /// Opens the arenas of blobs, treating every slot outside `live` as free.
    ///
    /// Overwritten blobs free their slots as fast as new blobs take slots,
    /// so the blob arenas reuse more than the page arenas do. Up to
    /// `keep_free` bytes of freed slots keep their disk space however long
    /// they stay free, and past that the slots freed longest ago release
    /// theirs. A blob takes a free slot that held a blob of its length or
    /// up to an eighth more ([`REUSE_SLACK`]), since compressed values of
    /// one size vary a little. An overwrite then lands on allocated blocks:
    /// the file system allocates nothing and journals nothing, and on ext4
    /// the write shares the file with other writes. Against releasing each
    /// slot two checkpoints after it was freed, with 16 threads overwriting
    /// 64 KiB values, reuse raised throughput by 29% and cut update p99
    /// from 5.1 to 2.5 ms; with 1 MiB values it cut update p99 from 24 to
    /// 13 ms.
    pub fn open_blobs(storage: Arc<dyn Storage>, live: &[PagePtr], keep_free: u64) -> Result<Self> {
        Ok(Self { keep_free, reuse_shorter: true, ..Self::open_named(storage, BLOBS, live)? })
    }

    fn open_named(storage: Arc<dyn Storage>, prefix: &'static str, live: &[PagePtr]) -> Result<Self> {
        let files: Vec<OnceLock<Arc<dyn File>>> = (0..CLASSES).map(|_| OnceLock::new()).collect();
        let mut alloc: Vec<ClassAlloc> = (0..CLASSES).map(|_| ClassAlloc::default()).collect();
        let lens: Vec<AtomicU64> = (0..CLASSES).map(|_| AtomicU64::new(0)).collect();
        let names = storage.list()?;
        for class in 0..CLASSES as u8 {
            if names.contains(&arena_name(prefix, class)) {
                let file = storage.open(&arena_name(prefix, class), false)?;
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
        // Without the pool, prefetches run in the caller.
        let prefetcher = match storage.prefetch_in_background() {
            true => rayon::ThreadPoolBuilder::new()
                .num_threads(PREFETCH_THREADS)
                .thread_name(|i| format!("lionturtle-prefetch-{i}"))
                .build()
                .ok(),
            false => None,
        };
        Ok(Self {
            storage,
            prefix,
            unsynced: AtomicU32::new(0),
            created: AtomicBool::new(false),
            syncing: Mutex::new(false),
            keep_free: 0,
            reuse_shorter: false,
            files,
            direct,
            create: Mutex::new(()),
            lens,
            alloc: Mutex::new(alloc),
            freed: Mutex::new(Vec::new()),
            reclaims: AtomicU64::new(0),
            prefetcher,
            pins: Mutex::new(HashMap::new()),
            pinned: AtomicUsize::new(0),
        })
    }

    /// Reserves a slot for a page image of `len` bytes.
    pub fn allocate(&self, id: PageId, len: u64, meta_off: u32) -> Result<PagePtr> {
        let class = class_for(len).ok_or_else(|| crate::Error::InvalidArgument(format!("page of {len} bytes too large")))?;
        let need = blocks(len);
        let mut alloc = self.alloc.lock();
        let a = &mut alloc[class as usize];
        // Of the free slots that hold disk space, the one whose last image
        // reached furthest without passing this one's end; or else, for a
        // blob, the one that reached least past it within the slack; or
        // else a slot whose space was released.
        let slack = if self.reuse_shorter { need / REUSE_SLACK } else { 0 };
        let fits = a.free.range((1, 0)..=(need, u32::MAX)).next_back().copied();
        let fits = fits.or_else(|| a.free.range((need + 1, 0)..(need + slack + 1, 0)).next().copied());
        let fits = fits.or_else(|| a.free.range(..(1, 0)).next_back().copied());
        let slot = match fits {
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
    /// [`PageStore::reclaim`] does the I/O. A slot a backup has yet to copy
    /// waits for the backup instead; see [`PageStore::pin`].
    pub fn free(&self, ptrs: impl IntoIterator<Item = PagePtr>) {
        if self.pinned.load(SeqCst) == 0 {
            self.freed.lock().extend(ptrs);
            return;
        }
        let mut pins = self.pins.lock();
        let mut freed = self.freed.lock();
        for ptr in ptrs {
            match pins.get_mut(&(ptr.class, ptr.slot)) {
                Some(pin) => pin.freed = Some(ptr),
                None => freed.push(ptr),
            }
        }
    }

    /// Holds `slots` for a backup: a held slot keeps its page, though the
    /// tree drops it, until [`PageStore::unpin`] lets it go. The caller
    /// holds a version of the tree that references every one of them, so
    /// none can be freed before this returns.
    ///
    /// A backup reads each slot once, in an order it knows, so it holds a
    /// slot only until it has copied it, and never holds a page written
    /// after it began. Holding a version instead would keep every page and
    /// blob superseded while the backup ran: copying a blob store at 200
    /// MiB/s while clients overwrote it at 250 MB/s grew it from 8.7 to
    /// 20 GB, and with pins it peaked at 9.7 GB.
    pub fn pin(&self, slots: impl IntoIterator<Item = (u8, u32)>) {
        let mut pins = self.pins.lock();
        for slot in slots {
            pins.entry(slot).or_default().holders += 1;
        }
        self.pinned.store(pins.len(), SeqCst);
    }

    /// Lets go of a slot one backup held, and queues it for release if the
    /// tree dropped its page meanwhile and no other backup holds it.
    pub fn unpin(&self, class: u8, slot: u32) {
        let mut pins = self.pins.lock();
        let Some(pin) = pins.get_mut(&(class, slot)) else { return };
        pin.holders -= 1;
        if pin.holders == 0 {
            if let Some(ptr) = pins.remove(&(class, slot)).and_then(|pin| pin.freed) {
                self.freed.lock().push(ptr);
            }
            self.pinned.store(pins.len(), SeqCst);
        }
    }

    /// Reads `buf.len()` bytes at `offset` within the slot of `ptr`, past
    /// the page cache where the storage allows. The address and length of
    /// `buf`, and `offset`, must be multiples of [`DIRECT_ALIGN`]. A slot
    /// holds its image padded to that, so a read may run to the padded end.
    pub fn read_direct(&self, ptr: &PagePtr, offset: u64, buf: &mut [u8]) -> Result<()> {
        let (file, _) = self.direct_file(ptr.class)?;
        file.read_exact_at(buf, ptr.offset() + offset)?;
        Ok(())
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
            // Of the slots left free for `PUNCH_AFTER` reclaims, release
            // those freed longest ago until the slots still holding space
            // hold `keep_free` bytes or less.
            let bytes = |reach: u32| reach as u64 * DIRECT_ALIGN as u64;
            let mut held: u64 = alloc.iter().flat_map(|a| a.unpunched.values()).map(|&(reach, _)| bytes(reach)).sum();
            let mut old: Vec<(u64, u8, u32, u32)> = Vec::new();
            for (class, a) in alloc.iter().enumerate() {
                let left = a.unpunched.iter().filter(|(_, (_, freed_at))| freed_at + PUNCH_AFTER <= round);
                old.extend(left.map(|(&slot, &(reach, freed_at))| (freed_at, class as u8, slot, reach)));
            }
            old.sort_unstable();
            for (_, class, slot, reach) in old {
                if held <= self.keep_free {
                    break;
                }
                held -= bytes(reach);
                let a = &mut alloc[class as usize];
                a.unpunched.remove(&slot);
                a.free.remove(&(reach, slot));
                idle.push((class, slot));
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
        let f = self.storage.open(&arena_name(self.prefix, class), true)?;
        f.advise_random();
        self.created.store(true, Release);
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
        let f = self.storage.open_direct(&arena_name(self.prefix, class), false)?;
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
        self.unsynced.fetch_or(1 << ptr.class, Release);
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

    /// Like [`PageStore::prefetch`], but on a background thread where the
    /// storage asks for one, so the caller waits for neither the system call
    /// nor the memory it allocates.
    pub fn prefetch_background(&self, ptr: &PagePtr, offset: u64, len: u64) {
        let Some(pool) = &self.prefetcher else {
            return self.prefetch(ptr, offset, len);
        };
        if let Some(file) = self.files[ptr.class as usize].get() {
            let (file, at) = (file.clone(), ptr.offset() + offset);
            pool.spawn(move || file.prefetch(at, len));
        }
    }

    /// Syncs every arena file.
    pub fn sync(&self) -> Result<()> {
        for f in self.files.iter().filter_map(OnceLock::get) {
            f.sync_data()?;
        }
        Ok(())
    }

    /// Makes durable every write that completed before this call, and the
    /// directory entry of any arena file created before it. With nothing to
    /// sync, it costs a lock and two loads.
    ///
    /// A write marks its arena after it completes, and this clears the
    /// marks before it syncs, so a write that completes while this runs is
    /// either covered or still marked for the next call. Calls take turns:
    /// one that finds no mark waits for the call that cleared it to finish
    /// syncing. A failed sync leaves writes that no later call would cover,
    /// so every call after it fails too.
    pub fn sync_new(&self) -> Result<()> {
        let mut failed = self.syncing.lock();
        if *failed {
            return Err(std::io::Error::other("an earlier arena sync failed").into());
        }
        if self.unsynced.load(Acquire) == 0 && !self.created.load(Acquire) {
            return Ok(());
        }
        *failed = true;
        let classes = self.unsynced.swap(0, AcqRel);
        for class in (0..CLASSES).filter(|c| classes & 1 << c != 0) {
            if let Some(f) = self.files[class].get() {
                f.sync_data()?;
            }
        }
        if self.created.swap(false, AcqRel) {
            self.storage.sync_dir()?;
        }
        *failed = false;
        Ok(())
    }

    pub fn sync_dir(&self) -> Result<()> {
        Ok(self.storage.sync_dir()?)
    }

    /// Slots in use per class, for leak checks.
    pub fn allocated(&self) -> Vec<(u8, u32)> {
        let mut freed: BTreeSet<(u8, u32)> =
            self.pins.lock().iter().filter(|(_, pin)| pin.freed.is_some()).map(|(&slot, _)| slot).collect();
        freed.extend(self.freed.lock().iter().map(|p| (p.class, p.slot)));
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
/// The magic of a store that may have range deletes in its log.
const SUPER_MAGIC_RANGES: u64 = 0x544b_565f_5355_5033;
/// The magic of a store whose superblock ends with a byte of feature flags,
/// each naming something a version must understand to open the store.
const SUPER_MAGIC_FEATURES: u64 = 0x544b_565f_5355_5046;
const FEATURE_RANGE_DELETES: u8 = 1;
const FEATURE_BLOBS: u8 = 2;

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
    /// Whether the log may hold range deletes. Such a superblock carries a
    /// magic that lionturtle 0.6 and earlier reject, so they refuse to open
    /// the store rather than read a range delete as a torn record and drop
    /// every write after it.
    pub range_deletes: bool,
    /// Whether the tree may hold blob pointers. Such a superblock carries a
    /// magic that versions without blobs reject, so they refuse to open the
    /// store rather than read a pointer as a value.
    pub blobs: bool,
    /// Every blob ever written has an id below this, whether or not
    /// anything still points to it. The store raises it, durably, before it
    /// uses an id at or above it, so no id is used twice, across crashes
    /// too. A blob's checksum covers its id, so a pointer finds its own blob
    /// in its slot or fails.
    pub next_blob_id: u64,
}

impl Superblock {
    /// Bytes of an encoded superblock.
    pub(crate) const ENCODED_LEN: usize = SUPER_SLOT as usize;

    pub(crate) fn encode(&self) -> Vec<u8> {
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
        // A store keeps the oldest magic that covers what it holds, so the
        // oldest version able to read it still opens it.
        let magic = match (self.blobs, self.range_deletes) {
            (true, _) => {
                payload.extend_from_slice(&self.next_blob_id.to_le_bytes());
                payload.push(FEATURE_BLOBS | if self.range_deletes { FEATURE_RANGE_DELETES } else { 0 });
                SUPER_MAGIC_FEATURES
            }
            (false, true) => SUPER_MAGIC_RANGES,
            (false, false) => SUPER_MAGIC,
        };
        let mut out = Vec::with_capacity(SUPER_SLOT as usize);
        out.extend_from_slice(&magic.to_le_bytes());
        out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        out.resize(SUPER_SLOT as usize, 0);
        out
    }

    /// Decodes [`Superblock::ENCODED_LEN`] bytes, or returns `None` if they
    /// fail their checksum or come from a version this one cannot read.
    pub(crate) fn decode(b: &[u8]) -> Option<Self> {
        let magic = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let crc = u32::from_le_bytes(b[8..12].try_into().unwrap());
        let len = u32::from_le_bytes(b[12..16].try_into().unwrap()) as usize;
        let payload = b.get(16..16 + len)?;
        if crc32c::crc32c(payload) != crc || len < 33 {
            return None;
        }
        let (payload, range_deletes, blobs, next_blob_id) = match magic {
            SUPER_MAGIC => (payload, false, false, 0),
            SUPER_MAGIC_RANGES => (payload, true, false, 0),
            SUPER_MAGIC_FEATURES => {
                let (&flags, payload) = payload.split_last()?;
                let (payload, next_blob_id) = payload.split_last_chunk::<8>()?;
                if flags & !(FEATURE_RANGE_DELETES | FEATURE_BLOBS) != 0 || payload.len() < 33 {
                    return None;
                }
                (payload, flags & FEATURE_RANGE_DELETES != 0, flags & FEATURE_BLOBS != 0, u64::from_le_bytes(*next_blob_id))
            }
            _ => return None,
        };
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
            range_deletes,
            blobs,
            next_blob_id,
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

    /// Writes this superblock durably into the slot its checkpoint id
    /// selects, with one durable direct write. The slot's block is
    /// allocated, so a drive that can write through its cache makes that one
    /// write durable, where an fsync would have it flush its whole cache
    /// while other writes wait. The pages and blobs a superblock names are
    /// synced before it is written.
    pub fn store(&self, storage: &dyn Storage) -> Result<()> {
        let file = storage.open_direct(SUPERBLOCK, true)?;
        let mut buf = vec![0u8; SUPER_SLOT as usize + DIRECT_ALIGN];
        let start = buf.as_ptr().align_offset(DIRECT_ALIGN);
        let slot = &mut buf[start..start + SUPER_SLOT as usize];
        slot.copy_from_slice(&self.encode());
        file.write_all_at_durable(slot, (self.checkpoint_id % 2) * SUPER_SLOT)?;
        Ok(())
    }

    /// Marks the store as one whose log may hold range deletes: writes this
    /// superblock, marked, into both slots under the next two checkpoint
    /// ids, so no slot an older version could read remains. A crash between
    /// the two leaves the first, which an older version also rejects.
    pub fn mark_range_deletes(&mut self, storage: &dyn Storage) -> Result<()> {
        self.replace_both(Self { range_deletes: true, ..self.clone() }, storage)
    }

    /// Reserves blob ids up to `next_blob_id`, durably, so the store may
    /// use the ids below it. The first reservation also marks the store as
    /// one whose tree may hold blob pointers, as
    /// [`Superblock::mark_range_deletes`] marks it for range deletes. A
    /// later one writes one slot: if that write tears, the other slot
    /// keeps the old reservation, whose ids are all the store has used.
    pub fn reserve_blob_ids(&mut self, next_blob_id: u64, storage: &dyn Storage) -> Result<()> {
        let mut sb = Self { blobs: true, next_blob_id, ..self.clone() };
        if !self.blobs {
            return self.replace_both(sb, storage);
        }
        sb.checkpoint_id += 1;
        sb.store(storage)?;
        *self = sb;
        Ok(())
    }

    /// Writes `sb` into both slots under the next two checkpoint ids and
    /// takes its place.
    fn replace_both(&mut self, mut sb: Self, storage: &dyn Storage) -> Result<()> {
        for _ in 0..2 {
            sb.checkpoint_id += 1;
            sb.store(storage)?;
        }
        *self = sb;
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

    /// A slot a backup holds keeps its page when the tree drops it, counts
    /// as freed for the leak check, and is released when the last backup
    /// that holds it lets go.
    #[test]
    fn a_pinned_slot_is_released_when_its_last_holder_lets_go() {
        let store = PageStore::open(Arc::new(SimStorage::new(1, FaultConfig::default())), &[]).unwrap();
        let ptrs: Vec<PagePtr> = (0..4).map(|id| store.allocate(id, 5000, 0).unwrap()).collect();
        let slot = |p: &PagePtr| (p.class, p.slot);
        let allocated = |store: &PageStore| -> BTreeSet<(u8, u32)> { store.allocated().into_iter().collect() };
        // Two backups hold slots 0 and 1; the second also holds slot 2.
        store.pin([slot(&ptrs[0]), slot(&ptrs[1])]);
        store.pin([slot(&ptrs[0]), slot(&ptrs[1]), slot(&ptrs[2])]);
        store.free([ptrs[0], ptrs[3]]);
        store.reclaim();
        // Slot 3 is free. Slot 0 counts as freed, and nothing takes it.
        assert_eq!(allocated(&store), [slot(&ptrs[1]), slot(&ptrs[2])].into_iter().collect());
        let next = store.allocate(9, 5000, 0).unwrap();
        assert_eq!(slot(&next), slot(&ptrs[3]));
        store.unpin(ptrs[0].class, ptrs[0].slot);
        store.reclaim();
        assert_ne!(slot(&store.allocate(10, 5000, 0).unwrap()), slot(&ptrs[0]));
        // The last holder lets go: the slot is released and reused.
        store.unpin(ptrs[0].class, ptrs[0].slot);
        store.reclaim();
        assert_eq!(slot(&store.allocate(11, 5000, 0).unwrap()), slot(&ptrs[0]));
        // A slot let go while its page lives stays allocated, and frees as
        // any other afterwards.
        store.unpin(ptrs[1].class, ptrs[1].slot);
        store.unpin(ptrs[1].class, ptrs[1].slot);
        store.unpin(ptrs[2].class, ptrs[2].slot);
        assert!(allocated(&store).contains(&slot(&ptrs[1])));
        store.free([ptrs[1]]);
        store.reclaim();
        assert!(!allocated(&store).contains(&slot(&ptrs[1])));
        assert_eq!(store.pinned.load(SeqCst), 0);
    }

    /// Marking writes both slots, so a reader that knows only the old magic
    /// finds no superblock, even after a crash between the two writes.
    #[test]
    fn marking_for_range_deletes_replaces_both_slots() {
        let old_reader = |disk: &SimStorage| {
            let file = disk.open(SUPERBLOCK, false).unwrap();
            (0..2).any(|slot| {
                let mut buf = vec![0u8; SUPER_SLOT as usize];
                file.read_exact_at(&mut buf, slot * SUPER_SLOT).unwrap();
                u64::from_le_bytes(buf[0..8].try_into().unwrap()) == SUPER_MAGIC && Superblock::decode(&buf).is_some()
            })
        };
        for crash_after in [None, Some(1), Some(2), Some(3)] {
            let disk = SimStorage::new(3, FaultConfig::default());
            let sb = Superblock { next_page_id: 1, last_seq: 7, ..Default::default() };
            sb.create(&disk).unwrap();
            assert!(old_reader(&disk));
            if let Some(ops) = crash_after {
                disk.arm_crash(ops);
            }
            let marked = sb.clone().mark_range_deletes(&disk).is_ok();
            disk.disarm_crash();
            disk.crash();
            let loaded = Superblock::load(&disk).unwrap().unwrap();
            assert_eq!(loaded.last_seq, 7);
            // A failed mark leaves the store unmarked for older versions,
            // which is safe: the store logs no range delete until marking
            // succeeds.
            if marked {
                assert!(loaded.range_deletes && !old_reader(&disk), "a marked store opens only with the new magic");
            }
        }
    }

    /// A store marked for blobs carries a magic that versions knowing only
    /// the earlier two reject, with or without range deletes, and a store
    /// with neither keeps the first magic.
    #[test]
    fn marking_for_blobs_replaces_both_slots_and_keeps_other_marks() {
        let magics = |disk: &SimStorage| -> Vec<u64> {
            let file = disk.open(SUPERBLOCK, false).unwrap();
            (0..2)
                .map(|slot| {
                    let mut buf = vec![0u8; SUPER_SLOT as usize];
                    file.read_exact_at(&mut buf, slot * SUPER_SLOT).unwrap();
                    u64::from_le_bytes(buf[0..8].try_into().unwrap())
                })
                .collect()
        };
        let root = Some((PagePtr { id: 5, class: 2, slot: 3, len: 100, meta_off: 50 }, 1));
        for (ranges, root) in [(false, None), (true, None), (false, root), (true, root)] {
            let disk = SimStorage::new(4, FaultConfig::default());
            let mut sb = Superblock { next_page_id: 9, last_seq: 7, root, ..Default::default() };
            sb.create(&disk).unwrap();
            assert_eq!(magics(&disk), [SUPER_MAGIC; 2]);
            if ranges {
                sb.mark_range_deletes(&disk).unwrap();
                assert_eq!(magics(&disk), [SUPER_MAGIC_RANGES; 2]);
            }
            sb.reserve_blob_ids(1000, &disk).unwrap();
            assert_eq!(magics(&disk), [SUPER_MAGIC_FEATURES; 2]);
            disk.crash();
            let loaded = Superblock::load(&disk).unwrap().unwrap();
            assert_eq!(loaded, Superblock { blobs: true, next_blob_id: 1000, range_deletes: ranges, ..sb.clone() });
            // A later reservation survives a crash, or the earlier one does.
            let mut later = loaded.clone();
            later.reserve_blob_ids(2000, &disk).unwrap();
            disk.crash();
            let loaded = Superblock::load(&disk).unwrap().unwrap();
            assert_eq!(loaded.next_blob_id, 2000);
            // A later checkpoint and a later mark keep the blob mark.
            let mut next = Superblock { checkpoint_id: loaded.checkpoint_id + 1, last_seq: 8, ..loaded };
            next.store(&disk).unwrap();
            next.mark_range_deletes(&disk).unwrap();
            let loaded = Superblock::load(&disk).unwrap().unwrap();
            assert!(loaded.blobs && loaded.range_deletes && loaded.last_seq == 8 && loaded.next_blob_id == 2000);
        }
        // Flags this version does not know make the superblock unreadable.
        let mut raw = Superblock { blobs: true, ..Default::default() }.encode();
        let len = u32::from_le_bytes(raw[12..16].try_into().unwrap()) as usize;
        raw[16 + len - 1] |= 0x80;
        let crc = crc32c::crc32c(&raw[16..16 + len]);
        raw[8..12].copy_from_slice(&crc.to_le_bytes());
        assert!(Superblock::decode(&raw).is_none());
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

    /// The blob arenas keep freed slots' space for reuse up to a budget,
    /// release the slots freed longest ago past it, and let a blob take a
    /// slot that held a slightly longer one.
    #[test]
    fn blob_slots_wait_for_reuse_within_a_budget() {
        let disk: Arc<dyn Storage> = Arc::new(SimStorage::new(1, FaultConfig::default()));
        let block = DIRECT_ALIGN as u64;
        // Room for two freed slots of sixteen blocks.
        let store = PageStore::open_blobs(disk, &[], 2 * 16 * block).unwrap();
        let write = |id, blocks: u64| {
            let image = vec![id as u8; (blocks * block) as usize];
            let ptr = store.allocate(id, image.len() as u64, 0).unwrap();
            store.write(&ptr, &image, usize::MAX, &|_| {}).unwrap();
            ptr
        };
        let holds = |ptr: &PagePtr| store.read(ptr, 0, ptr.len as u64).unwrap()[0] != 0;
        let slots: Vec<PagePtr> = (1..=4).map(|id| write(id, 16)).collect();
        // Freed one reclaim apart, so each is older than the next.
        for ptr in &slots {
            store.free([*ptr]);
            store.reclaim();
        }
        for _ in 0..PUNCH_AFTER {
            store.reclaim();
        }
        // The two freed first are released, and the two within the budget
        // keep their space through any number of reclaims.
        assert_eq!(slots.iter().map(holds).collect::<Vec<_>>(), [false, false, true, true]);
        // A blob within an eighth of a kept slot's takes it, and one shorter
        // still takes a released slot.
        let fits = write(5, 15);
        assert!(fits.slot == slots[2].slot || fits.slot == slots[3].slot);
        let shorter = write(6, 13);
        assert!(shorter.slot == slots[0].slot || shorter.slot == slots[1].slot);
        // A blob as long as the kept slot's takes the one still free.
        let exact = write(7, 16);
        assert!(exact.slot != fits.slot && (exact.slot == slots[2].slot || exact.slot == slots[3].slot));
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
