//! Backups: one checkpoint of a live store, copied as it is stored.
//!
//! [`crate::Db::backup`] brings every write made so far into the durable
//! tree and returns a [`Backup`] of that checkpoint: its superblock, and
//! each node page, data page and blob its tree references. The backup holds
//! each of those slots until it has copied it (see
//! [`crate::pagestore::PageStore::pin`]), while writes and checkpoints go
//! on. It reads each image past the page cache, verifies its checksums
//! without decompressing it, and yields the checkpoint one of two ways:
//!
//! - As an archive: a [`Backup`] is a [`std::io::Read`] whose bytes hold the
//!   images end to end, for a pipe or an upload. [`Backup::write_to`] writes
//!   the same bytes to a [`std::io::Write`], and [`Backup::archive_to`] to a
//!   file, past the page cache. [`restore`] writes an archive back into a
//!   storage, which then opens as a store, and [`verify`] checks one where
//!   it lies.
//! - As a store: [`Backup::copy_to`] writes each image to its own slot in
//!   another storage, which opens as it is. Its arenas keep the source's
//!   layout, free slots included, so they are sparse files.
//!
//! ```text
//! archive = header | record* | trailer
//! header  = magic: [u8; 8] | version: u32 | superblock: [u8; 4096]
//!         | pages: u64 | blobs: u64 | bytes: u64 | crc: u32
//! record  = kind: u8 | pointer: [u8; 21] | crc: u32 | image
//! trailer = 0: u8 | records: u64 | bytes: u64 | chain: u32 | crc: u32
//! ```
//!
//! A record's kind is 1 for a node page, 2 for a data page and 3 for a
//! blob, and its pointer says which slot the image came from and how long
//! it is. Each image carries its own checksums, which cover its id, so a
//! record's `crc` covers only its kind and pointer. `chain` is a checksum
//! over every record's kind and pointer in order, so an archive that lost,
//! gained or reordered a record fails, and an archive without its trailer
//! is incomplete. The chain leaves out each record's `crc`: a checksum that
//! runs over data followed by that data's own checksum comes out the same
//! whatever the data was. The superblock is encoded as the store encodes it, so a
//! version that cannot open the store cannot restore its archive either.
//!
//! Either way the superblock is written last, after everything it names is
//! durable: a copy or a restore that stops early leaves a storage that
//! refuses to open.

use crate::blob::{self, BlobPtr};
use crate::error::{Error, Result, corruption};
use crate::page;
use crate::pagestore::{self, PagePtr, Superblock};
use crate::storage::{DIRECT_ALIGN, File, Storage};
use crate::tree::{self, io::PageIo};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAGIC: &[u8; 8] = b"LTBACKUP";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 8 + 4 + Superblock::ENCODED_LEN + 3 * 8 + 4;
/// Bytes of a record before its image: kind, pointer and checksum.
const RECORD_HEAD: usize = 1 + 21 + 4;
/// Bytes of a record that the trailer's chain covers: kind and pointer.
const CHAINED: usize = 1 + 21;
const TRAILER_LEN: usize = 1 + 8 + 8 + 4 + 4;
const END: u8 = 0;

/// Bytes per read, and per write, of a rate-limited backup, so a client's
/// I/O queues behind at most this much of it. Checkpoints pace their writes
/// in pieces of the same size.
const PIECE: usize = 256 << 10;

/// How a backup runs.
#[derive(Clone, Debug)]
pub struct BackupOptions {
    /// Bytes per second the backup reads from the store, or `None` for as
    /// fast as the drive allows. The default is 256 MiB/s.
    ///
    /// At the default, reading a store of 100M small rows that served
    /// 500,000 operations a second moved the read p99 of a typical second
    /// from 5 to 5.5 ms. Unlimited, the backup read 2.1 GB/s there and moved
    /// it as little for the five seconds it read. A backup written to the
    /// drive the store is on costs more: an archive file beside that store
    /// at 100 MiB/s moved it to 8 ms.
    pub rate_limit: Option<u64>,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self { rate_limit: Some(256 << 20) }
    }
}

/// What a backup holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupInfo {
    /// The sequence number of the last write the backup holds. It holds
    /// every write up to that one and none after.
    pub seq: u64,
    /// Node and data pages.
    pub pages: u64,
    pub blobs: u64,
    /// Bytes of the pages and blobs as stored.
    pub bytes: u64,
}

/// One stored image: where it lives and what it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Image {
    Node(PagePtr),
    Page(PagePtr),
    Blob(BlobPtr),
}

impl Image {
    fn kind(&self) -> u8 {
        match self {
            Image::Node(_) => 1,
            Image::Page(_) => 2,
            Image::Blob(_) => 3,
        }
    }

    /// The image's slot, as its page store names it.
    fn slot(&self) -> PagePtr {
        match self {
            Image::Node(p) | Image::Page(p) => *p,
            Image::Blob(b) => b.page_ptr(),
        }
    }

    /// The file name prefix of the arenas that hold the image.
    fn arenas(&self) -> &'static str {
        match self {
            Image::Blob(_) => pagestore::BLOBS,
            _ => pagestore::PAGES,
        }
    }

    fn len(&self) -> usize {
        self.slot().len as usize
    }

    fn head(&self) -> [u8; RECORD_HEAD] {
        let mut out = [0u8; RECORD_HEAD];
        out[0] = self.kind();
        match self {
            Image::Node(p) | Image::Page(p) => {
                let mut ptr = Vec::with_capacity(PagePtr::ENCODED_LEN);
                p.encode(&mut ptr);
                out[1..22].copy_from_slice(&ptr);
            }
            Image::Blob(b) => out[1..22].copy_from_slice(&b.encode()),
        }
        let crc = crc32c::crc32c(&out[..CHAINED]);
        out[CHAINED..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn from_head(head: &[u8; RECORD_HEAD]) -> Result<Self> {
        let bad = || corruption("backup archive has a damaged record");
        if u32::from_le_bytes(head[CHAINED..].try_into().unwrap()) != crc32c::crc32c(&head[..CHAINED]) {
            return Err(bad());
        }
        let page = || PagePtr::decode(&head[1..22]).ok_or_else(bad);
        match head[0] {
            1 => Ok(Image::Node(page()?)),
            2 => Ok(Image::Page(page()?)),
            3 => Ok(Image::Blob(BlobPtr::decode(&head[1..22])?)),
            _ => Err(bad()),
        }
    }

    /// Verifies `stored`, the image's bytes, against its pointer and its
    /// own checksums.
    fn verify(&self, stored: &[u8]) -> Result<()> {
        match self {
            Image::Node(p) => tree::verify_node(stored, p.id),
            Image::Page(p) => page::verify_stored(stored, p.id, p.meta_off),
            Image::Blob(b) => blob::verify(b, stored),
        }
    }
}

/// A buffer for direct I/O: its start is aligned, and it hands out lengths
/// rounded up to [`DIRECT_ALIGN`].
#[derive(Default)]
struct Aligned {
    buf: Vec<u8>,
    start: usize,
}

impl Aligned {
    /// The first `len` bytes, rounded up to the alignment, kept from the
    /// last call where they fit.
    fn padded(&mut self, len: usize) -> &mut [u8] {
        let padded = len.next_multiple_of(DIRECT_ALIGN);
        if self.buf.len() < self.start + padded {
            self.buf = vec![0u8; padded + DIRECT_ALIGN];
            self.start = self.buf.as_ptr().align_offset(DIRECT_ALIGN);
        }
        &mut self.buf[self.start..self.start + padded]
    }
}

/// Holds I/O to a rate: each piece waits until the pieces before it have
/// had their time. Time spent idle is lost rather than saved for a burst.
struct Limiter {
    bytes_per_sec: Option<u64>,
    next: Instant,
}

impl Limiter {
    fn new(bytes_per_sec: Option<u64>) -> Self {
        Self { bytes_per_sec: bytes_per_sec.filter(|&rate| rate > 0), next: Instant::now() }
    }

    /// Bytes per I/O: a piece under a limit, and otherwise the whole image.
    fn piece(&self) -> usize {
        if self.bytes_per_sec.is_some() { PIECE } else { usize::MAX }
    }

    fn wait(&mut self, bytes: usize) {
        let Some(rate) = self.bytes_per_sec else { return };
        let start = self.next.max(Instant::now());
        self.next = start + Duration::from_secs_f64(bytes as f64 / rate as f64);
        std::thread::sleep(start.saturating_duration_since(Instant::now()));
    }
}

/// Writes images to their slots in the arenas of a storage that is to open
/// as a store.
struct Arenas {
    dest: Arc<dyn Storage>,
    /// Each arena written so far: its handle for direct writes, and the
    /// buffered handle that syncs it.
    files: HashMap<(&'static str, u8), (Arc<dyn File>, Arc<dyn File>)>,
}

impl Arenas {
    /// Starts a store in `dest`, which must hold nothing. It creates one
    /// arena durably before any image arrives: a storage that holds an
    /// arena and no superblock refuses to open, where an empty one would
    /// open as a new store.
    fn new(dest: Arc<dyn Storage>) -> Result<Self> {
        if !dest.list()?.is_empty() {
            return Err(Error::InvalidArgument("the destination of a backup must be empty".into()));
        }
        let mut arenas = Self { dest, files: HashMap::new() };
        arenas.file(pagestore::PAGES, 0)?;
        arenas.dest.sync_dir()?;
        Ok(arenas)
    }

    /// The handle for direct writes to an arena, which this creates if need be.
    fn file(&mut self, prefix: &'static str, class: u8) -> Result<&Arc<dyn File>> {
        if !self.files.contains_key(&(prefix, class)) {
            let name = pagestore::arena_name(prefix, class);
            let buffered = self.dest.open(&name, true)?;
            let direct = self.dest.open_direct(&name, false)?;
            self.files.insert((prefix, class), (direct, buffered));
        }
        Ok(&self.files[&(prefix, class)].0)
    }

    /// Writes `padded`, an image padded to [`DIRECT_ALIGN`] in an aligned
    /// buffer, to its slot, `piece` bytes at a time.
    fn write(&mut self, image: &Image, padded: &[u8], piece: usize) -> Result<()> {
        let slot = image.slot();
        let file = self.file(image.arenas(), slot.class)?;
        let piece = piece.min(padded.len()).max(DIRECT_ALIGN).next_multiple_of(DIRECT_ALIGN);
        for (i, part) in padded.chunks(piece).enumerate() {
            file.write_all_at(part, slot.offset() + (i * piece) as u64)?;
        }
        Ok(())
    }

    /// Makes the images durable, then writes the superblock that names them.
    fn finish(self, sb: &Superblock) -> Result<()> {
        for (_, buffered) in self.files.values() {
            buffered.sync_data()?;
        }
        self.dest.sync_dir()?;
        sb.create(&*self.dest)
    }
}

/// Gathers an archive's bytes into aligned chunks and writes each past the
/// page cache. [`Backup::archive_to`] writes what the last chunk holds.
struct DirectSink {
    direct: Arc<dyn File>,
    buf: Aligned,
    /// Bytes per write: a multiple of [`DIRECT_ALIGN`].
    chunk: usize,
    /// Bytes of the current chunk gathered so far.
    filled: usize,
    /// Where the current chunk starts in the file.
    offset: u64,
}

impl Write for DirectSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let n = bytes.len().min(self.chunk - self.filled);
        let chunk = self.buf.padded(self.chunk);
        chunk[self.filled..self.filled + n].copy_from_slice(&bytes[..n]);
        self.filled += n;
        if self.filled == self.chunk {
            self.direct.write_all_at(chunk, self.offset)?;
            self.offset += self.chunk as u64;
            self.filled = 0;
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Where a [`Backup`] read as an archive stands.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Header,
    Records,
    Trailer,
    Done,
}

/// One checkpoint of a store, pinned until it is copied or dropped.
///
/// Read it for an archive, or call [`Backup::copy_to`] for a copy that opens
/// as a store; see the [module documentation](self). Writes and checkpoints
/// go on meanwhile, and the backup holds what [`Backup::info`] says
/// whatever they do. A page or blob the store supersedes keeps its disk
/// space until the backup has copied it, so finish or drop a backup rather
/// than hold it, and drop it before reopening the store.
pub struct Backup {
    io: Arc<PageIo>,
    sb: Superblock,
    /// Every image, in the order they are copied: by arena and slot, so
    /// each arena is read front to back.
    images: Vec<Image>,
    /// How many images have been copied and let go.
    next: usize,
    info: BackupInfo,
    limiter: Limiter,
    buf: Aligned,
    stage: Stage,
    /// The bytes of the archive that precede the image in `buf`, or stand
    /// alone: the header, a record's head, or the trailer.
    head: Vec<u8>,
    head_pos: usize,
    /// How much of the image in `buf` the archive has yet to yield.
    body: std::ops::Range<usize>,
    /// Checksum over the kind and pointer of each record yielded so far.
    chain: u32,
    copied: u64,
}

impl Backup {
    /// Pins `images`, the images of the tree `sb` names. The caller holds a
    /// version of that tree until this returns.
    pub(crate) fn new(io: Arc<PageIo>, sb: Superblock, images: tree::Images, opts: &BackupOptions) -> Self {
        let mut pages: Vec<Image> = images.nodes.into_iter().map(Image::Node).chain(images.pages.into_iter().map(Image::Page)).collect();
        let mut blobs: Vec<Image> = images.blobs.into_iter().map(Image::Blob).collect();
        let by_slot = |image: &Image| (image.slot().class, image.slot().slot);
        pages.sort_unstable_by_key(by_slot);
        blobs.sort_unstable_by_key(by_slot);
        io.store.pin(pages.iter().map(by_slot));
        io.blobs.pin(blobs.iter().map(by_slot));
        let info = BackupInfo {
            seq: sb.last_seq,
            pages: pages.len() as u64,
            blobs: blobs.len() as u64,
            bytes: pages.iter().chain(&blobs).map(|image| image.len() as u64).sum(),
        };
        pages.append(&mut blobs);
        Self {
            io,
            sb,
            images: pages,
            next: 0,
            info,
            limiter: Limiter::new(opts.rate_limit),
            buf: Aligned::default(),
            stage: Stage::Header,
            head: Vec::new(),
            head_pos: 0,
            body: 0..0,
            chain: 0,
            copied: 0,
        }
    }

    /// What the backup holds.
    pub fn info(&self) -> BackupInfo {
        self.info
    }

    /// The sequence number of the last write the backup holds.
    pub fn seq(&self) -> u64 {
        self.info.seq
    }

    /// Bytes of pages and blobs copied so far, of [`BackupInfo::bytes`].
    pub fn copied_bytes(&self) -> u64 {
        self.copied
    }

    /// Reads the next image into `buf`, verifies it and lets its slot go.
    /// Returns `None` once every image has been copied.
    fn next_image(&mut self) -> Result<Option<Image>> {
        let Some(&image) = self.images.get(self.next) else { return Ok(None) };
        let slot = image.slot();
        let store = if matches!(image, Image::Blob(_)) { &self.io.blobs } else { &self.io.store };
        let padded = self.buf.padded(image.len());
        let piece = self.limiter.piece().min(padded.len()).max(DIRECT_ALIGN);
        for (i, part) in padded.chunks_mut(piece).enumerate() {
            self.limiter.wait(part.len());
            store.read_direct(&slot, (i * piece) as u64, part)?;
        }
        image.verify(&padded[..image.len()])?;
        // The image is in memory, so the store may reuse the slot.
        store.unpin(slot.class, slot.slot);
        self.next += 1;
        self.copied += image.len() as u64;
        Ok(Some(image))
    }

    /// Copies the checkpoint into `dest`, an empty storage, which then
    /// opens as a store that holds every write up to [`Backup::seq`]. Each
    /// image goes to the slot it has in the source, so the arenas in `dest`
    /// are as sparse as the source's.
    ///
    /// If this fails, `dest` holds no superblock and refuses to open.
    pub fn copy_to(mut self, dest: Arc<dyn Storage>) -> Result<BackupInfo> {
        if self.stage != Stage::Header {
            return Err(Error::InvalidArgument("the backup has been read as an archive".into()));
        }
        let mut arenas = Arenas::new(dest)?;
        while let Some(image) = self.next_image()? {
            let padded = self.buf.padded(image.len());
            padded[image.len()..].fill(0);
            arenas.write(&image, padded, self.limiter.piece())?;
        }
        arenas.finish(&self.sb)?;
        Ok(self.info)
    }

    /// Writes the backup to `out` as an archive: the bytes that reading the
    /// backup to its end yields.
    pub fn write_to(mut self, out: &mut dyn Write) -> Result<BackupInfo> {
        if self.stage != Stage::Header {
            return Err(Error::InvalidArgument("the backup has been read as an archive".into()));
        }
        out.write_all(&self.header())?;
        while let Some(image) = self.next_image()? {
            let head = image.head();
            self.chain = crc32c::crc32c_append(self.chain, &head[..CHAINED]);
            out.write_all(&head)?;
            out.write_all(&self.buf.padded(image.len())[..image.len()])?;
        }
        out.write_all(&self.trailer())?;
        out.flush()?;
        Ok(self.info)
    }

    /// Writes the backup as an archive to the file `name` in `dest`, which
    /// this creates or replaces, and makes it durable. The writes go past
    /// the page cache where the storage allows, at the backup's rate.
    ///
    /// Writing through the page cache costs clients more, most of all under
    /// a memory limit that counts it: with the archive beside a store of
    /// 100M small rows serving 500,000 operations a second, 100 MiB/s
    /// through the page cache held client p99 at 445 ms, against 27 ms past
    /// it.
    pub fn archive_to(self, dest: &dyn Storage, name: &str) -> Result<BackupInfo> {
        match dest.remove(name) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        let buffered = dest.open(name, true)?;
        let direct = dest.open_direct(name, false)?;
        let chunk = self.limiter.piece().min(1 << 20);
        let mut sink = DirectSink { direct, buf: Aligned::default(), chunk, filled: 0, offset: 0 };
        let info = self.write_to(&mut sink)?;
        // Whole blocks go the way the rest did, and what is left of the
        // last block through the page cache.
        let whole = sink.filled - sink.filled % DIRECT_ALIGN;
        let (blocks, tail) = sink.buf.padded(sink.chunk)[..sink.filled].split_at(whole);
        if !blocks.is_empty() {
            sink.direct.write_all_at(blocks, sink.offset)?;
        }
        if !tail.is_empty() {
            buffered.write_all_at(tail, sink.offset + whole as u64)?;
        }
        buffered.sync_data()?;
        dest.sync_dir()?;
        Ok(info)
    }

    fn header(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&self.sb.encode());
        for n in [self.info.pages, self.info.blobs, self.info.bytes] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(out.len(), HEADER_LEN);
        out
    }

    fn trailer(&self) -> Vec<u8> {
        let mut out = vec![END];
        out.extend_from_slice(&(self.images.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.info.bytes.to_le_bytes());
        out.extend_from_slice(&self.chain.to_le_bytes());
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(out.len(), TRAILER_LEN);
        out
    }
}

/// The backup as an archive; see the [module documentation](self).
impl Read for Backup {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.head_pos < self.head.len() {
                let n = out.len().min(self.head.len() - self.head_pos);
                out[..n].copy_from_slice(&self.head[self.head_pos..self.head_pos + n]);
                self.head_pos += n;
                return Ok(n);
            }
            if !self.body.is_empty() {
                let n = out.len().min(self.body.len());
                let image = &self.buf.buf[self.buf.start..];
                out[..n].copy_from_slice(&image[self.body.start..self.body.start + n]);
                self.body.start += n;
                return Ok(n);
            }
            self.head_pos = 0;
            match self.stage {
                Stage::Header => {
                    self.head = self.header();
                    self.stage = Stage::Records;
                }
                Stage::Records => match self.next_image() {
                    Ok(Some(image)) => {
                        let head = image.head();
                        self.chain = crc32c::crc32c_append(self.chain, &head[..CHAINED]);
                        self.head = head.to_vec();
                        self.body = 0..image.len();
                    }
                    Ok(None) => {
                        self.head = self.trailer();
                        self.stage = Stage::Trailer;
                    }
                    Err(Error::Io(e)) => return Err(e),
                    Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                },
                Stage::Trailer | Stage::Done => {
                    self.head.clear();
                    self.stage = Stage::Done;
                    return Ok(0);
                }
            }
        }
    }
}

impl Drop for Backup {
    fn drop(&mut self) {
        for image in &self.images[self.next..] {
            let slot = image.slot();
            let store = if matches!(image, Image::Blob(_)) { &self.io.blobs } else { &self.io.store };
            store.unpin(slot.class, slot.slot);
        }
    }
}

/// Reads exactly `buf.len()` bytes of an archive.
fn fill(reader: &mut dyn Read, buf: &mut [u8]) -> Result<()> {
    reader.read_exact(buf).map_err(|e| match e.kind() {
        std::io::ErrorKind::UnexpectedEof => corruption("backup archive ends early"),
        _ => Error::Io(e),
    })
}

/// Reads an archive to its end, verifying it, and hands each image to
/// `each` with its bytes padded to [`DIRECT_ALIGN`] in an aligned buffer.
fn read_archive(reader: &mut dyn Read, mut each: impl FnMut(&Image, &[u8]) -> Result<()>) -> Result<(Superblock, BackupInfo)> {
    let word = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
    let crc_at = |b: &[u8], at: usize| u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
    let mut header = [0u8; HEADER_LEN];
    fill(reader, &mut header)?;
    if &header[..8] != MAGIC {
        return Err(corruption("not a backup archive"));
    }
    if crc_at(&header, HEADER_LEN - 4) != crc32c::crc32c(&header[..HEADER_LEN - 4]) {
        return Err(corruption("backup archive header checksum mismatch"));
    }
    if crc_at(&header, 8) != VERSION {
        return Err(corruption(format!("backup archive of version {}, which this version cannot read", crc_at(&header, 8))));
    }
    let sb = Superblock::decode(&header[12..12 + Superblock::ENCODED_LEN])
        .ok_or_else(|| corruption("backup archive holds a superblock this version cannot read"))?;
    let counts = 12 + Superblock::ENCODED_LEN;
    let info =
        BackupInfo { seq: sb.last_seq, pages: word(&header, counts), blobs: word(&header, counts + 8), bytes: word(&header, counts + 16) };

    let (mut pages, mut blobs, mut bytes, mut chain) = (0u64, 0u64, 0u64, 0u32);
    let mut buf = Aligned::default();
    loop {
        let mut head = [0u8; RECORD_HEAD];
        fill(reader, &mut head[..1])?;
        if head[0] == END {
            let mut trailer = [0u8; TRAILER_LEN];
            fill(reader, &mut trailer[1..])?;
            if crc_at(&trailer, TRAILER_LEN - 4) != crc32c::crc32c(&trailer[..TRAILER_LEN - 4]) {
                return Err(corruption("backup archive trailer checksum mismatch"));
            }
            let complete = word(&trailer, 1) == pages + blobs && word(&trailer, 9) == bytes && crc_at(&trailer, 17) == chain;
            if !complete || (pages, blobs, bytes) != (info.pages, info.blobs, info.bytes) {
                return Err(corruption("backup archive does not hold the records it lists"));
            }
            let mut past = [0u8; 1];
            return match reader.read(&mut past)? {
                0 => Ok((sb, info)),
                _ => Err(corruption("backup archive runs past its trailer")),
            };
        }
        fill(reader, &mut head[1..])?;
        let image = Image::from_head(&head)?;
        chain = crc32c::crc32c_append(chain, &head[..CHAINED]);
        let padded = buf.padded(image.len());
        fill(reader, &mut padded[..image.len()])?;
        padded[image.len()..].fill(0);
        image.verify(&padded[..image.len()])?;
        match image {
            Image::Blob(_) => blobs += 1,
            _ => pages += 1,
        }
        bytes += image.len() as u64;
        each(&image, padded)?;
    }
}

/// Writes the store an archive holds into `dest`, an empty storage, which
/// then opens as a store with every write up to the returned sequence
/// number. Verifies every image on the way.
///
/// If this fails, `dest` holds no superblock and refuses to open.
pub fn restore(mut archive: impl Read, dest: Arc<dyn Storage>) -> Result<BackupInfo> {
    let mut arenas = Arenas::new(dest)?;
    let (sb, info) = read_archive(&mut archive, |image, padded| arenas.write(image, padded, usize::MAX))?;
    arenas.finish(&sb)?;
    Ok(info)
}

/// Checks an archive from end to end: that it is complete, and that every
/// image in it passes its checksums.
pub fn verify(mut archive: impl Read) -> Result<BackupInfo> {
    read_archive(&mut archive, |_, _| Ok(())).map(|(_, info)| info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_heads_round_trip_and_reject_damage() {
        let page = PagePtr { id: 9, class: 3, slot: 77, len: 20_000, meta_off: 16_384 };
        let blob = BlobPtr { id: 11, class: 9, slot: 5, len: 70_000, raw_len: 123_456 };
        for image in [Image::Node(page), Image::Page(page), Image::Blob(blob)] {
            let head = image.head();
            assert_eq!(Image::from_head(&head).unwrap(), image);
            for at in 0..RECORD_HEAD {
                let mut damaged = head;
                damaged[at] ^= 4;
                assert!(Image::from_head(&damaged).is_err(), "byte {at}");
            }
        }
    }

    #[test]
    fn aligned_buffers_are_aligned_and_padded() {
        let mut buf = Aligned::default();
        for len in [1, 4096, 4097, 100_000, 5] {
            let padded = buf.padded(len);
            assert_eq!(padded.len(), len.next_multiple_of(DIRECT_ALIGN));
            assert_eq!(padded.as_ptr().align_offset(DIRECT_ALIGN), 0);
        }
    }

    #[test]
    fn a_limiter_holds_its_rate() {
        let mut limiter = Limiter::new(Some(1 << 20));
        let started = Instant::now();
        for _ in 0..8 {
            limiter.wait(16 << 10);
        }
        // Eight pieces of 16 KiB at 1 MiB/s: the last starts 7/64 s in.
        assert!(started.elapsed() >= Duration::from_millis(100), "{:?}", started.elapsed());
        let mut unlimited = Limiter::new(None);
        unlimited.wait(1 << 30);
        assert_eq!(unlimited.piece(), usize::MAX);
    }
}
