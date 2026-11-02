//! A flat directory of files with positional I/O.
//!
//! The engine performs every read, write, and sync through these traits, so the
//! same code runs against a real directory ([`FsStorage`]) or the simulated disk
//! used for deterministic testing ([`crate::sim::SimStorage`]).

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Alignment of offsets, lengths and buffers for writes to a file opened
/// with [`Storage::open_direct`].
pub const DIRECT_ALIGN: usize = 4096;

pub trait Storage: Send + Sync {
    /// Opens `name`, creating an empty file when `create` is set and none exists.
    fn open(&self, name: &str, create: bool) -> io::Result<Arc<dyn File>>;
    /// Opens `name` for writes that bypass the page cache. Every write's
    /// offset, length and buffer address must be multiples of
    /// [`DIRECT_ALIGN`].
    fn open_direct(&self, name: &str, create: bool) -> io::Result<Arc<dyn File>> {
        self.open(name, create)
    }
    fn remove(&self, name: &str) -> io::Result<()>;
    /// Renames `from` to `to`, replacing any file named `to`.
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
    fn list(&self) -> io::Result<Vec<String>>;
    /// Makes earlier creations, removals and renames durable.
    fn sync_dir(&self) -> io::Result<()>;
    /// Whether scans should hand their prefetches to background threads.
    /// A Linux prefetch allocates its page cache in the calling thread, and
    /// under a memory limit each allocation may first have to reclaim memory.
    fn prefetch_in_background(&self) -> bool {
        false
    }
}

pub trait File: Send + Sync {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    /// Writes `buf` and returns once it, and what is needed to read it back,
    /// is durable. Earlier writes stay only as durable as they were.
    fn write_all_at_durable(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.write_all_at(buf, offset)?;
        self.sync_data()
    }
    /// Makes every completed write to this file durable.
    fn sync_data(&self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    /// Extends the file to at least `len` bytes. The new range reads as
    /// zeros and holds no disk space.
    fn extend(&self, _len: u64) -> io::Result<()> {
        Ok(())
    }
    /// Hints that reads are random, disabling kernel readahead.
    fn advise_random(&self) {}
    /// Releases the disk space behind `[offset, offset + len)`, which then
    /// reads as zeros. The file keeps its length.
    fn punch_hole(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
    /// Zeroes `[offset, offset + len)` as written blocks, extending the file
    /// to cover it, so that later writes there overwrite allocated space.
    /// Returns `Unsupported` where the file system cannot do this without
    /// the caller writing the zeros itself.
    fn write_zeroes(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Starts reading `[offset, offset + len)` into the page cache without
    /// waiting, so a later read of it waits less.
    fn prefetch(&self, _offset: u64, _len: u64) {}
    /// Drops `[offset, offset + len)` from the page cache.
    fn drop_cache(&self, _offset: u64, _len: u64) {}
}

/// Storage backed by a directory on the local file system.
///
/// On Linux, direct files open with O_DIRECT and durable writes use
/// `pwritev2` with RWF_DSYNC. macOS has neither, so there every file is
/// buffered, and a durable write is a write and an `F_FULLFSYNC`, which
/// Rust's `sync_data` issues there and which also flushes the drive's cache.
pub struct FsStorage {
    dir: PathBuf,
}

impl FsStorage {
    pub fn new(dir: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(dir.as_ref())?;
        Ok(Self { dir: dir.as_ref().to_path_buf() })
    }
}

impl Storage for FsStorage {
    fn open(&self, name: &str, create: bool) -> io::Result<Arc<dyn File>> {
        let file = OpenOptions::new().read(true).write(true).create(create).truncate(false).open(self.dir.join(name))?;
        Ok(Arc::new(FsFile(file)))
    }

    /// Opens with O_DIRECT, or buffered on a file system without it.
    #[cfg(target_os = "linux")]
    fn open_direct(&self, name: &str, create: bool) -> io::Result<Arc<dyn File>> {
        use std::os::unix::fs::OpenOptionsExt;
        let opened =
            OpenOptions::new().read(true).write(true).create(create).truncate(false).custom_flags(libc::O_DIRECT).open(self.dir.join(name));
        match opened {
            Ok(file) => Ok(Arc::new(FsFile(file))),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => self.open(name, create),
            Err(e) => Err(e),
        }
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        std::fs::remove_file(self.dir.join(name))
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        std::fs::rename(self.dir.join(from), self.dir.join(to))
    }

    fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    fn sync_dir(&self) -> io::Result<()> {
        std::fs::File::open(&self.dir)?.sync_all()
    }

    #[cfg(target_os = "linux")]
    fn prefetch_in_background(&self) -> bool {
        true
    }
}

struct FsFile(std::fs::File);

impl File for FsFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    /// One `pwritev2` with RWF_DSYNC: the kernel writes with FUA, or follows
    /// the write with a flush, so no separate fsync is needed.
    #[cfg(target_os = "linux")]
    fn write_all_at_durable(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let iov = libc::iovec { iov_base: buf.as_ptr() as *mut libc::c_void, iov_len: buf.len() };
            // SAFETY: `iov` describes `buf`, which outlives the call.
            let n = unsafe { libc::pwritev2(self.0.as_raw_fd(), &iov, 1, offset as libc::off_t, libc::RWF_DSYNC) };
            if n < 0 {
                let e = io::Error::last_os_error();
                match e.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) => {
                        self.0.write_all_at(buf, offset)?;
                        return self.0.sync_data();
                    }
                    _ => return Err(e),
                }
            }
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            buf = &buf[n as usize..];
            offset += n as u64;
        }
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn extend(&self, len: u64) -> io::Result<()> {
        if self.0.metadata()?.len() < len {
            self.0.set_len(len)?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn advise_random(&self) {
        // SAFETY: posix_fadvise only reads its integer arguments.
        unsafe {
            libc::posix_fadvise(self.0.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
        }
    }

    #[cfg(target_os = "linux")]
    fn prefetch(&self, offset: u64, len: u64) {
        // SAFETY: posix_fadvise only reads its integer arguments.
        unsafe {
            libc::posix_fadvise(self.0.as_raw_fd(), offset as libc::off_t, len as libc::off_t, libc::POSIX_FADV_WILLNEED);
        }
    }

    #[cfg(target_os = "linux")]
    fn drop_cache(&self, offset: u64, len: u64) {
        // SAFETY: posix_fadvise only reads its integer arguments.
        unsafe {
            libc::posix_fadvise(self.0.as_raw_fd(), offset as libc::off_t, len as libc::off_t, libc::POSIX_FADV_DONTNEED);
        }
    }

    #[cfg(target_os = "linux")]
    fn punch_hole(&self, offset: u64, len: u64) -> io::Result<()> {
        let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
        // SAFETY: fallocate only reads its integer arguments.
        let r = unsafe { libc::fallocate(self.0.as_raw_fd(), mode, offset as libc::off_t, len as libc::off_t) };
        if r == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }

    /// `fallocate` with FALLOC_FL_WRITE_ZEROES, from Linux 6.17, which has
    /// the drive zero the range where it can. Plain `fallocate` would leave
    /// the blocks unwritten, and the first write into one changes extent
    /// metadata that the sync after it must journal.
    #[cfg(target_os = "linux")]
    fn write_zeroes(&self, offset: u64, len: u64) -> io::Result<()> {
        // Not yet in the libc crate.
        const FALLOC_FL_WRITE_ZEROES: libc::c_int = 0x80;
        loop {
            // SAFETY: fallocate only reads its integer arguments.
            let r = unsafe { libc::fallocate(self.0.as_raw_fd(), FALLOC_FL_WRITE_ZEROES, offset as libc::off_t, len as libc::off_t) };
            if r == 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EOPNOTSUPP) | Some(libc::EINVAL) | Some(libc::ENOSYS) => return Err(io::ErrorKind::Unsupported.into()),
                _ => return Err(e),
            }
        }
    }

    /// Turns off read-ahead, as POSIX_FADV_RANDOM does on Linux.
    #[cfg(target_os = "macos")]
    fn advise_random(&self) {
        // SAFETY: fcntl with F_RDAHEAD only reads its integer arguments.
        unsafe {
            libc::fcntl(self.0.as_raw_fd(), libc::F_RDAHEAD, 0);
        }
    }

    /// Starts an asynchronous read, as POSIX_FADV_WILLNEED does on Linux.
    #[cfg(target_os = "macos")]
    fn prefetch(&self, offset: u64, len: u64) {
        let advice = libc::radvisory { ra_offset: offset as libc::off_t, ra_count: len.min(i32::MAX as u64) as libc::c_int };
        // SAFETY: fcntl reads `advice`, which outlives the call.
        unsafe {
            libc::fcntl(self.0.as_raw_fd(), libc::F_RDADVISE, &advice);
        }
    }

    /// APFS punches holes with F_PUNCHHOLE. Callers ignore a failure, which
    /// leaves the space allocated.
    #[cfg(target_os = "macos")]
    fn punch_hole(&self, offset: u64, len: u64) -> io::Result<()> {
        let hole = libc::fpunchhole_t { fp_flags: 0, reserved: 0, fp_offset: offset as libc::off_t, fp_length: len as libc::off_t };
        // SAFETY: fcntl reads `hole`, which outlives the call.
        let r = unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_PUNCHHOLE, &hole) };
        if r == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }
}
