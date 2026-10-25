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

pub trait Storage: Send + Sync {
    /// Opens `name`, creating an empty file when `create` is set and none exists.
    fn open(&self, name: &str, create: bool) -> io::Result<Arc<dyn File>>;
    fn remove(&self, name: &str) -> io::Result<()>;
    fn list(&self) -> io::Result<Vec<String>>;
    /// Makes earlier creations and removals durable.
    fn sync_dir(&self) -> io::Result<()>;
}

pub trait File: Send + Sync {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    /// Makes every completed write to this file durable.
    fn sync_data(&self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    /// Hints that reads are random, disabling kernel readahead.
    fn advise_random(&self) {}
    /// Releases the disk space behind `[offset, offset + len)`, which then
    /// reads as zeros. The file keeps its length.
    fn punch_hole(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

/// Storage backed by a directory on the local file system.
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(self.dir.join(name))?;
        Ok(Arc::new(FsFile(file)))
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        std::fs::remove_file(self.dir.join(name))
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
}

struct FsFile(std::fs::File);

impl File for FsFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn advise_random(&self) {
        // SAFETY: posix_fadvise only reads its integer arguments.
        unsafe {
            libc::posix_fadvise(self.0.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
        }
    }

    fn punch_hole(&self, offset: u64, len: u64) -> io::Result<()> {
        let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
        // SAFETY: fallocate only reads its integer arguments.
        let r = unsafe { libc::fallocate(self.0.as_raw_fd(), mode, offset as libc::off_t, len as libc::off_t) };
        if r == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }
}
