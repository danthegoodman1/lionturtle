//! C bindings for lionturtle.
//!
//! cbindgen generates `include/lionturtle.h` from this file, and
//! `include/lionturtle.hpp` wraps those functions in C++ classes.
#![allow(non_camel_case_types)]
// Every function follows the pointer contract in the header's opening
// comment, so none repeats it in a `# Safety` section.
#![allow(clippy::missing_safety_doc)]

use std::any::Any;
use std::cell::RefCell;
use std::ffi::{CStr, CString, OsStr, c_char};
use std::os::unix::ffi::OsStrExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;

use bytes::Bytes;
use lionturtle::storage::FsStorage;
use lionturtle::{Compression, Db, DbIter, Error, MemTableKind, Options, Result, Snapshot, TreeConfig, WriteOp};

/// The outcome of a call. A call that returns anything but LT_OK leaves a
/// message for lt_last_error().
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum lt_status {
    LT_OK = 0,
    /// The file system reported an error.
    LT_IO_ERROR = 1,
    /// Stored data failed a checksum or could not be decoded.
    LT_CORRUPTION = 2,
    /// An earlier write failed, and the store refuses work until reopened.
    LT_POISONED = 3,
    /// An argument was out of range: a null pointer, an unknown option, a
    /// key over 65,535 bytes, or a value over 16 MiB.
    LT_INVALID_ARGUMENT = 4,
    /// lionturtle panicked, which is a bug. The store may be unusable.
    LT_PANIC = 5,
}

/// The bbtree crate's concurrent B+-tree: the default memtable.
pub const LT_MEMTABLE_BBTREE: u32 = 0;
/// crossbeam's lock-free skip list.
pub const LT_MEMTABLE_SKIPLIST: u32 = 1;
/// Rust's BTreeMap behind a reader-writer lock.
pub const LT_MEMTABLE_BTREE: u32 = 2;

/// Data blocks stored as they are: the default.
pub const LT_COMPRESSION_NONE: u32 = 0;
/// Each data block compressed on its own with LZ4.
pub const LT_COMPRESSION_LZ4: u32 = 1;

/// Settings for lt_open(). Start from lt_options_default(); the
/// Configuration section of lionturtle's README describes each field.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct lt_options {
    /// Memtable size in leaf-sized batches.
    pub checkpoint_distance: u64,
    /// Block and page-image cache size in bytes.
    pub cache_bytes: u64,
    /// One of the LT_MEMTABLE_* values.
    pub memtable: u32,
    /// One of the LT_COMPRESSION_* values: how data pages written from now
    /// on are stored.
    pub compression: u32,
    /// Frozen memtables allowed to await checkpointing before writers wait.
    pub max_frozen: usize,
    /// Log bytes buffered before a write to the file.
    pub wal_buffer_bytes: usize,
    /// Threads for parallel merges and page writes; 0 uses a pool shared
    /// by the process.
    pub merge_threads: usize,
    /// Checkpoint on background threads. With false, the caller drives
    /// checkpoints with lt_checkpoint_step().
    pub background: bool,
    /// Spread each background checkpoint's I/O over most of the time its
    /// memtable took to fill.
    pub pace_checkpoints: bool,
    /// Zero-fill each log segment ahead of its writes, so that durable
    /// writes on a new store overwrite allocated space.
    pub preallocate_log: bool,
    /// Leaf and flush batch size in bytes.
    pub leaf_size: u64,
    /// Buffer levels per node.
    pub max_levels: usize,
    /// Bloom filter bits per key.
    pub filter_bits: u32,
    /// Pivots per node before it splits.
    pub max_pivots: usize,
}

impl lt_options {
    fn to_options(self) -> Result<Options> {
        let memtable = match self.memtable {
            LT_MEMTABLE_BBTREE => MemTableKind::BBTree,
            LT_MEMTABLE_SKIPLIST => MemTableKind::SkipList,
            LT_MEMTABLE_BTREE => MemTableKind::BTree,
            other => return Err(invalid(format!("unknown memtable {other}"))),
        };
        let compression = match self.compression {
            LT_COMPRESSION_NONE => Compression::None,
            LT_COMPRESSION_LZ4 => Compression::Lz4,
            other => return Err(invalid(format!("unknown compression {other}"))),
        };
        Ok(Options {
            tree: TreeConfig {
                leaf_size: self.leaf_size,
                max_levels: self.max_levels,
                filter_bits: self.filter_bits,
                max_pivots: self.max_pivots,
                ..TreeConfig::default()
            },
            checkpoint_distance: self.checkpoint_distance,
            cache_bytes: self.cache_bytes,
            memtable,
            wal_buffer_bytes: self.wal_buffer_bytes,
            max_frozen: self.max_frozen,
            background: self.background,
            merge_threads: self.merge_threads,
            pace_checkpoints: self.pace_checkpoints,
            preallocate_log: self.preallocate_log,
            compression,
        })
    }
}

/// Bytes owned by a handle, valid for as long as the call that returned
/// them says.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct lt_slice {
    pub data: *const c_char,
    pub len: usize,
}

const EMPTY: lt_slice = lt_slice { data: ptr::null(), len: 0 };

impl From<&Bytes> for lt_slice {
    fn from(b: &Bytes) -> Self {
        lt_slice { data: b.as_ptr().cast(), len: b.len() }
    }
}

/// An open store.
pub struct lt_db(Db);

/// A view of the store as of one moment.
pub struct lt_snapshot(Snapshot);

/// An iterator over one snapshot's entries.
pub struct lt_iter {
    iter: DbIter,
    entry: Option<(Bytes, Bytes)>,
}

/// A value read from the store.
pub struct lt_value(Bytes);

/// Writes that lt_write() applies atomically.
pub struct lt_batch(Vec<WriteOp>);

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::InvalidArgument(msg.into())
}

/// Records `msg` for `lt_last_error` and returns `status`.
fn fail(status: lt_status, msg: String) -> lt_status {
    let msg = CString::new(msg.replace('\0', " ")).expect("NULs replaced");
    LAST_ERROR.with(|e| *e.borrow_mut() = msg);
    status
}

/// Runs `f`, turning its error or panic into a status.
fn call(f: impl FnOnce() -> Result<()>) -> lt_status {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => lt_status::LT_OK,
        Ok(Err(e)) => {
            let status = match &e {
                Error::Io(_) => lt_status::LT_IO_ERROR,
                Error::Corruption(_) => lt_status::LT_CORRUPTION,
                Error::Poisoned => lt_status::LT_POISONED,
                Error::InvalidArgument(_) => lt_status::LT_INVALID_ARGUMENT,
            };
            fail(status, e.to_string())
        }
        Err(panic) => fail(lt_status::LT_PANIC, format!("lionturtle panicked: {}", panic_message(&*panic))),
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic.downcast_ref::<&str>().copied().or_else(|| panic.downcast_ref::<String>().map(String::as_str)).unwrap_or("no message")
}

unsafe fn handle<'a, T>(p: *const T) -> Result<&'a T> {
    unsafe { p.as_ref() }.ok_or_else(|| invalid("null handle"))
}

/// The `len` bytes at `data`, which may be null when `len` is 0.
unsafe fn bytes<'a>(data: *const c_char, len: usize) -> Result<&'a [u8]> {
    match len {
        0 => Ok(&[]),
        _ if data.is_null() => Err(invalid("null pointer with a nonzero length")),
        _ => Ok(unsafe { std::slice::from_raw_parts(data.cast(), len) }),
    }
}

/// An upper bound, absent when `data` is null.
unsafe fn bound<'a>(data: *const c_char, len: usize) -> Result<Option<&'a [u8]>> {
    if data.is_null() { Ok(None) } else { unsafe { bytes(data, len) }.map(Some) }
}

/// The output pointer `out`, set to null until the call succeeds.
unsafe fn output<'a, T>(out: *mut *mut T) -> Result<&'a mut *mut T> {
    let out = unsafe { out.as_mut() }.ok_or_else(|| invalid("null output pointer"))?;
    *out = ptr::null_mut();
    Ok(out)
}

/// Frees `p`, which came from `Box::into_raw`, unless it is null.
unsafe fn free<T>(p: *mut T) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p) });
    }
}

/// The reads a store and a snapshot share.
trait Reader {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    fn range(&self, start: &[u8], end: Option<&[u8]>, reverse: bool) -> DbIter;
    fn prefix(&self, prefix: &[u8], reverse: bool) -> DbIter;
}

impl Reader for lt_db {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.0.get(key)
    }

    fn range(&self, start: &[u8], end: Option<&[u8]>, reverse: bool) -> DbIter {
        if reverse { self.0.iter_rev(start, end) } else { self.0.iter(start, end) }
    }

    fn prefix(&self, prefix: &[u8], reverse: bool) -> DbIter {
        if reverse { self.0.prefix_rev(prefix) } else { self.0.prefix(prefix) }
    }
}

impl Reader for lt_snapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.0.get(key)
    }

    fn range(&self, start: &[u8], end: Option<&[u8]>, reverse: bool) -> DbIter {
        if reverse { self.0.iter_rev(start, end) } else { self.0.iter(start, end) }
    }

    fn prefix(&self, prefix: &[u8], reverse: bool) -> DbIter {
        if reverse { self.0.prefix_rev(prefix) } else { self.0.prefix(prefix) }
    }
}

unsafe fn read_get<R: Reader>(reader: *const R, key: *const c_char, key_len: usize, value: *mut *mut lt_value) -> lt_status {
    call(|| {
        let out = unsafe { output(value) }?;
        let found = unsafe { handle(reader)?.get(bytes(key, key_len)?) }?;
        *out = found.map_or(ptr::null_mut(), |v| Box::into_raw(Box::new(lt_value(v))));
        Ok(())
    })
}

unsafe fn read_range<R: Reader>(
    reader: *const R,
    start: *const c_char,
    start_len: usize,
    end: *const c_char,
    end_len: usize,
    reverse: bool,
    iter: *mut *mut lt_iter,
) -> lt_status {
    call(|| {
        let out = unsafe { output(iter) }?;
        let it = unsafe { handle(reader)?.range(bytes(start, start_len)?, bound(end, end_len)?, reverse) };
        *out = Box::into_raw(Box::new(lt_iter { iter: it, entry: None }));
        Ok(())
    })
}

unsafe fn read_prefix<R: Reader>(
    reader: *const R,
    prefix: *const c_char,
    prefix_len: usize,
    reverse: bool,
    iter: *mut *mut lt_iter,
) -> lt_status {
    call(|| {
        let out = unsafe { output(iter) }?;
        let it = unsafe { handle(reader)?.prefix(bytes(prefix, prefix_len)?, reverse) };
        *out = Box::into_raw(Box::new(lt_iter { iter: it, entry: None }));
        Ok(())
    })
}

/// The default options.
#[unsafe(no_mangle)]
pub extern "C" fn lt_options_default() -> lt_options {
    let o = Options::default();
    lt_options {
        checkpoint_distance: o.checkpoint_distance,
        cache_bytes: o.cache_bytes,
        memtable: match o.memtable {
            MemTableKind::BBTree => LT_MEMTABLE_BBTREE,
            MemTableKind::SkipList => LT_MEMTABLE_SKIPLIST,
            MemTableKind::BTree => LT_MEMTABLE_BTREE,
        },
        compression: match o.compression {
            Compression::None => LT_COMPRESSION_NONE,
            Compression::Lz4 => LT_COMPRESSION_LZ4,
        },
        max_frozen: o.max_frozen,
        wal_buffer_bytes: o.wal_buffer_bytes,
        merge_threads: o.merge_threads,
        background: o.background,
        pace_checkpoints: o.pace_checkpoints,
        preallocate_log: o.preallocate_log,
        leaf_size: o.tree.leaf_size,
        max_levels: o.tree.max_levels,
        filter_bits: o.tree.filter_bits,
        max_pivots: o.tree.max_pivots,
    }
}

/// Describes the last failed call on this thread. The string stays valid
/// until another call on this thread fails.
#[unsafe(no_mangle)]
pub extern "C" fn lt_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

/// Opens the store in the directory `path`, creating both if needed, and
/// recovers every logged write. `options` may be null for the defaults. On
/// success `*db` holds the store; close it with lt_close().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_open(path: *const c_char, options: *const lt_options, db: *mut *mut lt_db) -> lt_status {
    call(|| {
        let out = unsafe { output(db) }?;
        let path = unsafe { path.as_ref() }.ok_or_else(|| invalid("null path"))?;
        let path = OsStr::from_bytes(unsafe { CStr::from_ptr(path) }.to_bytes());
        let opts = match unsafe { options.as_ref() } {
            Some(o) => o.to_options()?,
            None => Options::default(),
        };
        let storage = FsStorage::new(path)?;
        *out = Box::into_raw(Box::new(lt_db(Db::open(Arc::new(storage), opts)?)));
        Ok(())
    })
}

/// Stops background work, syncs the log, and frees `db`. Snapshots and
/// iterators stay readable afterward; free them before opening the same
/// directory again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_close(db: *mut lt_db) {
    // A panic here would unwind into C. The store is gone either way.
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe { free(db) }));
}

/// Writes `value` under `key`. With `sync`, returns once the write is
/// durable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_put(
    db: *mut lt_db,
    key: *const c_char,
    key_len: usize,
    value: *const c_char,
    value_len: usize,
    sync: bool,
) -> lt_status {
    call(|| unsafe {
        let op = WriteOp::put(bytes(key, key_len)?.to_vec(), bytes(value, value_len)?.to_vec());
        handle(db)?.0.write(&[op], sync)
    })
}

/// Deletes `key`. With `sync`, returns once the delete is durable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_delete(db: *mut lt_db, key: *const c_char, key_len: usize, sync: bool) -> lt_status {
    call(|| unsafe { handle(db)?.0.write(&[WriteOp::delete(bytes(key, key_len)?.to_vec())], sync) })
}

/// Applies every write in `batch` as one: a crash keeps all of them or
/// none, and no read sees some without the rest. With `sync`, returns once
/// they are durable. `batch` keeps its writes for reuse.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_write(db: *mut lt_db, batch: *const lt_batch, sync: bool) -> lt_status {
    call(|| unsafe { handle(db)?.0.write(&handle(batch)?.0, sync) })
}

/// Reads `key`. On success `*value` holds the value, or null when the key
/// is absent; free it with lt_value_free().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_get(db: *const lt_db, key: *const c_char, key_len: usize, value: *mut *mut lt_value) -> lt_status {
    unsafe { read_get(db, key, key_len, value) }
}

/// Iterates over entries with keys in [start, end), in ascending order or,
/// with `reverse`, descending, as of a snapshot this call takes. A null
/// `end` leaves the range unbounded above. Free `*iter` with lt_iter_free().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_iter_new(
    db: *const lt_db,
    start: *const c_char,
    start_len: usize,
    end: *const c_char,
    end_len: usize,
    reverse: bool,
    iter: *mut *mut lt_iter,
) -> lt_status {
    unsafe { read_range(db, start, start_len, end, end_len, reverse, iter) }
}

/// Iterates over entries whose keys start with `prefix`, in ascending order
/// or, with `reverse`, descending, as of a snapshot this call takes. Free
/// `*iter` with lt_iter_free().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_iter_prefix(
    db: *const lt_db,
    prefix: *const c_char,
    prefix_len: usize,
    reverse: bool,
    iter: *mut *mut lt_iter,
) -> lt_status {
    unsafe { read_prefix(db, prefix, prefix_len, reverse, iter) }
}

/// Makes every completed write durable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_sync(db: *mut lt_db) -> lt_status {
    call(|| unsafe { handle(db)?.0.sync() })
}

/// Freezes the memtable and waits until every frozen memtable is
/// checkpointed into the tree.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_checkpoint(db: *mut lt_db) -> lt_status {
    call(|| unsafe { handle(db)?.0.checkpoint() })
}

/// Advances the checkpoint pipeline by one stage, for a store opened with
/// `background` false. Sets `*progressed` to false when there was nothing
/// to do.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_checkpoint_step(db: *mut lt_db, progressed: *mut bool) -> lt_status {
    call(|| unsafe {
        let out = progressed.as_mut().ok_or_else(|| invalid("null output pointer"))?;
        *out = handle(db)?.0.checkpoint_step()?;
        Ok(())
    })
}

/// Takes a snapshot: every read through it sees the writes completed before
/// this call and none after. Holding one keeps its data in memory and on
/// disk. Returns null when `db` is null; free it with lt_snapshot_free().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_snapshot_new(db: *const lt_db) -> *mut lt_snapshot {
    match unsafe { db.as_ref() } {
        Some(db) => Box::into_raw(Box::new(lt_snapshot(db.0.snapshot()))),
        None => ptr::null_mut(),
    }
}

/// lt_get() through a snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_snapshot_get(
    snapshot: *const lt_snapshot,
    key: *const c_char,
    key_len: usize,
    value: *mut *mut lt_value,
) -> lt_status {
    unsafe { read_get(snapshot, key, key_len, value) }
}

/// lt_iter_new() through a snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_snapshot_iter_new(
    snapshot: *const lt_snapshot,
    start: *const c_char,
    start_len: usize,
    end: *const c_char,
    end_len: usize,
    reverse: bool,
    iter: *mut *mut lt_iter,
) -> lt_status {
    unsafe { read_range(snapshot, start, start_len, end, end_len, reverse, iter) }
}

/// lt_iter_prefix() through a snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_snapshot_iter_prefix(
    snapshot: *const lt_snapshot,
    prefix: *const c_char,
    prefix_len: usize,
    reverse: bool,
    iter: *mut *mut lt_iter,
) -> lt_status {
    unsafe { read_prefix(snapshot, prefix, prefix_len, reverse, iter) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_snapshot_free(snapshot: *mut lt_snapshot) {
    unsafe { free(snapshot) }
}

/// Moves to the next entry and sets `*has_entry` to whether there was one.
/// When there was, `*key` and `*value` (either may be null) receive it,
/// valid until the next call on `iter`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_iter_next(iter: *mut lt_iter, has_entry: *mut bool, key: *mut lt_slice, value: *mut lt_slice) -> lt_status {
    call(|| unsafe {
        let has_entry = has_entry.as_mut().ok_or_else(|| invalid("null output pointer"))?;
        *has_entry = false;
        let it = iter.as_mut().ok_or_else(|| invalid("null handle"))?;
        it.entry = it.iter.next()?;
        *has_entry = it.entry.is_some();
        let (k, v) = it.entry.as_ref().map_or((EMPTY, EMPTY), |(k, v)| (k.into(), v.into()));
        if let Some(key) = key.as_mut() {
            *key = k;
        }
        if let Some(value) = value.as_mut() {
            *value = v;
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_iter_free(iter: *mut lt_iter) {
    unsafe { free(iter) }
}

/// The value's bytes, valid until lt_value_free().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_value_bytes(value: *const lt_value) -> lt_slice {
    unsafe { value.as_ref() }.map_or(EMPTY, |v| (&v.0).into())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_value_free(value: *mut lt_value) {
    unsafe { free(value) }
}

/// An empty batch. Free it with lt_batch_free().
#[unsafe(no_mangle)]
pub extern "C" fn lt_batch_new() -> *mut lt_batch {
    Box::into_raw(Box::new(lt_batch(Vec::new())))
}

/// Adds a write of `value` under `key`, copying both.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_batch_put(
    batch: *mut lt_batch,
    key: *const c_char,
    key_len: usize,
    value: *const c_char,
    value_len: usize,
) -> lt_status {
    call(|| unsafe {
        let batch = batch.as_mut().ok_or_else(|| invalid("null handle"))?;
        batch.0.push(WriteOp::put(bytes(key, key_len)?.to_vec(), bytes(value, value_len)?.to_vec()));
        Ok(())
    })
}

/// Adds a delete of `key`, copying it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_batch_delete(batch: *mut lt_batch, key: *const c_char, key_len: usize) -> lt_status {
    call(|| unsafe {
        let batch = batch.as_mut().ok_or_else(|| invalid("null handle"))?;
        batch.0.push(WriteOp::delete(bytes(key, key_len)?.to_vec()));
        Ok(())
    })
}

/// Removes every write from `batch`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_batch_clear(batch: *mut lt_batch) {
    if let Some(batch) = unsafe { batch.as_mut() } {
        batch.0.clear();
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lt_batch_free(batch: *mut lt_batch) {
    unsafe { free(batch) }
}
