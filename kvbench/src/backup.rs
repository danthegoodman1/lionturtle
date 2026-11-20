//! Backups of a live lionturtle store, two ways, for comparison.
//!
//! `rows` streams every row of a snapshot to a file, in frames of about
//! 1 MiB of records, each frame compressed with LZ4 (`rows`) or stored as it
//! is (`rows-raw`); restoring writes the rows into a new store. `copy` and
//! `archive` are the store's own backup ([`lionturtle::Db::backup`]), which
//! copies the newest checkpoint's pages and blobs as they are stored: into
//! a directory that opens as a store, or into an archive file that
//! [`restore_archive`] turns back into one.
//!
//! ```text
//! frame  = raw_len: u32 | stored_len: u32 | payload   (LZ4 when stored_len < raw_len)
//! record = key_len: u32 | value_len: u32 | key | value
//! ```

use lionturtle::{Db, DbIter};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

const FRAME: usize = 1 << 20;
/// Rows a restore writes per batch, and the bytes that close a batch early.
const BATCH_ROWS: usize = 256;
const BATCH_BYTES: usize = 1 << 20;

/// What a pass over a store's rows saw: how many, their key and value
/// bytes, and a checksum of every record in order.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub struct Fingerprint {
    pub rows: u64,
    pub bytes: u64,
    pub crc: u32,
}

impl Fingerprint {
    pub fn json(&self) -> String {
        format!(r#"{{"rows":{},"user_mb":{:.1},"crc":{}}}"#, self.rows, self.bytes as f64 / 1e6, self.crc)
    }
}

/// Calls `each` with every row of `it` encoded as a record.
fn records(mut it: DbIter, mut each: impl FnMut(&[u8], &[u8], &[u8])) -> Fingerprint {
    let mut fp = Fingerprint::default();
    while let Some((k, v)) = it.next().unwrap() {
        let mut head = [0u8; 8];
        head[..4].copy_from_slice(&(k.len() as u32).to_le_bytes());
        head[4..].copy_from_slice(&(v.len() as u32).to_le_bytes());
        for part in [&head[..], &k, &v] {
            fp.crc = crc32c::crc32c_append(fp.crc, part);
        }
        fp.rows += 1;
        fp.bytes += (k.len() + v.len()) as u64;
        each(&head, &k, &v);
    }
    fp
}

/// The fingerprint of every row `it` yields.
pub fn fingerprint(it: DbIter) -> Fingerprint {
    records(it, |_, _, _| {})
}

/// Streams a snapshot of `db` to the file at `path`.
pub fn backup_rows(db: &Db, path: &Path, lz4: bool) -> Fingerprint {
    let snap = db.snapshot();
    let file = std::fs::File::create(path).unwrap();
    let mut out = BufWriter::with_capacity(4 << 20, &file);
    let mut frame: Vec<u8> = Vec::with_capacity(2 * FRAME);
    let mut packed: Vec<u8> = Vec::new();
    let mut flush = |frame: &mut Vec<u8>| {
        if frame.is_empty() {
            return;
        }
        let stored: &[u8] = if lz4 {
            packed.resize(lz4_flex::block::get_maximum_output_size(frame.len()), 0);
            let n = lz4_flex::block::compress_into(frame, &mut packed).unwrap();
            if n < frame.len() { &packed[..n] } else { frame }
        } else {
            frame
        };
        out.write_all(&(frame.len() as u32).to_le_bytes()).unwrap();
        out.write_all(&(stored.len() as u32).to_le_bytes()).unwrap();
        out.write_all(stored).unwrap();
        frame.clear();
    };
    let fp = records(snap.iter(b"", None), |head, k, v| {
        frame.extend_from_slice(head);
        frame.extend_from_slice(k);
        frame.extend_from_slice(v);
        if frame.len() >= FRAME {
            flush(&mut frame);
        }
    });
    flush(&mut frame);
    drop(flush);
    out.flush().unwrap();
    drop(out);
    // A backup written to /dev/null measures the scan alone.
    if path != Path::new("/dev/null") {
        file.sync_data().unwrap();
    }
    fp
}

/// Writes the rows in the file at `path` into `db` with `threads` writers,
/// then checkpoints. Returns the rows written and the seconds the writes and
/// the checkpoint took.
pub fn restore_rows(db: &Db, path: &Path, threads: usize) -> (u64, f64, f64) {
    let started = Instant::now();
    let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, Vec<u8>)>(threads * 2);
    let rx = Arc::new(parking_lot::Mutex::new(rx));
    let rows = AtomicU64::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let (rx, rows) = (rx.clone(), &rows);
            scope.spawn(move || {
                let mut raw: Vec<u8> = Vec::new();
                loop {
                    let Ok((raw_len, stored)) = rx.lock().recv() else { break };
                    let frame: &[u8] = if stored.len() < raw_len {
                        raw.resize(raw_len, 0);
                        assert_eq!(lz4_flex::block::decompress_into(&stored, &mut raw).unwrap(), raw_len);
                        &raw
                    } else {
                        &stored
                    };
                    let mut batch: Vec<(&[u8], Option<&[u8]>)> = Vec::with_capacity(BATCH_ROWS);
                    let (mut at, mut bytes) = (0, 0);
                    while at < frame.len() {
                        let klen = u32::from_le_bytes(frame[at..at + 4].try_into().unwrap()) as usize;
                        let vlen = u32::from_le_bytes(frame[at + 4..at + 8].try_into().unwrap()) as usize;
                        let key = &frame[at + 8..at + 8 + klen];
                        let value = &frame[at + 8 + klen..at + 8 + klen + vlen];
                        at += 8 + klen + vlen;
                        bytes += klen + vlen;
                        batch.push((key, Some(value)));
                        if batch.len() == BATCH_ROWS || bytes >= BATCH_BYTES || at == frame.len() {
                            db.write(&batch, false).unwrap();
                            rows.fetch_add(batch.len() as u64, Relaxed);
                            batch.clear();
                            bytes = 0;
                        }
                    }
                }
            });
        }
        let mut input = BufReader::with_capacity(4 << 20, std::fs::File::open(path).unwrap());
        let mut head = [0u8; 8];
        while input.read_exact(&mut head).is_ok() {
            let raw_len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
            let mut stored = vec![0u8; u32::from_le_bytes(head[4..].try_into().unwrap()) as usize];
            input.read_exact(&mut stored).unwrap();
            tx.send((raw_len, stored)).unwrap();
        }
        drop(tx);
    });
    let wrote = started.elapsed().as_secs_f64();
    db.sync().unwrap();
    db.checkpoint().unwrap();
    (rows.load(Relaxed), wrote, started.elapsed().as_secs_f64() - wrote)
}

/// Runs the backup `mode` names into `dest`, and returns the result as
/// JSON fields. The row modes write a file. `copy` writes a store into a
/// directory, or discards it when `dest` is `null`, which leaves the cost
/// of reading the source. `archive` writes an archive file past the page
/// cache, and `archive-buffered` through it. `mbps` is the
/// rate of a page backup in MiB/s: 0 for no limit, and `None` for the
/// store's default.
pub fn run(db: &Db, mode: &str, dest: &Path, mbps: Option<u64>) -> String {
    let started = Instant::now();
    if mode == "rows" || mode == "rows-raw" {
        let fp = backup_rows(db, dest, mode == "rows");
        let secs = started.elapsed().as_secs_f64();
        let file_mb = std::fs::metadata(dest).map_or(0, |m| m.len()) as f64 / 1e6;
        return format!(r#""mode":"{mode}","backup_secs":{secs:.3},"backup_mb":{file_mb:.1},"fingerprint":{}"#, fp.json());
    }
    let opts = match mbps {
        None => lionturtle::BackupOptions::default(),
        Some(mbps) => lionturtle::BackupOptions { rate_limit: (mbps > 0).then_some(mbps << 20) },
    };
    let backup = db.backup(opts).unwrap();
    // How long the backup waited for the writes before it to reach the tree.
    let flush_secs = started.elapsed().as_secs_f64();
    let info = match mode {
        "copy" if dest == Path::new("null") => backup.copy_to(Arc::new(Null)).unwrap(),
        "copy" => {
            std::fs::create_dir_all(dest).unwrap();
            backup.copy_to(Arc::new(lionturtle::storage::FsStorage::new(dest).unwrap())).unwrap()
        }
        "archive" if dest == Path::new("/dev/null") => backup.write_to(&mut std::fs::File::create(dest).unwrap()).unwrap(),
        // Through the page cache, as a plain file write goes.
        "archive-buffered" => {
            let mut file = std::fs::File::create(dest).unwrap();
            let info = backup.write_to(&mut file).unwrap();
            file.sync_all().unwrap();
            info
        }
        "archive" => {
            let dir = lionturtle::storage::FsStorage::new(dest.parent().unwrap()).unwrap();
            backup.archive_to(&dir, dest.file_name().unwrap().to_str().unwrap()).unwrap()
        }
        other => panic!("unknown --backup {other}; use rows, rows-raw, copy, archive or archive-buffered"),
    };
    format!(
        r#""mode":"{mode}","flush_secs":{flush_secs:.3},"backup_secs":{:.3},"backup_mb":{:.1},"pages":{},"blobs":{},"seq":{}"#,
        started.elapsed().as_secs_f64(),
        info.bytes as f64 / 1e6,
        info.pages,
        info.blobs,
        info.seq
    )
}

/// Restores the archive at `archive` into the directory `dir`, which must
/// hold nothing, and returns the result as JSON fields.
pub fn restore_archive(archive: &Path, dir: &Path) -> String {
    let started = Instant::now();
    let reader = BufReader::with_capacity(1 << 20, std::fs::File::open(archive).unwrap());
    let info = lionturtle::backup::restore(reader, Arc::new(lionturtle::storage::FsStorage::new(dir).unwrap())).unwrap();
    format!(r#""restore_secs":{:.3},"restore_mb":{:.1},"seq":{}"#, started.elapsed().as_secs_f64(), info.bytes as f64 / 1e6, info.seq)
}

/// A storage that discards what it is given.
struct Null;

impl lionturtle::storage::Storage for Null {
    fn open(&self, _name: &str, _create: bool) -> std::io::Result<Arc<dyn lionturtle::storage::File>> {
        Ok(Arc::new(Null))
    }
    fn remove(&self, _name: &str) -> std::io::Result<()> {
        Ok(())
    }
    fn rename(&self, _from: &str, _to: &str) -> std::io::Result<()> {
        Ok(())
    }
    fn list(&self) -> std::io::Result<Vec<String>> {
        Ok(Vec::new())
    }
    fn sync_dir(&self) -> std::io::Result<()> {
        Ok(())
    }
}

impl lionturtle::storage::File for Null {
    fn read_exact_at(&self, buf: &mut [u8], _offset: u64) -> std::io::Result<()> {
        buf.fill(0);
        Ok(())
    }
    fn write_all_at(&self, _buf: &[u8], _offset: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn len(&self) -> std::io::Result<u64> {
        Ok(0)
    }
}
