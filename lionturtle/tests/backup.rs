//! Backups: a copy or an archive holds exactly the writes made before it
//! began, whatever is written meanwhile; a damaged or interrupted one fails
//! closed; and a backup keeps its slots while the store moves on.

use lionturtle::sim::{FaultConfig, SimStorage};
use lionturtle::storage::Storage;
use lionturtle::{BackupInfo, BackupOptions, Compression, Db, Options, TreeConfig, backup};
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

/// A small tree that callers checkpoint, with values on both sides of the
/// blob threshold.
fn options() -> Options {
    Options {
        tree: TreeConfig {
            leaf_size: 2048,
            max_pivots: 6,
            min_pivots: 2,
            max_segments: 8,
            max_levels: 3,
            parallel: false,
            ..TreeConfig::default()
        },
        checkpoint_distance: 2,
        cache_bytes: 1 << 20,
        background: false,
        merge_threads: 0,
        pace_checkpoints: false,
        compression: Compression::Lz4,
        blob_threshold: 100,
        ..Options::default()
    }
}

fn unlimited() -> BackupOptions {
    BackupOptions { rate_limit: None }
}

fn disk(seed: u64) -> SimStorage {
    SimStorage::new(seed, FaultConfig::default())
}

fn open(disk: &SimStorage, opts: &Options) -> Db {
    Db::open(Arc::new(disk.clone()), opts.clone()).unwrap()
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

/// A value of `len` bytes that differs by `seed`. Its second half repeats
/// its first, so it compresses.
fn value(seed: usize, len: usize) -> Vec<u8> {
    let mut x = (seed as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut v: Vec<u8> = (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    let half = len / 2;
    v.copy_within(..half, len - half);
    v
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn rows(db: &Db) -> Rows {
    let mut it = db.iter(b"", None);
    let mut out = Vec::new();
    while let Some((k, v)) = it.next().unwrap() {
        out.push((k.to_vec(), v.to_vec()));
    }
    out
}

fn model_rows(model: &Model) -> Rows {
    model.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Fills `db` over several checkpoints with puts, deletes and a range
/// delete, and leaves its last writes in the memtable.
fn fill(db: &Db) -> Model {
    let mut model = Model::new();
    for round in 0..6 {
        for i in 0..400 {
            if (i + round) % 3 == 0 {
                continue;
            }
            let v = value(i * 31 + round, [20, 90, 150, 700, 5000][(i * 7 + round) % 5]);
            db.put(&key(i), &v).unwrap();
            model.insert(key(i), v);
        }
        for i in (round * 10..400).step_by(17) {
            db.delete(&key(i)).unwrap();
            model.remove(&key(i));
        }
        if round % 2 == 1 {
            db.checkpoint().unwrap();
        }
    }
    db.delete_range(&key(100), Some(&key(140))).unwrap();
    model.retain(|k, _| !(key(100)..key(140)).contains(k));
    model
}

/// A backup of `db` as an archive.
fn archive(db: &Db, opts: BackupOptions) -> (BackupInfo, Vec<u8>) {
    let mut backup = db.backup(opts).unwrap();
    let mut bytes = Vec::new();
    backup.read_to_end(&mut bytes).unwrap();
    assert_eq!(backup.copied_bytes(), backup.info().bytes);
    (backup.info(), bytes)
}

/// Opens a restored or copied store and checks it against `want`.
fn check(copy: &SimStorage, opts: &Options, info: &BackupInfo, want: &Rows) {
    let db = open(copy, opts);
    db.check_invariants().unwrap();
    assert_eq!(db.last_seq(), info.seq);
    assert_eq!(&rows(&db), want);
}

#[test]
fn a_copy_and_an_archive_hold_the_writes_before_them_and_none_after() {
    let opts = options();
    let db = open(&disk(1), &opts);
    let model = fill(&db);
    let want = model_rows(&model);

    let copy = disk(2);
    let copied = db.backup(unlimited()).unwrap().copy_to(Arc::new(copy.clone())).unwrap();
    let (info, bytes) = archive(&db, unlimited());
    assert_eq!(info, copied);
    assert_eq!(info.seq, db.last_seq());
    assert!(info.pages > 1 && info.blobs > 0, "{info:?}");
    // The archive holds each image once, with little around it.
    assert!(bytes.len() as u64 > info.bytes && (bytes.len() as u64) < info.bytes + 4200 + 26 * (info.pages + info.blobs) + 100);

    // Writes after the backups stay out of them.
    for i in 0..400 {
        db.put(&key(i), &value(i + 9000, 300)).unwrap();
    }
    db.checkpoint().unwrap();

    assert_eq!(backup::verify(&bytes[..]).unwrap(), info);
    let restored = disk(3);
    assert_eq!(backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap(), info);
    for store in [&copy, &restored] {
        check(store, &opts, &info, &want);
        // The store takes writes of its own and survives a reopen.
        let db = open(store, &opts);
        db.put(b"zzz", &value(1, 4000)).unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let db = open(store, &opts);
        assert_eq!(db.get(b"zzz").unwrap().unwrap(), value(1, 4000));
        db.check_invariants().unwrap();
    }
    db.check_invariants().unwrap();
}

#[test]
fn a_store_without_blobs_or_rows_backs_up() {
    for blob_threshold in [usize::MAX, 100] {
        let opts = Options { blob_threshold, compression: Compression::None, ..options() };
        // An empty store.
        let db = open(&disk(1), &opts);
        let (info, bytes) = archive(&db, unlimited());
        assert_eq!((info.seq, info.pages, info.blobs, info.bytes), (0, 0, 0, 0));
        let restored = disk(2);
        backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap();
        check(&restored, &opts, &info, &Rows::new());
        let copy = disk(3);
        db.backup(unlimited()).unwrap().copy_to(Arc::new(copy.clone())).unwrap();
        check(&copy, &opts, &info, &Rows::new());
        // Rows that live only in the memtable, then rows all deleted.
        db.put(b"a", &value(1, 50)).unwrap();
        db.put(b"b", &value(2, 5000)).unwrap();
        let (info, bytes) = archive(&db, unlimited());
        let restored = disk(4);
        backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap();
        check(&restored, &opts, &info, &vec![(b"a".to_vec(), value(1, 50)), (b"b".to_vec(), value(2, 5000))]);
        db.delete_range(b"", None).unwrap();
        let (info, bytes) = archive(&db, unlimited());
        let restored = disk(5);
        backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap();
        check(&restored, &opts, &info, &Rows::new());
    }
}

/// One writer logs each write in the order it made them, which is the
/// order of their sequence numbers, so the rows as of a backup's sequence
/// number are the log replayed that far.
#[test]
fn a_backup_under_writes_restores_to_its_sequence_point() {
    #[derive(Clone)]
    enum Op {
        Put(usize, Vec<u8>),
        Delete(usize),
        DeleteRange(usize, usize),
    }
    const KEYS: usize = 1500;
    let opts = Options { background: true, merge_threads: 2, pace_checkpoints: true, ..options() };
    let db = Arc::new(open(&disk(7), &opts));
    assert_eq!(db.last_seq(), 0);
    let log: Arc<parking_lot::Mutex<Vec<Op>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, log, stop) = (db.clone(), log.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0usize;
            while !stop.load(SeqCst) {
                let i = n.wrapping_mul(7919) % KEYS;
                let op = match n % 97 {
                    13 => Op::DeleteRange(i, (i + 9).min(KEYS)),
                    k if k % 11 == 0 => Op::Delete(i),
                    _ => Op::Put(i, value(n, [30, 400, 3000][n % 3])),
                };
                match &op {
                    Op::Put(i, v) => db.put(&key(*i), v).unwrap(),
                    Op::Delete(i) => db.delete(&key(*i)).unwrap(),
                    Op::DeleteRange(lo, hi) => db.delete_range(&key(*lo), Some(&key(*hi))).unwrap(),
                }
                log.lock().push(op);
                n += 1;
            }
        })
    };
    let rows_at = |seq: u64| -> Rows {
        // The write with the backup's last sequence number has returned or
        // is about to, and its writer logs it next.
        while (log.lock().len() as u64) < seq {
            std::thread::yield_now();
        }
        let mut model = Model::new();
        for op in &log.lock()[..seq as usize] {
            match op {
                Op::Put(i, v) => drop(model.insert(key(*i), v.clone())),
                Op::Delete(i) => drop(model.remove(&key(*i))),
                Op::DeleteRange(lo, hi) => model.retain(|k, _| !(key(*lo)..key(*hi)).contains(k)),
            }
        }
        model_rows(&model)
    };
    let mut last = 0;
    for round in 0..8u64 {
        std::thread::sleep(std::time::Duration::from_millis(20));
        // A rate limit keeps the backup's slots pinned across checkpoints.
        let limit = BackupOptions { rate_limit: (round % 2 == 1).then_some(4 << 20) };
        let made = log.lock().len() as u64;
        let store = disk(100 + round);
        let info = if round % 4 < 2 {
            db.backup(limit).unwrap().copy_to(Arc::new(store.clone())).unwrap()
        } else {
            let (info, bytes) = archive(&db, limit);
            assert_eq!(backup::restore(&bytes[..], Arc::new(store.clone())).unwrap(), info);
            info
        };
        // It holds every write that had returned when it began.
        assert!(info.seq >= made && info.seq > last, "round {round}: {info:?} after {made} writes");
        last = info.seq;
        check(&store, &opts, &info, &rows_at(info.seq));
    }
    stop.store(true, SeqCst);
    writer.join().unwrap();
    db.checkpoint().unwrap();
    db.check_invariants().unwrap();
}

/// The store overwrites everything a backup holds before the backup reads
/// it. The backup still yields what it pinned, the leak check counts the
/// superseded slots as released, and they are free once the backup is done.
#[test]
fn a_backup_keeps_its_slots_while_the_store_moves_on() {
    let opts = options();
    let source = disk(1);
    let db = open(&source, &opts);
    let model = fill(&db);
    let mut held = db.backup(unlimited()).unwrap();
    let dropped = db.backup(unlimited()).unwrap();
    let info = held.info();
    let before = source.disk_usage();
    for round in 0..3 {
        for i in 0..400 {
            db.put(&key(i), &value(i + 5000 * (round + 1), [150, 700, 5000][(i + round) % 3])).unwrap();
        }
        db.checkpoint().unwrap();
        db.check_invariants().unwrap();
    }
    // A backup dropped unread lets go of what it held.
    drop(dropped);
    db.check_invariants().unwrap();
    let mut bytes = Vec::new();
    held.read_to_end(&mut bytes).unwrap();
    drop(held);
    let restored = disk(2);
    backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap();
    check(&restored, &opts, &info, &model_rows(&model));
    // With the backups gone, the next checkpoints release the slots they
    // held, and the store returns to about the size it had.
    for round in 0..3 {
        db.put(&key(round), &value(round, 300)).unwrap();
        db.checkpoint().unwrap();
        db.check_invariants().unwrap();
    }
    assert!(source.disk_usage() < before * 2, "{} after {before}", source.disk_usage());
}

#[test]
fn a_damaged_or_truncated_archive_fails_closed() {
    let opts = options();
    let db = open(&disk(1), &opts);
    let want = model_rows(&fill(&db));
    let (info, bytes) = archive(&db, unlimited());
    let fails = |archive: &[u8], what: &str| {
        assert!(backup::verify(archive).is_err(), "{what} verified");
        let dest = disk(9);
        assert!(backup::restore(archive, Arc::new(dest.clone())).is_err(), "{what} restored");
        assert!(Db::open(Arc::new(dest), opts.clone()).is_err(), "{what} opened");
    };
    for cut in [0, 5, 4135, 4136, 4137, 4162, bytes.len() / 2, bytes.len() - 25, bytes.len() - 1] {
        fails(&bytes[..cut], &format!("an archive cut at {cut}"));
    }
    let mut longer = bytes.clone();
    longer.push(0);
    fails(&longer, "an archive with a byte past its trailer");
    // The header, each record's first 26 bytes and the trailer are covered
    // by their own checksums.
    for at in [0, 9, 13, 2000, 4110, 4133, 4136, 4140, 4161, bytes.len() - 25, bytes.len() - 20, bytes.len() - 6, bytes.len() - 1] {
        let mut bad = bytes.clone();
        bad[at] ^= 0x04;
        fails(&bad, &format!("an archive with a flipped bit at {at}"));
    }
    // An image's checksums cover its data. A flipped bit elsewhere lands in
    // padding, and the archive then restores to the same rows.
    let mut caught = 0;
    for i in 0..300 {
        let at = 4200 + i * (bytes.len() - 4300) / 300;
        let mut bad = bytes.clone();
        bad[at] ^= 0x40;
        if backup::verify(&bad[..]).is_err() {
            caught += 1;
            continue;
        }
        let dest = disk(20 + i as u64);
        backup::restore(&bad[..], Arc::new(dest.clone())).unwrap();
        check(&dest, &opts, &info, &want);
    }
    assert!(caught > 200, "{caught} of 300 flips caught");
    // Records that swap places break the chain.
    let first = 4136;
    let image_len = u32::from_le_bytes(bytes[first + 14..first + 18].try_into().unwrap()) as usize;
    let second = first + 26 + image_len;
    let second_len = u32::from_le_bytes(bytes[second + 14..second + 18].try_into().unwrap()) as usize;
    let third = second + 26 + second_len;
    let mut swapped = bytes[..first].to_vec();
    swapped.extend_from_slice(&bytes[second..third]);
    swapped.extend_from_slice(&bytes[first..second]);
    swapped.extend_from_slice(&bytes[third..]);
    fails(&swapped, "an archive with two records swapped");
    // A record left out breaks it too.
    let mut missing = bytes[..first].to_vec();
    missing.extend_from_slice(&bytes[second..]);
    fails(&missing, "an archive without its first record");
}

#[test]
fn a_damaged_source_fails_the_backup() {
    // Stored as it is, every byte of a page is under a checksum.
    let opts = Options { compression: Compression::None, ..options() };
    let source = disk(1);
    let db = open(&source, &opts);
    let want = model_rows(&fill(&db));
    let damage = FaultConfig { read_corruption: 1.0, corrupt_classes: vec!["pages".into(), "blobs".into()], ..FaultConfig::default() };

    let mut backup = db.backup(unlimited()).unwrap();
    source.set_faults(damage.clone());
    let err = backup.read_to_end(&mut Vec::new()).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    drop(backup);

    source.set_faults(FaultConfig::default());
    let backup = db.backup(unlimited()).unwrap();
    source.set_faults(damage);
    let dest = disk(2);
    assert!(matches!(backup.copy_to(Arc::new(dest.clone())), Err(lionturtle::Error::Corruption(_))));
    assert!(Db::open(Arc::new(dest), opts.clone()).is_err());

    // The store itself is sound, and backs up once its reads are.
    source.set_faults(FaultConfig::default());
    let (info, bytes) = archive(&db, unlimited());
    let restored = disk(3);
    backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap();
    check(&restored, &opts, &info, &want);
    db.check_invariants().unwrap();
}

/// A destination that fails or loses power part-way holds no superblock, so
/// it refuses to open; one that loses power after the backup finished has
/// everything.
#[test]
fn an_interrupted_copy_or_restore_refuses_to_open() {
    let opts = options();
    let db = open(&disk(1), &opts);
    let want = model_rows(&fill(&db));
    let (info, bytes) = archive(&db, unlimited());
    let mut interrupted = 0;
    for seed in 0..40u64 {
        let dest = disk(50 + seed);
        // The disk goes down after some of the writes, or before any.
        dest.arm_crash(seed * 3);
        let result = if seed % 2 == 0 {
            db.backup(unlimited()).unwrap().copy_to(Arc::new(dest.clone()))
        } else {
            backup::restore(&bytes[..], Arc::new(dest.clone()))
        };
        dest.disarm_crash();
        dest.crash();
        match result {
            Ok(done) => {
                assert_eq!(done, info);
                check(&dest, &opts, &info, &want);
            }
            Err(_) => {
                interrupted += 1;
                // Nothing is left of a backup that never made its first
                // file durable, and such a storage opens as a new store.
                let empty = Storage::list(&dest).unwrap().is_empty();
                assert_eq!(Db::open(Arc::new(dest), opts.clone()).is_ok(), empty, "seed {seed}");
            }
        }
    }
    assert!(interrupted > 10, "{interrupted} of 40 runs were interrupted");
    // A write that fails without a crash ends the backup the same way.
    let dest = SimStorage::new(3, FaultConfig { write_error: 0.2, ..FaultConfig::default() });
    assert!(db.backup(unlimited()).unwrap().copy_to(Arc::new(dest.clone())).is_err());
    dest.set_faults(FaultConfig::default());
    assert!(Db::open(Arc::new(dest), opts.clone()).is_err());
}

/// The bytes of the file `name` in `disk`.
fn file_bytes(disk: &SimStorage, name: &str) -> Vec<u8> {
    let file = Storage::open(disk, name, false).unwrap();
    let mut bytes = vec![0u8; file.len().unwrap() as usize];
    file.read_exact_at(&mut bytes, 0).unwrap();
    bytes
}

/// An archive written to a file holds the bytes the backup yields when
/// read, whatever the size of its writes, and replaces what the file held.
#[test]
fn an_archive_file_holds_the_archive() {
    let opts = options();
    let db = open(&disk(1), &opts);
    let want = model_rows(&fill(&db));
    let (info, bytes) = archive(&db, unlimited());
    let dest = disk(2);
    Storage::open(&dest, "store.backup", true).unwrap().write_all_at(&vec![7u8; bytes.len() + 10_000], 0).unwrap();
    for rate_limit in [None, Some(64 << 20)] {
        let written = db.backup(BackupOptions { rate_limit }).unwrap().archive_to(&dest, "store.backup").unwrap();
        assert_eq!(written, info);
        assert_eq!(file_bytes(&dest, "store.backup"), bytes);
    }
    // The file is durable when the call returns.
    dest.crash();
    let on_disk = file_bytes(&dest, "store.backup");
    assert_eq!(backup::verify(&on_disk[..]).unwrap(), info);
    let restored = disk(3);
    backup::restore(&on_disk[..], Arc::new(restored.clone())).unwrap();
    check(&restored, &opts, &info, &want);
    // An archive whose disk went down part-way is absent or fails to
    // verify, unless only the last sync was lost and every byte arrived.
    let mut cut_short = 0;
    for ops in 0..12 {
        let dest = disk(10 + ops);
        dest.arm_crash(ops);
        let written = db.backup(BackupOptions { rate_limit: Some(64 << 20) }).unwrap().archive_to(&dest, "store.backup");
        dest.disarm_crash();
        dest.crash();
        if written.is_err() {
            cut_short += 1;
            if Storage::list(&dest).unwrap().contains(&"store.backup".to_string()) {
                let left = file_bytes(&dest, "store.backup");
                assert!(backup::verify(&left[..]).is_err() || left == bytes, "after {ops} operations");
            }
        }
    }
    assert!(cut_short > 2, "{cut_short} of 12 archives were cut short");
}

/// A backup started on one thread is copied on another.
#[test]
fn a_backup_moves_between_threads() {
    let opts = options();
    let db = open(&disk(1), &opts);
    let want = model_rows(&fill(&db));
    let mut backup = db.backup(unlimited()).unwrap();
    let info = backup.info();
    let bytes = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        backup.read_to_end(&mut bytes).unwrap();
        bytes
    })
    .join()
    .unwrap();
    let restored = disk(2);
    backup::restore(&bytes[..], Arc::new(restored.clone())).unwrap();
    check(&restored, &opts, &info, &want);
}

#[test]
fn a_backup_needs_an_empty_destination_and_one_use() {
    let opts = options();
    let db = open(&disk(1), &opts);
    fill(&db);
    let (_, bytes) = archive(&db, unlimited());
    let used = disk(2);
    open(&used, &opts).put(b"k", b"v").unwrap();
    let invalid = |r: lionturtle::Result<BackupInfo>| matches!(r, Err(lionturtle::Error::InvalidArgument(_)));
    assert!(invalid(db.backup(unlimited()).unwrap().copy_to(Arc::new(used.clone()))));
    assert!(invalid(backup::restore(&bytes[..], Arc::new(used.clone()))));
    assert_eq!(open(&used, &opts).get(b"k").unwrap().unwrap(), &b"v"[..]);
    // A backup read as an archive is not also copied.
    let mut backup = db.backup(unlimited()).unwrap();
    backup.read_exact(&mut [0u8; 16]).unwrap();
    assert!(invalid(backup.copy_to(Arc::new(disk(3)))));
    db.check_invariants().unwrap();
}
