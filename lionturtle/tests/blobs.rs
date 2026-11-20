//! Values stored apart from the tree: reads on either side of the threshold,
//! what frees a blob, what keeps one, and recovery.

use lionturtle::sim::{FaultConfig, SimStorage};
use lionturtle::{Compression, Db, Options, TreeConfig};
use std::collections::BTreeMap;
use std::sync::Arc;

const THRESHOLD: usize = 100;

/// A small tree that callers checkpoint step by step.
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
        compression: Compression::None,
        blob_threshold: THRESHOLD,
        ..Options::default()
    }
}

fn open(disk: &SimStorage, opts: &Options) -> Db {
    Db::open(Arc::new(disk.clone()), opts.clone()).unwrap()
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

/// A value of `len` bytes that differs by `seed` and does not compress.
fn value(seed: usize, len: usize) -> Vec<u8> {
    let mut x = (seed as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Checks every way of reading the store against `model`.
fn verify(db: &Db, model: &Model) {
    let pairs = |rows: Vec<(bytes::Bytes, bytes::Bytes)>| -> Vec<(Vec<u8>, Vec<u8>)> {
        rows.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
    };
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    for (k, v) in model {
        assert_eq!(db.get(k).unwrap().as_deref(), Some(&v[..]), "get {:?}", String::from_utf8_lossy(k));
    }
    assert_eq!(pairs(db.scan(b"", None, usize::MAX).unwrap()), want, "scan");
    let half = want.len() / 2;
    assert_eq!(pairs(db.scan(b"", None, half).unwrap()), want[..half], "limited scan");
    let mut forward = Vec::new();
    let mut it = db.iter(b"", None);
    while let Some(e) = it.next().unwrap() {
        forward.push(e);
    }
    assert_eq!(pairs(forward), want, "iter");
    let mut backward = Vec::new();
    let mut it = db.iter_rev(b"", None);
    while let Some(e) = it.next().unwrap() {
        backward.push(e);
    }
    backward.reverse();
    assert_eq!(pairs(backward), want, "iter_rev");
}

/// Values just under, at and over the threshold read back the same from the
/// memtable, from the tree, and after a reopen, and only those at or over it
/// become blobs.
#[test]
fn values_round_trip_on_either_side_of_the_threshold() {
    let disk = SimStorage::new(1, FaultConfig::default());
    let db = open(&disk, &options());
    let lens = [0, 1, THRESHOLD - 1, THRESHOLD, THRESHOLD + 1, 1000, 5000, 70_000];
    let mut model = Model::new();
    for round in 0..3 {
        for (i, &len) in lens.iter().enumerate() {
            for copy in 0..8 {
                let k = key(i * 100 + copy);
                let v = value(round * 1000 + i * 100 + copy, len);
                db.put(&k, &v).unwrap();
                model.insert(k, v);
            }
        }
        verify(&db, &model);
        db.checkpoint().unwrap();
        verify(&db, &model);
    }
    let blobs = lens.iter().filter(|&&len| len >= THRESHOLD).count() as u64 * 8;
    let stats = db.stats();
    assert!(stats.blobs >= blobs, "{} blobs for {blobs} large values", stats.blobs);
    assert!(stats.blob_reads > 0);
    db.check_invariants().unwrap();
    drop(db);
    let db = open(&disk, &options());
    verify(&db, &model);
    assert_eq!(db.stats().blobs, stats.blobs);
    db.check_invariants().unwrap();
}

/// A tree of one leaf merges every batch into that leaf, so an overwrite or
/// a delete frees the old blob at the next checkpoint.
#[test]
fn overwrites_and_deletes_free_their_blobs() {
    let disk = SimStorage::new(2, FaultConfig::default());
    let opts = Options { tree: TreeConfig { leaf_size: 1 << 20, ..options().tree }, ..options() };
    let db = open(&disk, &opts);
    let mut model = Model::new();
    for i in 0..40 {
        db.put(&key(i), &value(i, 500)).unwrap();
        model.insert(key(i), value(i, 500));
    }
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs, 40);
    // Ten become other blobs, ten become small values, and ten go.
    for i in 0..10 {
        db.put(&key(i), &value(1000 + i, 700)).unwrap();
        model.insert(key(i), value(1000 + i, 700));
    }
    for i in 10..20 {
        db.put(&key(i), b"small").unwrap();
        model.insert(key(i), b"small".to_vec());
    }
    for i in 20..30 {
        db.delete(&key(i)).unwrap();
        model.remove(&key(i));
    }
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs, 20);
    assert_eq!(db.stats().blob_bytes, 10 * (700 + 16) + 10 * (500 + 16));
    verify(&db, &model);
    db.check_invariants().unwrap();
}

/// Overwriting blobs for many checkpoints in a tree of several levels keeps
/// the blobs it holds within a small multiple of the live ones, and a range
/// delete of everything frees them all without reading any.
#[test]
fn churn_stays_bounded_and_a_full_range_delete_frees_every_blob() {
    const KEYS: usize = 300;
    let disk = SimStorage::new(3, FaultConfig::default());
    let db = open(&disk, &options());
    let mut model = Model::new();
    for round in 0..12 {
        for i in 0..KEYS {
            // A third of the keys change each round.
            if (i + round) % 3 == 0 {
                let v = value(round * KEYS + i, 150 + i % 200);
                db.put(&key(i), &v).unwrap();
                model.insert(key(i), v);
            }
        }
        db.checkpoint().unwrap();
        db.check_invariants().unwrap();
    }
    assert_eq!(model.len(), KEYS);
    verify(&db, &model);
    let stats = db.stats();
    assert!(stats.tree.height >= 1, "the test needs buffer levels");
    assert!(stats.blobs >= KEYS as u64 && stats.blobs <= 3 * KEYS as u64, "{} blobs for {KEYS} keys", stats.blobs);

    let reads = db.stats().blob_reads;
    db.delete_range(b"", None).unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs, 0);
    assert_eq!(db.stats().blob_reads, reads, "the range delete read a blob");
    assert!(db.scan(b"", None, usize::MAX).unwrap().is_empty());
    db.check_invariants().unwrap();
}

/// A range delete of part of the key space drops the blobs of the subtrees
/// it covers and keeps the rest readable.
#[test]
fn partial_range_delete_keeps_the_other_blobs() {
    const KEYS: usize = 400;
    let disk = SimStorage::new(4, FaultConfig::default());
    let db = open(&disk, &options());
    let mut model = Model::new();
    for i in 0..KEYS {
        db.put(&key(i), &value(i, 200)).unwrap();
        model.insert(key(i), value(i, 200));
        if i % 50 == 49 {
            db.checkpoint().unwrap();
        }
    }
    let before = db.stats().blobs;
    db.delete_range(&key(50), Some(&key(350))).unwrap();
    model.retain(|k, _| *k < key(50) || *k >= key(350));
    verify(&db, &model);
    db.checkpoint().unwrap();
    verify(&db, &model);
    let after = db.stats().blobs;
    assert!(after < before / 2, "{after} of {before} blobs left after deleting three quarters of the keys");
    db.check_invariants().unwrap();
    drop(db);
    let db = open(&disk, &options());
    verify(&db, &model);
    db.check_invariants().unwrap();
}

/// A snapshot reads the blobs of its version after newer checkpoints have
/// replaced them, and they free once it drops.
#[test]
fn snapshot_keeps_its_blobs() {
    let disk = SimStorage::new(5, FaultConfig::default());
    let opts = Options { tree: TreeConfig { leaf_size: 1 << 20, ..options().tree }, ..options() };
    let db = open(&disk, &opts);
    for i in 0..20 {
        db.put(&key(i), &value(i, 300)).unwrap();
    }
    db.checkpoint().unwrap();
    let snap = db.snapshot();
    let mut it = snap.iter(b"", None);
    assert_eq!(it.next().unwrap().unwrap().1, value(0, 300));
    for i in 0..20 {
        db.put(&key(i), &value(100 + i, 300)).unwrap();
    }
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    // The tree references only the new blobs; the snapshot still reads the
    // old ones from their slots.
    assert_eq!(db.stats().blobs, 20);
    for i in 0..20 {
        assert_eq!(snap.get(&key(i)).unwrap().unwrap(), value(i, 300));
        assert_eq!(db.get(&key(i)).unwrap().unwrap(), value(100 + i, 300));
    }
    for i in 1..20 {
        assert_eq!(it.next().unwrap().unwrap().1, value(i, 300));
    }
    assert!(it.next().unwrap().is_none());
    drop(it);
    drop(snap);
    db.put(b"poke", b"x").unwrap();
    db.checkpoint().unwrap();
    db.check_invariants().unwrap();
}

/// A crash after a checkpoint applied its memtable and before it committed
/// leaves the tree without the memtable's blobs. Replay finds them through
/// the log and keeps them for the next checkpoint.
#[test]
fn crash_between_apply_and_commit_keeps_the_logged_blobs() {
    let disk = SimStorage::new(6, FaultConfig::default());
    // A memtable large enough that only the test freezes it.
    let opts = Options { checkpoint_distance: 64, ..options() };
    let db = open(&disk, &opts);
    let mut model = Model::new();
    for i in 0..30 {
        db.put(&key(i), &value(i, 400)).unwrap();
        model.insert(key(i), value(i, 400));
    }
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs, 30);
    for i in 30..60 {
        db.put(&key(i), &value(i, 400)).unwrap();
        model.insert(key(i), value(i, 400));
    }
    db.sync().unwrap();
    db.freeze().unwrap();
    // One step applies the frozen memtable and leaves the commit undone.
    assert!(db.checkpoint_step().unwrap());
    assert_eq!((db.stats().blobs_written, db.stats().blobs), (60, 30));
    drop(db);
    disk.crash();
    let db = open(&disk, &opts);
    assert_eq!((db.stats().blobs_written, db.stats().blobs), (0, 30));
    verify(&db, &model);
    db.check_invariants().unwrap();
    db.checkpoint().unwrap();
    verify(&db, &model);
    assert_eq!((db.stats().blobs_written, db.stats().blobs), (0, 60));
    db.check_invariants().unwrap();
}

/// A large value logged whole, as under a higher threshold, gets its blob
/// from the checkpoint. A crash after the checkpoint wrote its blobs and
/// before it committed leaves their slots unreferenced: recovery frees them
/// and replays the values from the log.
#[test]
fn checkpoint_writes_blobs_for_values_logged_whole() {
    let disk = SimStorage::new(16, FaultConfig::default());
    let opts = Options { checkpoint_distance: 64, ..options() };
    let db = open(&disk, &Options { blob_threshold: usize::MAX, ..opts.clone() });
    let mut model = Model::new();
    for i in 0..30 {
        db.put(&key(i), &value(i, 400)).unwrap();
        model.insert(key(i), value(i, 400));
    }
    db.sync().unwrap();
    assert_eq!(db.stats().blobs_written, 0);
    drop(db);
    let db = open(&disk, &opts);
    verify(&db, &model);
    // One step applies the replayed memtable, writing its blobs, and leaves
    // the commit undone.
    assert!(db.checkpoint_step().unwrap());
    assert_eq!((db.stats().blobs_written, db.stats().blobs), (30, 0));
    drop(db);
    disk.crash();
    let db = open(&disk, &opts);
    assert_eq!((db.stats().blobs_written, db.stats().blobs), (0, 0));
    verify(&db, &model);
    db.check_invariants().unwrap();
    db.checkpoint().unwrap();
    assert_eq!((db.stats().blobs_written, db.stats().blobs), (30, 30));
    verify(&db, &model);
    db.check_invariants().unwrap();
}

/// With LZ4, a blob that compresses is stored smaller, one that does not is
/// stored as it is, and both read back whole.
#[test]
fn blobs_compress_when_it_pays() {
    for compression in [Compression::None, Compression::Lz4] {
        let disk = SimStorage::new(7, FaultConfig::default());
        let opts = Options { compression, ..options() };
        let db = open(&disk, &opts);
        let repetitive = vec![b'r'; 50_000];
        let random = value(9, 50_000);
        db.put(b"repetitive", &repetitive).unwrap();
        db.put(b"random", &random).unwrap();
        db.checkpoint().unwrap();
        let stored = db.stats().blob_bytes;
        match compression {
            Compression::None => assert_eq!(stored, 2 * (50_000 + 16)),
            Compression::Lz4 => assert!(stored > 50_000 && stored < 50_000 + 16 + 1000, "stored {stored}"),
        }
        drop(db);
        let db = open(&disk, &opts);
        assert_eq!(db.get(b"repetitive").unwrap().unwrap(), repetitive);
        assert_eq!(db.get(b"random").unwrap().unwrap(), random);
    }
}

/// The threshold may change between opens: each entry records whether its
/// value is a blob.
#[test]
fn threshold_changes_between_opens() {
    let disk = SimStorage::new(8, FaultConfig::default());
    let mut model = Model::new();
    let mut seed = 0;
    for threshold in [THRESHOLD, usize::MAX, 10, 1000, 0] {
        let opts = Options { blob_threshold: threshold, ..options() };
        let db = open(&disk, &opts);
        verify(&db, &model);
        for i in (seed % 3..60).step_by(3) {
            seed += 1;
            let v = value(seed, 20 + (seed * 37) % 400);
            db.put(&key(i), &v).unwrap();
            model.insert(key(i), v);
        }
        db.checkpoint().unwrap();
        verify(&db, &model);
        db.check_invariants().unwrap();
    }
}

/// A scan over many blobs returns them in order, each read whole.
#[test]
fn scan_reads_many_blobs() {
    let disk = SimStorage::new(9, FaultConfig::default());
    let opts = Options { tree: TreeConfig { leaf_size: 16 << 10, ..options().tree }, blob_threshold: 4096, ..options() };
    let db = open(&disk, &opts);
    let mut model = Model::new();
    for i in 0..120 {
        // Enough stored bytes that the scan's read-ahead window fills.
        let v = value(i, 100_000 + i);
        db.put(&key(i), &v).unwrap();
        model.insert(key(i), v);
        if i % 20 == 19 {
            db.checkpoint().unwrap();
        }
    }
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs, 120);
    verify(&db, &model);
    let rows = db.scan(&key(30), Some(&key(90)), 50).unwrap();
    assert_eq!(rows.len(), 50);
    for (i, (k, v)) in rows.iter().enumerate() {
        assert_eq!(k[..], key(30 + i)[..]);
        assert_eq!(v[..], value(30 + i, 100_000 + 30 + i)[..]);
    }
}

/// A store that keeps every value in the tree writes no blob.
#[test]
fn no_blobs_without_a_threshold() {
    let disk = SimStorage::new(10, FaultConfig::default());
    let opts = Options { blob_threshold: usize::MAX, ..options() };
    let db = open(&disk, &opts);
    db.put(b"big", &value(1, 200_000)).unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs_written, 0);
    assert_eq!(db.get(b"big").unwrap().unwrap(), value(1, 200_000));
}

/// A writer writes its blob before it logs, and the checkpoint gives the
/// tree that blob's pointer, so the value reaches the drive once.
#[test]
fn a_writers_blob_serves_the_tree() {
    let disk = SimStorage::new(11, FaultConfig::default());
    let opts = Options { checkpoint_distance: 64, ..options() };
    let db = open(&disk, &opts);
    let mut model = Model::new();
    for i in 0..25 {
        db.put(&key(i), &value(i, 600)).unwrap();
        model.insert(key(i), value(i, 600));
    }
    let stats = db.stats();
    assert_eq!((stats.blobs_written, stats.blobs), (25, 0));
    // The log holds pointers, not values.
    assert!(stats.wal_bytes < 25 * 100, "{} log bytes", stats.wal_bytes);
    db.check_invariants().unwrap();
    db.checkpoint().unwrap();
    let stats = db.stats();
    assert_eq!((stats.blobs_written, stats.blobs), (25, 25));
    verify(&db, &model);
    db.check_invariants().unwrap();
}

/// Of the blobs written for one memtable, those whose puts were overwritten,
/// deleted or range-deleted before the checkpoint never reach the tree, and
/// its commit frees them.
#[test]
fn blobs_superseded_within_a_memtable_are_freed() {
    let disk = SimStorage::new(12, FaultConfig::default());
    let opts = Options { checkpoint_distance: 64, ..options() };
    let db = open(&disk, &opts);
    let mut model = Model::new();
    for round in 0..3 {
        for i in 0..20 {
            db.put(&key(i), &value(round * 100 + i, 300)).unwrap();
            model.insert(key(i), value(round * 100 + i, 300));
        }
    }
    for i in 0..5 {
        db.delete(&key(i)).unwrap();
        model.remove(&key(i));
    }
    db.delete_range(&key(15), None).unwrap();
    model.retain(|k, _| *k < key(15));
    assert_eq!(db.stats().blobs_written, 60);
    db.check_invariants().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.stats().blobs, 10);
    verify(&db, &model);
    db.check_invariants().unwrap();
    drop(db);
    let db = open(&disk, &opts);
    assert_eq!(db.stats().blobs, 10);
    verify(&db, &model);
    db.check_invariants().unwrap();
}

/// After a crash, replay takes each logged put's value from its blob, keeps
/// the blob for the checkpoint, and frees the slots no surviving record
/// points to.
#[test]
fn replay_keeps_the_blobs_the_log_points_to() {
    let disk = SimStorage::new(13, FaultConfig::default());
    let opts = Options { checkpoint_distance: 64, ..options() };
    let db = open(&disk, &opts);
    let mut model = Model::new();
    for i in 0..10 {
        db.put(&key(i), &value(i, 800)).unwrap();
        model.insert(key(i), value(i, 800));
    }
    db.checkpoint().unwrap();
    for i in 5..30 {
        db.put(&key(i), &value(100 + i, 800)).unwrap();
        model.insert(key(i), value(100 + i, 800));
    }
    db.sync().unwrap();
    drop(db);
    disk.crash();
    for _ in 0..2 {
        let db = open(&disk, &opts);
        verify(&db, &model);
        db.check_invariants().unwrap();
        // The values came back from their blobs, and none was written again.
        assert_eq!(db.stats().blobs_written, 0);
        drop(db);
        disk.crash();
    }
    let db = open(&disk, &opts);
    db.checkpoint().unwrap();
    assert_eq!((db.stats().blobs, db.stats().blobs_written), (30, 0));
    verify(&db, &model);
    db.check_invariants().unwrap();
}

/// A logged put whose blob is not whole ends replay, as a torn record does:
/// it and every write after it are gone, and the writes before it stay.
#[test]
fn a_damaged_blob_ends_replay() {
    use lionturtle::storage::Storage;
    let disk = SimStorage::new(14, FaultConfig::default());
    let opts = Options { checkpoint_distance: 64, ..options() };
    let db = open(&disk, &opts);
    db.put(&key(0), &value(0, 5000)).unwrap();
    db.put(&key(1), b"small").unwrap();
    db.put(&key(2), &value(2, 5000)).unwrap();
    db.put(&key(3), b"after").unwrap();
    db.put(&key(4), &value(4, 5000)).unwrap();
    drop(db);
    // Each value takes a slot of 8 KiB; the second blob's is the second.
    let arena = disk.list().unwrap().into_iter().find(|n| n.starts_with("blobs-")).expect("a blob arena");
    disk.open(&arena, false).unwrap().write_all_at(&[0u8; 512], 8192 + 1024).unwrap();
    let db = open(&disk, &opts);
    assert_eq!(db.get(&key(0)).unwrap().unwrap(), value(0, 5000));
    assert_eq!(db.get(&key(1)).unwrap().as_deref(), Some(&b"small"[..]));
    for i in 2..5 {
        assert_eq!(db.get(&key(i)).unwrap(), None, "key {i} survived a damaged blob before it");
    }
    assert_eq!(db.last_seq(), 2);
    db.check_invariants().unwrap();
    // The store takes writes again. The new blob takes the lost one's slot
    // under an id of its own, so the dropped record's pointer still fails
    // there, and the record stays dropped, with the writes after it.
    db.put(&key(2), &value(22, 5000)).unwrap();
    db.sync().unwrap();
    drop(db);
    disk.crash();
    let db = open(&disk, &opts);
    assert_eq!(db.last_seq(), 3);
    assert_eq!(db.get(&key(2)).unwrap().unwrap(), value(22, 5000));
    assert_eq!(db.get(&key(3)).unwrap(), None);
    assert_eq!(db.get(&key(4)).unwrap(), None);
    db.checkpoint().unwrap();
    assert_eq!(db.get(&key(2)).unwrap().unwrap(), value(22, 5000));
    assert_eq!(db.stats().blobs, 2);
    db.check_invariants().unwrap();
}

/// Writers on many threads write their blobs at once while a background
/// thread checkpoints and readers read them back.
#[test]
fn concurrent_blob_writers_and_readers() {
    const WRITERS: usize = 6;
    const KEYS: usize = 40;
    const ROUNDS: usize = 30;
    let disk = SimStorage::new(15, FaultConfig::default());
    let opts = Options { background: true, merge_threads: 2, pace_checkpoints: true, ..options() };
    let db = Arc::new(open(&disk, &opts));
    let wkey = |w: usize, i: usize| key(w * 1000 + i);
    // A value names its writer, key and round, so a reader can tell a whole
    // value of any round from a mixed one.
    let wvalue = |w: usize, i: usize, round: usize| value(w * 1_000_000 + i * 1000 + round, 150 + (i * 13) % 500);
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|r| {
            let (db, done) = (db.clone(), done.clone());
            std::thread::spawn(move || {
                let mut reads = 0u64;
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    let (w, i) = ((reads as usize + r) % WRITERS, (reads as usize * 7) % KEYS);
                    if let Some(v) = db.get(&wkey(w, i)).unwrap() {
                        assert!((0..ROUNDS).any(|round| v[..] == wvalue(w, i, round)[..]), "a value no round wrote");
                    }
                    if reads.is_multiple_of(50) {
                        for (k, v) in db.scan(&wkey(w, 0), Some(&wkey(w, KEYS)), 10).unwrap() {
                            let i = (0..KEYS).find(|&i| k[..] == wkey(w, i)[..]).expect("a key of this writer");
                            assert!((0..ROUNDS).any(|round| v[..] == wvalue(w, i, round)[..]), "a scanned value no round wrote");
                        }
                    }
                    reads += 1;
                }
            })
        })
        .collect();
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let db = db.clone();
            std::thread::spawn(move || {
                for round in 0..ROUNDS {
                    for i in 0..KEYS {
                        if (i + round) % 4 == 3 {
                            db.delete(&wkey(w, i)).unwrap();
                        } else {
                            db.write(&[(&wkey(w, i)[..], Some(&wvalue(w, i, round)[..]))], round % 5 == 0).unwrap();
                        }
                    }
                }
            })
        })
        .collect();
    for t in writers {
        t.join().unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in readers {
        t.join().unwrap();
    }
    db.checkpoint().unwrap();
    let last = ROUNDS - 1;
    for w in 0..WRITERS {
        for i in 0..KEYS {
            let want = ((i + last) % 4 != 3).then(|| wvalue(w, i, last));
            assert_eq!(db.get(&wkey(w, i)).unwrap().map(|v| v.to_vec()), want);
        }
    }
    let db = Arc::try_unwrap(db).ok().expect("every thread has finished");
    // A background thread may hold an older version for a moment after the
    // last commit; another checkpoint lets it go.
    db.put(b"poke", b"x").unwrap();
    db.checkpoint().unwrap();
    db.check_invariants().unwrap();
    drop(db);
    let db = open(&disk, &options());
    for w in 0..WRITERS {
        for i in 0..KEYS {
            let want = ((i + last) % 4 != 3).then(|| wvalue(w, i, last));
            assert_eq!(db.get(&wkey(w, i)).unwrap().map(|v| v.to_vec()), want);
        }
    }
    db.check_invariants().unwrap();
}
