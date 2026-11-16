//! Range deletes: what reads see before and after the checkpoint that applies
//! them, across memtables, snapshots, iterators, recovery and reopening, and
//! what a checkpoint gives back.

use bytes::Bytes;
use lionturtle::sim::{FaultConfig, Rng, SimStorage};
use lionturtle::storage::Storage;
use lionturtle::{Db, Error, MemTableKind, OpRef, Options, Snapshot, TreeConfig, WriteOp};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn small_options(rng: &mut Rng) -> Options {
    let max_pivots = rng.range(4, 12) as usize;
    Options {
        tree: TreeConfig {
            leaf_size: 1 << rng.range(9, 13),
            max_pivots,
            min_pivots: rng.range(2, (max_pivots / 2) as u64) as usize,
            max_segments: rng.range(3, 16) as usize,
            max_levels: rng.range(1, 5) as usize,
            filter_bits: *rng.pick(&[0, 8]),
            merge_readahead: 1 << 14,
            parallel: false,
        },
        checkpoint_distance: rng.range(1, 6),
        cache_bytes: *rng.pick(&[0, 1 << 16, 1 << 22]),
        memtable: *rng.pick(&[MemTableKind::BBTree, MemTableKind::SkipList, MemTableKind::BTree]),
        wal_buffer_bytes: 4096,
        max_frozen: rng.range(1, 3) as usize,
        background: false,
        merge_threads: 0,
        pace_checkpoints: false,
        preallocate_log: false,
        compression: *rng.pick(&[lionturtle::Compression::None, lionturtle::Compression::Lz4]),
    }
}

fn fixed_options() -> Options {
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
        ..Options::default()
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

fn model_range<'a>(model: &'a Model, start: &[u8], end: Option<&[u8]>) -> Vec<(&'a Vec<u8>, &'a Vec<u8>)> {
    if end.is_some_and(|e| e <= start) {
        return Vec::new();
    }
    let hi = end.map_or(Bound::Unbounded, |e| Bound::Excluded(e.to_vec()));
    model.range::<Vec<u8>, _>((Bound::Included(start.to_vec()), hi)).collect()
}

fn delete_model_range(model: &mut Model, start: &[u8], end: Option<&[u8]>) {
    let doomed: Vec<Vec<u8>> = model_range(model, start, end).into_iter().map(|(k, _)| k.clone()).collect();
    for k in doomed {
        model.remove(&k);
    }
}

fn drain(mut it: lionturtle::DbIter) -> Vec<(Bytes, Bytes)> {
    let mut out = Vec::new();
    while let Some(e) = it.next().unwrap() {
        out.push(e);
    }
    out
}

fn same(got: &[(Bytes, Bytes)], want: &[(&Vec<u8>, &Vec<u8>)]) -> bool {
    got.len() == want.len() && got.iter().zip(want).all(|((gk, gv), (wk, wv))| gk[..] == wk[..] && gv[..] == wv[..])
}

/// Checks every kind of read of `snap` over `[start, end)` against `model`.
fn check_reads(snap: &Snapshot, model: &Model, start: &[u8], end: Option<&[u8]>, what: &str) {
    let want = model_range(model, start, end);
    let scanned = snap.scan(start, end, usize::MAX).unwrap();
    assert!(same(&scanned, &want), "{what}: scan [{start:?}, {end:?}) got {} entries, want {}", scanned.len(), want.len());
    assert!(same(&drain(snap.iter(start, end)), &want), "{what}: iter");
    let mut reversed = drain(snap.iter_rev(start, end));
    reversed.reverse();
    assert!(same(&reversed, &want), "{what}: iter_rev [{start:?}, {end:?}) got {} entries, want {}", reversed.len(), want.len());
    let limit = want.len() / 2 + 1;
    let limited = snap.scan(start, end, limit).unwrap();
    assert!(same(&limited, &want[..limit.min(want.len())]), "{what}: limited scan");
    for (k, v) in want.iter().take(20) {
        assert_eq!(snap.get(k).unwrap().as_deref(), Some(&v[..]), "{what}: get {k:?}");
    }
}

fn check_all(db: &Db, model: &Model, what: &str) {
    let snap = db.snapshot();
    check_reads(&snap, model, b"", None, what);
    for i in (0..2000).step_by(37) {
        let k = key(i);
        assert_eq!(db.get(&k).unwrap().as_deref(), model.get(&k).map(|v| &v[..]), "{what}: get {i}");
    }
}

#[test]
fn reads_honor_range_deletes_in_every_layer() {
    let disk = SimStorage::new(1, FaultConfig::default());
    let opts = fixed_options();
    let db = Db::open(Arc::new(disk.clone()), opts.clone()).unwrap();
    let mut model = Model::new();
    let put = |db: &Db, model: &mut Model, i: u64, tag: &str| {
        let v = format!("{tag}-{i}").into_bytes();
        db.put(&key(i), &v).unwrap();
        model.insert(key(i), v);
    };
    // Tree.
    for i in 0..1500 {
        put(&db, &mut model, i, "tree");
    }
    db.checkpoint().unwrap();
    // A frozen memtable over part of it.
    for i in (0..1500).step_by(3) {
        put(&db, &mut model, i, "frozen");
    }
    db.freeze().unwrap();
    // The active memtable deletes across all three layers, then writes some
    // keys back inside the range.
    db.delete_range(&key(200), Some(&key(900))).unwrap();
    delete_model_range(&mut model, &key(200), Some(&key(900)));
    for i in (300..400).step_by(7) {
        put(&db, &mut model, i, "after");
    }
    db.delete_prefix(b"key014").unwrap();
    delete_model_range(&mut model, b"key014", Some(b"key015"));
    assert!(db.stats().range_deletes >= 2);
    check_all(&db, &model, "before checkpoint");
    for (lo, hi) in [(150, 250), (850, 950), (199, 901), (300, 400), (1390, 1510)] {
        check_reads(&db.snapshot(), &model, &key(lo), Some(&key(hi)), "window");
    }
    db.checkpoint().unwrap();
    assert_eq!(db.stats().range_deletes, 0);
    check_all(&db, &model, "after checkpoint");
    db.check_invariants().unwrap();
    drop(db);
    let db = Db::open(Arc::new(disk), opts).unwrap();
    check_all(&db, &model, "after reopen");
}

/// Ops in a batch apply in order: a put before a range delete that covers
/// it dies with it, and a put after survives.
#[test]
fn batches_apply_in_order() {
    let db = Db::open(Arc::new(SimStorage::new(2, FaultConfig::default())), fixed_options()).unwrap();
    db.put(b"b-old", b"1").unwrap();
    db.write(
        &[WriteOp::put(&b"b-before"[..], &b"x"[..]), WriteOp::delete_prefix(&b"b-"[..]), WriteOp::put(&b"b-after"[..], &b"y"[..])],
        false,
    )
    .unwrap();
    let check = |db: &Db| {
        assert_eq!(db.get(b"b-old").unwrap(), None);
        assert_eq!(db.get(b"b-before").unwrap(), None);
        assert_eq!(db.get(b"b-after").unwrap().as_deref(), Some(&b"y"[..]));
        assert_eq!(drain(db.prefix(b"b-")).len(), 1);
    };
    check(&db);
    db.checkpoint().unwrap();
    check(&db);
    // Borrowed ops work the same.
    db.write(&[OpRef::Put(b"c1", b"1"), OpRef::DeleteRange(b"c", None), OpRef::Put(b"c2", b"2")], true).unwrap();
    assert_eq!(db.get(b"c1").unwrap(), None);
    assert_eq!(db.get(b"c2").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(db.get(b"b-after").unwrap().as_deref(), Some(&b"y"[..]));
}

#[test]
fn snapshots_keep_what_a_later_range_delete_removes() {
    let db = Db::open(Arc::new(SimStorage::new(3, FaultConfig::default())), fixed_options()).unwrap();
    let mut model = Model::new();
    for i in 0..800 {
        db.put(&key(i), b"v").unwrap();
        model.insert(key(i), b"v".to_vec());
    }
    db.checkpoint().unwrap();
    let before = db.snapshot();
    let old = model.clone();
    db.delete_range(&key(100), None).unwrap();
    delete_model_range(&mut model, &key(100), None);
    let after = db.snapshot();
    check_reads(&before, &old, b"", None, "old snapshot");
    check_reads(&after, &model, b"", None, "new snapshot");
    // Both hold across the checkpoint that applies the delete.
    db.checkpoint().unwrap();
    check_reads(&before, &old, &key(50), Some(&key(200)), "old snapshot after checkpoint");
    check_reads(&after, &model, b"", None, "new snapshot after checkpoint");
    check_all(&db, &model, "store");
}

#[test]
fn empty_inverted_and_total_ranges() {
    let db = Db::open(Arc::new(SimStorage::new(4, FaultConfig::default())), fixed_options()).unwrap();
    for i in 0..300 {
        db.put(&key(i), b"v").unwrap();
    }
    db.delete_range(&key(50), Some(&key(50))).unwrap();
    db.delete_range(&key(60), Some(&key(10))).unwrap();
    assert_eq!(db.scan(b"", None, usize::MAX).unwrap().len(), 300);
    db.checkpoint().unwrap();
    assert_eq!(db.scan(b"", None, usize::MAX).unwrap().len(), 300);
    // Everything.
    db.delete_range(b"", None).unwrap();
    assert!(db.scan(b"", None, usize::MAX).unwrap().is_empty());
    assert_eq!(db.get(&key(7)).unwrap(), None);
    db.checkpoint().unwrap();
    assert_eq!(db.stats().tree.leaves, 0, "the tree is empty");
    db.check_invariants().unwrap();
    db.put(b"again", b"1").unwrap();
    assert_eq!(db.scan(b"", None, usize::MAX).unwrap().len(), 1);
    // A prefix of 0xff bytes reaches the end of the key space.
    db.put(b"\xff\xff\x01", b"1").unwrap();
    db.delete_prefix(b"\xff\xff").unwrap();
    assert_eq!(db.get(b"\xff\xff\x01").unwrap(), None);
    assert_eq!(db.get(b"again").unwrap().as_deref(), Some(&b"1"[..]));
}

#[test]
fn range_bounds_are_limited_like_keys() {
    let db = Db::open(Arc::new(SimStorage::new(5, FaultConfig::default())), fixed_options()).unwrap();
    let long = vec![b'k'; u16::MAX as usize + 1];
    assert!(matches!(db.delete_range(&long, None), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.delete_range(b"a", Some(&long)), Err(Error::InvalidArgument(_))));
}

/// The checkpoint that applies a wide range delete drops its subtrees, and
/// their pages return to the allocator.
#[test]
fn checkpoint_frees_the_deleted_pages() {
    let disk = SimStorage::new(6, FaultConfig::default());
    let db = Db::open(Arc::new(disk.clone()), fixed_options()).unwrap();
    for i in 0..20_000 {
        db.put(&key(i), &[b'v'; 40]).unwrap();
    }
    db.checkpoint().unwrap();
    let full = db.stats().tree;
    db.delete_range(&key(1_000), Some(&key(19_000))).unwrap();
    db.checkpoint().unwrap();
    let after = db.stats().tree;
    assert!(after.leaves * 5 < full.leaves, "leaves {} -> {}", full.leaves, after.leaves);
    assert_eq!(db.scan(b"", None, usize::MAX).unwrap().len(), 2_000);
    // Pages the dropped subtrees held are free once no reader holds them.
    db.check_invariants().unwrap();
    // The freed space takes new pages.
    let arena = db.stats().arena_bytes;
    for i in 0..10_000 {
        db.put(&key(100_000 + i), &[b'w'; 40]).unwrap();
    }
    db.checkpoint().unwrap();
    assert!(db.stats().arena_bytes <= arena + arena / 4, "arena grew from {arena} to {}", db.stats().arena_bytes);
}

const OLD_MAGIC: u64 = 0x544b_565f_5355_5052;

fn superblock_magics(disk: &SimStorage) -> [u64; 2] {
    let file = disk.open("superblock", false).unwrap();
    [0, 1].map(|slot| {
        let mut buf = [0u8; 8];
        file.read_exact_at(&mut buf, slot * 4096).unwrap();
        u64::from_le_bytes(buf)
    })
}

/// A store that never takes a range delete keeps a superblock older
/// versions read. The first range delete replaces both of its slots, before
/// it reaches the log, with ones they reject.
#[test]
fn first_range_delete_marks_the_superblock() {
    let disk = SimStorage::new(7, FaultConfig::default());
    let opts = fixed_options();
    let db = Db::open(Arc::new(disk.clone()), opts.clone()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.checkpoint().unwrap();
    assert_eq!(superblock_magics(&disk), [OLD_MAGIC; 2]);
    db.delete_range(b"a", Some(b"b")).unwrap();
    assert!(superblock_magics(&disk).iter().all(|&m| m != OLD_MAGIC));
    // Checkpoints and reopens keep the mark.
    for _ in 0..3 {
        db.put(b"c", b"1").unwrap();
        db.checkpoint().unwrap();
    }
    drop(db);
    let db = Db::open(Arc::new(disk.clone()), opts).unwrap();
    db.put(b"d", b"1").unwrap();
    db.checkpoint().unwrap();
    assert!(superblock_magics(&disk).iter().all(|&m| m != OLD_MAGIC));
    assert_eq!(db.get(b"a").unwrap(), None);
}

/// A crash keeps a prefix of history holding every synced write, range
/// deletes included, from the log or from a checkpoint.
#[test]
fn recovery_keeps_synced_range_deletes() {
    for seed in 0..30 {
        let mut rng = Rng::new(seed);
        let disk = SimStorage::new(seed, FaultConfig { crash_keep_sector: 0.5, ..FaultConfig::default() });
        let opts = small_options(&mut rng);
        let db = Db::open(Arc::new(disk.clone()), opts.clone()).unwrap();
        // States after each batch, by the batch's last sequence number.
        let mut model = Model::new();
        let mut history: Vec<(u64, Model)> = vec![(0, Model::new())];
        let mut synced = 0;
        for step in 0..300 {
            let sync = rng.chance(0.1);
            let i = rng.below(400);
            if rng.chance(0.15) {
                let hi = (!rng.chance(0.1)).then(|| key(i + rng.range(0, 60)));
                db.write(&[WriteOp::DeleteRange { start: key(i).into(), end: hi.clone().map(Bytes::from) }], sync).unwrap();
                delete_model_range(&mut model, &key(i), hi.as_deref());
            } else {
                let v = format!("{step}").into_bytes();
                db.write(&[WriteOp::put(key(i), v.clone())], sync).unwrap();
                model.insert(key(i), v);
            }
            let seq = db.last_seq();
            history.push((seq, model.clone()));
            if sync {
                synced = seq;
            }
            if rng.chance(0.05) {
                db.freeze().unwrap();
            }
            if rng.chance(0.1) {
                db.checkpoint_step().unwrap();
            }
        }
        // Power fails with the store open: unsynced log writes may be lost.
        disk.crash();
        drop(db);
        let db = Db::open(Arc::new(disk), opts).unwrap();
        let recovered = db.last_seq();
        assert!(recovered >= synced, "seed {seed}: recovered {recovered}, synced {synced}");
        let (_, state) = history.iter().find(|(s, _)| *s == recovered).expect("recovery lands on a batch boundary");
        check_all(&db, state, &format!("seed {seed} recovered"));
        db.check_invariants().unwrap();
    }
}

/// Random histories of puts, deletes, range deletes and checkpoints on small
/// stores, with every kind of read checked against a model, live and through
/// held snapshots, and across reopens.
#[test]
fn random_histories_match_model() {
    for seed in 0..40 {
        let mut rng = Rng::new(seed * 7 + 1);
        let disk = SimStorage::new(seed, FaultConfig::default());
        let opts = small_options(&mut rng);
        let mut db = Db::open(Arc::new(disk.clone()), opts.clone()).unwrap();
        let keyspace = rng.range(200, 5000);
        let mut model = Model::new();
        let mut held: Vec<(Snapshot, Model)> = Vec::new();
        let pick = |rng: &mut Rng| key(rng.below(keyspace));
        for step in 0..3000 {
            match rng.below(1000) {
                0..550 => {
                    let k = pick(&mut rng);
                    let v = vec![b'a' + (step % 26) as u8; rng.range(0, 60) as usize];
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
                550..610 => {
                    let k = pick(&mut rng);
                    db.delete(&k).unwrap();
                    model.remove(&k);
                }
                610..650 => {
                    // Narrow, empty and inverted ranges, and now and then a
                    // wide or unbounded one.
                    let i = rng.below(keyspace);
                    let start = if rng.chance(0.02) { Vec::new() } else { key(i) };
                    let end = match rng.below(40) {
                        0 => None,
                        1 => Some(key(i + keyspace / 3)),
                        2..6 => Some(key(i.saturating_sub(3))),
                        _ => Some(key(i + rng.range(0, 25))),
                    };
                    db.delete_range(&start, end.as_deref()).unwrap();
                    delete_model_range(&mut model, &start, end.as_deref());
                }
                650..670 => {
                    // Keys are "key" and five digits: a prefix of six to
                    // eight bytes holds 100, 10 or 1 of them.
                    let mut p = pick(&mut rng);
                    p.truncate(rng.range(6, 9) as usize);
                    db.delete_prefix(&p).unwrap();
                    let end = lionturtle::types::prefix_end(&p);
                    delete_model_range(&mut model, &p, end.as_deref());
                }
                670..700 => {
                    // A batch mixing everything, applied in order.
                    let mut ops = Vec::new();
                    for _ in 0..rng.range(2, 8) {
                        let i = rng.below(keyspace);
                        let k = key(i);
                        match rng.below(4) {
                            0 => {
                                let hi = key(i + rng.range(0, 10));
                                delete_model_range(&mut model, &k, Some(&hi));
                                ops.push(WriteOp::delete_range(k, hi));
                            }
                            1 => {
                                model.remove(&k);
                                ops.push(WriteOp::delete(k));
                            }
                            _ => {
                                model.insert(k.clone(), b"batch".to_vec());
                                ops.push(WriteOp::put(k, &b"batch"[..]));
                            }
                        }
                    }
                    db.write(&ops, rng.chance(0.2)).unwrap();
                }
                700..760 => {
                    let start = pick(&mut rng);
                    let end = rng.chance(0.7).then(|| pick(&mut rng));
                    let (snap, state) = match held.is_empty() || rng.chance(0.5) {
                        true => (db.snapshot(), &model),
                        false => {
                            let (s, m) = &held[rng.below(held.len() as u64) as usize];
                            (s.clone(), m)
                        }
                    };
                    check_reads(&snap, state, &start, end.as_deref(), &format!("seed {seed} step {step}"));
                }
                760..860 => {
                    let k = pick(&mut rng);
                    assert_eq!(db.get(&k).unwrap().as_deref(), model.get(&k).map(|v| &v[..]), "seed {seed} step {step}: get {k:?}");
                }
                860..880 => {
                    if held.len() < 3 {
                        held.push((db.snapshot(), model.clone()));
                    } else {
                        held.swap_remove(rng.below(3) as usize);
                    }
                }
                880..960 => {
                    db.checkpoint_step().unwrap();
                }
                960..975 => db.freeze().unwrap(),
                975..985 => db.checkpoint().unwrap(),
                985..988 => {
                    held.clear();
                    drop(db);
                    db = Db::open(Arc::new(disk.clone()), opts.clone()).unwrap();
                }
                _ => {
                    if held.is_empty() {
                        db.check_invariants().unwrap();
                    }
                }
            }
        }
        held.clear();
        check_all(&db, &model, &format!("seed {seed} final"));
        db.checkpoint().unwrap();
        check_all(&db, &model, &format!("seed {seed} final checkpoint"));
        db.check_invariants().unwrap();
        let stats = db.stats();
        eprintln!(
            "seed {seed}: {} keys, {} checkpoints since the last open, tree height {}",
            model.len(),
            stats.checkpoints,
            stats.tree.height
        );
    }
}
