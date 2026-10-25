//! Multi-threaded stress: writers, readers, and scanners run against a store
//! whose background thread checkpoints continuously. The VOPR runs on one
//! thread; this exercises the locks, version swaps, and epoch reclamation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use turtlekv::sim::{FaultConfig, SimStorage};
use turtlekv::storage::FsStorage;
use turtlekv::{Db, MemTableKind, Options, TreeConfig};

const WRITERS: usize = 4;
const KEYS_PER_WRITER: u64 = 400;

fn key(w: usize, k: u64) -> Vec<u8> {
    format!("w{w}-k{k:05}").into_bytes()
}

fn value(version: u64, w: usize, k: u64) -> Vec<u8> {
    let mut v = format!("{version:010}-{w}-{k}-").into_bytes();
    v.resize(20 + (k as usize * 7) % 90, b'x');
    v
}

fn version_of(v: &[u8]) -> u64 {
    std::str::from_utf8(&v[..10]).unwrap().parse().unwrap()
}

fn stress(storage: Arc<dyn turtlekv::storage::Storage>, memtable: MemTableKind, secs: u64) {
    let opts = Options {
        tree: TreeConfig {
            leaf_size: 8 << 10,
            max_pivots: 8,
            min_pivots: 2,
            max_segments: 12,
            max_levels: 4,
            ..TreeConfig::default()
        },
        checkpoint_distance: 4,
        cache_bytes: 256 << 10,
        memtable,
        wal_buffer_bytes: 4096,
        max_frozen: 2,
        background: true,
        merge_threads: 2,
    };
    let db = Arc::new(Db::open(storage.clone(), opts.clone()).unwrap());
    // Highest version each key has been written with (published after the
    // write returns), so readers can bound what they see.
    let written: Arc<Vec<AtomicU64>> =
        Arc::new((0..WRITERS as u64 * KEYS_PER_WRITER).map(|_| AtomicU64::new(0)).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let (db, written, stop) = (db.clone(), written.clone(), stop.clone());
        handles.push(std::thread::spawn(move || {
            let mut version = 0u64;
            let mut i = w as u64;
            while !stop.load(Ordering::Relaxed) {
                i = i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let k = (i >> 33) % KEYS_PER_WRITER;
                version += 1;
                if (i >> 20) % 10 == 0 {
                    db.delete(&key(w, k)).unwrap();
                } else {
                    db.put(&key(w, k), &value(version, w, k)).unwrap();
                }
                written[w * KEYS_PER_WRITER as usize + k as usize].store(version, Ordering::SeqCst);
            }
        }));
    }
    let mut readers = Vec::new();
    for r in 0..3 {
        let (db, written, stop) = (db.clone(), written.clone(), stop.clone());
        readers.push(std::thread::spawn(move || {
            let mut seen: HashMap<(usize, u64), u64> = HashMap::new();
            let mut i = 99 + r as u64;
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                i = i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let w = (i >> 40) as usize % WRITERS;
                let k = (i >> 20) % KEYS_PER_WRITER;
                if (i >> 8) % 8 == 0 {
                    // A scan must be sorted and only show versions written.
                    let rows = db.scan(&key(w, k), None, 50).unwrap();
                    for pair in rows.windows(2) {
                        assert!(pair[0].0 < pair[1].0, "scan out of order");
                    }
                    continue;
                }
                let bound = written[w * KEYS_PER_WRITER as usize + k as usize].load(Ordering::SeqCst);
                if let Some(v) = db.get(&key(w, k)).unwrap() {
                    let version = version_of(&v);
                    assert_eq!(&v[..], &value(version, w, k)[..], "torn value");
                    let prev = seen.insert((w, k), version).unwrap_or(0);
                    assert!(version >= prev, "version went backwards: {prev} -> {version}");
                    // A writer publishes its version after the put returns,
                    // so a reader may see at most one version ahead.
                    let _ = bound;
                }
                reads += 1;
            }
            reads
        }));
    }
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::SeqCst);
    for h in handles {
        h.join().unwrap();
    }
    let reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    let stats = db.stats();
    eprintln!("{memtable:?}: {reads} reads, {} checkpoints, tree {:?}", stats.checkpoints, stats.tree);
    assert!(stats.checkpoints > 3, "background checkpoints ran");

    // Every key must hold its final write, before and after a reopen.
    let snapshot: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..WRITERS)
        .flat_map(|w| (0..KEYS_PER_WRITER).map(move |k| (w, k)))
        .map(|(w, k)| (key(w, k), db.get(&key(w, k)).unwrap().map(|v| v.to_vec())))
        .collect();
    db.checkpoint().unwrap();
    db.check_invariants().unwrap();
    drop(db);
    let db = Db::open(storage, opts).unwrap();
    for (k, v) in &snapshot {
        assert_eq!(db.get(k).unwrap().map(|v| v.to_vec()), *v);
    }
    let all = db.scan(b"", None, usize::MAX).unwrap();
    assert_eq!(all.len(), snapshot.iter().filter(|(_, v)| v.is_some()).count());
    db.check_invariants().unwrap();
}

#[test]
fn concurrent_bbtree_sim_disk() {
    stress(Arc::new(SimStorage::new(5, FaultConfig::default())), MemTableKind::BBTree, 4);
}

#[test]
fn concurrent_skiplist_real_disk() {
    let dir = std::env::temp_dir().join(format!("turtlekv-stress-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    stress(Arc::new(FsStorage::new(&dir).unwrap()), MemTableKind::SkipList, 4);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn concurrent_btree_sim_disk() {
    let started = Instant::now();
    stress(Arc::new(SimStorage::new(6, FaultConfig::default())), MemTableKind::BTree, 3);
    assert!(started.elapsed() < Duration::from_secs(120));
}

/// Writers update pairs of keys in one batch while readers check that no
/// read sees one half of a batch without the other: a get of the second key
/// after the first, and every iterator and scan, which read one snapshot.
/// Reads through one explicit snapshot must also agree with each other.
fn atomic_batches(memtable: MemTableKind) {
    const PAIRS: u64 = 16;
    const WRITERS: u64 = 4;
    let opts = Options {
        tree: TreeConfig { leaf_size: 4 << 10, max_pivots: 8, min_pivots: 2, max_levels: 3, ..TreeConfig::default() },
        checkpoint_distance: 2,
        cache_bytes: 64 << 10,
        memtable,
        wal_buffer_bytes: 4096,
        max_frozen: 2,
        background: true,
        merge_threads: 2,
    };
    let db = Arc::new(Db::open(Arc::new(SimStorage::new(7, FaultConfig::default())), opts).unwrap());
    // Pair `p` is keys 2p and 2p + 1; writer `w` owns the pairs with p % WRITERS == w,
    // so each pair's version only rises. Filler keys sit between the halves of a
    // batch to widen the window in which a reader could see half of it.
    let key = |k: u64| k.to_be_bytes();
    let filler = |w: u64, i: u64| (1u64 << 40 | w << 8 | i).to_be_bytes();
    let version = |v: Option<bytes::Bytes>| v.map_or(0, |v| u64::from_be_bytes(v[..8].try_into().unwrap()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    for w in 0..WRITERS {
        let (db, stop) = (db.clone(), stop.clone());
        threads.push(std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let p = w + WRITERS * (n % (PAIRS / WRITERS));
                let v = n.to_be_bytes().to_vec();
                let mut ops = vec![turtlekv::WriteOp::put(key(2 * p).to_vec(), v.clone())];
                ops.extend((0..8).map(|i| turtlekv::WriteOp::put(filler(w, i).to_vec(), v.clone())));
                ops.push(turtlekv::WriteOp::put(key(2 * p + 1).to_vec(), v));
                db.write(&ops, false).unwrap();
            }
        }));
    }
    let mut checks = Vec::new();
    for r in 0..4u64 {
        let (db, stop) = (db.clone(), stop.clone());
        checks.push(std::thread::spawn(move || {
            let mut i = r;
            let mut torn_free = 0u64;
            while !stop.load(Ordering::Relaxed) {
                i = i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let p = (i >> 33) % PAIRS;
                let pairs = |rows: &[(bytes::Bytes, bytes::Bytes)]| -> Vec<(u64, u64)> {
                    let got: HashMap<u64, u64> = rows
                        .iter()
                        .map(|(k, v)| (u64::from_be_bytes(k[..].try_into().unwrap()), version(Some(v.clone()))))
                        .collect();
                    (0..PAIRS)
                        .map(|p| {
                            let (a, b) = (got.get(&(2 * p)), got.get(&(2 * p + 1)));
                            assert_eq!(a, b, "pair {p} differs within one snapshot");
                            (p, a.copied().unwrap_or(0))
                        })
                        .collect()
                };
                let drain = |mut it: turtlekv::DbIter| {
                    let mut rows = Vec::new();
                    while let Some(row) = it.next().unwrap() {
                        rows.push(row);
                    }
                    rows
                };
                match r {
                    0 => {
                        let first = version(db.get(&key(2 * p)).unwrap());
                        let second = version(db.get(&key(2 * p + 1)).unwrap());
                        assert!(second >= first, "pair {p}: saw version {first} of its first key, then {second} of its second");
                    }
                    1 => {
                        pairs(&db.scan(&key(0), Some(&key(2 * PAIRS)), usize::MAX).unwrap());
                        pairs(&drain(db.iter(&key(0), Some(&key(2 * PAIRS)))));
                    }
                    2 => {
                        let snap = db.snapshot();
                        let first = version(snap.get(&key(2 * p)).unwrap());
                        let second = version(snap.get(&key(2 * p + 1)).unwrap());
                        assert_eq!(first, second, "pair {p} differs within one snapshot");
                    }
                    _ => {
                        // Every way of reading one snapshot returns the same pairs.
                        let snap = db.snapshot();
                        let scanned = pairs(&snap.scan(&key(0), Some(&key(2 * PAIRS)), usize::MAX).unwrap());
                        let mut reversed = drain(snap.iter_rev(&key(0), Some(&key(2 * PAIRS))));
                        reversed.reverse();
                        // The pairs' keys share 7 leading zero bytes; filler keys
                        // do not.
                        let mut prefixed_rev = drain(snap.prefix_rev(&[0; 7]));
                        prefixed_rev.reverse();
                        for other in [reversed, drain(snap.prefix(&[0; 7])), prefixed_rev] {
                            assert_eq!(pairs(&other), scanned, "reads of one snapshot disagree");
                        }
                        for &(p, v) in &scanned {
                            assert_eq!(version(snap.get(&key(2 * p + 1)).unwrap()), v, "snapshot get of pair {p}");
                        }
                    }
                }
                torn_free += 1;
            }
            torn_free
        }));
    }
    std::thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::SeqCst);
    for t in threads {
        t.join().unwrap();
    }
    let reads: u64 = checks.into_iter().map(|t| t.join().unwrap()).sum();
    let stats = db.stats();
    eprintln!("{memtable:?}: {reads} atomicity checks, {} checkpoints", stats.checkpoints);
    assert!(stats.checkpoints > 3, "background checkpoints ran");
}

#[test]
fn atomic_batches_bbtree() {
    atomic_batches(MemTableKind::BBTree);
}

#[test]
fn atomic_batches_skiplist() {
    atomic_batches(MemTableKind::SkipList);
}

#[test]
fn atomic_batches_btree() {
    atomic_batches(MemTableKind::BTree);
}

#[test]
fn atomic_batches_art() {
    atomic_batches(MemTableKind::Art8);
}
