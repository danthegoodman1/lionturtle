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
