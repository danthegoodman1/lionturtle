//! Recovery of synchronous writes: from reused log segments, and from
//! writers sharing fsyncs.

use lionturtle::sim::{FaultConfig, SimStorage};
use lionturtle::{Db, Options, TreeConfig, WriteOp};
use parking_lot::Mutex;
use std::sync::Arc;

/// One 512-byte log record per put, filling one simulated sector.
fn put(round: u32, i: u32) -> WriteOp {
    let key = format!("round{round:02}-key{i:05}");
    assert_eq!(key.len(), 16);
    WriteOp::put(key.into_bytes(), vec![b'v'; 467])
}

/// A reused segment still holds its old records past the new ones. With
/// fixed-size records, each old record sits exactly where a new one goes, so
/// a crash that loses one new record but keeps a later one leaves an old
/// record in the gap. Recovery must stop at the gap, keep every synced write,
/// and keep them through another crash.
#[test]
fn reused_segments_keep_synced_writes() {
    for seed in 0..200 {
        let disk = SimStorage::new(seed, FaultConfig { sector_size: 512, crash_keep_sector: 0.5, ..Default::default() });
        let opts = Options {
            tree: TreeConfig { leaf_size: 16 << 10, max_pivots: 8, min_pivots: 2, max_levels: 2, ..TreeConfig::default() },
            checkpoint_distance: 64,
            wal_buffer_bytes: 0,
            background: false,
            preallocate_log: seed % 2 == 1,
            ..Options::default()
        };
        let open = || Db::open(Arc::new(disk.clone()), opts.clone()).unwrap();
        let db = open();
        // Each checkpoint retires a segment, and the next rotation reuses
        // it, so after a few rounds the active segment is a reused one.
        for round in 0..4 {
            for i in 0..20 {
                db.write(&[put(round, i)], true).unwrap();
            }
            db.checkpoint().unwrap();
        }
        for i in 0..10 {
            db.write(&[put(9, i)], true).unwrap();
        }
        for i in 10..20 {
            db.write(&[put(9, i)], false).unwrap();
        }
        disk.crash();
        drop(db);

        for attempt in 0..2 {
            let db = open();
            for i in 0..10 {
                let key = format!("round09-key{i:05}");
                assert!(db.get(key.as_bytes()).unwrap().is_some(), "seed {seed}, recovery {attempt}: synced write {i} lost");
            }
            // Writes after a gap must not reappear.
            let recovered = (10..20).take_while(|&i| db.get(format!("round09-key{i:05}").as_bytes()).unwrap().is_some()).count();
            let present = (10..20).filter(|&i| db.get(format!("round09-key{i:05}").as_bytes()).unwrap().is_some()).count();
            assert_eq!(recovered, present, "seed {seed}, recovery {attempt}: recovered writes are not a prefix");
            disk.crash();
            drop(db);
        }
    }
}

/// Writers share fsyncs while memtables rotate and checkpoints run, until
/// the disk loses power partway through. Every write acknowledged as
/// durable must survive.
#[test]
fn shared_fsyncs_keep_acknowledged_writes() {
    for seed in 0..30 {
        let disk = SimStorage::new(seed, FaultConfig { crash_keep_sector: 0.5, ..Default::default() });
        let opts = Options {
            tree: TreeConfig { leaf_size: 4 << 10, max_pivots: 8, min_pivots: 2, max_levels: 3, ..TreeConfig::default() },
            checkpoint_distance: 2,
            cache_bytes: 64 << 10,
            wal_buffer_bytes: 4096,
            max_frozen: 2,
            background: true,
            merge_threads: 2,
            preallocate_log: seed % 2 == 1,
            ..Options::default()
        };
        let db = Arc::new(Db::open(Arc::new(disk.clone()), opts.clone()).unwrap());
        disk.arm_crash(2_000 + seed * 300);
        let acked = Arc::new(Mutex::new(Vec::new()));
        let writers: Vec<_> = (0..8u64)
            .map(|t| {
                let (db, acked) = (db.clone(), acked.clone());
                std::thread::spawn(move || {
                    // Two of the eight writers skip the fsync.
                    let sync = t % 4 != 0;
                    for i in 0u64.. {
                        let key = format!("t{t}-{i:06}").into_bytes();
                        match db.write(&[WriteOp::put(key.clone(), vec![t as u8; 100])], sync) {
                            Ok(()) if sync => acked.lock().push(key),
                            Ok(()) => {}
                            Err(_) => break,
                        }
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        drop(db);
        disk.crash();
        let db = Db::open(Arc::new(disk.clone()), opts).unwrap();
        let acked = acked.lock();
        assert!(acked.len() > 50, "seed {seed}: only {} synchronous writes before the crash", acked.len());
        for key in acked.iter() {
            assert!(db.get(key).unwrap().is_some(), "seed {seed}: acknowledged write {} lost", String::from_utf8_lossy(key));
        }
    }
}
