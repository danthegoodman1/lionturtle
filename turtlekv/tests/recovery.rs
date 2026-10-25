//! Recovery from reused log segments.
//!
//! A reused segment still holds its old records past the new ones. With
//! fixed-size records, each old record sits exactly where a new one goes, so
//! a crash that loses one new record but keeps a later one leaves an old
//! record in the gap. Recovery must stop at the gap, keep every synced write,
//! and keep them through another crash.

use std::sync::Arc;
use turtlekv::sim::{FaultConfig, SimStorage};
use turtlekv::{Db, Options, TreeConfig, WriteOp};

/// One 512-byte log record per put, filling one simulated sector.
fn put(round: u32, i: u32) -> WriteOp {
    let key = format!("round{round:02}-key{i:05}");
    assert_eq!(key.len(), 16);
    WriteOp::put(key.into_bytes(), vec![b'v'; 467])
}

#[test]
fn reused_segments_keep_synced_writes() {
    for seed in 0..200 {
        let disk = SimStorage::new(seed, FaultConfig { sector_size: 512, crash_keep_sector: 0.5, ..Default::default() });
        let opts = Options {
            tree: TreeConfig { leaf_size: 16 << 10, max_pivots: 8, min_pivots: 2, max_levels: 2, ..TreeConfig::default() },
            checkpoint_distance: 64,
            wal_buffer_bytes: 0,
            background: false,
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
