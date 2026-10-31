//! Pages stored compressed read back through every path that decompresses:
//! point reads and scans from disk, merges of pages read from disk, and
//! reopens, including ones that switch compression off and on again.

use lionturtle::sim::{FaultConfig, SimStorage};
use lionturtle::{Compression, Db, Options, TreeConfig};
use std::sync::Arc;

const KEYS: u64 = 5_000;

fn key(i: u64) -> Vec<u8> {
    // Scattered, so each round's updates land in every leaf.
    format!("key{:010}", i.wrapping_mul(0x9E37_79B9) % 1_000_000_007).into_bytes()
}

/// About 120 bytes that compress to a third of that.
fn value(i: u64, round: u64) -> Vec<u8> {
    format!("{round:04}-{i:08}-").repeat(8).into_bytes()
}

/// A small tree, and no cache, so that every read of a written page goes to
/// disk.
fn options(compression: Compression) -> Options {
    Options {
        tree: TreeConfig { leaf_size: 64 << 10, max_pivots: 8, min_pivots: 2, max_levels: 3, ..TreeConfig::default() },
        checkpoint_distance: 4,
        cache_bytes: 0,
        background: false,
        compression,
        ..Options::default()
    }
}

fn fill(db: &Db, rounds: std::ops::Range<u64>) {
    for round in rounds {
        for i in 0..KEYS {
            db.put(&key(i), &value(i, round)).unwrap();
        }
        db.checkpoint().unwrap();
    }
}

fn verify(db: &Db, round: u64) {
    for i in 0..KEYS {
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(&value(i, round)[..]), "key {i}");
    }
    let rows = db.scan(b"", None, usize::MAX).unwrap();
    assert_eq!(rows.len() as u64, KEYS);
    let mut want: Vec<(Vec<u8>, Vec<u8>)> = (0..KEYS).map(|i| (key(i), value(i, round))).collect();
    want.sort();
    for ((k, v), (wk, wv)) in rows.iter().zip(&want) {
        assert_eq!((&k[..], &v[..]), (&wk[..], &wv[..]));
    }
}

#[test]
fn compressed_pages_read_back() {
    let disk = SimStorage::new(1, FaultConfig::default());
    let open = |c| Db::open(Arc::new(disk.clone()), options(c)).unwrap();
    let db = open(Compression::Lz4);
    fill(&db, 0..3);
    let stats = db.stats();
    assert!(stats.span_reads > 0, "merges read compressed pages from disk");
    verify(&db, 2);
    assert!(db.stats().block_reads > stats.block_reads, "point reads decompressed blocks from disk");
    drop(db);

    // Reopened without compression, the store reads its compressed pages and
    // stores new ones as they are, then mixes both through merges.
    let db = open(Compression::None);
    verify(&db, 2);
    fill(&db, 3..5);
    verify(&db, 4);
    drop(db);
    let db = open(Compression::Lz4);
    verify(&db, 4);
    fill(&db, 5..6);
    verify(&db, 5);
}

#[test]
fn compression_shrinks_stored_pages() {
    let written = |c| {
        let db = Db::open(Arc::new(SimStorage::new(2, FaultConfig::default())), options(c)).unwrap();
        fill(&db, 0..3);
        db.stats().page_bytes_written
    };
    let (plain, lz4) = (written(Compression::None), written(Compression::Lz4));
    assert!(lz4 * 2 < plain, "LZ4 wrote {lz4} bytes, uncompressed {plain}");
}
