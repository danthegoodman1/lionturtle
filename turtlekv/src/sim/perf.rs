//! Deterministic performance simulation.
//!
//! Runs a fixed workload against the simulated disk on one thread and counts
//! every byte the store reads and writes, so amplification is measured
//! exactly and reproducibly, independent of hardware and timing. This is the
//! paper's write-memory trade-off experiment: hold the data set fixed, vary
//! the checkpoint distance χ, and watch write amplification fall.

use super::{FaultConfig, SimStorage};
use crate::db::{Db, Options};
use crate::memtable::MemTableKind;
use crate::tree::TreeConfig;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct PerfConfig {
    pub records: u64,
    pub value_size: usize,
    pub leaf_size: u64,
    pub chi: u64,
    pub cache_bytes: u64,
    pub gets: u64,
    pub seed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PerfReport {
    pub user_bytes: u64,
    pub wal_bytes: u64,
    pub page_bytes: u64,
    /// Device bytes written per user byte, log included.
    pub waf: f64,
    /// Page bytes written per user byte.
    pub page_waf: f64,
    pub checkpoints: u64,
    pub height: u32,
    pub disk_bytes: u64,
    /// Device reads and bytes per point lookup after the load.
    pub reads_per_get: f64,
    pub read_bytes_per_get: f64,
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Loads `records` keys in scrambled order, checkpoints everything, then
/// performs `gets` uniform point lookups.
pub fn run(cfg: &PerfConfig) -> PerfReport {
    let disk = SimStorage::new(cfg.seed, FaultConfig::default());
    let opts = Options {
        tree: TreeConfig { leaf_size: cfg.leaf_size, parallel: false, ..TreeConfig::default() },
        checkpoint_distance: cfg.chi,
        cache_bytes: cfg.cache_bytes,
        memtable: MemTableKind::BTree,
        wal_buffer_bytes: 1 << 20,
        max_frozen: 1,
        background: false,
        merge_threads: 0,
        pace_checkpoints: false,
    };
    let db = Db::open(Arc::new(disk.clone()), opts).expect("open");
    let mut value = vec![0u8; cfg.value_size];
    for i in 0..cfg.records {
        let k = mix(i ^ cfg.seed);
        value[..8].copy_from_slice(&k.to_le_bytes());
        db.put(&k.to_be_bytes(), &value).expect("put");
    }
    db.checkpoint().expect("checkpoint");
    let stats = disk.stats();
    let user = cfg.records * (8 + cfg.value_size as u64);
    let wal = stats.class("wal").write_bytes;
    let pages = stats.class("pages").write_bytes;
    let total = stats.total_write_bytes();
    let s = db.stats();

    disk.reset_stats();
    for i in 0..cfg.gets {
        let k = mix(mix(i) % cfg.records ^ cfg.seed);
        assert!(db.get(&k.to_be_bytes()).expect("get").is_some(), "loaded key missing");
    }
    let reads = disk.stats().class("pages");
    PerfReport {
        user_bytes: user,
        wal_bytes: wal,
        page_bytes: pages,
        waf: total as f64 / user as f64,
        page_waf: pages as f64 / user as f64,
        checkpoints: s.checkpoints,
        height: s.tree.height,
        disk_bytes: disk.disk_usage(),
        reads_per_get: reads.reads as f64 / cfg.gets.max(1) as f64,
        read_bytes_per_get: reads.read_bytes as f64 / cfg.gets.max(1) as f64,
    }
}
