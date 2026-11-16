//! What range deletes cost: dropping whole prefixes against deleting the
//! same keys one by one, many narrow range deletes, and what range deletes
//! waiting in memtables cost reads.
//!
//! Loads `tenants` × `keys` records of 100-byte values under keys
//! `t<tenant>/k<key>`, then measures each scenario on that store.
//!
//! Usage: range_deletes [dir] [tenants] [keys]

use lionturtle::storage::FsStorage;
use lionturtle::{Db, Options, WriteOp};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn key(tenant: u64, k: u64) -> Vec<u8> {
    format!("t{tenant:05}/k{k:08}").into_bytes()
}

fn prefix(tenant: u64) -> Vec<u8> {
    format!("t{tenant:05}/").into_bytes()
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

/// Mean and 99th and 99.9th percentiles, in microseconds.
fn summary(mut samples: Vec<Duration>) -> String {
    samples.sort_unstable();
    let mean = samples.iter().sum::<Duration>() / samples.len() as u32;
    let at = |q: f64| samples[((samples.len() as f64 * q) as usize).min(samples.len() - 1)];
    format!("mean {:.1} µs, p99 {:.1} µs, p99.9 {:.1} µs", us(mean), us(at(0.99)), us(at(0.999)))
}

/// Latency of `n` calls of `f`.
fn sample(n: usize, mut f: impl FnMut()) -> Vec<Duration> {
    (0..n)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed()
        })
        .collect()
}

/// Runs `f`, then a checkpoint, and reports the time of each and what the
/// checkpoint wrote.
fn with_checkpoint(db: &Db, what: &str, f: impl FnOnce()) {
    db.checkpoint().unwrap();
    let before = db.stats();
    let t = Instant::now();
    f();
    let writes = t.elapsed();
    let t = Instant::now();
    db.checkpoint().unwrap();
    let checkpoint = t.elapsed();
    let after = db.stats();
    println!(
        "{what}: writes {:.3} s, checkpoint {:.3} s writing {:.1} MB; leaves {} -> {}, arenas {:.0} -> {:.0} MB",
        writes.as_secs_f64(),
        checkpoint.as_secs_f64(),
        (after.page_bytes_written - before.page_bytes_written) as f64 / 1e6,
        before.tree.leaves,
        after.tree.leaves,
        before.arena_bytes as f64 / 1e6,
        after.arena_bytes as f64 / 1e6,
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| "bench-data/range-deletes".into());
    let tenants: u64 = args.next().map_or(1000, |a| a.parse().unwrap());
    let keys: u64 = args.next().map_or(4000, |a| a.parse().unwrap());
    let _ = std::fs::remove_dir_all(&dir);
    let db = Db::open(Arc::new(FsStorage::new(&dir).unwrap()), Options::default()).unwrap();
    let value: &'static [u8] = &[0x5a; 100];
    let t = Instant::now();
    for tenant in 0..tenants {
        let batch: Vec<WriteOp> = (0..keys).map(|k| WriteOp::put(key(tenant, k), value)).collect();
        for chunk in batch.chunks(256) {
            db.write(chunk, false).unwrap();
        }
    }
    db.checkpoint().unwrap();
    let loaded = db.stats();
    println!(
        "loaded {} records in {:.1} s: {} leaves, tree height {}, {:.0} MB of arenas",
        tenants * keys,
        t.elapsed().as_secs_f64(),
        loaded.tree.leaves,
        loaded.tree.height,
        loaded.arena_bytes as f64 / 1e6
    );
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);

    // Reads with range deletes waiting in the active memtable, none of
    // which covers the keys read.
    let live = tenants / 2;
    let probes = |db: &Db, rng: &mut Rng, label: &str| {
        let reads = db.stats().block_reads;
        let gets = sample(200_000, || {
            db.get(&key(rng.below(live), rng.below(keys))).unwrap();
        });
        let scans = sample(20_000, || {
            db.scan(&key(rng.below(live), rng.below(keys)), None, 50).unwrap();
        });
        let rev = sample(5_000, || {
            let tenant = rng.below(live);
            let mut it = db.iter_rev(&prefix(tenant), Some(&key(tenant, rng.below(keys))));
            for _ in 0..50 {
                if it.next().unwrap().is_none() {
                    break;
                }
            }
        });
        println!(
            "{label}: get {}; 50-entry scan {}; 50-entry reverse scan {}; {} blocks read from disk",
            summary(gets),
            summary(scans),
            summary(rev),
            db.stats().block_reads - reads
        );
    };
    probes(&db, &mut rng, "reads, no range deletes");
    let mut held = 0u64;
    for target in [1, 100, 10_000] {
        while held < target {
            // Narrow deletes of keys past each tenant's last, which hold nothing.
            let tenant = held % tenants;
            db.delete_range(&key(tenant, keys + held), Some(&key(tenant, keys + held + 1))).unwrap();
            held += 1;
        }
        probes(&db, &mut rng, &format!("reads, {target} range deletes in the memtable"));
    }
    db.checkpoint().unwrap();

    // Dropping 10% of the tenants by prefix, against deleting as many keys
    // of other tenants one by one.
    let dropped: Vec<u64> = (0..tenants / 10).map(|i| live + i).collect();
    with_checkpoint(&db, &format!("delete_prefix of {} tenants of {keys} keys", dropped.len()), || {
        let mut i = 0;
        let deletes = sample(dropped.len(), || {
            db.delete_prefix(&prefix(dropped[i])).unwrap();
            i += 1;
        });
        println!("  delete_prefix calls: {}", summary(deletes));
        let gets = sample(100_000, || {
            db.get(&key(dropped[rng.below(dropped.len() as u64) as usize], rng.below(keys))).unwrap();
        });
        let scans = sample(5_000, || {
            let tenant = dropped[rng.below(dropped.len() as u64) as usize];
            assert!(db.scan(&prefix(tenant), Some(&prefix(tenant + 1)), 50).unwrap().is_empty());
        });
        let rev = sample(1_000, || {
            let tenant = dropped[rng.below(dropped.len() as u64) as usize];
            assert!(db.iter_rev(&prefix(tenant), Some(&prefix(tenant + 1))).next().unwrap().is_none());
        });
        println!(
            "  reads of dropped tenants before their checkpoint: get {}; scan {}; reverse scan {}",
            summary(gets),
            summary(scans),
            summary(rev)
        );
    });
    let pointed: Vec<u64> = (0..tenants / 10).map(|i| live + tenants / 10 + i).filter(|&t| t < tenants).collect();
    with_checkpoint(&db, &format!("point deletes of {} tenants' keys", pointed.len()), || {
        for &tenant in &pointed {
            let batch: Vec<WriteOp> = (0..keys).map(|k| WriteOp::delete(key(tenant, k))).collect();
            for chunk in batch.chunks(256) {
                db.write(chunk, false).unwrap();
            }
        }
    });

    // Many narrow range deletes: each of one key, as a row delete by prefix
    // would be, against point deletes of as many keys.
    let n = 100_000.min(live * keys / 2);
    with_checkpoint(&db, &format!("{n} narrow range deletes"), || {
        let t = Instant::now();
        for i in 0..n {
            let (tenant, k) = (i % live, 2 * (i / live));
            let k0 = key(tenant, k);
            let mut k1 = k0.clone();
            k1.push(0);
            db.delete_range(&k0, Some(&k1)).unwrap();
        }
        println!("  {:.0} narrow range deletes per second", n as f64 / t.elapsed().as_secs_f64());
        probes(&db, &mut rng, &format!("  reads with {n} narrow range deletes in memtables"));
    });
    with_checkpoint(&db, &format!("{n} point deletes"), || {
        for i in 0..n {
            let (tenant, k) = (i % live, 2 * (i / live) + 1);
            db.delete(&key(tenant, k)).unwrap();
        }
    });
    let end = db.stats();
    println!("finally: {} leaves, {:.0} MB of arenas", end.tree.leaves, end.arena_bytes as f64 / 1e6);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
