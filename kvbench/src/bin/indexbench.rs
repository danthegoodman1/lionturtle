//! In-memory index comparison: bbtree vs congee (ART-OLC) vs crossbeam's
//! skip list vs a locked BTreeMap, first as raw u64 -> u64 maps, then as
//! TurtleKV memtables holding 8-byte keys and 120-byte values.
//! Usage: indexbench [keys] [threads,threads,...]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::ops::{Bound, ControlFlow};
use std::sync::Arc;
use std::time::Instant;
use turtlekv::memtable::{MemTableKind, new_memtable};
use turtlekv::Value;

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

trait Index: Send + Sync {
    fn insert(&self, k: u64, v: u64);
    fn get(&self, k: u64) -> Option<u64>;
    /// Sums up to `n` values from `k` on.
    fn scan(&self, k: u64, n: usize) -> u64;
}

struct BB(bbtree::BBTreeMap<u64, u64>);
impl Index for BB {
    fn insert(&self, k: u64, v: u64) {
        let _ = self.0.insert(k, v);
    }
    fn get(&self, k: u64) -> Option<u64> {
        self.0.get_copied(&k)
    }
    fn scan(&self, k: u64, n: usize) -> u64 {
        let (mut sum, mut i) = (0, 0);
        self.0.scan(k.., |_, v| {
            sum += v;
            i += 1;
            if i >= n { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
        sum
    }
}

struct Skip(crossbeam_skiplist::SkipMap<u64, u64>);
impl Index for Skip {
    fn insert(&self, k: u64, v: u64) {
        self.0.insert(k, v);
    }
    fn get(&self, k: u64) -> Option<u64> {
        self.0.get(&k).map(|e| *e.value())
    }
    fn scan(&self, k: u64, n: usize) -> u64 {
        self.0.range(k..).take(n).map(|e| *e.value()).sum()
    }
}

struct Locked(RwLock<BTreeMap<u64, u64>>);
impl Index for Locked {
    fn insert(&self, k: u64, v: u64) {
        self.0.write().insert(k, v);
    }
    fn get(&self, k: u64) -> Option<u64> {
        self.0.read().get(&k).copied()
    }
    fn scan(&self, k: u64, n: usize) -> u64 {
        self.0.read().range(k..).take(n).map(|(_, v)| *v).sum()
    }
}

struct Art(congee::Congee<usize, usize>);
impl Index for Art {
    fn insert(&self, k: u64, v: u64) {
        let g = self.0.pin();
        self.0.insert(k as usize, v as usize, &g).unwrap();
    }
    fn get(&self, k: u64) -> Option<u64> {
        let g = self.0.pin();
        self.0.get(&(k as usize), &g).map(|v| v as u64)
    }
    fn scan(&self, k: u64, n: usize) -> u64 {
        let g = self.0.pin();
        let mut buf = [(0usize, 0usize); 128];
        let got = self.0.range(&(k as usize), &usize::MAX, &mut buf[..n.min(128)], &g);
        buf[..got].iter().map(|(_, v)| *v as u64).sum()
    }
}

fn run<F: Fn(u64) + Sync>(threads: usize, ops: u64, f: F) -> f64 {
    let t = Instant::now();
    std::thread::scope(|s| {
        for th in 0..threads as u64 {
            let f = &f;
            s.spawn(move || {
                let per = ops / threads as u64;
                for i in th * per..(th + 1) * per {
                    f(i);
                }
            });
        }
    });
    ops as f64 / t.elapsed().as_secs_f64() / 1e6
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: u64 = args.get(1).map_or(10_000_000, |s| s.parse().unwrap());
    let threads: Vec<usize> = args.get(2).map_or(vec![1, 8, 16], |s| s.split(',').map(|t| t.parse().unwrap()).collect());
    println!("# raw u64 -> u64 index, {n} keys; Mops/s");
    println!("index,threads,insert,get,scan100");
    for &t in &threads {
        let indexes: Vec<(&str, Arc<dyn Index>)> = vec![
            ("bbtree", Arc::new(BB(bbtree::BBTreeMap::new()))),
            ("congee_art", Arc::new(Art(congee::Congee::default()))),
            ("crossbeam_skiplist", Arc::new(Skip(crossbeam_skiplist::SkipMap::new()))),
            ("rwlock_btreemap", Arc::new(Locked(RwLock::new(BTreeMap::new())))),
        ];
        for (name, idx) in indexes {
            let ins = run(t, n, |i| idx.insert(mix(i), i));
            let get = run(t, n, |i| {
                std::hint::black_box(idx.get(mix(mix(i) % n)));
            });
            let scan = run(t, n / 100, |i| {
                std::hint::black_box(idx.scan(mix(mix(i) % n), 100));
            });
            println!("{name},{t},{ins:.2},{get:.2},{scan:.3}");
        }
    }
    println!("\n# TurtleKV memtables: 8-byte keys, 120-byte values; Mops/s");
    println!("memtable,threads,insert,get,scan100");
    let value = Bytes::from(vec![7u8; 120]);
    for &t in &threads {
        for (name, kind) in [
            ("bbtree", MemTableKind::BBTree),
            ("art", MemTableKind::Art8),
            ("skiplist", MemTableKind::SkipList),
            ("btree", MemTableKind::BTree),
        ] {
            let table = new_memtable(kind);
            let ins = run(t, n, |i| table.insert(&mix(i).to_be_bytes(), Value::Put(value.clone()), i, 0));
            let get = run(t, n, |i| {
                std::hint::black_box(table.get(&mix(mix(i) % n).to_be_bytes(), &|_, _| true));
            });
            let scan = run(t, n / 100, |i| {
                let mut out = Vec::with_capacity(100);
                table.range(Bound::Included(&mix(mix(i) % n).to_be_bytes()), None, &|_, _| true, 100, &mut out);
                std::hint::black_box(out);
            });
            println!("{name},{t},{ins:.2},{get:.2},{scan:.3}");
        }
    }
}
