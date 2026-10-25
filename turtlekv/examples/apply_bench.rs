//! Applies random leaf-sized batches straight to a TurtleTree, checkpointing
//! every `chi` batches, to measure the checkpoint path without writers.
//! Usage: apply_bench <dir> <batches> <chi> [leaf_kb] [parallel 0|1]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use turtlekv::page::{EntryRef, PageBuilder};
use turtlekv::pagestore::PageStore;
use turtlekv::storage::FsStorage;
use turtlekv::tree::io::{DataPage, PageIo};
use turtlekv::tree::{self, Ctx, TreeConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let batches: u64 = args[2].parse().unwrap();
    let chi: u64 = args[3].parse().unwrap();
    let leaf_kb: u64 = args.get(4).map_or(4096, |s| s.parse().unwrap());
    let parallel = args.get(5).is_none_or(|s| s == "1");
    let _ = std::fs::remove_dir_all(dir);
    let storage = Arc::new(FsStorage::new(dir).unwrap());
    let cfg = TreeConfig { leaf_size: leaf_kb << 10, parallel, ..Default::default() };
    let io = PageIo::new(PageStore::open(storage, &[]).unwrap(), 1 << 30, cfg.leaf_size * 3);
    let ids = AtomicU64::new(1);
    let ctx = Ctx { cfg: &cfg, io: &io, next_page_id: &ids };
    let mut root = None;
    let mut x = 88172645463325252u64;
    let value = [7u8; 120];
    let (mut t_build, mut t_apply, mut t_write) = (0f64, 0f64, 0f64);
    let started = Instant::now();
    for b in 0..batches {
        let t = Instant::now();
        let n = cfg.leaf_size / 134;
        let mut keys: Vec<u64> = (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            })
            .collect();
        keys.sort_unstable();
        keys.dedup();
        let mut pb = PageBuilder::with_capacity(ids.fetch_add(1, std::sync::atomic::Ordering::Relaxed), cfg.leaf_size as usize + 65536);
        for k in keys {
            pb.push(EntryRef { key: &k.to_be_bytes(), value: Some(&value) });
        }
        let batch = Arc::new(DataPage::unwritten(pb.finish(cfg.filter_bits)));
        t_build += t.elapsed().as_secs_f64();
        let t = Instant::now();
        root = tree::apply_batch(&ctx, root.as_ref(), batch).unwrap();
        t_apply += t.elapsed().as_secs_f64();
        if (b + 1) % chi == 0 {
            let t = Instant::now();
            tree::write_tree(&ctx, root.as_ref().unwrap()).unwrap();
            t_write += t.elapsed().as_secs_f64();
        }
    }
    let secs = started.elapsed().as_secs_f64();
    let user = batches * cfg.leaf_size;
    let written = io.counters.page_bytes_written.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "{batches} batches of {} KiB: {:.2}s ({:.0} MB/s user data), build {t_build:.2}s apply {t_apply:.2}s write {t_write:.2}s, page WAF {:.2}, stats {:?}",
        leaf_kb,
        secs,
        user as f64 / secs / 1e6,
        written as f64 / user as f64,
        tree::stats(root.as_ref().unwrap())
    );
    let _ = std::fs::remove_dir_all(dir);
}
