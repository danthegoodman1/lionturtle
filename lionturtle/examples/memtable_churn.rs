//! What a memtable costs to fill and to drop when one follows another, as
//! in a store under steady writes: one thread fills each table while the
//! last one drops on another thread, as a checkpoint drops it.
//!
//! Entries go in as batches of `batch` ops, as a write batch's do, or one
//! at a time with a `batch` of 1.
//!
//! Usage: memtable_churn <bbtree|skiplist|btree> [entries] [rounds] [batch]

use lionturtle::arena::ChunkPool;
use lionturtle::memtable::{MemTableKind, new_memtable};
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The calling thread's minor page faults so far.
fn minor_faults() -> u64 {
    // SAFETY: getrusage fills the struct it is given.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        #[cfg(target_os = "linux")]
        libc::getrusage(libc::RUSAGE_THREAD, &mut usage);
        #[cfg(not(target_os = "linux"))]
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage.ru_minflt as u64
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let kind: MemTableKind = args.next().unwrap_or_else(|| "bbtree".into()).parse().unwrap();
    let entries: u64 = args.next().map_or(2_300_000, |a| a.parse().unwrap());
    let rounds: u64 = args.next().map_or(6, |a| a.parse().unwrap());
    let batch: usize = args.next().map_or(257, |a| a.parse().unwrap());
    let pool = ChunkPool::new(128 << 20);
    let (key_len, value) = (24, [0x5au8; 26]);
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut dropper: Option<std::thread::JoinHandle<f64>> = None;
    for round in 0..rounds {
        let table = new_memtable(kind, &pool);
        let mut keys = vec![[0u8; 24]; batch];
        let (t, faults) = (Instant::now(), minor_faults());
        let mut seq = 1;
        while seq <= entries {
            for key in &mut keys {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                key[..8].copy_from_slice(&state.to_be_bytes());
            }
            if batch == 1 {
                table.insert(&keys[0][..key_len], Some(&value), seq, seq);
            } else {
                let ops: Vec<_> = keys.iter().map(|key| (&key[..key_len], Some(&value[..]))).collect();
                table.insert_many(&ops, seq, seq);
            }
            seq += batch as u64;
        }
        let (fill, faults) = (t.elapsed(), minor_faults() - faults);
        let drop_ms = dropper.take().map_or(0.0, |d| d.join().unwrap());
        println!(
            "{kind:?} round {round}: insert {:.0} ns, {:.2} faults per 257 inserts, previous table dropped in {drop_ms:.2} ms",
            fill.as_nanos() as f64 / entries as f64,
            faults as f64 * 257.0 / entries as f64,
        );
        dropper = Some(std::thread::spawn(move || {
            let t = Instant::now();
            drop(table);
            t.elapsed().as_secs_f64() * 1e3
        }));
    }
    let drop_ms = dropper.take().map_or(0.0, |d| d.join().unwrap());
    println!("{kind:?}: last table dropped in {drop_ms:.2} ms");
}
