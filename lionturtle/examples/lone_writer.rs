//! One thread committing durable batches of small entries, as a metadata
//! store built on lionturtle does: a get, then one synchronous write of a
//! few hundred ops. Each commit's latency and the minor page faults its
//! thread took are recorded, so stalls and memory churn in the write path
//! show up in the tail and the fault count.
//!
//! With `--rate`, commits start on a fixed schedule of that many a second,
//! so the store's background work keeps up as it would under a client-bound
//! load; without it, each commit starts when the last one returns.
//!
//! Usage: lone_writer <dir> [--secs 60] [--rate commits/s] [--ops 257] [--key 24]
//!                    [--value 26] [--preallocate 0|1] [--cache-mb 256] [--label name]

use lionturtle::storage::FsStorage;
use lionturtle::{Db, Options, WriteOp};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

fn arg(name: &str, default: u64) -> u64 {
    let args: Vec<String> = std::env::args().collect();
    args.iter().position(|a| a == name).map_or(default, |i| args[i + 1].parse().expect(name))
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn main() {
    let dir = std::env::args().nth(1).expect("usage: lone_writer <dir> [options]");
    let (secs, rate) = (arg("--secs", 60), arg("--rate", 0));
    let (ops, key_len, value_len) = (arg("--ops", 257) as usize, arg("--key", 24) as usize, arg("--value", 26) as usize);
    let label = std::env::args().skip_while(|a| a != "--label").nth(1).unwrap_or_default();
    let _ = std::fs::remove_dir_all(&dir);
    let opts = Options { cache_bytes: arg("--cache-mb", 256) << 20, preallocate_log: arg("--preallocate", 0) == 1, ..Options::default() };
    let db = Db::open(Arc::new(FsStorage::new(&dir).unwrap()), opts).unwrap();

    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let value = vec![0x5au8; value_len];
    let mut key = vec![0u8; key_len.max(8)];
    // Nanoseconds and faults of each commit, and when it began.
    let mut commits: Vec<(u64, u64, Duration)> = Vec::new();
    let start = Instant::now();
    let deadline = Duration::from_secs(secs);
    while start.elapsed() < deadline {
        if rate > 0 {
            // Sleep to within a tick of the commit's slot, then spin to it.
            let slot = Duration::from_secs_f64(commits.len() as f64 / rate as f64);
            if let Some(wait) = slot.checked_sub(start.elapsed() + Duration::from_micros(100)) {
                std::thread::sleep(wait);
            }
            while start.elapsed() < slot {
                std::hint::spin_loop();
            }
        }
        let batch: Vec<WriteOp> = (0..ops)
            .map(|_| {
                key[..8].copy_from_slice(&next().to_be_bytes());
                WriteOp::put(key.clone(), value.clone())
            })
            .collect();
        let began = start.elapsed();
        let (t, faults) = (Instant::now(), minor_faults());
        db.get(&batch[0].key).unwrap();
        db.write(&batch, true).unwrap();
        commits.push((t.elapsed().as_nanos() as u64, minor_faults() - faults, began));
    }
    let elapsed = start.elapsed().as_secs_f64();
    let stats = db.stats();

    let mut ns: Vec<u64> = commits.iter().map(|c| c.0).collect();
    ns.sort_unstable();
    let us = |n: u64| n as f64 / 1e3;
    let faults: u64 = commits.iter().map(|c| c.1).sum();
    // The first memtables fault their memory in; later ones show what a
    // commit costs once the store has warmed up.
    let late = &commits[commits.len() / 2..];
    let late_faults: u64 = late.iter().map(|c| c.1).sum();
    println!(
        "{{\"label\":\"{label}\",\"commits\":{},\"commits_per_sec\":{:.0},\"ops_per_sec\":{:.0},\"p50_us\":{:.0},\"p99_us\":{:.0},\"p999_us\":{:.0},\"p9999_us\":{:.0},\"max_us\":{:.0},\"over_1ms\":{},\"over_4ms\":{},\"faults_per_commit\":{:.2},\"faults_per_commit_second_half\":{:.2},\"checkpoints\":{},\"stall_secs\":{:.3}}}",
        ns.len(),
        ns.len() as f64 / elapsed,
        (ns.len() * ops) as f64 / elapsed,
        us(percentile(&ns, 0.50)),
        us(percentile(&ns, 0.99)),
        us(percentile(&ns, 0.999)),
        us(percentile(&ns, 0.9999)),
        us(*ns.last().unwrap()),
        ns.iter().filter(|&&n| n > 1_000_000).count(),
        ns.iter().filter(|&&n| n > 4_000_000).count(),
        faults as f64 / ns.len() as f64,
        late_faults as f64 / late.len() as f64,
        stats.checkpoints,
        stats.stage_secs[5],
    );
    let mut slowest = commits.clone();
    slowest.sort_unstable_by_key(|c| std::cmp::Reverse(c.0));
    for (n, faults, began) in slowest.iter().take(8) {
        eprintln!("slow commit: {:8.0} us at {:7.3} s, {faults} faults", us(*n), began.as_secs_f64());
    }
}
