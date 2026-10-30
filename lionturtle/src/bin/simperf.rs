//! Deterministic write/read amplification sweep over checkpoint distance χ
//! and data set size, on the simulated disk. Same arguments, same numbers.
//! Usage: simperf [records,records,...] [chi,chi,...] [leaf_kb]

use lionturtle::sim::perf::{PerfConfig, run};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let list = |i: usize, default: &str| -> Vec<u64> {
        args.get(i).map_or(default, |s| s.as_str()).split(',').map(|v| v.parse().unwrap()).collect()
    };
    let records = list(1, "250000,500000,1000000");
    let chis = list(2, "1,2,4,8,16,32,64");
    let leaf_kb = args.get(3).map_or(64, |s| s.parse().unwrap());
    println!("records,chi,leaf_kb,checkpoints,height,waf_total,waf_pages,wal_mb,page_mb,disk_mb,reads_per_get,read_kb_per_get");
    for &n in &records {
        for &chi in &chis {
            let cfg =
                PerfConfig { records: n, value_size: 120, leaf_size: leaf_kb << 10, chi, cache_bytes: 1 << 20, gets: 20_000, seed: 42 };
            let r = run(&cfg);
            println!(
                "{n},{chi},{leaf_kb},{},{},{:.3},{:.3},{:.1},{:.1},{:.1},{:.3},{:.2}",
                r.checkpoints,
                r.height,
                r.waf,
                r.page_waf,
                r.wal_bytes as f64 / 1e6,
                r.page_bytes as f64 / 1e6,
                r.disk_bytes as f64 / 1e6,
                r.reads_per_get,
                r.read_bytes_per_get / 1024.0,
            );
        }
    }
}
