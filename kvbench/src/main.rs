//! YCSB-style benchmark driver for TurtleKV (Rust) and fjall.
//!
//! Runs a sequence of phases against one store, like the TurtleKV paper's
//! evaluation: `load` inserts `--records` keys in scrambled order, then each
//! of `a b c e f` runs `--ops` operations with scrambled-Zipfian key choice.
//! Prints one JSON object per phase with throughput, per-operation latency
//! percentiles, and the bytes the process read from and wrote to storage.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod engines;
mod ycsb;

use engines::Engine;
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use ycsb::{Rng, Zipfian, key_of};

pub struct Args {
    pub engine: String,
    pub dir: String,
    pub phases: Vec<String>,
    pub records: u64,
    pub ops: u64,
    pub threads: usize,
    pub value_size: usize,
    pub cache_mb: u64,
    pub leaf_kb: u64,
    pub chi: u64,
    pub memtable: String,
    pub max_frozen: usize,
    pub wal_buffer_kb: usize,
    pub fjall_workers: usize,
    pub fjall_memtable_mb: u64,
    pub fjall_manual_persist: bool,
    pub max_levels: usize,
    pub filter_bits: u32,
    pub label: String,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let get = |name: &str| argv.iter().position(|a| a == name).and_then(|i| argv.get(i + 1)).cloned();
    let num = |name: &str, default: u64| get(name).map(|v| v.parse().expect(name)).unwrap_or(default);
    let threads = std::thread::available_parallelism().map_or(8, |n| n.get());
    Args {
        engine: get("--engine").unwrap_or_else(|| "turtlekv".into()),
        dir: get("--dir").unwrap_or_else(|| "bench-data".into()),
        phases: get("--phases").unwrap_or_else(|| "load,a,b,c,e,f".into()).split(',').map(String::from).collect(),
        records: num("--records", 10_000_000),
        ops: num("--ops", 5_000_000),
        threads: num("--threads", threads as u64) as usize,
        value_size: num("--value-size", 120) as usize,
        cache_mb: num("--cache-mb", 1024),
        leaf_kb: num("--leaf-kb", 4096),
        chi: num("--chi", 32),
        memtable: get("--memtable").unwrap_or_else(|| "bbtree".into()),
        max_frozen: num("--max-frozen", 2) as usize,
        wal_buffer_kb: num("--wal-buffer-kb", 1024) as usize,
        fjall_workers: num("--fjall-workers", 8) as usize,
        fjall_memtable_mb: num("--fjall-memtable-mb", 64),
        fjall_manual_persist: num("--fjall-manual-persist", 1) == 1,
        max_levels: num("--max-levels", 4) as usize,
        filter_bits: num("--filter-bits", 12) as u32,
        label: get("--label").unwrap_or_default(),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Read,
    Update,
    Insert,
    Scan,
    Rmw,
}

const OPS: [Op; 5] = [Op::Read, Op::Update, Op::Insert, Op::Scan, Op::Rmw];

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Read => "read",
        Op::Update => "update",
        Op::Insert => "insert",
        Op::Scan => "scan",
        Op::Rmw => "rmw",
    }
}

/// Fraction of each operation in a YCSB workload.
fn mix(phase: &str) -> Vec<(Op, f64)> {
    match phase {
        "a" => vec![(Op::Read, 0.5), (Op::Update, 0.5)],
        "b" => vec![(Op::Read, 0.95), (Op::Update, 0.05)],
        "c" => vec![(Op::Read, 1.0)],
        "e" => vec![(Op::Scan, 0.95), (Op::Insert, 0.05)],
        "f" => vec![(Op::Read, 0.5), (Op::Rmw, 0.5)],
        "w" => vec![(Op::Update, 1.0)],
        _ => panic!("unknown phase {phase}"),
    }
}

fn io_counters() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    let field = |name: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    (field("read_bytes:"), field("write_bytes:"))
}

fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .map_or(0, |kb| kb * 1024)
}

/// Peak memory of this process's cgroup, page cache included, if limited.
fn cgroup_peak_bytes() -> u64 {
    let cg = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let path = cg.lines().find_map(|l| l.strip_prefix("0::")).unwrap_or("");
    std::fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.peak"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let Ok(meta) = e.metadata() else { continue };
            if meta.is_dir() {
                total += dir_bytes(&e.path());
            } else {
                use std::os::unix::fs::MetadataExt;
                total += meta.blocks() * 512;
            }
        }
    }
    total
}

struct PhaseResult {
    ops: u64,
    elapsed: Duration,
    hist: Vec<Histogram<u64>>,
    scanned: u64,
}

fn new_hists() -> Vec<Histogram<u64>> {
    OPS.iter().map(|_| Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()).collect()
}

fn run_load(engine: &Arc<dyn Engine>, args: &Args) -> PhaseResult {
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let handles: Vec<_> = (0..args.threads)
        .map(|t| {
            let (engine, next) = (engine.clone(), next.clone());
            let (records, vsize) = (args.records, args.value_size);
            std::thread::spawn(move || {
                let mut hist = new_hists();
                let mut rng = Rng::new(0xC0FFEE + t as u64);
                let mut value = vec![0u8; vsize];
                loop {
                    let start = next.fetch_add(1024, Ordering::Relaxed);
                    if start >= records {
                        break;
                    }
                    for i in start..(start + 1024).min(records) {
                        rng.fill(&mut value);
                        let t0 = Instant::now();
                        engine.put(&key_of(i), &value);
                        hist[2].record(t0.elapsed().as_nanos() as u64).ok();
                    }
                }
                hist
            })
        })
        .collect();
    let mut hist = new_hists();
    for h in handles {
        for (a, b) in hist.iter_mut().zip(h.join().unwrap()) {
            a.add(b).unwrap();
        }
    }
    PhaseResult { ops: args.records, elapsed: started.elapsed(), hist, scanned: 0 }
}

fn run_phase(engine: &Arc<dyn Engine>, args: &Args, phase: &str, records: &Arc<AtomicU64>) -> PhaseResult {
    let mix = mix(phase);
    let n = records.load(Ordering::SeqCst);
    let zipf = Arc::new(Zipfian::new(n, 0.99));
    // Scans touch up to 100 entries each; like the paper, run a tenth as many.
    let ops = if phase == "e" { args.ops / 10 } else { args.ops };
    let per_thread = ops / args.threads as u64;
    let started = Instant::now();
    let scanned = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..args.threads)
        .map(|t| {
            let (engine, zipf, records, scanned) = (engine.clone(), zipf.clone(), records.clone(), scanned.clone());
            let mix = mix.clone();
            let vsize = args.value_size;
            let seed = phase.as_bytes()[0] as u64 * 1000 + t as u64;
            std::thread::spawn(move || {
                let mut hist = new_hists();
                let mut rng = Rng::new(seed);
                let mut value = vec![0u8; vsize];
                let mut local_scanned = 0u64;
                for _ in 0..per_thread {
                    let r = rng.unit();
                    let mut acc = 0.0;
                    let op = mix.iter().find(|(_, p)| { acc += p; r < acc }).map_or(mix[0].0, |(op, _)| *op);
                    let item = zipf.next_scrambled(&mut rng) % records.load(Ordering::Relaxed).max(1);
                    let t0 = Instant::now();
                    match op {
                        Op::Read => {
                            engine.get(&key_of(item));
                        }
                        Op::Update => {
                            rng.fill(&mut value);
                            engine.put(&key_of(item), &value);
                        }
                        Op::Insert => {
                            let id = records.fetch_add(1, Ordering::Relaxed);
                            rng.fill(&mut value);
                            engine.put(&key_of(id), &value);
                        }
                        Op::Scan => {
                            let len = 1 + rng.below(100) as usize;
                            local_scanned += engine.scan(&key_of(item), len) as u64;
                        }
                        Op::Rmw => {
                            engine.get(&key_of(item));
                            rng.fill(&mut value);
                            engine.put(&key_of(item), &value);
                        }
                    }
                    let idx = OPS.iter().position(|o| *o == op).unwrap();
                    hist[idx].record(t0.elapsed().as_nanos() as u64).ok();
                }
                scanned.fetch_add(local_scanned, Ordering::Relaxed);
                hist
            })
        })
        .collect();
    let mut hist = new_hists();
    for h in handles {
        for (a, b) in hist.iter_mut().zip(h.join().unwrap()) {
            a.add(b).unwrap();
        }
    }
    PhaseResult { ops: per_thread * args.threads as u64, elapsed: started.elapsed(), hist, scanned: scanned.load(Ordering::Relaxed) }
}

fn main() {
    let args = parse_args();
    let _ = std::fs::remove_dir_all(&args.dir);
    std::fs::create_dir_all(&args.dir).unwrap();
    let engine: Arc<dyn Engine> = engines::open(&args);
    let records = Arc::new(AtomicU64::new(0));
    let user_entry = 8 + args.value_size as u64;
    for phase in args.phases.clone() {
        let (r0, w0) = io_counters();
        let result = if phase == "load" {
            let r = run_load(&engine, &args);
            records.store(args.records, Ordering::SeqCst);
            r
        } else {
            if records.load(Ordering::SeqCst) == 0 {
                records.store(args.records, Ordering::SeqCst);
            }
            run_phase(&engine, &args, &phase, &records)
        };
        let (r1, w1) = io_counters();
        let secs = result.elapsed.as_secs_f64();
        let mut lat = String::new();
        for (op, h) in OPS.iter().zip(&result.hist) {
            if h.is_empty() {
                continue;
            }
            lat.push_str(&format!(
                r#","{}":{{"count":{},"mean_us":{:.2},"p50_us":{:.2},"p99_us":{:.2},"p999_us":{:.2},"max_us":{:.1}}}"#,
                op_name(*op),
                h.len(),
                h.mean() / 1e3,
                h.value_at_quantile(0.5) as f64 / 1e3,
                h.value_at_quantile(0.99) as f64 / 1e3,
                h.value_at_quantile(0.999) as f64 / 1e3,
                h.max() as f64 / 1e3
            ));
        }
        let written_user: u64 = result.hist[1].len() + result.hist[2].len() + result.hist[4].len();
        println!(
            r#"{{"engine":"{}","label":"{}","phase":"{}","threads":{},"records":{},"ops":{},"secs":{:.3},"kops":{:.1},"read_mb":{:.1},"write_mb":{:.1},"waf":{:.2},"scanned":{},"rss_mb":{:.0},"cgroup_peak_mb":{:.0},"disk_mb":{:.0},"engine_stats":{}{}}}"#,
            args.engine,
            args.label,
            phase,
            args.threads,
            records.load(Ordering::SeqCst),
            result.ops,
            secs,
            result.ops as f64 / secs / 1e3,
            (r1 - r0) as f64 / 1e6,
            (w1 - w0) as f64 / 1e6,
            if written_user == 0 { 0.0 } else { (w1 - w0) as f64 / (written_user * user_entry) as f64 },
            result.scanned,
            rss_bytes() as f64 / 1e6,
            cgroup_peak_bytes() as f64 / 1e6,
            dir_bytes(std::path::Path::new(&args.dir)) as f64 / 1e6,
            engine.stats_json(),
            lat
        );
    }
    drop(engine);
    let _ = std::fs::remove_dir_all(&args.dir);
}
