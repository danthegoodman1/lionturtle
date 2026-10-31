//! YCSB-style benchmark driver for TurtleKV (Rust) and fjall.
//!
//! Runs a sequence of phases against one store, like the TurtleKV paper's
//! evaluation: `load` inserts `--records` keys in scrambled order, then each
//! of `a b c e f` runs `--ops` operations with scrambled-Zipfian key choice.
//! Prints one JSON object per phase with throughput, per-operation latency
//! percentiles, and the bytes the process read from and wrote to storage.
//!
//! With `--rate`, client threads issue operations on a fixed schedule rather
//! than back to back, and each latency counts from the operation's scheduled
//! start, so time spent queued behind a stalled operation counts too. A rate
//! of `10000:20,40000:10` cycles through 10,000 operations per second for 20
//! seconds and 40,000 for 10.
//! `--timeline <file>` writes each second's throughput and latency
//! percentiles there.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod engines;
mod ycsb;

use engines::Engine;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::io::Write;
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
    /// Client threads for the load phase; RocksDB loads fastest with fewer.
    pub load_threads: usize,
    pub value_size: usize,
    pub cache_mb: u64,
    pub leaf_kb: u64,
    pub chi: u64,
    pub memtable: String,
    pub max_frozen: usize,
    pub merge_threads: usize,
    pub wal_buffer_kb: usize,
    pub fjall_workers: usize,
    pub fjall_manual_persist: bool,
    pub max_levels: usize,
    pub filter_bits: u32,
    pub label: String,
    /// Leave the data directory in place after the run.
    pub keep: bool,
    /// Finish background work after the load, outside any timed phase.
    pub settle: bool,
    /// Make every write in the timed phases durable before it returns. The
    /// load stays buffered.
    pub sync: bool,
    /// RocksDB reuses retired log files, as lionturtle always does.
    pub rocksdb_recycle_logs: bool,
    /// Upper bound in MB/s for RocksDB's auto-tuned limiter on flush and
    /// compaction writes; 0 leaves it off.
    pub rocksdb_rate_mb: u64,
    /// ScyllaDB node the scylla engine connects to.
    pub scylla_host: String,
    /// Operations per second across all client threads in the timed phases,
    /// as a cycle of (rate, seconds) segments; empty issues them back to back.
    pub rate: Vec<(f64, f64)>,
    /// Pace lionturtle checkpoints.
    pub pace: bool,
    /// Zero-fill lionturtle log segments ahead of their writes.
    pub preallocate_log: bool,
    /// Choose keys uniformly instead of by scrambled Zipfian.
    pub uniform: bool,
    pub timeline: Option<String>,
}

impl Args {
    /// Memtable size for every engine: lionturtle's checkpoint distance in
    /// leaf-sized batches.
    pub fn memtable_bytes(&self) -> u64 {
        self.chi * (self.leaf_kb << 10)
    }
}

/// Parses `rate[:seconds],...`; a lone rate holds for the whole phase.
fn parse_rate(s: &str) -> Vec<(f64, f64)> {
    s.split(',')
        .map(|seg| match seg.split_once(':') {
            Some((r, secs)) => (r.parse().expect("--rate"), secs.parse().expect("--rate")),
            None => (seg.parse().expect("--rate"), f64::INFINITY),
        })
        .filter(|(r, _)| *r > 0.0)
        .collect()
}

/// The rate `elapsed` into a cycling schedule.
fn rate_at(schedule: &[(f64, f64)], elapsed: f64) -> f64 {
    let cycle: f64 = schedule.iter().map(|(_, secs)| secs).sum();
    let mut t = if cycle.is_finite() { elapsed % cycle } else { elapsed };
    for &(rate, secs) in schedule {
        if t < secs {
            return rate;
        }
        t -= secs;
    }
    schedule[0].0
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let get = |name: &str| argv.iter().position(|a| a == name).and_then(|i| argv.get(i + 1)).cloned();
    let num = |name: &str, default: u64| get(name).map(|v| v.parse().expect(name)).unwrap_or(default);
    let threads = std::thread::available_parallelism().map_or(8, |n| n.get());
    Args {
        engine: get("--engine").unwrap_or_else(|| "lionturtle".into()),
        dir: get("--dir").unwrap_or_else(|| "bench-data".into()),
        phases: get("--phases").unwrap_or_else(|| "load,a,b,c,e,f".into()).split(',').map(String::from).collect(),
        records: num("--records", 10_000_000),
        ops: num("--ops", 5_000_000),
        threads: num("--threads", threads as u64) as usize,
        load_threads: num("--load-threads", num("--threads", threads as u64)) as usize,
        value_size: num("--value-size", 120) as usize,
        cache_mb: num("--cache-mb", 1024),
        leaf_kb: num("--leaf-kb", 4096),
        chi: num("--chi", 32),
        memtable: get("--memtable").unwrap_or_else(|| "bbtree".into()),
        max_frozen: num("--max-frozen", 2) as usize,
        merge_threads: num("--merge-threads", 8) as usize,
        wal_buffer_kb: num("--wal-buffer-kb", 1024) as usize,
        fjall_workers: num("--fjall-workers", 8) as usize,
        fjall_manual_persist: num("--fjall-manual-persist", 1) == 1,
        max_levels: num("--max-levels", 4) as usize,
        filter_bits: num("--filter-bits", 12) as u32,
        label: get("--label").unwrap_or_default(),
        keep: num("--keep", 0) == 1,
        settle: num("--settle", 0) == 1,
        sync: num("--sync", 0) == 1,
        rocksdb_recycle_logs: num("--rocksdb-recycle-logs", 0) == 1,
        rocksdb_rate_mb: num("--rocksdb-rate-mb", 0),
        scylla_host: get("--scylla-host").unwrap_or_else(|| "127.0.0.1:9042".into()),
        rate: get("--rate").map_or_else(Vec::new, |r| parse_rate(&r)),
        pace: num("--pace", 1) == 1,
        preallocate_log: num("--preallocate-log", 0) == 1,
        uniform: num("--uniform", 0) == 1,
        timeline: get("--timeline"),
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
    let field = |name: &str| s.lines().find_map(|l| l.strip_prefix(name)).and_then(|v| v.trim().parse::<u64>().ok()).unwrap_or(0);
    (field("read_bytes:"), field("write_bytes:"))
}

/// A memory figure from /proc/self/status: `VmRSS:` now, `VmHWM:` the peak
/// so far.
fn status_bytes(field: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find_map(|l| l.strip_prefix(field))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .map_or(0, |kb| kb * 1024)
}

/// Peak memory of this process's cgroup, page cache included, if limited.
fn cgroup_peak_bytes() -> u64 {
    let cg = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let path = cg.lines().find_map(|l| l.strip_prefix("0::")).unwrap_or("");
    std::fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.peak")).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0)
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

/// Latencies by second of the phase, merged from every client thread.
struct Timeline {
    secs: Mutex<Vec<Vec<Histogram<u64>>>>,
    /// The engine's cumulative write-stall seconds, sampled once a second.
    stalls: Mutex<Vec<f64>>,
}

impl Timeline {
    fn new() -> Self {
        Self { secs: Mutex::new(Vec::new()), stalls: Mutex::new(Vec::new()) }
    }

    fn add(&self, sec: usize, hist: &[Histogram<u64>]) {
        let mut secs = self.secs.lock();
        while secs.len() <= sec {
            secs.push(OPS.iter().map(|_| Histogram::new_with_bounds(1, 60_000_000_000, 2).unwrap()).collect());
        }
        for (a, b) in secs[sec].iter_mut().zip(hist) {
            a.add(b).unwrap();
        }
    }

    fn write(&self, path: &str, args: &Args, phase: &str) {
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
        let stalls = self.stalls.lock();
        for (sec, hists) in self.secs.lock().iter().enumerate() {
            let ops: u64 = hists.iter().map(|h| h.len()).sum();
            let stall = stalls.get(sec + 1).zip(stalls.get(sec)).map_or(0.0, |(b, a)| b - a);
            let mut line = format!(
                r#"{{"engine":"{}","label":"{}","phase":"{}","sec":{},"kops":{:.1},"stall_s":{:.3}"#,
                args.engine,
                args.label,
                phase,
                sec,
                ops as f64 / 1e3,
                stall
            );
            for (op, h) in OPS.iter().zip(hists) {
                if !h.is_empty() {
                    line.push_str(&format!(
                        r#","{}":{{"p50_us":{:.1},"p99_us":{:.1},"max_us":{:.1}}}"#,
                        op_name(*op),
                        h.value_at_quantile(0.5) as f64 / 1e3,
                        h.value_at_quantile(0.99) as f64 / 1e3,
                        h.max() as f64 / 1e3
                    ));
                }
            }
            writeln!(f, "{line}}}").unwrap();
        }
    }
}

fn run_load(engine: &Arc<dyn Engine>, args: &Args) -> PhaseResult {
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let handles: Vec<_> = (0..args.load_threads)
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
                        engine.put(&key_of(i), &value, false);
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

unsafe extern "C" {
    fn prctl(option: i32, ...) -> i32;
}

fn run_phase(engine: &Arc<dyn Engine>, args: &Args, phase: &str, records: &Arc<AtomicU64>, timeline: &Arc<Timeline>) -> PhaseResult {
    let mix = mix(phase);
    let n = records.load(Ordering::SeqCst);
    let zipf = Arc::new(Zipfian::new(n, 0.99));
    // Scans touch up to 100 entries each; like the paper, run a tenth as many.
    let ops = if phase == "e" { args.ops / 10 } else { args.ops };
    let per_thread = ops / args.threads as u64;
    let started = Instant::now();
    let scanned = Arc::new(AtomicU64::new(0));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (engine, timeline, done) = (engine.clone(), timeline.clone(), done.clone());
        std::thread::spawn(move || {
            let mut next = started;
            while !done.load(Ordering::Relaxed) {
                timeline.stalls.lock().push(engine.stall_secs());
                next += Duration::from_secs(1);
                while !done.load(Ordering::Relaxed) && Instant::now() < next {
                    std::thread::park_timeout(next.saturating_duration_since(Instant::now()));
                }
            }
        })
    };
    let handles: Vec<_> = (0..args.threads)
        .map(|t| {
            let (engine, zipf, records, scanned) = (engine.clone(), zipf.clone(), records.clone(), scanned.clone());
            let timeline = timeline.clone();
            let mix = mix.clone();
            let (vsize, sync, uniform, threads) = (args.value_size, args.sync, args.uniform, args.threads as f64);
            let schedule = args.rate.clone();
            let seed = phase.as_bytes()[0] as u64 * 1000 + t as u64;
            std::thread::spawn(move || {
                // Wake from sleeps within microseconds of the schedule:
                // PR_SET_TIMERSLACK to 1 ns, from the default 50 µs.
                // SAFETY: prctl with these arguments changes only this
                // thread's timer slack.
                unsafe { prctl(29, 1u64) };
                let mut hist = new_hists();
                let mut sec_hist = new_hists();
                let mut sec = 0usize;
                let mut rng = Rng::new(seed);
                let mut value = vec![0u8; vsize];
                let mut local_scanned = 0u64;
                // Each thread's operations start `threads / rate` apart,
                // offset from the other threads' by `t / rate`.
                let mut next = started + schedule.first().map_or(Duration::ZERO, |&(rate, _)| Duration::from_secs_f64(t as f64 / rate));
                for _ in 0..per_thread {
                    let r = rng.unit();
                    let mut acc = 0.0;
                    let op = mix
                        .iter()
                        .find(|(_, p)| {
                            acc += p;
                            r < acc
                        })
                        .map_or(mix[0].0, |(op, _)| *op);
                    let n = records.load(Ordering::Relaxed).max(1);
                    let item = if uniform { rng.below(n) } else { zipf.next_scrambled(&mut rng) % n };
                    let t0 = if schedule.is_empty() {
                        Instant::now()
                    } else {
                        std::thread::sleep(next.saturating_duration_since(Instant::now()));
                        let t0 = next;
                        next += Duration::from_secs_f64(threads / rate_at(&schedule, (next - started).as_secs_f64()));
                        t0
                    };
                    match op {
                        Op::Read => {
                            engine.get(&key_of(item));
                        }
                        Op::Update => {
                            rng.fill(&mut value);
                            engine.put(&key_of(item), &value, sync);
                        }
                        Op::Insert => {
                            let id = records.fetch_add(1, Ordering::Relaxed);
                            rng.fill(&mut value);
                            engine.put(&key_of(id), &value, sync);
                        }
                        Op::Scan => {
                            let len = 1 + rng.below(100) as usize;
                            local_scanned += engine.scan(&key_of(item), len) as u64;
                        }
                        Op::Rmw => {
                            engine.get(&key_of(item));
                            rng.fill(&mut value);
                            engine.put(&key_of(item), &value, sync);
                        }
                    }
                    let idx = OPS.iter().position(|o| *o == op).unwrap();
                    let end = Instant::now();
                    let ns = end.saturating_duration_since(t0).as_nanos() as u64;
                    hist[idx].record(ns).ok();
                    let s = end.duration_since(started).as_secs() as usize;
                    if s != sec {
                        timeline.add(sec, &sec_hist);
                        sec_hist.iter_mut().for_each(Histogram::reset);
                        sec = s;
                    }
                    sec_hist[idx].record(ns).ok();
                }
                timeline.add(sec, &sec_hist);
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
    let elapsed = started.elapsed();
    done.store(true, Ordering::Relaxed);
    sampler.thread().unpark();
    sampler.join().unwrap();
    timeline.stalls.lock().push(engine.stall_secs());
    PhaseResult { ops: per_thread * args.threads as u64, elapsed, hist, scanned: scanned.load(Ordering::Relaxed) }
}

fn main() {
    let args = parse_args();
    // Without a load phase, the run reuses the data a kept run left behind.
    if args.phases.iter().any(|p| p == "load") {
        let _ = std::fs::remove_dir_all(&args.dir);
    }
    std::fs::create_dir_all(&args.dir).unwrap();
    let engine: Arc<dyn Engine> = engines::open(&args);
    let records = Arc::new(AtomicU64::new(0));
    let user_entry = 8 + args.value_size as u64;
    for phase in args.phases.clone() {
        let (r0, w0) = io_counters();
        let result = if phase == "load" {
            let r = run_load(&engine, &args);
            records.store(args.records, Ordering::SeqCst);
            if args.settle {
                engine.settle();
            }
            r
        } else {
            if records.load(Ordering::SeqCst) == 0 {
                records.store(args.records, Ordering::SeqCst);
            }
            let timeline = Arc::new(Timeline::new());
            let r = run_phase(&engine, &args, &phase, &records, &timeline);
            if let Some(path) = &args.timeline {
                timeline.write(path, &args, &phase);
            }
            r
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
            r#"{{"engine":"{}","label":"{}","phase":"{}","threads":{},"records":{},"ops":{},"secs":{:.3},"kops":{:.1},"read_mb":{:.1},"write_mb":{:.1},"waf":{:.2},"scanned":{},"rss_mb":{:.0},"peak_rss_mb":{:.0},"cgroup_peak_mb":{:.0},"disk_mb":{:.0},"engine_stats":{}{}}}"#,
            args.engine,
            args.label,
            phase,
            if phase == "load" { args.load_threads } else { args.threads },
            records.load(Ordering::SeqCst),
            result.ops,
            secs,
            result.ops as f64 / secs / 1e3,
            (r1 - r0) as f64 / 1e6,
            (w1 - w0) as f64 / 1e6,
            if written_user == 0 { 0.0 } else { (w1 - w0) as f64 / (written_user * user_entry) as f64 },
            result.scanned,
            status_bytes("VmRSS:") as f64 / 1e6,
            status_bytes("VmHWM:") as f64 / 1e6,
            cgroup_peak_bytes() as f64 / 1e6,
            dir_bytes(std::path::Path::new(&args.dir)) as f64 / 1e6,
            engine.stats_json(),
            lat
        );
    }
    drop(engine);
    if !args.keep {
        let _ = std::fs::remove_dir_all(&args.dir);
    }
}
