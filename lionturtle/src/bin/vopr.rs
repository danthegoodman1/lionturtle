//! Runs VOPR simulations: `vopr [--seed N] [--steps N] [--seeds N] [--verbose]`.
//! Runs `--seeds` consecutive seeds starting at `--seed` (one seed by default)
//! or, without `--seed`, at a random one (100 seeds by default). Stops at the
//! first failure, printing the seed that reproduces it.

use lionturtle::sim::vopr;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).map(|v| v.parse::<u64>().expect("number"));
    let verbose = args.iter().any(|a| a == "--verbose");
    let steps = arg("--steps").unwrap_or(5_000);
    let (first, count) = match arg("--seed") {
        Some(seed) => (seed, arg("--seeds").unwrap_or(1)),
        None => {
            let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
            (t ^ (std::process::id() as u64) << 32, arg("--seeds").unwrap_or(100))
        }
    };
    let started = Instant::now();
    let mut total = vopr::Report::default();
    for seed in first..first + count {
        match vopr::run(seed, steps, verbose) {
            Ok(r) => {
                if verbose || count == 1 {
                    println!("seed {seed}: ok {r:?}");
                }
                total.steps += r.steps;
                total.crashes += r.crashes;
                total.writes += r.writes;
                total.reads += r.reads;
                total.scans += r.scans;
                total.snapshots += r.snapshots;
                total.iterators += r.iterators;
                total.read_errors += r.read_errors;
                total.max_height = total.max_height.max(r.max_height);
            }
            Err(e) => {
                println!("FAILED: {e}");
                println!("reproduce: cargo run --release --bin vopr -- --seed {seed} --steps {steps} --verbose");
                std::process::exit(1);
            }
        }
    }
    println!(
        "{count} seeds ok in {:.1}s: {} steps, {} writes, {} reads, {} scans, {} snapshots, {} iterators, {} crashes, {} read faults, max height {}",
        started.elapsed().as_secs_f64(),
        total.steps,
        total.writes,
        total.reads,
        total.scans,
        total.snapshots,
        total.iterators,
        total.crashes,
        total.read_errors,
        total.max_height
    );
}
