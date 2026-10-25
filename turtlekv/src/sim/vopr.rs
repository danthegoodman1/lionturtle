//! VOPR-style simulation: one seed drives a randomly configured store through
//! a random mix of reads, writes, checkpoints, crashes, and storage faults,
//! checking every answer against a model.
//!
//! Everything runs on one thread against [`SimStorage`], with checkpoints
//! driven explicitly, so a seed replays the same run exactly.
//!
//! What the model checks:
//! - every `get` and `scan` matches the model, unless it returns an error;
//! - after a crash, the store recovers exactly the state after some write
//!   `s` with `synced ≤ s ≤ last`, where `synced` is the newest write known
//!   durable: a prefix of history that keeps every synced write;
//! - after a clean reopen, nothing is lost;
//! - a long-lived iterator returns exactly the model's entries in its range
//!   when it was created, whatever is written while it runs;
//! - tree invariants hold, and no page slot leaks.

use super::{FaultConfig, Rng, SimStorage};
use crate::db::{Db, DbIter, Options};
use crate::error::Error;
use crate::memtable::MemTableKind;
use crate::tree::TreeConfig;
use crate::types::WriteOp;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub steps: u64,
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub crashes: u64,
    pub reopens: u64,
    pub checkpoints: u64,
    pub read_errors: u64,
    pub write_failures: u64,
    pub max_height: u32,
    pub final_keys: usize,
}

struct Undo {
    seq: u64,
    key: Vec<u8>,
    prev: Option<Vec<u8>>,
}

/// The expected contents of the store and enough history to rewind it.
#[derive(Default)]
struct Model {
    state: BTreeMap<Vec<u8>, Vec<u8>>,
    undo: Vec<Undo>,
    last_seq: u64,
    synced_seq: u64,
}

impl Model {
    fn apply(&mut self, ops: &[(Vec<u8>, Option<Vec<u8>>)]) {
        for (key, value) in ops {
            self.last_seq += 1;
            let prev = match value {
                Some(v) => self.state.insert(key.clone(), v.clone()),
                None => self.state.remove(key),
            };
            self.undo.push(Undo { seq: self.last_seq, key: key.clone(), prev });
        }
    }

    fn rewind_to(&mut self, seq: u64) {
        while let Some(u) = self.undo.last() {
            if u.seq <= seq {
                break;
            }
            let u = self.undo.pop().unwrap();
            match u.prev {
                Some(v) => self.state.insert(u.key, v),
                None => self.state.remove(&u.key),
            };
        }
        self.last_seq = seq;
    }

    fn mark_durable(&mut self, seq: u64) {
        self.synced_seq = self.synced_seq.max(seq);
        let keep_from = self.undo.partition_point(|u| u.seq <= self.synced_seq);
        self.undo.drain(..keep_from);
    }
}

struct Iter {
    it: DbIter,
    /// The model's entries in the iterator's range when it was created, which
    /// it must return in order.
    expected: VecDeque<(Vec<u8>, Vec<u8>)>,
}

struct Sim {
    rng: Rng,
    disk: SimStorage,
    opts: Options,
    db: Option<Db>,
    model: Model,
    keys: Vec<Vec<u8>>,
    max_value: usize,
    iters: Vec<Iter>,
    report: Report,
    verbose: bool,
}

fn random_options(rng: &mut Rng) -> Options {
    let leaf_size = 1u64 << rng.range(8, 14);
    let max_pivots = rng.range(4, 16) as usize;
    Options {
        tree: TreeConfig {
            leaf_size,
            max_pivots,
            min_pivots: rng.range(2, (max_pivots / 2) as u64) as usize,
            max_segments: rng.range(3, 24) as usize,
            max_levels: rng.range(1, 6) as usize,
            filter_bits: *rng.pick(&[0, 4, 10]),
            merge_readahead: rng.range(4096, 1 << 16),
            // Parallel merges run on a one-thread pool (see `run`), which
            // keeps them deterministic.
            parallel: rng.chance(0.5),
        },
        checkpoint_distance: rng.range(1, 8),
        cache_bytes: *rng.pick(&[0, 1 << 14, 1 << 20, 1 << 26]),
        memtable: *rng.pick(&[MemTableKind::SkipList, MemTableKind::BBTree, MemTableKind::BTree]),
        wal_buffer_bytes: *rng.pick(&[0, 512, 1 << 16]),
        max_frozen: rng.range(1, 3) as usize,
        background: false,
        merge_threads: 0,
    }
}

fn random_faults(rng: &mut Rng) -> FaultConfig {
    let mut f = FaultConfig { crash_keep_sector: *rng.pick(&[0.0, 0.5, 0.9, 1.0]), ..Default::default() };
    if rng.chance(0.5) {
        f.read_error = *rng.pick(&[0.001, 0.01]);
    }
    if rng.chance(0.3) {
        f.read_corruption = *rng.pick(&[0.001, 0.01]);
    }
    if rng.chance(0.3) {
        f.write_error = *rng.pick(&[0.0005, 0.002]);
        f.sync_error = *rng.pick(&[0.0005, 0.002]);
    }
    f
}

impl Sim {
    fn new(seed: u64, verbose: bool) -> Self {
        let mut rng = Rng::new(seed);
        let opts = random_options(&mut rng);
        let faults = random_faults(&mut rng);
        let disk = SimStorage::new(rng.next_u64(), faults);
        let nkeys = rng.range(8, 3000) as usize;
        let mut keys = BTreeSet::new();
        while keys.len() < nkeys {
            let len = rng.range(0, 24) as usize;
            let mut k = vec![0u8; len];
            // A small alphabet makes shared prefixes and near-collisions common.
            for b in &mut k {
                *b = b"aab\x00\xff"[rng.below(5) as usize];
            }
            if rng.chance(0.7) {
                k.extend_from_slice(format!("{:06}", rng.below(1_000_000)).as_bytes());
            }
            keys.insert(k);
        }
        let max_value = rng.range(0, (opts.tree.leaf_size / 4).max(1)) as usize;
        if verbose {
            eprintln!("options: {opts:?}\nfaults: {:?}\nkeys: {nkeys}, max value: {max_value}", disk.faults());
        }
        Self {
            rng,
            disk,
            opts,
            db: None,
            model: Model::default(),
            keys: keys.into_iter().collect(),
            max_value,
            iters: Vec::new(),
            report: Report::default(),
            verbose,
        }
    }

    fn db(&self) -> &Db {
        self.db.as_ref().expect("db open")
    }

    /// Opens the store, retrying through transient read faults.
    fn open(&mut self) -> Result<(), String> {
        let mut last_err = String::new();
        let faults = self.disk.faults();
        for attempt in 0..100 {
            // Recovery reads the whole tree's metadata, so at high fault rates
            // it rarely gets through; after some attempts under faults, let it.
            if attempt >= 20 {
                self.disk.set_faults(FaultConfig { read_error: 0.0, read_corruption: 0.0, ..faults.clone() });
            }
            let opened = Db::open(Arc::new(self.disk.clone()), self.opts.clone());
            if attempt >= 20 {
                self.disk.set_faults(faults.clone());
            }
            match opened {
                Ok(db) => {
                    self.db = Some(db);
                    return Ok(());
                }
                Err(e @ (Error::Io(_) | Error::Corruption(_))) => {
                    // A transient fault, or a write fault during open. Treat
                    // the open as a crash and try again.
                    last_err = e.to_string();
                    self.disk.crash();
                }
                Err(e) => return Err(format!("open failed: {e}")),
            }
        }
        Err(format!("open kept failing: {last_err}"))
    }

    fn key(&mut self) -> Vec<u8> {
        // Skew: a small hot set takes most operations.
        let n = self.keys.len() as u64;
        let i = if self.rng.chance(0.6) { self.rng.below(n.div_ceil(10)) } else { self.rng.below(n) };
        self.keys[i as usize].clone()
    }

    fn value(&mut self) -> Vec<u8> {
        let len = if self.rng.chance(0.1) { self.max_value } else { self.rng.range(0, self.max_value as u64) as usize };
        let mut v = vec![0u8; len];
        self.rng.fill(&mut v);
        v
    }

    fn write(&mut self, ops: Vec<(Vec<u8>, Option<Vec<u8>>)>, sync: bool) -> Result<(), String> {
        let wops: Vec<WriteOp> = ops
            .iter()
            .map(|(k, v)| match v {
                Some(v) => WriteOp::put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v)),
                None => WriteOp::delete(Bytes::copy_from_slice(k)),
            })
            .collect();
        let before = self.db().last_seq();
        let result = self.db().write(&wops, sync);
        let after = self.db().last_seq();
        match result {
            Ok(()) => {
                self.model.apply(&ops);
                if after != self.model.last_seq {
                    return Err(format!("sequence mismatch: db {after}, model {}", self.model.last_seq));
                }
                if sync {
                    self.model.mark_durable(after);
                }
                self.report.writes += 1;
                Ok(())
            }
            Err(_) => {
                // The batch may or may not have reached the log; recovery
                // decides. Record it if it consumed sequence numbers.
                self.report.write_failures += 1;
                if after == before + ops.len() as u64 {
                    self.model.apply(&ops);
                }
                self.crash_and_recover()
            }
        }
    }

    fn crash_and_recover(&mut self) -> Result<(), String> {
        self.iters.clear();
        self.db = None;
        self.disk.crash();
        self.report.crashes += 1;
        self.open()?;
        let recovered = self.db().last_seq();
        if recovered < self.model.synced_seq || recovered > self.model.last_seq {
            return Err(format!(
                "recovered to seq {recovered}, outside [{}, {}]",
                self.model.synced_seq, self.model.last_seq
            ));
        }
        if self.verbose {
            eprintln!(
                "crash: recovered seq {recovered} (synced {}, last {})",
                self.model.synced_seq, self.model.last_seq
            );
        }
        self.model.rewind_to(recovered);
        // What recovery read back is on the media now.
        self.model.mark_durable(recovered);
        self.verify_all()?;
        self.check_invariants()
    }

    fn clean_reopen(&mut self) -> Result<(), String> {
        self.iters.clear();
        let poisoned = self.db().is_poisoned();
        self.db = None;
        if poisoned {
            return self.crash_and_recover();
        }
        self.report.reopens += 1;
        self.open()?;
        let recovered = self.db().last_seq();
        if recovered != self.model.last_seq {
            // Closing syncs the log; a sync fault there is a crash.
            if recovered < self.model.synced_seq || recovered > self.model.last_seq {
                return Err(format!("clean reopen lost writes: seq {recovered}, synced {}, last {}", self.model.synced_seq, self.model.last_seq));
            }
            self.model.rewind_to(recovered);
        }
        self.model.mark_durable(recovered);
        self.verify_all()
    }

    fn check_invariants(&mut self) -> Result<(), String> {
        self.quietly(|s| s.check_invariants_inner())
    }

    fn check_invariants_inner(&mut self) -> Result<(), String> {
        if self.iters.is_empty() {
            let db = self.db.as_ref().unwrap();
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.check_invariants())) {
                Ok(Ok(())) => {}
                Ok(Err(Error::Io(_))) | Ok(Err(Error::Corruption(_))) => self.report.read_errors += 1,
                Ok(Err(e)) => return Err(format!("invariant check failed: {e}")),
                Err(p) => {
                    let msg = p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()));
                    return Err(format!("invariant violated: {}", msg.unwrap_or_default()));
                }
            }
            let h = self.db().stats().tree.height;
            self.report.max_height = self.report.max_height.max(h);
        }
        Ok(())
    }

    fn verify_get(&mut self, key: &[u8]) -> Result<(), String> {
        self.report.reads += 1;
        match self.db().get(key) {
            Ok(v) => {
                let expected = self.model.state.get(key);
                if v.as_deref() != expected.map(|v| &v[..]) {
                    return Err(format!(
                        "get {:?}: got {:?}, expected {:?}",
                        Bytes::copy_from_slice(key),
                        v.map(|v| v.len()),
                        expected.map(|v| v.len())
                    ));
                }
                Ok(())
            }
            Err(Error::Io(_)) | Err(Error::Corruption(_)) => {
                self.report.read_errors += 1;
                Ok(())
            }
            Err(e) => Err(format!("get failed: {e}")),
        }
    }

    fn verify_scan(&mut self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<(), String> {
        self.report.scans += 1;
        if self.rng.chance(0.5) {
            crate::tree::io::arm_prefetch();
        }
        let got = match self.db().scan(start, end, limit) {
            Ok(v) => v,
            Err(Error::Io(_)) | Err(Error::Corruption(_)) => {
                self.report.read_errors += 1;
                return Ok(());
            }
            Err(e) => return Err(format!("scan failed: {e}")),
        };
        let expected: Vec<(&Vec<u8>, &Vec<u8>)> = self
            .model
            .state
            .range(start.to_vec()..)
            .take_while(|(k, _)| end.is_none_or(|e| &k[..] < e))
            .take(limit)
            .collect();
        if got.len() != expected.len()
            || got.iter().zip(&expected).any(|((gk, gv), (ek, ev))| &gk[..] != &ek[..] || &gv[..] != &ev[..])
        {
            let first_diff = got.iter().zip(&expected).position(|((gk, gv), (ek, ev))| &gk[..] != &ek[..] || &gv[..] != &ev[..]);
            return Err(format!(
                "scan from {:?} limit {limit}: got {} entries, expected {}, first difference at {first_diff:?}",
                Bytes::copy_from_slice(start),
                got.len(),
                expected.len()
            ));
        }
        Ok(())
    }

    /// Runs `f` with read faults suspended, for checks that read everything.
    fn quietly<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        let faults = self.disk.faults();
        self.disk.set_faults(FaultConfig { read_error: 0.0, read_corruption: 0.0, ..faults.clone() });
        let out = f(self);
        self.disk.set_faults(faults);
        out
    }

    /// Compares the whole store with the model.
    fn verify_all(&mut self) -> Result<(), String> {
        self.quietly(|s| s.verify_all_inner())
    }

    fn verify_all_inner(&mut self) -> Result<(), String> {
        for _ in 0..100 {
            match self.db().scan(b"", None, usize::MAX) {
                Ok(got) => {
                    let ok = got.len() == self.model.state.len()
                        && got.iter().zip(&self.model.state).all(|((gk, gv), (ek, ev))| &gk[..] == &ek[..] && &gv[..] == &ev[..]);
                    if !ok {
                        let missing = self.model.state.keys().filter(|k| !got.iter().any(|(g, _)| &g[..] == &k[..])).count();
                        return Err(format!(
                            "full scan has {} entries, model has {} ({missing} model keys missing)",
                            got.len(),
                            self.model.state.len()
                        ));
                    }
                    return Ok(());
                }
                Err(Error::Io(_)) | Err(Error::Corruption(_)) => self.report.read_errors += 1,
                Err(e) => return Err(format!("scan failed: {e}")),
            }
        }
        Err("full scan kept failing".into())
    }

    /// Creates or advances a long-lived iterator, which must return the
    /// model's entries as they stood when it was created.
    fn step_iter(&mut self) -> Result<(), String> {
        if self.iters.is_empty() || (self.iters.len() < 3 && self.rng.chance(0.3)) {
            let start = self.key();
            let end = self.rng.chance(0.3).then(|| self.key()).filter(|e| *e > start);
            if self.rng.chance(0.5) {
                crate::tree::io::arm_prefetch();
            }
            let it = self.db().iter(&start, end.as_deref());
            let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
            let expected = self.model.state.range::<Vec<u8>, _>((Bound::Included(start), hi)).map(|(k, v)| (k.clone(), v.clone())).collect();
            self.iters.push(Iter { it, expected });
            return Ok(());
        }
        let i = self.rng.below(self.iters.len() as u64) as usize;
        for _ in 0..self.rng.range(1, 20) {
            let it = &mut self.iters[i];
            let next = match it.it.next() {
                Ok(n) => n,
                Err(Error::Io(_)) | Err(Error::Corruption(_)) => {
                    self.report.read_errors += 1;
                    self.iters.remove(i);
                    return Ok(());
                }
                Err(e) => return Err(format!("iterator failed: {e}")),
            };
            let want = it.expected.pop_front();
            match (next, want) {
                (None, None) => {
                    self.iters.remove(i);
                    return Ok(());
                }
                (Some((k, v)), Some((wk, wv))) if k[..] == wk[..] && v[..] == wv[..] => {}
                (got, want) => {
                    let show = |e: Option<(&[u8], usize)>| e.map(|(k, n)| format!("{:?} ({n} value bytes)", Bytes::copy_from_slice(k)));
                    return Err(format!(
                        "iterator returned {:?}, expected {:?} from its snapshot",
                        show(got.as_ref().map(|(k, v)| (&k[..], v.len()))),
                        show(want.as_ref().map(|(k, v)| (&k[..], v.len())))
                    ));
                }
            }
        }
        Ok(())
    }

    fn step(&mut self) -> Result<(), String> {
        self.report.steps += 1;
        let r = self.rng.below(1000);
        match r {
            0..300 => {
                let (k, v) = (self.key(), self.value());
                let sync = self.rng.chance(0.05);
                self.write(vec![(k, Some(v))], sync)
            }
            300..380 => {
                let k = self.key();
                let sync = self.rng.chance(0.05);
                self.write(vec![(k, None)], sync)
            }
            380..430 => {
                let n = self.rng.range(2, 10);
                let mut ops = BTreeMap::new();
                for _ in 0..n {
                    let k = self.key();
                    let v = self.rng.chance(0.8).then(|| self.value());
                    ops.insert(k, v);
                }
                let sync = self.rng.chance(0.2);
                self.write(ops.into_iter().collect(), sync)
            }
            430..680 => {
                let k = if self.rng.chance(0.9) { self.key() } else { b"missing-key".to_vec() };
                self.verify_get(&k)
            }
            680..740 => {
                let start = self.key();
                let end = self.rng.chance(0.3).then(|| self.key());
                let limit = self.rng.range(0, 60) as usize;
                self.verify_scan(&start, end.as_deref(), limit)
            }
            740..770 => {
                let r = self.db().sync();
                match r {
                    Ok(()) => {
                        let seq = self.db().last_seq();
                        self.model.mark_durable(seq);
                        Ok(())
                    }
                    Err(_) => self.crash_and_recover(),
                }
            }
            770..850 => {
                self.report.checkpoints += 1;
                match self.db().checkpoint_step() {
                    Ok(_) => Ok(()),
                    Err(_) => self.crash_and_recover(),
                }
            }
            850..870 => match self.db().freeze() {
                Ok(()) => Ok(()),
                Err(_) => self.crash_and_recover(),
            },
            870..880 => match self.db().checkpoint() {
                Ok(()) => Ok(()),
                Err(_) => self.crash_and_recover(),
            },
            880..895 => self.crash_and_recover(),
            895..900 => self.clean_reopen(),
            900..960 => self.step_iter(),
            960..980 => self.check_invariants(),
            _ => {
                // Arm a crash a few storage operations ahead, so it lands
                // inside whatever the next steps do.
                let ops = self.rng.range(0, 40);
                self.disk.arm_crash(ops);
                Ok(())
            }
        }
    }
}

/// Runs one seed for `steps` steps.
pub fn run(seed: u64, steps: u64, verbose: bool) -> Result<Report, String> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(1).build().expect("thread pool");
    pool.install(|| run_inner(seed, steps, verbose))
}

fn run_inner(seed: u64, steps: u64, verbose: bool) -> Result<Report, String> {
    let mut sim = Sim::new(seed, verbose);
    sim.open()?;
    for i in 0..steps {
        if let Err(e) = sim.step() {
            return Err(format!("seed {seed} step {i}: {e}"));
        }
        // An armed crash that has fired leaves the disk down.
        if sim.disk.is_down() {
            sim.crash_and_recover().map_err(|e| format!("seed {seed} step {i}: {e}"))?;
        }
    }
    sim.iters.clear();
    sim.disk.disarm_crash();
    sim.check_invariants().map_err(|e| format!("seed {seed} final: {e}"))?;
    sim.verify_all().map_err(|e| format!("seed {seed} final: {e}"))?;
    sim.report.final_keys = sim.model.state.len();
    Ok(sim.report)
}
