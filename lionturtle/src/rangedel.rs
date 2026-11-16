//! Range deletes held in a memtable.
//!
//! A memtable keeps its range deletes beside its point versions, each tagged
//! with its sequence number and its batch's ticket, and a read asks for the
//! newest one it sees that covers a key. Deletes live in immutable runs, after
//! Bentley and Saxe: an insert adds a run of one, and runs of equal size merge
//! as in a binary counter, so a store with `n` deletes in a memtable holds at
//! most one run per bit of `n`, and each delete is rebuilt into a larger run
//! at most once per bit. Each run cuts its key space into fragments of
//! constant cover and records the newest delete of each, so a read that sees
//! that delete costs one binary search per run. A reader that cannot see it,
//! because it is uncommitted or newer than the reader's snapshot, searches
//! the run's deletes for the newest it can see.
//!
//! The tree never holds a range delete: a checkpoint applies its memtable's
//! deletes to the tree before the memtable's other entries; see
//! [`crate::tree::delete_ranges`].

use crate::memtable::View;
use crate::tree::ranges::KeyRanges;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// One range delete: every key in `[start, end)`, at sequence number `seq`,
/// written by batch `ticket`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeDelete {
    pub start: Bytes,
    /// Exclusive; `None` for the end of the key space.
    pub end: Option<Bytes>,
    pub seq: u64,
    pub ticket: u64,
}

impl RangeDelete {
    pub fn covers(&self, key: &[u8]) -> bool {
        &self.start[..] <= key && below(key, self.end.as_deref())
    }

    /// Whether the range holds no key.
    pub fn is_empty(&self) -> bool {
        self.end.as_ref().is_some_and(|e| *e <= self.start)
    }
}

/// Whether `key` lies below the exclusive bound `end`, where `None` is the
/// end of the key space.
#[inline]
fn below(key: &[u8], end: Option<&[u8]>) -> bool {
    end.is_none_or(|e| key < e)
}

/// The lesser of two exclusive bounds, where `None` is the end of the key
/// space.
fn min_end(a: Option<Bytes>, b: Option<&Bytes>) -> Option<Bytes> {
    match (a, b) {
        (None, b) => b.cloned(),
        (a, None) => a,
        (Some(a), Some(b)) => Some(if a <= *b { a } else { b.clone() }),
    }
}

/// A key range with one set of covering deletes.
#[derive(Clone, Debug)]
struct Fragment {
    start: Bytes,
    end: Option<Bytes>,
    /// Sequence number and ticket of the newest delete covering it.
    newest: (u64, u64),
}

/// An immutable set of range deletes.
#[derive(Debug)]
struct Run {
    /// Sorted by start.
    dels: Vec<RangeDelete>,
    /// `reach[i]` is the greatest end among `dels[..=i]`, `None` if one is
    /// unbounded. A search for the deletes covering a key walks back from
    /// the last delete starting at or before it, and stops where the reach
    /// falls to the key.
    reach: Vec<Option<Bytes>>,
    /// Disjoint, in key order, each covered by at least one delete.
    frags: Vec<Fragment>,
}

impl Run {
    fn build(mut dels: Vec<RangeDelete>) -> Self {
        dels.sort_by(|a, b| a.start.cmp(&b.start).then(a.seq.cmp(&b.seq)));
        let mut reach: Vec<Option<Bytes>> = Vec::with_capacity(dels.len());
        for (i, d) in dels.iter().enumerate() {
            let far = match (i.checked_sub(1).map(|j| &reach[j]), &d.end) {
                (None, end) => end.clone(),
                (Some(None), _) | (_, None) => None,
                (Some(Some(f)), Some(e)) => Some(if e > f { e.clone() } else { f.clone() }),
            };
            reach.push(far);
        }
        let frags = fragments(&dels);
        Self { dels, reach, frags }
    }

    fn merge(a: &Run, b: &Run) -> Self {
        Self::build(a.dels.iter().chain(&b.dels).cloned().collect())
    }

    /// The newest delete `view` sees among those covering `key`, by a walk
    /// over the deletes; 0 for none.
    fn search(&self, key: &[u8], view: View) -> u64 {
        let p = self.dels.partition_point(|d| d.start[..] <= *key);
        let mut best = 0;
        for i in (0..p).rev() {
            if !below(key, self.reach[i].as_deref()) {
                break;
            }
            let d = &self.dels[i];
            if d.seq > best && below(key, d.end.as_deref()) && view(d.seq, d.ticket) {
                best = d.seq;
            }
        }
        best
    }

    /// The newest delete `view` sees among those covering fragment `f`.
    fn newest_in(&self, f: &Fragment, view: View) -> u64 {
        if view(f.newest.0, f.newest.1) { f.newest.0 } else { self.search(&f.start, view) }
    }

    /// The fragment holding `key`, or the index of the first fragment after it.
    fn find(&self, key: &[u8]) -> Result<&Fragment, usize> {
        let i = self.frags.partition_point(|f| f.start[..] <= *key);
        match i.checked_sub(1).map(|j| &self.frags[j]) {
            Some(f) if below(key, f.end.as_deref()) => Ok(f),
            _ => Err(i),
        }
    }

    fn covering(&self, key: &[u8], view: View) -> u64 {
        match self.find(key) {
            Ok(f) => self.newest_in(f, view),
            Err(_) => 0,
        }
    }

    /// The newest delete `view` sees covering `key`, and the bound below
    /// which every key from `key` on gets the same answer.
    fn span(&self, key: &[u8], view: View) -> (u64, Option<Bytes>) {
        match self.find(key) {
            Ok(f) => (self.newest_in(f, view), f.end.clone()),
            Err(i) => (0, self.frags.get(i).map(|f| f.start.clone())),
        }
    }

    /// The start of the fragment holding the keys just below `hi` (all keys,
    /// for `None`), if `view` sees a delete covering it.
    fn covered_below(&self, hi: Option<&[u8]>, view: View) -> Option<&Bytes> {
        let j = match hi {
            None => self.frags.len(),
            Some(h) => self.frags.partition_point(|f| f.start[..] < *h),
        };
        let f = self.frags.get(j.checked_sub(1)?)?;
        let reaches = match (hi, f.end.as_deref()) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(h), Some(e)) => e >= h,
        };
        (reaches && self.newest_in(f, view) > 0).then_some(&f.start)
    }
}

/// Cuts the key space of `dels` into fragments of constant cover.
fn fragments(dels: &[RangeDelete]) -> Vec<Fragment> {
    let mut points: Vec<&Bytes> = dels.iter().flat_map(|d| std::iter::once(&d.start).chain(d.end.as_ref())).collect();
    points.sort_unstable();
    points.dedup();
    let mut by_start: Vec<&RangeDelete> = dels.iter().filter(|d| !d.is_empty()).collect();
    by_start.sort_by(|a, b| a.start.cmp(&b.start));
    let mut by_end: Vec<&RangeDelete> = by_start.iter().copied().filter(|d| d.end.is_some()).collect();
    by_end.sort_by(|a, b| a.end.cmp(&b.end));
    let (mut s, mut e) = (0, 0);
    // The deletes covering the current point, by sequence number and ticket.
    let mut active: BTreeMap<(u64, u64), usize> = BTreeMap::new();
    let mut frags = Vec::new();
    for (i, &p) in points.iter().enumerate() {
        while e < by_end.len() && by_end[e].end.as_ref().is_some_and(|end| end <= p) {
            let d = by_end[e];
            let n = active.get_mut(&(d.seq, d.ticket)).expect("ended deletes started");
            *n -= 1;
            if *n == 0 {
                active.remove(&(d.seq, d.ticket));
            }
            e += 1;
        }
        while s < by_start.len() && by_start[s].start <= *p {
            let d = by_start[s];
            *active.entry((d.seq, d.ticket)).or_default() += 1;
            s += 1;
        }
        if let Some((&newest, _)) = active.last_key_value() {
            frags.push(Fragment { start: p.clone(), end: points.get(i + 1).map(|&n| n.clone()), newest });
        }
    }
    frags
}

/// A memtable's range deletes as of one moment.
#[derive(Debug, Default)]
pub struct Index {
    /// Largest first.
    runs: Vec<Arc<Run>>,
}

impl Index {
    /// The newest delete `view` sees that covers `key`; 0 for none.
    pub fn covering(&self, key: &[u8], view: View) -> u64 {
        self.runs.iter().map(|r| r.covering(key, view)).max().unwrap_or(0)
    }

    /// The newest delete `view` sees that covers `key` (0 for none), and the
    /// bound below which every key from `key` on gets the same answer
    /// (`None` for the end of the key space).
    pub fn span(&self, key: &[u8], view: View) -> (u64, Option<Bytes>) {
        let mut out = (0, None);
        for (i, r) in self.runs.iter().enumerate() {
            let (seq, until) = r.span(key, view);
            out.0 = out.0.max(seq);
            out.1 = if i == 0 { until } else { min_end(out.1, until.as_ref()) };
        }
        out
    }

    /// The lowest start among this index's fragments that hold the keys
    /// just below `hi` and that `view` sees a delete covering.
    fn covered_below(&self, hi: Option<&[u8]>, view: View) -> Option<Bytes> {
        self.runs.iter().filter_map(|r| r.covered_below(hi, view)).min().cloned()
    }

    /// Every delete, in no particular order.
    pub fn all(&self) -> impl Iterator<Item = &RangeDelete> {
        self.runs.iter().flat_map(|r| r.dels.iter())
    }

    pub fn len(&self) -> usize {
        self.runs.iter().map(|r| r.dels.len()).sum()
    }
}

/// The range deletes of one memtable, which writers add to while readers
/// read.
#[derive(Default)]
pub struct RangeDeletes {
    /// Set once the memtable holds a delete, so a read of a memtable without
    /// any costs one load.
    any: AtomicBool,
    index: ArcSwapOption<Index>,
    /// Serializes inserts.
    insert: Mutex<()>,
}

impl RangeDeletes {
    pub fn is_empty(&self) -> bool {
        !self.any.load(Ordering::Acquire)
    }

    /// The deletes as of now, or `None` if there are none.
    pub fn load(&self) -> Option<Arc<Index>> {
        if self.is_empty() { None } else { self.index.load_full() }
    }

    /// Adds `d`. A reader that sees the batch holding `d` commit, and then
    /// loads the index, finds `d` in it: the insert precedes the commit.
    pub fn insert(&self, d: RangeDelete) {
        if d.is_empty() {
            return;
        }
        let _g = self.insert.lock();
        let mut runs = self.index.load().as_ref().map_or_else(Vec::new, |i| i.runs.clone());
        let mut run = Run::build(vec![d]);
        while let Some(last) = runs.last()
            && last.dels.len() <= run.dels.len()
        {
            run = Run::merge(&runs.pop().expect("checked above"), &run);
        }
        runs.push(Arc::new(run));
        self.index.store(Some(Arc::new(Index { runs })));
        self.any.store(true, Ordering::Release);
    }

    /// The newest delete `view` sees that covers `key`; 0 for none.
    #[inline]
    pub fn covering(&self, key: &[u8], view: View) -> u64 {
        match self.load() {
            Some(i) => i.covering(key, view),
            None => 0,
        }
    }

    pub fn len(&self) -> usize {
        self.load().map_or(0, |i| i.len())
    }
}

/// The deletes of a frozen memtable, all committed, as its checkpoint
/// applies them: which keys of the tree they cover, and which of the
/// memtable's entries they hide.
pub struct FrozenDeletes {
    run: Run,
}

impl FrozenDeletes {
    pub fn new(dels: Vec<RangeDelete>) -> Self {
        Self { run: Run::build(dels) }
    }

    pub fn is_empty(&self) -> bool {
        self.run.frags.is_empty()
    }

    /// The union of the deletes' ranges.
    pub fn ranges(&self) -> KeyRanges {
        KeyRanges::from_sorted(self.run.frags.iter().map(|f| (f.start.clone(), f.end.clone())))
    }

    /// A cursor answering, for keys in ascending order, the newest delete
    /// covering each.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor { frags: &self.run.frags, next: 0 }
    }
}

/// See [`FrozenDeletes::cursor`].
pub struct Cursor<'a> {
    frags: &'a [Fragment],
    next: usize,
}

impl Cursor<'_> {
    /// The newest delete covering `key`, which is at least the key of the
    /// last call; 0 for none.
    pub fn newest(&mut self, key: &[u8]) -> u64 {
        while self.next < self.frags.len() && !below(key, self.frags[self.next].end.as_deref()) {
            self.next += 1;
        }
        match self.frags.get(self.next) {
            Some(f) if f.start[..] <= *key => f.newest.0,
            _ => 0,
        }
    }
}

/// The range deletes one snapshot sees, across its memtables.
pub struct Overlay {
    indexes: Vec<Arc<Index>>,
    snapshot: u64,
}

impl Overlay {
    /// The deletes of `indexes` at or below sequence number `snapshot`, or
    /// `None` if there are no deletes at all.
    pub fn new(indexes: Vec<Arc<Index>>, snapshot: u64) -> Option<Self> {
        (!indexes.is_empty()).then_some(Self { indexes, snapshot })
    }

    /// The newest delete the snapshot sees covering `key` (0 for none), and
    /// the bound below which every key from `key` on gets the same answer
    /// (`None` for the end of the key space).
    pub fn span(&self, key: &[u8]) -> (u64, Option<Bytes>) {
        let snapshot = self.snapshot;
        let view: View = &|seq, _| seq <= snapshot;
        let mut out = (0, None);
        for (i, idx) in self.indexes.iter().enumerate() {
            let (seq, until) = idx.span(key, view);
            out.0 = out.0.max(seq);
            out.1 = if i == 0 { until } else { min_end(out.1, until.as_ref()) };
        }
        out
    }

    /// The lowest key `lo` such that the snapshot sees a delete covering
    /// every key in `[lo, hi)`, or `None` if it sees none covering the keys
    /// just below `hi`.
    pub fn covered_below(&self, hi: Option<&[u8]>) -> Option<Bytes> {
        let snapshot = self.snapshot;
        let view: View = &|seq, _| seq <= snapshot;
        let mut bound: Option<Bytes> = hi.map(Bytes::copy_from_slice);
        let mut found = false;
        loop {
            let next = self.indexes.iter().filter_map(|i| i.covered_below(bound.as_deref(), view)).min();
            match next {
                // Each fragment found starts below the bound, so this ends.
                Some(lo) if bound.as_ref().is_none_or(|b| lo < *b) => {
                    bound = Some(lo);
                    found = true;
                }
                _ => break,
            }
        }
        if found { bound } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::Rng;

    fn key(i: u64) -> Bytes {
        Bytes::from(format!("k{i:03}"))
    }

    fn random_del(rng: &mut Rng, seq: u64) -> RangeDelete {
        let a = rng.below(60);
        let end = if rng.chance(0.1) { None } else { Some(key(a + rng.range(0, 12))) };
        RangeDelete { start: key(a), end, seq, ticket: seq / 3 + 1 }
    }

    /// Every query matches a brute-force search, for views that see all,
    /// some, or none of the deletes, at every step of a run of inserts.
    #[test]
    fn queries_match_brute_force() {
        for seed in 0..60 {
            let mut rng = Rng::new(seed);
            let rd = RangeDeletes::default();
            let mut all: Vec<RangeDelete> = Vec::new();
            let n = rng.range(1, 80);
            for seq in 1..=n {
                let d = random_del(&mut rng, seq);
                rd.insert(d.clone());
                if !d.is_empty() {
                    all.push(d);
                }
                let idx = rd.load();
                assert_eq!(idx.as_ref().map_or(0, |i| i.len()), all.len());
                let Some(idx) = idx else { continue };
                assert!(idx.runs.len() <= 64 - (all.len() as u64).leading_zeros() as usize, "one run per bit");
                let cut = rng.range(0, seq + 1);
                let hidden = rng.below(seq + 1);
                let views: [&dyn Fn(u64, u64) -> bool; 3] = [&|_, _| true, &|s, _| s <= cut, &|s, _| s != hidden];
                for view in views {
                    let brute = |k: &[u8]| all.iter().filter(|d| d.covers(k) && view(d.seq, d.ticket)).map(|d| d.seq).max().unwrap_or(0);
                    for i in 0..80 {
                        let k = key(i);
                        assert_eq!(idx.covering(&k, view), brute(&k), "seed {seed} key {i}");
                        // The span's answer holds up to its bound.
                        let (seq, until) = idx.span(&k, view);
                        assert_eq!(seq, brute(&k));
                        for j in i..80 {
                            let kj = key(j);
                            if !below(&kj, until.as_deref()) {
                                break;
                            }
                            assert_eq!(brute(&kj), seq, "seed {seed}: span from {i} changes at {j}");
                        }
                    }
                }
            }
        }
    }

    /// Probes: every endpoint, and a key just past each, which shares the
    /// cover of every key up to the next endpoint.
    fn probes() -> Vec<Bytes> {
        (0..80).flat_map(|i| [key(i), Bytes::from(format!("k{i:03}~"))]).collect()
    }

    #[test]
    fn overlay_finds_covered_spans() {
        for seed in 0..60 {
            let mut rng = Rng::new(1000 + seed);
            let mems: Vec<RangeDeletes> = (0..3).map(|_| RangeDeletes::default()).collect();
            let mut all = Vec::new();
            for seq in 1..=rng.range(1, 40) {
                let d = random_del(&mut rng, seq);
                mems[rng.below(3) as usize].insert(d.clone());
                all.push(d);
            }
            let snapshot = rng.range(0, 41);
            let Some(overlay) = Overlay::new(mems.iter().filter_map(|m| m.load()).collect(), snapshot) else { continue };
            let newest = |k: &[u8]| all.iter().filter(|d| d.seq <= snapshot && d.covers(k)).map(|d| d.seq).max().unwrap_or(0);
            for k in probes() {
                assert_eq!(overlay.span(&k).0, newest(&k), "seed {seed} at {k:?}");
            }
            for hi in (0..80).map(|i| Some(key(i))).chain([None]) {
                let below_hi = |k: &[u8]| hi.as_deref().is_none_or(|h| k < h);
                // The probe just below `hi`: the last one below it.
                let last = probes().into_iter().filter(|k| below_hi(k)).max();
                match overlay.covered_below(hi.as_deref()) {
                    Some(lo) => {
                        for k in probes().into_iter().filter(|k| *k >= lo && below_hi(k)) {
                            assert!(newest(&k) > 0, "seed {seed}: {k:?} in [{lo:?}, {hi:?}) is not covered");
                        }
                        // The span is as long as it can be: `lo` is an
                        // endpoint, and the probe before it shares the cover
                        // of the keys just below it.
                        if let Some(before) = probes().into_iter().filter(|k| *k < lo).max() {
                            assert_eq!(newest(&before), 0, "seed {seed}: the span below {hi:?} could start before {lo:?}");
                        }
                    }
                    None => {
                        if let Some(last) = last {
                            assert_eq!(newest(&last), 0, "seed {seed}: {last:?} below {hi:?} is covered");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn checkpoint_cursor_and_union() {
        let mut rng = Rng::new(5);
        for _ in 0..100 {
            let dels: Vec<RangeDelete> = (1..=rng.range(1, 30)).map(|s| random_del(&mut rng, s)).collect();
            let applied = FrozenDeletes::new(dels.clone());
            let ranges = applied.ranges();
            ranges.check();
            let mut cursor = applied.cursor();
            for i in 0..80 {
                let k = key(i);
                let newest = dels.iter().filter(|d| d.covers(&k)).map(|d| d.seq).max().unwrap_or(0);
                assert_eq!(cursor.newest(&k), newest);
                assert_eq!(ranges.contains(&k), newest > 0);
            }
        }
    }
}
