//! Range deletes applied to the tree.
//!
//! A checkpoint applies its memtable's range deletes to the tree before the
//! memtable's other entries. Everything in the tree is older than they are,
//! so every key they cover goes, and the tree never holds a range delete.
//!
//! [`delete_ranges`] removes what it can with little reading. It drops each
//! subtree that lies inside a range, handing its key range to a neighbour,
//! and marks the ranges dead in each buffered segment they reach, as a flush
//! marks the keys it moves to a child. It also drops each leaf whose keys all
//! lie inside the ranges, though the gaps between them do not: deletes of
//! adjacent prefixes leave gaps that hold no key. What remains are the keys
//! of the leaves a range cuts: at most two per range. [`live_keys`] finds
//! them, and the checkpoint writes a tombstone for each into its batches.

use super::ranges::successor;
use super::*;

/// Dead ranges a segment may hold per block of its page before a range
/// delete rewrites it. Each range a segment holds covers at least one key,
/// and a node page stores every range of its segments, so without a bound
/// many small range deletes would grow node pages without limit.
const DEAD_PER_BLOCK: usize = 1;
/// Dead ranges any segment may hold before that bound applies.
const DEAD_MIN: usize = 16;

/// Removes from the tree rooted at `root` what `ranges` covers, without
/// reading data, and returns the new root. The leaves a range cuts keep the
/// keys in it; see [`live_keys`].
pub fn delete_ranges(ctx: &Ctx, root: Option<&Subtree>, ranges: &KeyRanges) -> Result<Option<Subtree>> {
    let Some(root) = root else { return Ok(None) };
    if ranges.is_empty() {
        return Ok(Some(root.clone()));
    }
    if ranges.covers_range(b"", None) {
        return Ok(None);
    }
    match root {
        Subtree::Leaf(_) => Ok(Some(root.clone())),
        Subtree::Node(n) => {
            let pieces = delete_in_node(ctx, n, ranges)?;
            settle_root(ctx, pieces)
        }
    }
}

/// The keys `ranges` covers that the tree rooted at `root` holds live, in
/// order. After [`delete_ranges`], these lie in the leaves a range cuts.
pub fn live_keys(io: &PageIo, root: Option<&Subtree>, ranges: &KeyRanges, parallel: bool) -> Result<Vec<Bytes>> {
    let Some(root) = root else { return Ok(Vec::new()) };
    let scan = |(lo, hi): &(Bytes, Option<Bytes>)| -> Result<Vec<Bytes>> {
        let mut scan = TreeScan::new(Some(root.clone()), lo, hi.clone());
        let mut keys = Vec::new();
        loop {
            scan.settle(io)?;
            let Some((k, _)) = scan.peek() else { break };
            keys.push(Bytes::copy_from_slice(k));
            scan.advance(io)?;
        }
        Ok(keys)
    };
    let ranges = ranges.as_slice();
    let parts: Vec<Vec<Bytes>> = if parallel && ranges.len() > 1 {
        use rayon::prelude::*;
        ranges.par_iter().map(scan).collect::<Result<_>>()?
    } else {
        ranges.iter().map(scan).collect::<Result<_>>()?
    };
    Ok(parts.into_iter().flatten().collect())
}

/// Applies `ranges`, which lie within node `n`'s key range and do not cover
/// all of it, to a copy of `n`, and returns the pieces the copy became.
fn delete_in_node(ctx: &Ctx, n: &Node, ranges: &KeyRanges) -> Result<Vec<Piece>> {
    let mut node = n.clone();
    mark_dead(ctx, &mut node, ranges)?;
    let k = n.children.len();
    let mut pivots: Vec<Bytes> = Vec::with_capacity(k);
    let mut children: Vec<Subtree> = Vec::with_capacity(k);
    let mut pending: Vec<u64> = Vec::with_capacity(k);
    // Set while the children dropped so far start at the node's lower bound,
    // which the next child kept takes over.
    let mut dropped_first = false;
    for i in 0..k {
        let (lo, hi) = (&n.pivots[i], n.pivot_hi(i));
        let pieces: Vec<Piece> = if ranges.covers_range(lo, hi.map(|h| &h[..])) {
            Vec::new()
        } else if !ranges.overlaps(lo, hi.map(|h| &h[..])) {
            vec![(lo.clone(), n.children[i].clone())]
        } else {
            match &n.children[i] {
                // The keys a range cuts from a leaf go by tombstone.
                Subtree::Leaf(p) if holds_only(ctx.io, p, ranges)? => Vec::new(),
                leaf @ Subtree::Leaf(_) => vec![(lo.clone(), leaf.clone())],
                Subtree::Node(c) => delete_in_node(ctx, c, &ranges.clip(lo, hi.map(|h| &h[..])))?,
            }
        };
        if pieces.is_empty() {
            // The child's range joins the child before it, or the next one
            // kept. Either way it holds no key now.
            match children.last_mut() {
                Some(last) => *last = with_hi(last, hi),
                None => dropped_first = true,
            }
            continue;
        }
        let share = n.pending[i] / pieces.len() as u64;
        let rest = n.pending[i] - share * pieces.len() as u64;
        for (j, (mut key, mut child)) in pieces.into_iter().enumerate() {
            if dropped_first {
                key = n.pivots[0].clone();
                child = with_lo(&child, &key);
                dropped_first = false;
            }
            pivots.push(key);
            children.push(child);
            pending.push(share + if j == 0 { rest } else { 0 });
        }
    }
    if children.is_empty() {
        if node.buffer_is_empty() {
            return Ok(Vec::new());
        }
        // The node's buffer holds keys outside the ranges, so the node stays,
        // over a child that holds nothing.
        children.push(empty_subtree(ctx, n.height - 1, &n.pivots[0], n.hi.as_ref()));
        pivots.push(n.pivots[0].clone());
        pending.push(0);
    }
    node.pivots = pivots;
    node.children = children;
    node.pending = pending;
    // What the node buffers for the pivots a range reached shrank.
    for p in 0..node.pivots.len() {
        let (lo, hi) = (node.pivots[p].clone(), node.pivot_hi(p).cloned());
        if ranges.overlaps(&lo, hi.as_deref()) {
            let segs: Vec<&Segment> = node.levels.iter().flatten().filter(|s| s.overlaps(&lo, hi.as_deref())).collect();
            node.pending[p] = node.pending[p].min(buffered_bytes(&segs, &lo, hi.as_deref()));
        }
    }
    Ok(node_pieces(split_if_needed(ctx, node)?))
}

/// Marks `ranges` dead in every segment of `node`'s buffer they reach, drops
/// the segments left with no live key, and rewrites those left holding more
/// dead ranges than their pages have blocks.
fn mark_dead(ctx: &Ctx, node: &mut Node, ranges: &KeyRanges) -> Result<()> {
    for level in &mut node.levels {
        if !level.iter().any(|s| ranges.overlaps(s.min_key(), s.hi.as_deref())) {
            continue;
        }
        let mut out = Vec::with_capacity(level.len());
        for mut seg in std::mem::take(level) {
            let max = successor(seg.page.meta.max_key());
            let hi = match &seg.hi {
                Some(h) if *h < max => h.clone(),
                _ => max,
            };
            let cut = ranges.clip(seg.min_key(), Some(&hi));
            if cut.is_empty() {
                out.push(seg);
                continue;
            }
            seg.dead.union(&cut);
            if seg.is_dead() {
                continue;
            }
            if seg.dead.len() > (seg.page.meta.n_blocks as usize * DEAD_PER_BLOCK).max(DEAD_MIN) {
                let run = [seg];
                let pages = merge_runs(ctx, &[Run::Level(&run)], &run[0].lo, run[0].hi.as_deref(), false)?;
                out.extend(pages.into_iter().map(Segment::whole));
            } else {
                out.push(seg);
            }
        }
        *level = out;
    }
    Ok(())
}

/// Whether every key of `leaf` lies inside `ranges`: its first and last do,
/// and no gap between the ranges holds one. Reads a block per gap, until a
/// gap holds a key.
fn holds_only(io: &PageIo, leaf: &Arc<DataPage>, ranges: &KeyRanges) -> Result<bool> {
    let meta = &leaf.meta;
    if meta.n_items == 0 {
        return Ok(true);
    }
    if !ranges.contains(meta.min_key()) || !ranges.contains(meta.max_key()) {
        return Ok(false);
    }
    let inside = ranges.clip(meta.min_key(), Some(&successor(meta.max_key())));
    for pair in inside.as_slice().windows(2) {
        let gap_lo = pair[0].1.as_ref().expect("a range below another ends");
        let cursor = PageCursor::new(io, leaf.clone(), gap_lo, Some(pair[1].0.clone()), KeyRanges::new(), ReadMode::Cached)?;
        if cursor.peek().is_some() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A subtree of `height` over `[lo, hi)` that holds nothing.
fn empty_subtree(ctx: &Ctx, height: u32, lo: &Bytes, hi: Option<&Bytes>) -> Subtree {
    match height {
        0 => Subtree::Leaf(Arc::new(DataPage::unwritten(PageBuilder::new(ctx.new_page_id()).finish(0)))),
        _ => Subtree::Node(Arc::new(Node {
            height,
            pivots: vec![lo.clone()],
            hi: hi.cloned(),
            children: vec![empty_subtree(ctx, height - 1, lo, hi)],
            pending: vec![0],
            levels: vec![Vec::new(); ctx.cfg.max_levels],
            ptr: OnceLock::new(),
        })),
    }
}

/// `t` with its lower bound moved down to `lo`, over keys it does not hold.
fn with_lo(t: &Subtree, lo: &Bytes) -> Subtree {
    match t {
        Subtree::Leaf(_) => t.clone(),
        Subtree::Node(n) => {
            let mut n = (**n).clone();
            n.pivots[0] = lo.clone();
            n.children[0] = with_lo(&n.children[0], lo);
            Subtree::Node(Arc::new(n))
        }
    }
}

/// `t` with its upper bound moved up to `hi`, over keys it does not hold.
fn with_hi(t: &Subtree, hi: Option<&Bytes>) -> Subtree {
    match t {
        Subtree::Leaf(_) => t.clone(),
        Subtree::Node(n) => {
            let mut n = (**n).clone();
            n.hi = hi.cloned();
            let last = n.children.len() - 1;
            n.children[last] = with_hi(&n.children[last], hi);
            Subtree::Node(Arc::new(n))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pagestore::PageStore;
    use crate::sim::{FaultConfig, Rng, SimStorage};
    use std::collections::BTreeMap;

    struct Harness {
        cfg: TreeConfig,
        io: PageIo,
        ids: AtomicU64,
        root: Option<Subtree>,
        model: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    fn key(i: u64) -> Vec<u8> {
        format!("k{i:05}").into_bytes()
    }

    impl Harness {
        fn new(cfg: TreeConfig, seed: u64) -> Self {
            let disk = SimStorage::new(seed, FaultConfig::default());
            let store = PageStore::open(Arc::new(disk), &[]).unwrap();
            Self {
                io: PageIo::new(store, 1 << 18, 1 << 16, crate::page::Compression::None),
                cfg,
                ids: AtomicU64::new(1),
                root: None,
                model: BTreeMap::new(),
            }
        }

        fn ctx(&self) -> Ctx<'_> {
            Ctx { cfg: &self.cfg, io: &self.io, next_page_id: &self.ids }
        }

        fn apply_entries(&mut self, ops: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) {
            if ops.is_empty() {
                return;
            }
            let entries: Vec<EntryRef> = ops.iter().map(|(k, v)| EntryRef { key: k, value: v.as_deref() }).collect();
            let id = self.ids.fetch_add(1, Ordering::Relaxed);
            let page = Arc::new(DataPage::unwritten(page_from_entries(id, entries, 10)));
            self.root = apply_batch(&self.ctx(), self.root.as_ref(), page).unwrap();
        }

        fn write(&mut self, ops: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) {
            self.apply_entries(ops);
            for (k, v) in ops {
                match v {
                    Some(v) => self.model.insert(k.clone(), v.clone()),
                    None => self.model.remove(k),
                };
            }
        }

        /// Deletes `ranges` as a checkpoint does: the structural pass, then
        /// tombstones for the live keys left in them.
        fn delete(&mut self, ranges: &KeyRanges) {
            let pages_before = self.root.as_ref().map_or(0, |r| stats(r).leaves);
            self.root = delete_ranges(&self.ctx(), self.root.as_ref(), ranges).unwrap();
            if let Some(r) = &self.root {
                check_invariants(&self.io, &self.cfg, r).unwrap();
            }
            let left = live_keys(&self.io, self.root.as_ref(), ranges, self.cfg.parallel).unwrap();
            // Only the two boundary leaves of each range can keep keys in it.
            let leaf_bound = 2 * ranges.len() as u64 * (self.cfg.leaf_size * 2 + 4096);
            assert!(
                left.iter().map(|k| k.len() as u64).sum::<u64>() <= leaf_bound.max(1),
                "too many keys left: {} of {pages_before} leaves",
                left.len()
            );
            let tombstones: BTreeMap<Vec<u8>, Option<Vec<u8>>> = left.iter().map(|k| (k.to_vec(), None)).collect();
            self.apply_entries(&tombstones);
            self.model.retain(|k, _| !ranges.contains(k));
        }

        fn verify(&self, rng: &mut Rng) {
            let Some(root) = &self.root else {
                assert!(self.model.is_empty(), "tree empty, model has {}", self.model.len());
                return;
            };
            check_invariants(&self.io, &self.cfg, root).unwrap();
            let mut scan = TreeScan::new(Some(root.clone()), b"", None);
            let mut got = BTreeMap::new();
            while let Some((k, v)) = scan.next(&self.io).unwrap() {
                got.insert(k.to_vec(), v.to_vec());
            }
            assert!(got == self.model, "scan differs from model: {} vs {}", got.len(), self.model.len());
            for _ in 0..50 {
                let k = key(rng.below(3000));
                let v = get(&self.io, root, &k, crate::bloom::hash_key(&k)).unwrap().and_then(Value::into_put).map(|b| b.to_vec());
                assert_eq!(v.as_ref(), self.model.get(&k));
            }
        }

        fn checkpoint_and_reload(&mut self) {
            let Some(root) = &self.root else { return };
            write_tree(&self.ctx(), root).unwrap();
            self.root = Some(load_tree(&self.io, root_ptr(root).unwrap()).unwrap());
        }
    }

    fn random_ranges(rng: &mut Rng, keyspace: u64) -> KeyRanges {
        let mut ranges = KeyRanges::new();
        for _ in 0..rng.range(1, 6) {
            let lo = rng.below(keyspace);
            // Narrow, wide, and unbounded ranges.
            let width = *rng.pick(&[1, 3, 20, keyspace / 4, keyspace]);
            let lo_key = if rng.chance(0.1) { Vec::new() } else { key(lo) };
            let hi = if rng.chance(0.1) { None } else { Some(Bytes::from(key(lo + rng.range(0, width + 1)))) };
            ranges.insert(Bytes::from(lo_key), hi);
        }
        ranges
    }

    /// Random batches and range deletes on small trees, checked against a
    /// model after each step and after reloads.
    #[test]
    fn range_deletes_match_model() {
        for seed in 0..40 {
            let mut rng = Rng::new(seed * 104729 + 3);
            let max_pivots = rng.range(4, 12) as usize;
            let cfg = TreeConfig {
                leaf_size: rng.range(300, 4000),
                max_pivots,
                min_pivots: rng.range(2, (max_pivots / 2) as u64) as usize,
                max_segments: rng.range(3, 16) as usize,
                max_levels: rng.range(1, 5) as usize,
                filter_bits: 8,
                merge_readahead: rng.range(4096, 20000),
                parallel: rng.chance(0.5),
            };
            let mut h = Harness::new(cfg, seed);
            let keyspace = rng.range(50, 3000);
            for step in 0..300 {
                if rng.chance(0.15) {
                    let ranges = random_ranges(&mut rng, keyspace);
                    h.delete(&ranges);
                } else {
                    let mut ops = BTreeMap::new();
                    for _ in 0..rng.range(1, 150) {
                        let k = key(rng.below(keyspace));
                        let v = if rng.chance(0.1) { None } else { Some(vec![b'v'; rng.range(0, 40) as usize]) };
                        ops.insert(k, v);
                    }
                    h.write(&ops);
                }
                if step % 5 == 0 {
                    h.verify(&mut rng);
                }
                if rng.chance(0.05) {
                    h.checkpoint_and_reload();
                }
            }
            h.verify(&mut rng);
        }
    }

    fn tenant_key(t: u64, i: u64) -> Vec<u8> {
        format!("t{t:03}/k{i:04}").into_bytes()
    }

    /// The range a prefix delete of tenant `t` covers: it ends at "t{t}0",
    /// short of the next tenant's prefix, so adjacent tenants' ranges leave
    /// gaps that hold keys only when a test puts some there.
    fn tenant_range(t: u64) -> (Bytes, Option<Bytes>) {
        let p = format!("t{t:03}/").into_bytes();
        let end = crate::types::prefix_end(&p);
        (Bytes::from(p), end)
    }

    /// Prefix deletes of adjacent tenants drop the leaves that hold only
    /// their keys, though gaps lie between their ranges, and keep the keys
    /// written into those gaps, whether in leaves or in buffers.
    #[test]
    fn adjacent_prefixes_drop_leaves() {
        for seed in 0..30 {
            let mut rng = Rng::new(seed + 500);
            let max_pivots = rng.range(4, 10) as usize;
            let cfg = TreeConfig {
                leaf_size: rng.range(1024, 4096),
                max_pivots,
                min_pivots: 2,
                max_segments: rng.range(4, 12) as usize,
                max_levels: rng.range(1, 4) as usize,
                filter_bits: 8,
                merge_readahead: 8192,
                parallel: rng.chance(0.5),
            };
            let mut h = Harness::new(cfg, seed);
            let tenants = 60;
            for t in 0..tenants {
                let ops = (0..rng.range(20, 120)).map(|i| (tenant_key(t, i), Some(vec![b'v'; 20]))).collect();
                h.write(&ops);
            }
            h.checkpoint_and_reload();
            // Keys in the gaps between tenant ranges: "t{t}5" sorts after
            // "t{t}0" and before "t{t+1}/".
            let gaps: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
                (0..tenants).filter(|_| rng.chance(0.3)).map(|t| (format!("t{t:03}5").into_bytes(), Some(b"gap".to_vec()))).collect();
            h.write(&gaps);
            if rng.chance(0.5) {
                h.checkpoint_and_reload();
            }
            let before = stats(h.root.as_ref().unwrap()).leaves;
            let (lo, hi) = (rng.range(0, 10), rng.range(40, tenants));
            let mut ranges = KeyRanges::new();
            for t in lo..hi {
                let (a, b) = tenant_range(t);
                ranges.insert(a, b);
            }
            assert_eq!(ranges.len() as u64, hi - lo, "the ranges do not touch");
            h.delete(&ranges);
            h.verify(&mut rng);
            let after = h.root.as_ref().map_or(0, |r| stats(r).leaves);
            assert!(after * 2 < before.max(4), "seed {seed}: leaves {before} -> {after}");
            for k in gaps.keys() {
                assert!(h.model.contains_key(k));
            }
        }
    }

    /// A wide range drops whole subtrees without reading them, and leaves
    /// tombstones for at most its two boundary leaves.
    #[test]
    fn wide_range_drops_subtrees() {
        let cfg = TreeConfig { leaf_size: 2048, max_pivots: 6, min_pivots: 2, max_segments: 8, max_levels: 3, ..TreeConfig::default() };
        let mut h = Harness::new(cfg, 9);
        for b in 0..60 {
            let ops = (0..100).map(|i| (key(b * 100 + i), Some(vec![b'x'; 30]))).collect();
            h.write(&ops);
        }
        h.checkpoint_and_reload();
        let before = stats(h.root.as_ref().unwrap());
        assert!(before.height >= 2, "{before:?}");
        let mut ranges = KeyRanges::new();
        ranges.insert(Bytes::from(key(1000)), Some(Bytes::from(key(5000))));
        h.root = delete_ranges(&h.ctx(), h.root.as_ref(), &ranges).unwrap();
        let after = stats(h.root.as_ref().unwrap());
        assert!(after.leaves * 3 < before.leaves * 2, "subtrees dropped: {before:?} -> {after:?}");
        let left = live_keys(&h.io, h.root.as_ref(), &ranges, false).unwrap();
        let per_leaf = (h.cfg.leaf_size * 2 / 30) as usize;
        assert!(left.len() <= 2 * per_leaf, "{} keys left", left.len());
        let tombstones = left.iter().map(|k| (k.to_vec(), None)).collect();
        h.apply_entries(&tombstones);
        h.model.retain(|k, _| !ranges.contains(k));
        h.verify(&mut Rng::new(1));
    }
}
