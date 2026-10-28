//! The TurtleTree: a B^ε+-tree whose interior nodes buffer updates in levels
//! of exponentially growing size.
//!
//! Every interior node holds pivots (child key ranges) and an update buffer of
//! `max_levels` levels. Level `i` is a sorted run of up to `2^i` leaf-sized
//! batches, stored as segment pages. Inserting a batch works like
//! incrementing a binary counter: the batch merges with the occupied prefix of
//! levels and lands in the first empty one. After each insert, the node
//! flushes at most one leaf-sized batch to the child whose key range has the
//! most buffered bytes. Leaves are single data pages of at most `leaf_size`
//! bytes; tombstones die when a batch merges into a leaf.
//!
//! Nodes are immutable once shared (`Arc`), so applying batches builds a new
//! tree that shares untouched subtrees with the old one. New pages stay in
//! memory until the next checkpoint writes them; pages that a later batch
//! supersedes before then are never written at all.
//!
//! A segment records what part of its page belongs to the node: a clip range
//! `[lo, hi)` (set when a node split leaves one page shared by both halves)
//! and a set of dead keys (flushed to a child). Both are key-based, so pivot
//! changes never touch segment metadata.

pub mod codec;
pub mod cursor;
pub mod io;
pub mod ranges;

use crate::error::{Result, corruption};
use crate::page::{EntryRef, PageBuilder, PageId, PageImage};
use crate::pagestore::PagePtr;
use crate::types::{Value, cmp_keys, key_prefix};
use bytes::Bytes;
use codec::{Reader, put_bytes};
use cursor::{LevelCursor, Merger, PageCursor, ReadMode, Source};
use io::{DataPage, PageIo};
use ranges::KeyRanges;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Debug)]
pub struct TreeConfig {
    /// Target size of a leaf and of a flushed batch, in logical entry bytes.
    pub leaf_size: u64,
    pub max_pivots: usize,
    pub min_pivots: usize,
    /// A node with more segments than this splits.
    pub max_segments: usize,
    pub max_levels: usize,
    pub filter_bits: u32,
    /// Bytes per read when a merge streams a page from disk.
    pub merge_readahead: u64,
    /// Write pages of a checkpoint on multiple threads.
    pub parallel: bool,
}

impl Default for TreeConfig {
    fn default() -> Self {
        Self {
            leaf_size: 4 << 20,
            max_pivots: 64,
            min_pivots: 4,
            max_segments: 63,
            max_levels: 4,
            filter_bits: 12,
            merge_readahead: 1 << 20,
            parallel: true,
        }
    }
}

/// Cumulative nanoseconds in the main steps of `apply_batch`, for tuning.
pub mod profile {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    pub static PENDING: AtomicU64 = AtomicU64::new(0);
    pub static INSERT: AtomicU64 = AtomicU64::new(0);
    pub static EXTRACT: AtomicU64 = AtomicU64::new(0);
    pub static LEAF: AtomicU64 = AtomicU64::new(0);
    pub static RESHAPE: AtomicU64 = AtomicU64::new(0);

    pub fn add(counter: &AtomicU64, since: std::time::Instant) {
        counter.fetch_add(since.elapsed().as_nanos() as u64, Relaxed);
    }

    /// Seconds in: pending-byte accounting, buffer inserts, flush
    /// extraction, leaf merges, and splits/merges of nodes.
    pub fn snapshot() -> [f64; 5] {
        [&PENDING, &INSERT, &EXTRACT, &LEAF, &RESHAPE].map(|c| c.load(Relaxed) as f64 / 1e9)
    }
}

/// Shared state for tree operations.
pub struct Ctx<'a> {
    pub cfg: &'a TreeConfig,
    pub io: &'a PageIo,
    pub next_page_id: &'a AtomicU64,
}

impl Ctx<'_> {
    fn new_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    fn merge_mode(&self) -> ReadMode {
        ReadMode::Span(self.cfg.merge_readahead)
    }

    fn finish(&self, b: PageBuilder) -> Arc<DataPage> {
        Arc::new(DataPage::unwritten(b.finish(self.cfg.filter_bits)))
    }
}

/// A buffered sorted run's page, as seen by one node.
#[derive(Clone, Debug)]
pub struct Segment {
    pub page: Arc<DataPage>,
    /// Clip range: keys outside `[lo, hi)` belong to another node.
    pub lo: Bytes,
    pub hi: Option<Bytes>,
    /// Keys already flushed to a child.
    pub dead: KeyRanges,
    /// [`key_prefix`] of [`Segment::min_key`], so that searching a level
    /// compares integers before reaching into each page's metadata.
    min_prefix: u64,
}

impl Segment {
    pub fn new(page: Arc<DataPage>, lo: Bytes, hi: Option<Bytes>, dead: KeyRanges) -> Self {
        let mut s = Self { page, lo, hi, dead, min_prefix: 0 };
        s.min_prefix = key_prefix(s.min_key());
        s
    }

    pub fn whole(page: Arc<DataPage>) -> Self {
        Self::new(page, Bytes::new(), None, KeyRanges::new())
    }

    /// Moves the clip range's lower end to `lo`.
    pub fn set_lo(&mut self, lo: Bytes) {
        self.lo = lo;
        self.min_prefix = key_prefix(self.min_key());
    }

    /// Whether the segment's range starts at or before `key`, whose
    /// [`key_prefix`] is `prefix`.
    #[inline]
    fn starts_by(&self, key: &[u8], prefix: u64) -> bool {
        match self.min_prefix.cmp(&prefix) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => cmp_keys(self.min_key(), key).is_le(),
        }
    }

    /// Smallest key the segment may hold.
    pub fn min_key(&self) -> &[u8] {
        let m = self.page.meta.min_key();
        if cmp_keys(&self.lo, m).is_gt() { &self.lo } else { m }
    }

    /// Whether `key` lies in the segment's clipped key range.
    pub fn covers_key(&self, key: &[u8]) -> bool {
        cmp_keys(key, self.min_key()).is_ge()
            && cmp_keys(key, self.page.meta.max_key()).is_le()
            && self.hi.as_ref().is_none_or(|h| cmp_keys(key, h).is_lt())
    }

    /// Whether the clipped range intersects `[lo, hi)`.
    pub fn overlaps(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        self.page.meta.n_items > 0
            && cmp_keys(self.page.meta.max_key(), lo).is_ge()
            && self.hi.as_ref().is_none_or(|h| cmp_keys(h, lo).is_gt())
            && hi.is_none_or(|hi| cmp_keys(self.min_key(), hi).is_lt())
    }

    fn range_is_empty(&self) -> bool {
        let meta = &self.page.meta;
        meta.n_items == 0
            || self.hi.as_ref().is_some_and(|h| &h[..] <= self.min_key())
            || self.min_key() > meta.max_key()
    }

    /// True when no key of the clipped range is live.
    pub fn is_dead(&self) -> bool {
        if self.range_is_empty() {
            return true;
        }
        let lo = self.min_key();
        let max = self.page.meta.max_key();
        match &self.hi {
            Some(h) if &h[..] <= max => match self.dead.skip(lo) {
                Ok(None) => false,
                Ok(Some(next)) => next >= h,
                Err(()) => true,
            },
            _ => self.dead.covers(lo, max),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Subtree {
    Leaf(Arc<DataPage>),
    Node(Arc<Node>),
}

impl Subtree {
    pub fn height(&self) -> u32 {
        match self {
            Subtree::Leaf(_) => 0,
            Subtree::Node(n) => n.height,
        }
    }
}

#[derive(Debug)]
pub struct Node {
    /// 1 for a node whose children are leaves.
    pub height: u32,
    /// `pivots[i]` is the smallest key routed to `children[i]`; `pivots[0]` is
    /// the node's lower bound.
    pub pivots: Vec<Bytes>,
    /// Exclusive upper bound of the node's key range.
    pub hi: Option<Bytes>,
    pub children: Vec<Subtree>,
    /// Estimated buffered bytes per pivot.
    pub pending: Vec<u64>,
    /// Buffer levels, newest first; an empty level is a hole.
    pub levels: Vec<Vec<Segment>>,
    ptr: OnceLock<PagePtr>,
}

impl Clone for Node {
    /// A clone is a new, unwritten node.
    fn clone(&self) -> Self {
        Self {
            height: self.height,
            pivots: self.pivots.clone(),
            hi: self.hi.clone(),
            children: self.children.clone(),
            pending: self.pending.clone(),
            levels: self.levels.clone(),
            ptr: OnceLock::new(),
        }
    }
}

/// A piece of a split subtree and the smallest key routed to it.
type Piece = (Bytes, Subtree);

impl Node {
    pub fn ptr(&self) -> Option<PagePtr> {
        self.ptr.get().copied()
    }

    fn pivot_hi(&self, i: usize) -> Option<&Bytes> {
        self.pivots.get(i + 1).or(self.hi.as_ref())
    }

    /// The child whose key range holds `key`.
    pub fn find_pivot(&self, key: &[u8]) -> usize {
        self.pivots[1..].partition_point(|p| cmp_keys(p, key).is_le())
    }

    pub fn segment_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    fn buffer_is_empty(&self) -> bool {
        self.levels.iter().all(Vec::is_empty)
    }

    fn remove_dead_segments(&mut self) {
        for level in &mut self.levels {
            level.retain(|s| !s.is_dead());
        }
    }
}

/// Merges `sources` (newest first) into pages of about `target` bytes each.
fn build_pages(
    ctx: &Ctx,
    merger: &mut Merger,
    target: u64,
    drop_tombstones: bool,
) -> Result<Vec<Arc<DataPage>>> {
    let capacity = (target + target / 16) as usize + 4096;
    let mut pages = Vec::new();
    let mut b = PageBuilder::with_capacity(ctx.new_page_id(), capacity);
    while let Some(e) = merger.peek() {
        if !(drop_tombstones && e.value.is_none()) {
            if !b.is_empty() && b.data_bytes() + e.size() > target {
                let next = PageBuilder::with_capacity(ctx.new_page_id(), capacity);
                pages.push(ctx.finish(std::mem::replace(&mut b, next)));
            }
            b.push(e);
        }
        merger.advance(ctx.io)?;
    }
    if !b.is_empty() {
        pages.push(ctx.finish(b));
    }
    Ok(pages)
}

/// Per-page size target that splits `total` bytes into balanced pages of at
/// most `limit`.
fn balanced_target(total: u64, limit: u64) -> u64 {
    let parts = total.div_ceil(limit).max(1);
    total.div_ceil(parts).max(1)
}

/// One input of a merge.
#[derive(Clone, Copy)]
enum Run<'a> {
    Page(&'a Arc<DataPage>),
    Level(&'a [Segment]),
}

impl Run<'_> {
    /// Estimated bytes in `[lo, hi)`.
    fn bytes_in(&self, lo: &[u8], hi: Option<&[u8]>) -> u64 {
        match self {
            Run::Page(p) => p.meta.estimate_bytes(lo, hi),
            Run::Level(l) => {
                let segs: Vec<&Segment> = l.iter().filter(|s| s.overlaps(lo, hi)).collect();
                buffered_bytes(&segs, lo, hi)
            }
        }
    }

    fn source(&self, ctx: &Ctx, lo: &[u8], hi: Option<&[u8]>) -> Result<Source> {
        Ok(match self {
            Run::Page(p) => Source::Page(PageCursor::new(
                ctx.io,
                (*p).clone(),
                lo,
                hi.map(Bytes::copy_from_slice),
                KeyRanges::new(),
                ctx.merge_mode(),
            )?),
            Run::Level(l) => Source::Level(LevelCursor::new(ctx.io, l, lo, hi, ctx.merge_mode())?),
        })
    }

    /// Keys inside `(lo, hi)` that cut this run into pieces of about `every`
    /// bytes, taken from the block index without reading data.
    fn split_keys(&self, lo: &[u8], hi: Option<&[u8]>, every: u64, out: &mut Vec<Bytes>) {
        let pages: Vec<&Arc<DataPage>> = match self {
            Run::Page(p) => vec![p],
            Run::Level(l) => l.iter().filter(|s| s.overlaps(lo, hi)).map(|s| &s.page).collect(),
        };
        let mut acc = 0;
        for page in pages {
            let meta = &page.meta;
            for b in meta.blocks_for_range(lo, hi) {
                acc += meta.block(b).len as u64;
                if acc >= every {
                    let key = meta.block_first_key(b);
                    if key > lo && hi.is_none_or(|h| key < h) && out.last().is_none_or(|l| &l[..] < key) {
                        out.push(Bytes::copy_from_slice(key));
                        acc = 0;
                    }
                }
            }
        }
    }
}

/// Merges `runs` (newest first) over `[lo, hi)` into balanced pages of at
/// most a leaf each. Large merges split the key range into leaf-sized
/// pieces, merged in parallel when configured.
fn merge_runs(ctx: &Ctx, runs: &[Run], lo: &[u8], hi: Option<&[u8]>, drop_tombstones: bool) -> Result<Vec<Arc<DataPage>>> {
    let limit = ctx.cfg.leaf_size;
    let sizes: Vec<u64> = runs.iter().map(|r| r.bytes_in(lo, hi)).collect();
    let total: u64 = sizes.iter().sum();
    let mut cuts = Vec::new();
    let chunks = total.div_ceil(limit);
    if ctx.cfg.parallel && chunks > 1 {
        // Cut the largest run into equal pieces; the others, sharing its key
        // distribution, divide in about the same proportion.
        let (largest, size) = runs.iter().zip(&sizes).max_by_key(|(_, s)| **s).unwrap();
        largest.split_keys(lo, hi, size.div_ceil(chunks).max(1), &mut cuts);
    }
    let mut ranges: Vec<(Bytes, Option<Bytes>)> = Vec::with_capacity(cuts.len() + 1);
    let mut start = Bytes::copy_from_slice(lo);
    for cut in cuts {
        ranges.push((std::mem::replace(&mut start, cut.clone()), Some(cut)));
    }
    ranges.push((start, hi.map(Bytes::copy_from_slice)));
    let target = balanced_target(total.div_ceil(ranges.len() as u64), limit);
    let merge = |(a, b): &(Bytes, Option<Bytes>)| -> Result<Vec<Arc<DataPage>>> {
        let sources = runs.iter().map(|r| r.source(ctx, a, b.as_deref())).collect::<Result<Vec<_>>>()?;
        build_pages(ctx, &mut Merger::new(sources), target, drop_tombstones)
    };
    if ranges.len() == 1 {
        return merge(&ranges[0]);
    }
    use rayon::prelude::*;
    let parts = ranges.par_iter().map(merge).collect::<Result<Vec<_>>>()?;
    Ok(parts.into_iter().flatten().collect())
}

/// A sorted run of pages with disjoint key ranges, applied to a node or
/// leaf as one unit.
type Batch = Vec<Segment>;

/// Merges `batch` into a leaf (or into nothing), dropping tombstones, and
/// splits the result into balanced leaves.
fn leaf_apply(ctx: &Ctx, leaf: Option<&Arc<DataPage>>, batch: &[Segment]) -> Result<Vec<Arc<DataPage>>> {
    let mut runs = vec![Run::Level(batch)];
    if let Some(leaf) = leaf {
        runs.push(Run::Page(leaf));
    }
    merge_runs(ctx, &runs, b"", None, true)
}

fn leaf_pieces(pages: Vec<Arc<DataPage>>, lo: &Bytes) -> Vec<Piece> {
    pages
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let key = if i == 0 { lo.clone() } else { Bytes::copy_from_slice(p.meta.min_key()) };
            (key, Subtree::Leaf(p))
        })
        .collect()
}

/// Adds each pivot's share of `batch` to its pending bytes, estimated from
/// the batch pages' block indexes.
fn add_pending(node: &mut Node, batch: &[Segment]) {
    for seg in batch {
        let meta = &seg.page.meta;
        if meta.n_items == 0 {
            continue;
        }
        let first = node.find_pivot(meta.min_key());
        let last = node.find_pivot(meta.max_key());
        for p in first..=last {
            node.pending[p] += meta.estimate_bytes(&node.pivots[p], node.pivot_hi(p).map(|h| &h[..]));
        }
    }
}

/// Inserts `batch` as the newest run: it merges with the occupied prefix of
/// levels and lands in the first empty level (or the last level, if all are
/// occupied).
fn buffer_insert(ctx: &Ctx, node: &mut Node, batch: Batch) -> Result<()> {
    let n = node.levels.len();
    let first_empty = node.levels.iter().position(Vec::is_empty).unwrap_or(n);
    if first_empty == 0 {
        node.levels[0] = batch;
        return Ok(());
    }
    let target = first_empty.min(n - 1);
    let merged_levels = if first_empty == n { n } else { first_empty };
    let mut runs = vec![Run::Level(&batch)];
    runs.extend(node.levels[..merged_levels].iter().map(|l| Run::Level(l)));
    let pages = merge_runs(ctx, &runs, b"", None, false)?;
    for level in &mut node.levels[..merged_levels] {
        level.clear();
    }
    node.levels[target] = pages.into_iter().map(Segment::whole).collect();
    Ok(())
}

/// Flushes one batch to the pivot with the most pending bytes, if it has at
/// least a leaf's worth (or any, when `force` is set).
fn flush_if_necessary(ctx: &Ctx, node: &mut Node, force: bool) -> Result<()> {
    let (mut p, mut most) = (0, 0);
    for (i, &b) in node.pending.iter().enumerate() {
        if b > most {
            (p, most) = (i, b);
        }
    }
    if most > 0 && (force || most >= ctx.cfg.leaf_size) {
        flush_to_pivot(ctx, node, p)?;
    }
    Ok(())
}

/// Where a segment's live keys at or after `lo` begin, or `None` if it has
/// none from there on.
fn live_start<'a>(s: &'a Segment, lo: &'a [u8]) -> Option<&'a [u8]> {
    let start = s.min_key().max(lo);
    match s.dead.skip(start) {
        Ok(None) => Some(start),
        Ok(Some(next)) => Some(next),
        Err(()) => None,
    }
}

/// Estimated live bytes buffered in `[lo, hi)`. Keys flushed from the
/// front of a segment's range are excluded; scattered dead keys are not.
fn buffered_bytes(segs: &[&Segment], lo: &[u8], hi: Option<&[u8]>) -> u64 {
    segs.iter()
        .filter_map(|s| {
            let lo = live_start(s, lo)?;
            let hi = match (s.hi.as_deref(), hi) {
                (None, h) | (h, None) => h,
                (Some(a), Some(b)) => Some(a.min(b)),
            };
            Some(if hi.is_some_and(|h| h <= lo) { 0 } else { s.page.meta.estimate_bytes(lo, hi) })
        })
        .sum()
}

/// The end of the smallest-keyed prefix of `[lo, hi)` that holds about
/// `limit` buffered bytes, or `None` if the whole range holds less. The cut
/// lies past the first live key position, so every flush makes progress.
fn choose_cut(segs: &[&Segment], lo: &[u8], hi: Option<&[u8]>, limit: u64) -> Option<Bytes> {
    if buffered_bytes(segs, lo, hi) <= limit {
        return None;
    }
    let first_live = segs.iter().filter_map(|s| live_start(s, lo)).min()?;
    let mut candidates: Vec<&[u8]> = Vec::new();
    for s in segs {
        let meta = &s.page.meta;
        for b in meta.blocks_for_range(first_live, hi) {
            let k = meta.block_first_key(b);
            if k > first_live && hi.is_none_or(|h| k < h) {
                candidates.push(k);
            }
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    let fits = candidates.partition_point(|c| buffered_bytes(segs, lo, Some(c)) <= limit);
    candidates.get(fits.saturating_sub(1)).map(|c| Bytes::copy_from_slice(c))
}

/// Moves the smallest-keyed leaf-sized prefix of pivot `p`'s buffered data
/// into child `p`.
fn flush_to_pivot(ctx: &Ctx, node: &mut Node, p: usize) -> Result<()> {
    let t = std::time::Instant::now();
    let lo = node.pivots[p].clone();
    let hi = node.pivot_hi(p).cloned();
    let segs: Vec<&Segment> = node.levels.iter().flatten().filter(|s| s.overlaps(&lo, hi.as_deref())).collect();
    let cut = choose_cut(&segs, &lo, hi.as_deref(), ctx.cfg.leaf_size);
    let end = cut.as_ref().or(hi.as_ref()).map(|b| &b[..]);
    let runs: Vec<Run> = node.levels.iter().map(|l| Run::Level(l)).collect();
    let pages = merge_runs(ctx, &runs, &lo, end, false)?;

    // Everything below the cut has moved to the child.
    for seg in node.levels.iter_mut().flatten() {
        if seg.overlaps(&lo, end) {
            seg.dead.insert(lo.clone(), end.map(Bytes::copy_from_slice));
        }
    }
    node.remove_dead_segments();
    node.pending[p] = match &cut {
        Some(cut) => {
            let segs: Vec<&Segment> = node.levels.iter().flatten().filter(|s| s.overlaps(cut, hi.as_deref())).collect();
            buffered_bytes(&segs, cut, hi.as_deref()).max(1)
        }
        None => 0,
    };
    profile::add(&profile::EXTRACT, t);
    if pages.is_empty() {
        return Ok(());
    }
    let batch: Batch = pages.into_iter().map(Segment::whole).collect();
    let child = node.children[p].clone();
    let pieces = apply_to_child(ctx, child, batch, &lo, hi.as_ref())?;
    replace_child(node, p, pieces);
    Ok(())
}

/// Applies `batch` to a non-root subtree covering `[lo, hi)` and returns the
/// viable pieces it became (possibly none, if a leaf emptied).
fn apply_to_child(ctx: &Ctx, child: Subtree, batch: Batch, lo: &Bytes, hi: Option<&Bytes>) -> Result<Vec<Piece>> {
    match child {
        Subtree::Leaf(leaf) => {
            let t = std::time::Instant::now();
            let pieces = leaf_pieces(leaf_apply(ctx, Some(&leaf), &batch)?, lo);
            profile::add(&profile::LEAF, t);
            Ok(pieces)
        }
        Subtree::Node(n) => {
            debug_assert_eq!(&n.pivots[0], lo);
            debug_assert_eq!(n.hi.as_ref(), hi);
            let mut n = (*n).clone();
            node_apply(ctx, &mut n, batch)?;
            Ok(split_if_needed(ctx, n)?
                .into_iter()
                .map(|n| (n.pivots[0].clone(), Subtree::Node(Arc::new(n))))
                .collect())
        }
    }
}

fn node_apply(ctx: &Ctx, node: &mut Node, batch: Batch) -> Result<()> {
    let t = std::time::Instant::now();
    add_pending(node, &batch);
    profile::add(&profile::PENDING, t);
    let t = std::time::Instant::now();
    buffer_insert(ctx, node, batch)?;
    profile::add(&profile::INSERT, t);
    flush_if_necessary(ctx, node, false)
}

/// Replaces child `p` with `pieces`, then merges undersized children into
/// their siblings.
fn replace_child(node: &mut Node, p: usize, pieces: Vec<Piece>) {
    let pending = node.pending[p];
    let n = pieces.len();
    if n == 0 {
        // The child's leaf emptied. Its range joins a neighbour's; a node
        // keeps at least one child, so an only child becomes an empty leaf.
        if node.children.len() == 1 {
            node.children[0] = Subtree::Leaf(Arc::new(DataPage::unwritten(PageBuilder::new(0).finish(0))));
            return;
        }
        node.children.remove(p);
        node.pending.remove(p);
        if p == 0 {
            node.pivots.remove(1);
            node.pending[0] += pending;
        } else {
            node.pivots.remove(p);
            node.pending[p - 1] += pending;
        }
        return;
    }
    let share = pending / n as u64;
    let mut keys = Vec::with_capacity(n);
    let mut children = Vec::with_capacity(n);
    for (key, child) in pieces {
        keys.push(key);
        children.push(child);
    }
    keys[0] = node.pivots[p].clone();
    node.pivots.splice(p..p + 1, keys);
    node.children.splice(p..p + 1, children);
    let mut shares = vec![share; n];
    shares[0] += pending - share * n as u64;
    node.pending.splice(p..p + 1, shares);
}

fn needs_merge(ctx: &Ctx, child: &Subtree) -> bool {
    match child {
        Subtree::Leaf(p) => p.data_bytes() < ctx.cfg.leaf_size / 4,
        Subtree::Node(n) => n.pivots.len() < ctx.cfg.min_pivots,
    }
}

/// Merges every undersized child with a sibling.
fn fix_undersized_children(ctx: &Ctx, node: &mut Node) -> Result<()> {
    let mut i = 0;
    let mut rounds = 0;
    while i < node.children.len() && node.children.len() >= 2 {
        rounds += 1;
        assert!(rounds < 1_000_000, "child merging is not making progress");
        if !needs_merge(ctx, &node.children[i]) {
            i += 1;
            continue;
        }
        let left = if i + 1 < node.children.len() { i } else { i - 1 };
        let pieces = merge_siblings(ctx, &node.children[left], &node.children[left + 1], &node.pivots[left])?;
        debug_assert!(!pieces.is_empty());
        let pending = node.pending[left] + node.pending[left + 1];
        let n = pieces.len();
        let (mut keys, children): (Vec<Bytes>, Vec<Subtree>) = pieces.into_iter().unzip();
        keys[0] = node.pivots[left].clone();
        node.pivots.splice(left..left + 2, keys);
        node.children.splice(left..left + 2, children);
        let share = pending / n as u64;
        let mut shares = vec![share; n];
        shares[0] += pending - share * n as u64;
        node.pending.splice(left..left + 2, shares);
        // A single merged child may still be undersized; look at it again.
        // Each pass either removes a child or moves forward, so this ends.
        i = if n == 1 { left } else { left + n };
    }
    Ok(())
}

/// Merges two adjacent siblings (left keys below right keys) and splits the
/// result again if it is oversized.
fn merge_siblings(ctx: &Ctx, left: &Subtree, right: &Subtree, lo: &Bytes) -> Result<Vec<Piece>> {
    match (left, right) {
        (Subtree::Leaf(a), Subtree::Leaf(b)) => {
            let mut pages = merge_runs(ctx, &[Run::Page(a), Run::Page(b)], b"", None, true)?;
            if pages.is_empty() {
                pages.push(Arc::new(DataPage::unwritten(PageBuilder::new(ctx.new_page_id()).finish(0))));
            }
            Ok(leaf_pieces(pages, lo))
        }
        (Subtree::Node(a), Subtree::Node(b)) => {
            let merged = concat_nodes(a, b);
            Ok(split_if_needed(ctx, merged)?
                .into_iter()
                .map(|n| (n.pivots[0].clone(), Subtree::Node(Arc::new(n))))
                .collect())
        }
        _ => unreachable!("siblings have equal height"),
    }
}

/// Joins two adjacent nodes of equal height. A page shared across the seam
/// (left over from an earlier split) becomes one segment again.
fn concat_nodes(a: &Node, b: &Node) -> Node {
    debug_assert_eq!(a.height, b.height);
    debug_assert_eq!(a.hi.as_ref(), Some(&b.pivots[0]));
    let mut levels = Vec::with_capacity(a.levels.len());
    for (la, lb) in a.levels.iter().zip(&b.levels) {
        let mut level = la.clone();
        let mut rest = &lb[..];
        if let (Some(last), Some(first)) = (level.last_mut(), lb.first())
            && last.page.id() == first.page.id()
        {
            let mut dead = last.dead.clip(&last.lo, last.hi.as_deref());
            dead.union(&first.dead.clip(&first.lo, first.hi.as_deref()));
            if let Some(gap_lo) = &last.hi
                && gap_lo < &first.lo
            {
                dead.insert(gap_lo.clone(), Some(first.lo.clone()));
            }
            last.hi = first.hi.clone();
            last.dead = dead;
            rest = &lb[1..];
        }
        level.extend_from_slice(rest);
        levels.push(level);
    }
    Node {
        height: a.height,
        pivots: a.pivots.iter().chain(&b.pivots).cloned().collect(),
        hi: b.hi.clone(),
        children: a.children.iter().chain(&b.children).cloned().collect(),
        pending: a.pending.iter().chain(&b.pending).copied().collect(),
        levels,
        ptr: OnceLock::new(),
    }
}

/// Splits a node that has too many pivots or segments into viable halves.
fn split_if_needed(ctx: &Ctx, node: Node) -> Result<Vec<Node>> {
    let t = std::time::Instant::now();
    let out = split_if_needed_inner(ctx, node);
    profile::add(&profile::RESHAPE, t);
    out
}

fn split_if_needed_inner(ctx: &Ctx, mut node: Node) -> Result<Vec<Node>> {
    fix_undersized_children(ctx, &mut node)?;
    if node.segment_count() > ctx.cfg.max_segments {
        flush_if_necessary(ctx, &mut node, true)?;
        fix_undersized_children(ctx, &mut node)?;
    }
    let too_many_pivots = node.pivots.len() > ctx.cfg.max_pivots;
    if !too_many_pivots && node.segment_count() <= ctx.cfg.max_segments {
        return Ok(vec![node]);
    }
    if node.pivots.len() < 2 * ctx.cfg.min_pivots.max(1) {
        compact_levels(ctx, &mut node)?;
        return Ok(vec![node]);
    }
    // Usually two halves; more when one flush grew the node by more than
    // `max_pivots`, so every piece ends up viable.
    let n = node.pivots.len();
    let parts = n.div_ceil(ctx.cfg.max_pivots).max(2);
    let mut pieces = Vec::with_capacity(parts);
    let mut rest = node;
    for k in (1..parts).rev() {
        // Split off the last piece of `k + 1` balanced pieces.
        let remaining = rest.pivots.len();
        let (left, right) = split_node(&rest, remaining - remaining / (k + 1));
        pieces.push(right);
        rest = left;
    }
    pieces.push(rest);
    pieces.reverse();
    for piece in &mut pieces {
        if piece.segment_count() > ctx.cfg.max_segments {
            compact_levels(ctx, piece)?;
        }
    }
    Ok(pieces)
}

/// Splits `node` before pivot `s`. Pages that straddle the split key are
/// shared, each half clipping them to its own range.
fn split_node(node: &Node, s: usize) -> (Node, Node) {
    let m = node.pivots[s].clone();
    let mut left_levels = Vec::with_capacity(node.levels.len());
    let mut right_levels = Vec::with_capacity(node.levels.len());
    for level in &node.levels {
        let mut l = Vec::new();
        let mut r = Vec::new();
        for seg in level {
            if seg.overlaps(&node.pivots[0], Some(&m)) {
                let mut c = seg.clone();
                if c.hi.as_ref().is_none_or(|h| *h > m) {
                    c.hi = Some(m.clone());
                }
                c.dead = c.dead.clip(&c.lo, c.hi.as_deref());
                if !c.is_dead() {
                    l.push(c);
                }
            }
            if seg.overlaps(&m, node.hi.as_deref()) {
                let mut c = seg.clone();
                if c.lo < m {
                    c.set_lo(m.clone());
                }
                c.dead = c.dead.clip(&c.lo, c.hi.as_deref());
                if !c.is_dead() {
                    r.push(c);
                }
            }
        }
        left_levels.push(l);
        right_levels.push(r);
    }
    let left = Node {
        height: node.height,
        pivots: node.pivots[..s].to_vec(),
        hi: Some(m),
        children: node.children[..s].to_vec(),
        pending: node.pending[..s].to_vec(),
        levels: left_levels,
        ptr: OnceLock::new(),
    };
    let right = Node {
        height: node.height,
        pivots: node.pivots[s..].to_vec(),
        hi: node.hi.clone(),
        children: node.children[s..].to_vec(),
        pending: node.pending[s..].to_vec(),
        levels: right_levels,
        ptr: OnceLock::new(),
    };
    (left, right)
}

/// Merges every buffer level into one run in the deepest level.
fn compact_levels(ctx: &Ctx, node: &mut Node) -> Result<()> {
    let runs: Vec<Run> = node.levels.iter().map(|l| Run::Level(l)).collect();
    let pages = merge_runs(ctx, &runs, &node.pivots[0], node.hi.as_deref(), false)?;
    for level in &mut node.levels {
        level.clear();
    }
    let last = node.levels.len() - 1;
    node.levels[last] = pages.into_iter().map(Segment::whole).collect();
    Ok(())
}

fn new_root(ctx: &Ctx, pieces: Vec<Piece>) -> Node {
    let height = pieces[0].1.height() + 1;
    let n = pieces.len();
    let (mut pivots, children): (Vec<Bytes>, Vec<Subtree>) = pieces.into_iter().unzip();
    pivots[0] = Bytes::new();
    Node {
        height,
        pivots,
        hi: None,
        children,
        pending: vec![0; n],
        levels: vec![Vec::new(); ctx.cfg.max_levels],
        ptr: OnceLock::new(),
    }
}

/// Applies one sorted batch to the tree rooted at `root` and returns the new
/// root. The old tree is unchanged.
pub fn apply_batch(ctx: &Ctx, root: Option<&Subtree>, batch: Arc<DataPage>) -> Result<Option<Subtree>> {
    if batch.meta.n_items == 0 {
        return Ok(root.cloned());
    }
    let batch = vec![Segment::whole(batch)];
    let mut pieces: Vec<Piece> = match root {
        None => leaf_pieces(leaf_apply(ctx, None, &batch)?, &Bytes::new()),
        Some(Subtree::Leaf(leaf)) => leaf_pieces(leaf_apply(ctx, Some(leaf), &batch)?, &Bytes::new()),
        Some(Subtree::Node(n)) => {
            let mut n = (**n).clone();
            node_apply(ctx, &mut n, batch)?;
            split_if_needed(ctx, n)?
                .into_iter()
                .map(|n| (n.pivots[0].clone(), Subtree::Node(Arc::new(n))))
                .collect()
        }
    };
    loop {
        match pieces.len() {
            0 => return Ok(None),
            1 => break,
            _ => {
                let root = new_root(ctx, pieces);
                pieces = split_if_needed(ctx, root)?
                    .into_iter()
                    .map(|n| (n.pivots[0].clone(), Subtree::Node(Arc::new(n))))
                    .collect();
            }
        }
    }
    let (_, mut root) = pieces.pop().unwrap();
    // A root with a single child sheds its buffer into that child, then
    // gives way to it.
    for round in 0.. {
        assert!(round < 1_000_000, "root shrink is not making progress");
        match root {
            Subtree::Leaf(p) if p.meta.n_items == 0 => return Ok(None),
            Subtree::Node(n) if n.children.len() == 1 => {
                if n.buffer_is_empty() {
                    root = n.children[0].clone();
                    continue;
                }
                let mut n = (*n).clone();
                flush_to_pivot(ctx, &mut n, 0)?;
                fix_undersized_children(ctx, &mut n)?;
                if n.children.len() > 1 {
                    let mut out = split_if_needed(ctx, n)?;
                    if out.len() == 1 {
                        return Ok(Some(Subtree::Node(Arc::new(out.pop().unwrap()))));
                    }
                    let pieces = out.into_iter().map(|n| (n.pivots[0].clone(), Subtree::Node(Arc::new(n)))).collect();
                    return Ok(Some(Subtree::Node(Arc::new(new_root(ctx, pieces)))));
                }
                root = Subtree::Node(Arc::new(n));
            }
            other => return Ok(Some(other)),
        }
    }
    unreachable!()
}

/// Looks `key` up in one page.
fn lookup_page(io: &PageIo, page: &DataPage, key: &[u8]) -> Result<Option<Value>> {
    let Some(b) = page.meta.find_block(key) else { return Ok(None) };
    let block = io.block(page, b)?;
    Ok(match block.search(key) {
        Ok(i) => Some(match block.value_bytes(i) {
            Some(v) => Value::Put(v),
            None => Value::Delete,
        }),
        Err(_) => None,
    })
}

/// The newest state of `key`, whose [`crate::bloom::hash_key`] is `hash`, in the
/// tree: `Some(Value::Delete)` if a buffered tombstone hides it, `None` if
/// the tree has no entry for it.
pub fn get(io: &PageIo, root: &Subtree, key: &[u8], hash: u64) -> Result<Option<Value>> {
    let prefix = key_prefix(key);
    let mut cur = root;
    loop {
        match cur {
            Subtree::Leaf(page) => {
                if !page.meta.may_contain(hash) {
                    return Ok(None);
                }
                return lookup_page(io, page, key);
            }
            Subtree::Node(n) => {
                for level in &n.levels {
                    let i = level.partition_point(|s| s.starts_by(key, prefix));
                    let Some(seg) = i.checked_sub(1).map(|i| &level[i]) else { continue };
                    if seg.covers_key(key)
                        && !seg.dead.contains(key)
                        && seg.page.meta.may_contain(hash)
                        && let Some(v) = lookup_page(io, &seg.page, key)?
                    {
                        return Ok(Some(v));
                    }
                }
                cur = &n.children[n.find_pivot(key)];
            }
        }
    }
}

/// The start of a key range that ends at `hi` (or at the end of the key
/// space) and covers about `blocks` leaf blocks: the first key of the
/// `blocks`th block back from the one holding the greatest key below `hi`,
/// or the lower bound of the leaf's range when the leaf holds fewer blocks
/// below `hi`. Reverse iteration reads the range before `hi` one such window
/// at a time.
pub fn window_start(root: &Subtree, hi: Option<&[u8]>, blocks: usize) -> Bytes {
    let mut cur = root;
    let mut lo = Bytes::new();
    loop {
        match cur {
            Subtree::Node(n) => {
                // The child holding the greatest key below `hi`.
                let i = match hi {
                    None => n.children.len() - 1,
                    Some(h) => n.pivots[1..].partition_point(|p| cmp_keys(p, h).is_lt()),
                };
                lo = n.pivots[i].clone();
                cur = &n.children[i];
            }
            Subtree::Leaf(page) => {
                let meta = &page.meta;
                let below = match hi {
                    None => meta.n_blocks as usize,
                    Some(h) => meta.blocks_before(h),
                };
                return match below.checked_sub(blocks.max(1)) {
                    Some(b) if meta.n_items > 0 => Bytes::copy_from_slice(meta.block_first_key(b)).max(lo),
                    _ => lo,
                };
            }
        }
    }
}

/// Iterates live entries of a tree in key order from a start key, one leaf's
/// key range at a time: each range merges the leaf with the buffer levels of
/// every node on its root-to-leaf path.
pub struct TreeScan {
    root: Option<Subtree>,
    end: Option<Bytes>,
    merger: Option<Merger>,
    /// Start of the next leaf range, or `None` when this is the last one.
    next_start: Option<Bytes>,
    /// Entries the caller expects to read, which decides how far ahead in
    /// the leaf a cold range prefetches.
    expected: usize,
}

impl TreeScan {
    pub fn new(root: Option<Subtree>, start: &[u8], end: Option<Bytes>) -> Self {
        Self { root, end, merger: None, next_start: Some(Bytes::copy_from_slice(start)), expected: usize::MAX }
    }

    /// Sets how many entries the caller expects to read.
    pub fn expect(&mut self, entries: usize) {
        self.expected = entries;
    }

    fn open_range(&mut self, io: &PageIo, start: Bytes) -> Result<()> {
        let Some(root) = &self.root else {
            self.next_start = None;
            return Ok(());
        };
        let mut hi = self.end.clone();
        let mut path: Vec<&Node> = Vec::new();
        let mut cur = root;
        let leaf = loop {
            match cur {
                Subtree::Leaf(p) => break p,
                Subtree::Node(n) => {
                    let i = n.find_pivot(&start);
                    if let Some(h) = n.pivot_hi(i)
                        && hi.as_ref().is_none_or(|e| h < e)
                    {
                        hi = Some(h.clone());
                    }
                    path.push(n);
                    cur = &n.children[i];
                }
            }
        };
        // Each source reads its first block as it opens. Starting the cold
        // ones together makes a cold range cost about one read latency
        // rather than one per source.
        if io.should_prefetch() {
            let mut first_blocks: Vec<(&DataPage, usize)> = Vec::new();
            for n in &path {
                for level in &n.levels {
                    let first = level.iter().find_map(|s| {
                        let key = live_start(s, &start).filter(|k| hi.as_deref().is_none_or(|h| *k < h))?;
                        s.page.meta.find_block(key).map(|b| (&*s.page, b))
                    });
                    first_blocks.extend(first);
                }
            }
            if leaf.meta.n_items > 0
                && let Some(b) = leaf.meta.find_block(leaf.meta.min_key().max(&start[..]))
            {
                first_blocks.push((leaf, b));
                // The leaf holds most of a range's entries, so a scan that
                // expects more than about half a block likely reads the next.
                let next = b + 1;
                if next < leaf.meta.n_blocks as usize
                    && self.expected > leaf.meta.block_items(b) / 2
                    && hi.as_deref().is_none_or(|h| leaf.meta.block_first_key(next) < h)
                {
                    first_blocks.push((leaf, next));
                }
            }
            io.prefetch(&first_blocks);
        }
        let mut sources: Vec<Source> = Vec::new();
        for n in &path {
            for level in &n.levels {
                sources.push(Source::Level(LevelCursor::new(io, level, &start, hi.as_deref(), ReadMode::Cached)?));
            }
        }
        sources.push(Source::Page(PageCursor::new(io, leaf.clone(), &start, hi.clone(), KeyRanges::new(), ReadMode::Cached)?));
        self.merger = Some(Merger::new(sources));
        self.next_start = match (&hi, &self.end) {
            (Some(h), Some(e)) if h >= e => None,
            (Some(h), _) => Some(h.clone()),
            (None, _) => None,
        };
        Ok(())
    }

    /// Moves to the next live entry, skipping tombstones and opening leaf
    /// ranges as needed. At a live entry, does nothing.
    pub fn settle(&mut self, io: &PageIo) -> Result<()> {
        loop {
            if let Some(m) = self.merger.as_mut() {
                match m.peek() {
                    Some(e) if e.value.is_some() => return Ok(()),
                    Some(_) => {
                        m.advance(io)?;
                        continue;
                    }
                    None => self.merger = None,
                }
            }
            match self.next_start.take() {
                Some(start) => self.open_range(io, start)?,
                None => return Ok(()),
            }
        }
    }

    /// The current live entry, after [`TreeScan::settle`].
    #[inline]
    pub fn peek(&self) -> Option<(&[u8], &[u8])> {
        let e = self.merger.as_ref()?.peek()?;
        Some((e.key, e.value?))
    }

    /// The current entry as slices sharing its block's buffer.
    pub fn peek_owned(&self) -> Option<(Bytes, Bytes)> {
        match self.merger.as_ref()?.peek_owned()? {
            (k, Some(v)) => Some((k, v)),
            (_, None) => None,
        }
    }

    /// Moves past the current entry. Call [`TreeScan::settle`] before the
    /// next peek.
    pub fn advance(&mut self, io: &PageIo) -> Result<()> {
        match self.merger.as_mut() {
            Some(m) => m.advance(io),
            None => Ok(()),
        }
    }

    /// The next live entry.
    pub fn next(&mut self, io: &PageIo) -> Result<Option<(Bytes, Bytes)>> {
        self.settle(io)?;
        let out = self.peek_owned();
        self.advance(io)?;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Persistence

const NODE_MAGIC: u32 = 0x4e4b_5654;
const KIND_LEAF: u8 = 0;
const KIND_NODE: u8 = 1;

fn subtree_ptr(t: &Subtree) -> (u8, PagePtr) {
    match t {
        Subtree::Leaf(p) => (KIND_LEAF, p.ptr().expect("child written before parent")),
        Subtree::Node(n) => (KIND_NODE, n.ptr().expect("child written before parent")),
    }
}

/// Node page: `magic: u32 | crc: u32 | id: u64 | payload`, where the checksum
/// covers the id and payload.
fn encode_node(node: &Node, id: PageId) -> Vec<u8> {
    let mut out = Vec::with_capacity(4096);
    out.extend_from_slice(&NODE_MAGIC.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&node.height.to_le_bytes());
    out.extend_from_slice(&(node.pivots.len() as u32).to_le_bytes());
    for p in &node.pivots {
        put_bytes(&mut out, p);
    }
    match &node.hi {
        None => out.push(0),
        Some(h) => {
            out.push(1);
            put_bytes(&mut out, h);
        }
    }
    for (child, pending) in node.children.iter().zip(&node.pending) {
        let (kind, ptr) = subtree_ptr(child);
        out.push(kind);
        ptr.encode(&mut out);
        out.extend_from_slice(&pending.to_le_bytes());
    }
    out.extend_from_slice(&(node.levels.len() as u32).to_le_bytes());
    for level in &node.levels {
        out.extend_from_slice(&(level.len() as u32).to_le_bytes());
        for seg in level {
            seg.page.ptr().expect("segment written before node").encode(&mut out);
            put_bytes(&mut out, &seg.lo);
            match &seg.hi {
                None => out.push(0),
                Some(h) => {
                    out.push(1);
                    put_bytes(&mut out, h);
                }
            }
            seg.dead.encode(&mut out);
        }
    }
    let crc = crc32c::crc32c(&out[8..]);
    out[4..8].copy_from_slice(&crc.to_le_bytes());
    out
}

struct DecodedNode {
    height: u32,
    pivots: Vec<Bytes>,
    hi: Option<Bytes>,
    children: Vec<(u8, PagePtr, u64)>,
    levels: Vec<Vec<(PagePtr, Bytes, Option<Bytes>, KeyRanges)>>,
}

fn decode_node(raw: &Bytes, id: PageId) -> Result<DecodedNode> {
    let bad = || corruption(format!("node page {id} is malformed"));
    if raw.len() < 16 || u32::from_le_bytes(raw[0..4].try_into().unwrap()) != NODE_MAGIC {
        return Err(bad());
    }
    if u32::from_le_bytes(raw[4..8].try_into().unwrap()) != crc32c::crc32c(&raw[8..]) {
        return Err(corruption(format!("node page {id} checksum mismatch")));
    }
    if u64::from_le_bytes(raw[8..16].try_into().unwrap()) != id {
        return Err(corruption(format!("expected node page {id}")));
    }
    let body = raw.slice(16..);
    let mut r = Reader::new(&body);
    let parse = |r: &mut Reader| -> Option<DecodedNode> {
        let height = r.u32()?;
        let n = r.u32()? as usize;
        let mut pivots = Vec::with_capacity(n);
        for _ in 0..n {
            pivots.push(r.bytes()?);
        }
        let hi = match r.u8()? {
            0 => None,
            1 => Some(r.bytes()?),
            _ => return None,
        };
        let mut children = Vec::with_capacity(n);
        for _ in 0..n {
            let kind = r.u8()?;
            let ptr = PagePtr::decode(r.raw(PagePtr::ENCODED_LEN)?)?;
            children.push((kind, ptr, r.u64()?));
        }
        let nl = r.u32()? as usize;
        let mut levels = Vec::with_capacity(nl);
        for _ in 0..nl {
            let ns = r.u32()? as usize;
            let mut level = Vec::with_capacity(ns);
            for _ in 0..ns {
                let ptr = PagePtr::decode(r.raw(PagePtr::ENCODED_LEN)?)?;
                let lo = r.bytes()?;
                let hi = match r.u8()? {
                    0 => None,
                    1 => Some(r.bytes()?),
                    _ => return None,
                };
                level.push((ptr, lo, hi, KeyRanges::decode(r)?));
            }
            levels.push(level);
        }
        (r.is_empty() && n > 0).then_some(DecodedNode { height, pivots, hi, children, levels })
    };
    parse(&mut r).ok_or_else(bad)
}

/// Writes every unwritten page of the tree: data pages first (in parallel when
/// configured), then nodes bottom-up. Returns whether an arena file was
/// created, which requires a directory sync before the checkpoint commits.
pub fn write_tree(ctx: &Ctx, root: &Subtree) -> Result<bool> {
    let mut pages: Vec<Arc<DataPage>> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    collect_unwritten(root, &mut pages, &mut seen);
    let created = write_pages(ctx, &pages)?;
    Ok(write_nodes(ctx, root)? | created)
}

/// The unwritten data pages below the root's own buffer: those of every
/// child subtree. A later batch of the same checkpoint rewrites the root's
/// buffer far more often than anything below it.
pub fn unwritten_below_root(root: &Subtree) -> Vec<Arc<DataPage>> {
    let mut pages = Vec::new();
    if let Subtree::Node(n) = root {
        let mut seen = std::collections::HashSet::new();
        for c in &n.children {
            collect_unwritten(c, &mut pages, &mut seen);
        }
    }
    pages
}

/// Writes data pages, in parallel when configured. Returns whether an
/// arena file was created.
pub fn write_pages(ctx: &Ctx, pages: &[Arc<DataPage>]) -> Result<bool> {
    Ok(if ctx.cfg.parallel {
        // Buffered writes to one file serialize on its inode lock, so write
        // each arena file from one thread.
        use rayon::prelude::*;
        let mut by_class: std::collections::BTreeMap<u8, Vec<&Arc<DataPage>>> = Default::default();
        for p in pages {
            let class = crate::pagestore::class_for(p.meta.image_len()).unwrap_or(0);
            by_class.entry(class).or_default().push(p);
        }
        let groups: Vec<Vec<&Arc<DataPage>>> = by_class.into_values().collect();
        let write_group = |group: &Vec<&Arc<DataPage>>| -> Result<bool> {
            let mut any = false;
            for p in group {
                any |= ctx.io.write(p)?;
            }
            Ok(any)
        };
        groups.par_iter().map(write_group).collect::<Result<Vec<bool>>>()?.into_iter().any(|c| c)
    } else {
        let mut any = false;
        for p in pages {
            any |= ctx.io.write(p)?;
        }
        any
    })
}

fn collect_unwritten(t: &Subtree, out: &mut Vec<Arc<DataPage>>, seen: &mut std::collections::HashSet<PageId>) {
    match t {
        Subtree::Leaf(p) => {
            if !p.is_written() && seen.insert(p.id()) {
                out.push(p.clone());
            }
        }
        Subtree::Node(n) => {
            if n.ptr().is_some() {
                return;
            }
            for seg in n.levels.iter().flatten() {
                if !seg.page.is_written() && seen.insert(seg.page.id()) {
                    out.push(seg.page.clone());
                }
            }
            for c in &n.children {
                collect_unwritten(c, out, seen);
            }
        }
    }
}

fn write_nodes(ctx: &Ctx, t: &Subtree) -> Result<bool> {
    let Subtree::Node(n) = t else { return Ok(false) };
    if n.ptr().is_some() {
        return Ok(false);
    }
    let mut created = false;
    for c in &n.children {
        created |= write_nodes(ctx, c)?;
    }
    let id = ctx.new_page_id();
    let raw = encode_node(n, id);
    let ptr = ctx.io.store.allocate(id, raw.len() as u64, raw.len() as u32)?;
    created |= ctx.io.store.write(&ptr, &raw)?;
    n.ptr.set(ptr).expect("node written once");
    Ok(created)
}

pub fn root_ptr(t: &Subtree) -> Option<(PagePtr, u32)> {
    let ptr = match t {
        Subtree::Leaf(p) => p.ptr()?,
        Subtree::Node(n) => n.ptr()?,
    };
    Some((ptr, t.height()))
}

/// Reads a checkpointed tree: every node page and the metadata region of
/// every data page. Data blocks stay on disk.
pub fn load_tree(io: &PageIo, root: (PagePtr, u32)) -> Result<Subtree> {
    let mut pages: HashMap<PageId, Arc<DataPage>> = HashMap::new();
    load_subtree(io, root.0, root.1, &mut pages)
}

fn load_data_page(io: &PageIo, ptr: PagePtr, pages: &mut HashMap<PageId, Arc<DataPage>>) -> Result<Arc<DataPage>> {
    if let Some(p) = pages.get(&ptr.id) {
        if p.ptr() != Some(ptr) {
            return Err(corruption(format!("page {} referenced at two locations", ptr.id)));
        }
        return Ok(p.clone());
    }
    let mut meta = io.load_meta(&ptr)?;
    meta.detach();
    let page = Arc::new(DataPage::written(meta, ptr));
    pages.insert(ptr.id, page.clone());
    Ok(page)
}

fn load_subtree(
    io: &PageIo,
    ptr: PagePtr,
    height: u32,
    pages: &mut HashMap<PageId, Arc<DataPage>>,
) -> Result<Subtree> {
    if height == 0 {
        return Ok(Subtree::Leaf(load_data_page(io, ptr, pages)?));
    }
    let raw = io.store.read(&ptr, 0, ptr.len as u64)?;
    let d = decode_node(&raw, ptr.id)?;
    if d.height != height {
        return Err(corruption(format!("node {} has height {} where {height} was expected", ptr.id, d.height)));
    }
    let mut children = Vec::with_capacity(d.children.len());
    let mut pending = Vec::with_capacity(d.children.len());
    for (kind, child, p) in d.children {
        let expected = if height == 1 { KIND_LEAF } else { KIND_NODE };
        if kind != expected {
            return Err(corruption(format!("node {} has a child of the wrong kind", ptr.id)));
        }
        children.push(load_subtree(io, child, height - 1, pages)?);
        pending.push(p);
    }
    let mut levels = Vec::with_capacity(d.levels.len());
    for level in d.levels {
        let mut segs = Vec::with_capacity(level.len());
        for (p, lo, hi, dead) in level {
            segs.push(Segment::new(load_data_page(io, p, pages)?, lo, hi, dead));
        }
        levels.push(segs);
    }
    let node = Node { height, pivots: d.pivots, hi: d.hi, children, pending, levels, ptr: OnceLock::new() };
    node.ptr.set(ptr).unwrap();
    Ok(Subtree::Node(Arc::new(node)))
}

/// Every page location the tree references.
pub fn reachable(t: &Subtree, out: &mut std::collections::HashSet<PagePtr>) {
    match t {
        Subtree::Leaf(p) => {
            out.insert(p.ptr().expect("reachable pages are written"));
        }
        Subtree::Node(n) => {
            if !out.insert(n.ptr().expect("reachable nodes are written")) {
                return;
            }
            for seg in n.levels.iter().flatten() {
                out.insert(seg.page.ptr().expect("reachable pages are written"));
            }
            for c in &n.children {
                reachable(c, out);
            }
        }
    }
}

/// Every data page id referenced by the tree, with its block count.
pub fn data_pages(t: &Subtree, out: &mut HashMap<PageId, u32>) {
    match t {
        Subtree::Leaf(p) => {
            out.insert(p.id(), p.meta.n_blocks);
        }
        Subtree::Node(n) => {
            for seg in n.levels.iter().flatten() {
                out.insert(seg.page.id(), seg.page.meta.n_blocks);
            }
            for c in &n.children {
                data_pages(c, out);
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TreeStats {
    pub height: u32,
    pub nodes: u64,
    pub leaves: u64,
    pub segments: u64,
    pub leaf_bytes: u64,
    pub buffer_bytes: u64,
    pub filter_bytes: u64,
    pub index_bytes: u64,
}

pub fn stats(t: &Subtree) -> TreeStats {
    let mut s = TreeStats { height: t.height(), ..Default::default() };
    let mut seen = std::collections::HashSet::new();
    fn walk(t: &Subtree, s: &mut TreeStats, seen: &mut std::collections::HashSet<PageId>) {
        match t {
            Subtree::Leaf(p) => {
                s.leaves += 1;
                s.leaf_bytes += p.data_bytes();
                s.filter_bytes += p.meta.filter_bytes() as u64;
                s.index_bytes += p.meta.len() as u64;
            }
            Subtree::Node(n) => {
                s.nodes += 1;
                for seg in n.levels.iter().flatten() {
                    s.segments += 1;
                    if seen.insert(seg.page.id()) {
                        s.buffer_bytes += seg.page.data_bytes();
                        s.filter_bytes += seg.page.meta.filter_bytes() as u64;
                        s.index_bytes += seg.page.meta.len() as u64;
                    }
                }
                for c in &n.children {
                    walk(c, s, seen);
                }
            }
        }
    }
    walk(t, &mut s, &mut seen);
    s
}

/// Checks structural invariants of the whole tree, panicking on violation.
/// Reads every page, so it is meant for tests and simulation.
pub fn check_invariants(io: &PageIo, cfg: &TreeConfig, root: &Subtree) -> Result<()> {
    check_subtree(io, cfg, root, &Bytes::new(), None, true)
}

fn check_page_sorted(io: &PageIo, page: &Arc<DataPage>, tombstones_ok: bool) -> Result<()> {
    let mut c = PageCursor::new(io, page.clone(), b"", None, KeyRanges::new(), ReadMode::Span(u64::MAX))?;
    let mut prev: Option<Vec<u8>> = None;
    let mut n = 0u32;
    while let Some(e) = c.peek() {
        assert!(prev.as_deref().is_none_or(|p| p < e.key), "page {} keys out of order", page.id());
        assert!(tombstones_ok || e.value.is_some(), "leaf {} holds a tombstone", page.id());
        prev = Some(e.key.to_vec());
        n += 1;
        c.advance(io)?;
    }
    assert_eq!(n, page.meta.n_items, "page {} item count", page.id());
    Ok(())
}

fn check_subtree(io: &PageIo, cfg: &TreeConfig, t: &Subtree, lo: &Bytes, hi: Option<&Bytes>, is_root: bool) -> Result<()> {
    match t {
        Subtree::Leaf(p) => {
            check_page_sorted(io, p, false)?;
            if p.meta.n_items > 0 {
                assert!(p.meta.min_key() >= &lo[..], "leaf {} below its range", p.id());
                assert!(hi.is_none_or(|h| p.meta.max_key() < &h[..]), "leaf {} above its range", p.id());
            }
            assert!(p.data_bytes() <= 2 * cfg.leaf_size + 2 * crate::page::BLOCK_SIZE as u64, "leaf {} oversized", p.id());
        }
        Subtree::Node(n) => {
            let k = n.pivots.len();
            assert!(k >= 1 && k == n.children.len() && k == n.pending.len());
            assert!(k <= cfg.max_pivots, "node has {k} pivots");
            assert!(is_root || k >= 2 || n.height >= 1);
            assert_eq!(&n.pivots[0], lo, "node lower bound");
            assert_eq!(n.hi.as_ref(), hi, "node upper bound");
            assert_eq!(n.levels.len(), cfg.max_levels);
            for w in n.pivots.windows(2) {
                assert!(w[0] < w[1], "pivots out of order");
            }
            if let Some(h) = hi {
                assert!(n.pivots[k - 1] < *h);
            }
            for (li, level) in n.levels.iter().enumerate() {
                for (si, seg) in level.iter().enumerate() {
                    seg.dead.check();
                    assert!(!seg.is_dead(), "dead segment kept in level {li}");
                    assert!(!seg.overlaps(b"", Some(lo)), "segment {} overlaps keys below node", seg.page.id());
                    if let Some(h) = hi {
                        assert!(!seg.overlaps(h, None), "segment {} overlaps keys above node", seg.page.id());
                    }
                    if si > 0 {
                        let prev = &level[si - 1];
                        let prev_max = prev.page.meta.max_key();
                        let disjoint = prev_max < seg.min_key() || prev.hi.as_ref().is_some_and(|h| &h[..] <= seg.min_key());
                        assert!(disjoint, "level {li} segments overlap");
                    }
                    check_page_sorted(io, &seg.page, true)?;
                }
            }
            for (i, child) in n.children.iter().enumerate() {
                assert_eq!(child.height() + 1, n.height, "unbalanced tree");
                check_subtree(io, cfg, child, &n.pivots[i], n.pivot_hi(i), false)?;
            }
        }
    }
    Ok(())
}

/// Builds a page from sorted `(key, value)` pairs; for tests and batching.
pub fn page_from_entries<'a>(
    id: PageId,
    entries: impl IntoIterator<Item = EntryRef<'a>>,
    filter_bits: u32,
) -> PageImage {
    let mut b = PageBuilder::new(id);
    for e in entries {
        b.push(e);
    }
    b.finish(filter_bits)
}

#[cfg(test)]
mod tests;
