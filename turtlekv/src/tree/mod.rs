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

use crate::bloom;
use crate::error::{Result, corruption};
use crate::page::{EntryRef, PageBuilder, PageId, PageImage};
use crate::pagestore::PagePtr;
use crate::types::Value;
use bytes::Bytes;
use codec::{Reader, put_bytes};
use cursor::{EntrySource, LevelCursor, Merger, PageCursor, ReadMode};
use io::{DataPage, PageIo};
use ranges::{KeyRanges, successor};
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
            max_levels: 6,
            filter_bits: 12,
            merge_readahead: 1 << 20,
            parallel: true,
        }
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
}

impl Segment {
    pub fn whole(page: Arc<DataPage>) -> Self {
        Self { page, lo: Bytes::new(), hi: None, dead: KeyRanges::new() }
    }

    /// Smallest key the segment may hold.
    pub fn min_key(&self) -> &[u8] {
        let m = self.page.meta.min_key();
        if &self.lo[..] > m { &self.lo } else { m }
    }

    /// Whether `key` lies in the segment's clipped key range.
    pub fn covers_key(&self, key: &[u8]) -> bool {
        key >= self.min_key() && key <= self.page.meta.max_key() && self.hi.as_ref().is_none_or(|h| key < &h[..])
    }

    /// Whether the clipped range intersects `[lo, hi)`.
    pub fn overlaps(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        self.page.meta.n_items > 0
            && self.page.meta.max_key() >= lo
            && self.hi.as_ref().is_none_or(|h| &h[..] > lo)
            && hi.is_none_or(|hi| self.min_key() < hi)
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
        self.pivots[1..].partition_point(|p| &p[..] <= key)
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

/// Merges `sources` (newest first) into pages of about `target` bytes each,
/// never over `max`.
fn build_pages(
    ctx: &Ctx,
    merger: &mut Merger,
    target: u64,
    drop_tombstones: bool,
) -> Result<Vec<Arc<DataPage>>> {
    let mut pages = Vec::new();
    let mut b = PageBuilder::new(ctx.new_page_id());
    while let Some(e) = merger.peek() {
        if !(drop_tombstones && e.value.is_none()) {
            if !b.is_empty() && b.data_bytes() + e.size() > target {
                pages.push(ctx.finish(std::mem::replace(&mut b, PageBuilder::new(ctx.new_page_id()))));
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

fn page_source(ctx: &Ctx, page: &Arc<DataPage>) -> Result<Box<dyn EntrySource + Send>> {
    Ok(Box::new(PageCursor::new(ctx.io, page.clone(), b"", None, KeyRanges::new(), ctx.merge_mode())?))
}

/// Merges `batch` into a leaf (or into nothing), dropping tombstones, and
/// splits the result into balanced leaves.
fn leaf_apply(ctx: &Ctx, leaf: Option<&Arc<DataPage>>, batch: &Arc<DataPage>) -> Result<Vec<Arc<DataPage>>> {
    let mut sources = vec![page_source(ctx, batch)?];
    let mut total = batch.data_bytes();
    if let Some(leaf) = leaf {
        sources.push(page_source(ctx, leaf)?);
        total += leaf.data_bytes();
    }
    let mut merger = Merger::new(sources);
    build_pages(ctx, &mut merger, balanced_target(total, ctx.cfg.leaf_size), true)
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

/// Adds each pivot's share of `batch` to its pending bytes.
fn add_pending(ctx: &Ctx, node: &mut Node, batch: &Arc<DataPage>) -> Result<()> {
    let mut c = PageCursor::new(ctx.io, batch.clone(), b"", None, KeyRanges::new(), ctx.merge_mode())?;
    let mut p = 0;
    while let Some(e) = c.peek() {
        while node.pivots.get(p + 1).is_some_and(|next| e.key >= &next[..]) {
            p += 1;
        }
        node.pending[p] += e.size();
        c.advance(ctx.io)?;
    }
    Ok(())
}

/// Inserts `batch` as the newest run: it merges with the occupied prefix of
/// levels and lands in the first empty level (or the last level, if all are
/// occupied).
fn buffer_insert(ctx: &Ctx, node: &mut Node, batch: Arc<DataPage>) -> Result<()> {
    let n = node.levels.len();
    let first_empty = node.levels.iter().position(Vec::is_empty).unwrap_or(n);
    if first_empty == 0 {
        node.levels[0] = vec![Segment::whole(batch)];
        return Ok(());
    }
    let target = first_empty.min(n - 1);
    let merged_levels = if first_empty == n { n } else { first_empty };
    let mut total = batch.data_bytes();
    let mut sources = vec![page_source(ctx, &batch)?];
    for level in &node.levels[..merged_levels] {
        total += level.iter().map(|s| s.page.data_bytes()).sum::<u64>();
        sources.push(Box::new(LevelCursor::new(ctx.io, level, b"", None, ctx.merge_mode())?));
    }
    let mut merger = Merger::new(sources);
    let pages = build_pages(ctx, &mut merger, balanced_target(total, ctx.cfg.leaf_size), false)?;
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

/// Moves the smallest-keyed leaf-sized prefix of pivot `p`'s buffered data
/// into child `p`.
fn flush_to_pivot(ctx: &Ctx, node: &mut Node, p: usize) -> Result<()> {
    let lo = node.pivots[p].clone();
    let hi = node.pivot_hi(p).cloned();
    let limit = ctx.cfg.leaf_size;

    let mut sources: Vec<Box<dyn EntrySource + Send>> = Vec::with_capacity(node.levels.len());
    for level in &node.levels {
        sources.push(Box::new(LevelCursor::new(ctx.io, level, &lo, hi.as_deref(), ctx.merge_mode())?));
    }
    let mut merger = Merger::new(sources);
    let mut b = PageBuilder::new(ctx.new_page_id());
    while let Some(e) = merger.peek() {
        if !b.is_empty() && b.data_bytes() + e.size() > limit {
            break;
        }
        b.push(e);
        merger.advance(ctx.io)?;
    }
    let level_has_more: Vec<bool> = (0..merger.source_count()).map(|i| merger.source_has_more(i)).collect();
    drop(merger);

    let cut = b.last_key().map(successor);
    for (level, &more) in node.levels.iter_mut().zip(&level_has_more) {
        let dead_hi = match (&cut, more) {
            (Some(cut), true) => Some(cut.clone()),
            _ => hi.clone(),
        };
        for seg in level.iter_mut() {
            if seg.overlaps(&lo, hi.as_deref()) {
                seg.dead.insert(lo.clone(), dead_hi.clone());
            }
        }
    }
    node.remove_dead_segments();
    node.pending[p] = match &cut {
        Some(cut) if level_has_more.iter().any(|&m| m) => node
            .levels
            .iter()
            .flatten()
            .filter(|s| s.overlaps(cut, hi.as_deref()))
            .map(|s| s.page.meta.estimate_bytes(cut.max(&s.lo), hi.as_deref()))
            .sum::<u64>()
            .max(1),
        _ => 0,
    };
    if b.is_empty() {
        return Ok(());
    }
    let batch = ctx.finish(b);
    let child = node.children[p].clone();
    let pieces = apply_to_child(ctx, child, &batch, &lo, hi.as_ref())?;
    replace_child(node, p, pieces);
    Ok(())
}

/// Applies `batch` to a non-root subtree covering `[lo, hi)` and returns the
/// viable pieces it became (possibly none, if a leaf emptied).
fn apply_to_child(
    ctx: &Ctx,
    child: Subtree,
    batch: &Arc<DataPage>,
    lo: &Bytes,
    hi: Option<&Bytes>,
) -> Result<Vec<Piece>> {
    match child {
        Subtree::Leaf(leaf) => Ok(leaf_pieces(leaf_apply(ctx, Some(&leaf), batch)?, lo)),
        Subtree::Node(n) => {
            debug_assert_eq!(&n.pivots[0], lo);
            debug_assert_eq!(n.hi.as_ref(), hi);
            let mut n = (*n).clone();
            node_apply(ctx, &mut n, batch.clone())?;
            Ok(split_if_needed(ctx, n)?
                .into_iter()
                .map(|n| (n.pivots[0].clone(), Subtree::Node(Arc::new(n))))
                .collect())
        }
    }
}

fn node_apply(ctx: &Ctx, node: &mut Node, batch: Arc<DataPage>) -> Result<()> {
    add_pending(ctx, node, &batch)?;
    buffer_insert(ctx, node, batch)?;
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
    while i < node.children.len() && node.children.len() >= 2 {
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
            let sources = vec![page_source(ctx, a)?, page_source(ctx, b)?];
            let mut merger = Merger::new(sources);
            let total = a.data_bytes() + b.data_bytes();
            let mut pages = build_pages(ctx, &mut merger, balanced_target(total, ctx.cfg.leaf_size), true)?;
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
fn split_if_needed(ctx: &Ctx, mut node: Node) -> Result<Vec<Node>> {
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
    let (mut left, mut right) = split_node(&node);
    for half in [&mut left, &mut right] {
        if half.segment_count() > ctx.cfg.max_segments {
            compact_levels(ctx, half)?;
        }
    }
    Ok(vec![left, right])
}

/// Splits `node` at its middle pivot. Pages that straddle the split key are
/// shared, each half clipping them to its own range.
fn split_node(node: &Node) -> (Node, Node) {
    let s = node.pivots.len() / 2;
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
                    c.lo = m.clone();
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
    let lo = node.pivots[0].clone();
    let mut sources: Vec<Box<dyn EntrySource + Send>> = Vec::new();
    let mut total = 0;
    for level in &node.levels {
        total += level.iter().map(|s| s.page.data_bytes()).sum::<u64>();
        sources.push(Box::new(LevelCursor::new(ctx.io, level, &lo, node.hi.as_deref(), ctx.merge_mode())?));
    }
    let mut merger = Merger::new(sources);
    let pages = build_pages(ctx, &mut merger, balanced_target(total, ctx.cfg.leaf_size), false)?;
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
    loop {
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

/// The newest state of `key` in the tree: `Some(Value::Delete)` if a buffered
/// tombstone hides it, `None` if the tree has no entry for it.
pub fn get(io: &PageIo, root: &Subtree, key: &[u8]) -> Result<Option<Value>> {
    let hash = bloom::hash_key(key);
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
                    let i = level.partition_point(|s| s.min_key() <= key);
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

/// Iterates live entries of a tree in key order from a start key, one leaf's
/// key range at a time: each range merges the leaf with the buffer levels of
/// every node on its root-to-leaf path.
pub struct TreeScan {
    root: Option<Subtree>,
    end: Option<Bytes>,
    merger: Option<Merger>,
    /// Start of the next leaf range, or `None` when this is the last one.
    next_start: Option<Bytes>,
}

impl TreeScan {
    pub fn new(root: Option<Subtree>, start: &[u8], end: Option<Bytes>) -> Self {
        Self { root, end, merger: None, next_start: Some(Bytes::copy_from_slice(start)) }
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
        let mut sources: Vec<Box<dyn EntrySource + Send>> = Vec::new();
        for n in &path {
            for level in &n.levels {
                sources.push(Box::new(LevelCursor::new(io, level, &start, hi.as_deref(), ReadMode::Cached)?));
            }
        }
        sources.push(Box::new(PageCursor::new(io, leaf.clone(), &start, hi.clone(), KeyRanges::new(), ReadMode::Cached)?));
        self.merger = Some(Merger::new(sources));
        self.next_start = match (&hi, &self.end) {
            (Some(h), Some(e)) if h >= e => None,
            (Some(h), _) => Some(h.clone()),
            (None, _) => None,
        };
        Ok(())
    }

    /// The next live entry, skipping tombstones.
    pub fn next(&mut self, io: &PageIo) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if let Some(m) = self.merger.as_mut() {
                if let Some(e) = m.peek() {
                    let out = e.value.map(|v| (Bytes::copy_from_slice(e.key), Bytes::copy_from_slice(v)));
                    m.advance(io)?;
                    match out {
                        Some(kv) => return Ok(Some(kv)),
                        None => continue,
                    }
                }
                self.merger = None;
            }
            match self.next_start.take() {
                Some(start) => self.open_range(io, start)?,
                None => return Ok(None),
            }
        }
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
    let created = if ctx.cfg.parallel {
        use rayon::prelude::*;
        pages.par_iter().map(|p| ctx.io.write(p)).collect::<Result<Vec<bool>>>()?.into_iter().any(|c| c)
    } else {
        let mut any = false;
        for p in &pages {
            any |= ctx.io.write(p)?;
        }
        any
    };
    Ok(write_nodes(ctx, root)? | created)
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
            segs.push(Segment { page: load_data_page(io, p, pages)?, lo, hi, dead });
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
