//! Ordered iteration over pages, buffer levels, and merges of them.
//!
//! Merging is the checkpoint's inner loop, so cursors decode each entry once
//! when they reach it, sources dispatch statically, and short keys compare as
//! integers.

use super::Segment;
use super::io::{DataPage, PageIo};
use super::ranges::KeyRanges;
use crate::error::Result;
use crate::page::{Block, EntryRef};
use bytes::Bytes;
use std::cmp::Ordering;
use std::sync::Arc;

/// Compares keys, taking the first eight bytes as one integer when both
/// keys have them.
#[inline]
pub fn cmp_keys(a: &[u8], b: &[u8]) -> Ordering {
    if a.len() >= 8 && b.len() >= 8 {
        let x = u64::from_be_bytes(a[..8].try_into().unwrap());
        let y = u64::from_be_bytes(b[..8].try_into().unwrap());
        if x != y {
            return x.cmp(&y);
        }
    }
    a.cmp(b)
}

#[inline]
fn keys_equal(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && cmp_keys(a, b) == Ordering::Equal
}

/// How a cursor fetches blocks: one at a time through the cache (point and
/// scan reads), or in spans of up to this many bytes straight from disk
/// (merges).
#[derive(Clone, Copy, Debug)]
pub enum ReadMode {
    Cached,
    Span(u64),
}

/// The live entries of one page within `[lo, hi)`, skipping keys in `dead`.
pub struct PageCursor {
    page: Arc<DataPage>,
    hi: Option<Bytes>,
    dead: KeyRanges,
    /// Index of the first dead range that ends after the current key.
    dead_next: usize,
    mode: ReadMode,
    /// Loaded run of blocks: its bytes, the blocks it covers, and whether it
    /// came from disk.
    span: Bytes,
    span_blocks: std::ops::Range<usize>,
    span_from_disk: bool,
    block_idx: usize,
    block: Option<Block>,
    entry: usize,
    /// The current entry's offsets in the block, decoded once.
    cur: Option<(usize, usize, Option<(usize, usize)>)>,
}

impl PageCursor {
    pub fn new(
        io: &PageIo,
        page: Arc<DataPage>,
        lo: &[u8],
        hi: Option<Bytes>,
        dead: KeyRanges,
        mode: ReadMode,
    ) -> Result<Self> {
        let mut c = Self {
            page,
            hi,
            dead,
            dead_next: 0,
            mode,
            span: Bytes::new(),
            span_blocks: 0..0,
            span_from_disk: false,
            block_idx: 0,
            block: None,
            entry: 0,
            cur: None,
        };
        c.seek(io, lo)?;
        Ok(c)
    }

    fn load_block(&mut self, io: &PageIo, b: usize) -> Result<()> {
        let meta = &self.page.meta;
        let block = match self.mode {
            ReadMode::Cached => io.block(&self.page, b)?,
            ReadMode::Span(limit) => {
                if !self.span_blocks.contains(&b) {
                    let hi = self.hi.as_deref();
                    let end_block = meta.blocks_for_range(meta.block_first_key(b), hi).end.max(b + 1);
                    let mut end = b + 1;
                    let mut bytes = meta.block(b).len as u64;
                    while end < end_block && bytes + meta.block(end).len as u64 <= limit {
                        bytes += meta.block(end).len as u64;
                        end += 1;
                    }
                    (self.span, self.span_from_disk) = io.span(&self.page, b..end)?;
                    self.span_blocks = b..end;
                }
                let base = meta.block(self.span_blocks.start).offset;
                let info = meta.block(b);
                let start = (info.offset - base) as usize;
                let bytes = self.span.slice(start..start + info.len as usize);
                if self.span_from_disk { Block::decode(bytes, self.page.id())? } else { Block::decode_trusted(bytes)? }
            }
        };
        self.block = Some(block);
        self.block_idx = b;
        Ok(())
    }

    /// Positions at the first live entry at or after `key`, moving forward only.
    fn seek(&mut self, io: &PageIo, key: &[u8]) -> Result<()> {
        let meta = &self.page.meta;
        if meta.n_items == 0 || key > meta.max_key() {
            self.cur = None;
            return Ok(());
        }
        let b = meta.find_block(key).unwrap_or(0);
        if self.block.is_none() || b > self.block_idx {
            self.load_block(io, b)?;
            self.entry = 0;
        }
        let block = self.block.as_ref().unwrap();
        let pos = match block.search(key) {
            Ok(i) | Err(i) => i,
        };
        self.entry = pos.max(self.entry);
        self.settle(io)
    }

    /// Moves forward from the current position to a live entry below `hi`.
    fn settle(&mut self, io: &PageIo) -> Result<()> {
        loop {
            let block = self.block.as_ref().unwrap();
            if self.entry >= block.len() {
                let next = self.block_idx + 1;
                if next >= self.page.meta.n_blocks as usize
                    || self.hi.as_ref().is_some_and(|hi| self.page.meta.block_first_key(next) >= &hi[..])
                {
                    self.cur = None;
                    return Ok(());
                }
                self.load_block(io, next)?;
                self.entry = 0;
                continue;
            }
            let span = block.entry_span(self.entry);
            let key = &block.raw()[span.0..span.0 + span.1];
            if self.hi.as_ref().is_some_and(|hi| cmp_keys(key, hi) != Ordering::Less) {
                self.cur = None;
                return Ok(());
            }
            let dead = self.dead.as_slice();
            while self.dead_next < dead.len() && dead[self.dead_next].1.as_ref().is_some_and(|h| cmp_keys(h, key) != Ordering::Greater) {
                self.dead_next += 1;
            }
            match dead.get(self.dead_next) {
                Some((lo, hi)) if cmp_keys(lo, key) != Ordering::Greater => match hi {
                    // Inside a dead range: skip to its end.
                    Some(hi) => {
                        let hi = hi.clone();
                        self.cur = None;
                        return self.seek(io, &hi);
                    }
                    None => {
                        self.cur = None;
                        return Ok(());
                    }
                },
                _ => {
                    self.cur = Some(span);
                    return Ok(());
                }
            }
        }
    }

    pub fn page(&self) -> &Arc<DataPage> {
        &self.page
    }

    #[inline]
    pub fn peek(&self) -> Option<EntryRef<'_>> {
        let (k, kl, v) = self.cur?;
        let raw = self.block.as_ref()?.raw();
        Some(EntryRef { key: &raw[k..k + kl], value: v.map(|(vs, vl)| &raw[vs..vs + vl]) })
    }

    pub fn advance(&mut self, io: &PageIo) -> Result<()> {
        if self.cur.is_none() {
            return Ok(());
        }
        self.entry += 1;
        self.settle(io)
    }
}

/// The live entries of one buffer level within `[lo, hi)`. A level's segments
/// hold disjoint, increasing key ranges, so this chains page cursors.
pub struct LevelCursor {
    segments: Vec<Segment>,
    next: usize,
    lo: Bytes,
    hi: Option<Bytes>,
    mode: ReadMode,
    current: Option<PageCursor>,
}

impl LevelCursor {
    pub fn new(io: &PageIo, level: &[Segment], lo: &[u8], hi: Option<&[u8]>, mode: ReadMode) -> Result<Self> {
        let segments: Vec<Segment> = level.iter().filter(|s| s.overlaps(lo, hi)).cloned().collect();
        let mut c = Self {
            segments,
            next: 0,
            lo: Bytes::copy_from_slice(lo),
            hi: hi.map(Bytes::copy_from_slice),
            mode,
            current: None,
        };
        c.open_next(io)?;
        Ok(c)
    }

    fn open_next(&mut self, io: &PageIo) -> Result<()> {
        self.current = None;
        while self.next < self.segments.len() {
            let seg = &self.segments[self.next];
            self.next += 1;
            let lo = if seg.lo > self.lo { seg.lo.clone() } else { self.lo.clone() };
            let hi = match (&seg.hi, &self.hi) {
                (None, h) | (h, None) => h.clone(),
                (Some(a), Some(b)) => Some(if a < b { a.clone() } else { b.clone() }),
            };
            let cursor = PageCursor::new(io, seg.page.clone(), &lo, hi, seg.dead.clone(), self.mode)?;
            if cursor.peek().is_some() {
                self.current = Some(cursor);
                return Ok(());
            }
        }
        Ok(())
    }

    #[inline]
    pub fn peek(&self) -> Option<EntryRef<'_>> {
        self.current.as_ref()?.peek()
    }

    pub fn advance(&mut self, io: &PageIo) -> Result<()> {
        if let Some(c) = self.current.as_mut() {
            c.advance(io)?;
            if c.peek().is_none() {
                self.open_next(io)?;
            }
        }
        Ok(())
    }
}

/// One input of a merge.
pub enum Source {
    Page(PageCursor),
    Level(LevelCursor),
}

impl Source {
    #[inline]
    pub fn peek(&self) -> Option<EntryRef<'_>> {
        match self {
            Source::Page(c) => c.peek(),
            Source::Level(c) => c.peek(),
        }
    }

    #[inline]
    pub fn advance(&mut self, io: &PageIo) -> Result<()> {
        match self {
            Source::Page(c) => c.advance(io),
            Source::Level(c) => c.advance(io),
        }
    }
}

/// Merges sources ordered newest first: for each key, the newest source's
/// entry wins and older entries for the key are skipped.
pub struct Merger {
    sources: Vec<Source>,
    winner: Option<usize>,
}

impl Merger {
    pub fn new(sources: Vec<Source>) -> Self {
        let mut m = Self { sources, winner: None };
        m.pick();
        m
    }

    fn pick(&mut self) {
        let mut best: Option<(usize, &[u8])> = None;
        for (i, s) in self.sources.iter().enumerate() {
            if let Some(e) = s.peek()
                && best.is_none_or(|(_, k)| cmp_keys(e.key, k) == Ordering::Less)
            {
                best = Some((i, e.key));
            }
        }
        self.winner = best.map(|(i, _)| i);
    }

    #[inline]
    pub fn peek(&self) -> Option<EntryRef<'_>> {
        self.sources[self.winner?].peek()
    }

    pub fn advance(&mut self, io: &PageIo) -> Result<()> {
        let Some(w) = self.winner else { return Ok(()) };
        // Each source is strictly increasing, so at most one entry per older
        // source shares the winner's key.
        for j in 0..self.sources.len() {
            if j == w {
                continue;
            }
            let (winner, other) = if j < w {
                let (l, r) = self.sources.split_at_mut(w);
                (&r[0], &mut l[j])
            } else {
                let (l, r) = self.sources.split_at_mut(j);
                (&l[w], &mut r[0])
            };
            let same = match (other.peek(), winner.peek()) {
                (Some(o), Some(k)) => keys_equal(o.key, k.key),
                _ => false,
            };
            if same {
                other.advance(io)?;
            }
        }
        self.sources[w].advance(io)?;
        self.pick();
        Ok(())
    }

    /// True if source `i` has an entry left.
    pub fn source_has_more(&self, i: usize) -> bool {
        self.sources[i].peek().is_some()
    }

    pub fn source_count(&self) -> usize {
        self.sources.len()
    }
}
