//! Ordered iteration over pages, buffer levels, and merges of them.

use super::Segment;
use super::io::{DataPage, PageIo};
use super::ranges::KeyRanges;
use crate::error::Result;
use crate::page::{Block, EntryRef};
use bytes::Bytes;
use std::sync::Arc;

/// A source of entries in strictly increasing key order.
pub trait EntrySource {
    fn peek(&self) -> Option<EntryRef<'_>>;
    fn advance(&mut self, io: &PageIo) -> Result<()>;
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
    mode: ReadMode,
    /// Loaded run of blocks: its bytes and the index of its first block.
    span: Bytes,
    span_blocks: std::ops::Range<usize>,
    block_idx: usize,
    block: Option<Block>,
    entry: usize,
    done: bool,
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
            mode,
            span: Bytes::new(),
            span_blocks: 0..0,
            block_idx: 0,
            block: None,
            entry: 0,
            done: false,
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
                    self.span = io.span(&self.page, b..end)?;
                    self.span_blocks = b..end;
                }
                let base = meta.block(self.span_blocks.start).offset;
                let info = meta.block(b);
                let start = (info.offset - base) as usize;
                Block::decode(self.span.slice(start..start + info.len as usize), self.page.id())?
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
            self.done = true;
            return Ok(());
        }
        let b = meta.find_block(key).unwrap_or(0);
        if self.block.is_none() || b > self.block_idx {
            self.load_block(io, b)?;
            self.entry = 0;
        }
        let block = self.block.as_ref().unwrap();
        let from = self.entry;
        let pos = match block.search(key) {
            Ok(i) | Err(i) => i,
        };
        self.entry = pos.max(from);
        self.settle(io)
    }

    /// Moves forward from the current position to a live entry below `hi`.
    fn settle(&mut self, io: &PageIo) -> Result<()> {
        loop {
            let block = self.block.as_ref().unwrap();
            if self.entry >= block.len() {
                let next = self.block_idx + 1;
                if next >= self.page.meta.n_blocks as usize {
                    self.done = true;
                    return Ok(());
                }
                if let Some(hi) = &self.hi
                    && self.page.meta.block_first_key(next) >= &hi[..]
                {
                    self.done = true;
                    return Ok(());
                }
                self.load_block(io, next)?;
                self.entry = 0;
                continue;
            }
            let key = block.entry(self.entry).key;
            if self.hi.as_ref().is_some_and(|hi| key >= &hi[..]) {
                self.done = true;
                return Ok(());
            }
            match self.dead.skip(key) {
                Ok(None) => return Ok(()),
                Ok(Some(next)) => {
                    let next = next.clone();
                    return self.seek(io, &next);
                }
                Err(()) => {
                    self.done = true;
                    return Ok(());
                }
            }
        }
    }

    pub fn page(&self) -> &Arc<DataPage> {
        &self.page
    }
}

impl EntrySource for PageCursor {
    fn peek(&self) -> Option<EntryRef<'_>> {
        if self.done {
            return None;
        }
        Some(self.block.as_ref()?.entry(self.entry))
    }

    fn advance(&mut self, io: &PageIo) -> Result<()> {
        if self.done {
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
}

impl EntrySource for LevelCursor {
    fn peek(&self) -> Option<EntryRef<'_>> {
        self.current.as_ref()?.peek()
    }

    fn advance(&mut self, io: &PageIo) -> Result<()> {
        if let Some(c) = self.current.as_mut() {
            c.advance(io)?;
            if c.peek().is_none() {
                self.open_next(io)?;
            }
        }
        Ok(())
    }
}

/// Merges sources ordered newest first: for each key, the newest source's
/// entry wins and older entries for the key are skipped.
pub struct Merger {
    sources: Vec<Box<dyn EntrySource + Send>>,
    winner: Option<usize>,
    scratch: Vec<u8>,
}

impl Merger {
    pub fn new(sources: Vec<Box<dyn EntrySource + Send>>) -> Self {
        let mut m = Self { sources, winner: None, scratch: Vec::new() };
        m.pick();
        m
    }

    fn pick(&mut self) {
        let mut best: Option<(usize, &[u8])> = None;
        for (i, s) in self.sources.iter().enumerate() {
            if let Some(e) = s.peek()
                && best.is_none_or(|(_, k)| e.key < k)
            {
                best = Some((i, e.key));
            }
        }
        self.winner = best.map(|(i, _)| i);
    }

    pub fn peek(&self) -> Option<EntryRef<'_>> {
        self.sources[self.winner?].peek()
    }

    /// Index of the source the current entry comes from.
    pub fn winner(&self) -> Option<usize> {
        self.winner
    }

    pub fn advance(&mut self, io: &PageIo) -> Result<()> {
        let Some(w) = self.winner else { return Ok(()) };
        self.scratch.clear();
        self.scratch.extend_from_slice(self.sources[w].peek().unwrap().key);
        for s in self.sources.iter_mut() {
            while s.peek().is_some_and(|e| e.key == &self.scratch[..]) {
                s.advance(io)?;
            }
        }
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
