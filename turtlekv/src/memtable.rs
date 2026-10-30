//! In-memory tables of recent writes.
//!
//! A memtable keeps every version of a key, each tagged with its sequence
//! number and its batch's ticket, and a read takes the newest version its
//! [`View`] accepts. Writers insert outside the writer lock, so a version
//! may sit in the table before its batch commits; the view decides which
//! versions a reader sees. Concurrent writers to one key may insert in any
//! order; versions stay sorted by sequence number. Four implementations share
//! the [`MemTable`] trait so the in-memory layer can be compared head to
//! head.

use crate::types::Value;
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::ops::{Bound, ControlFlow};
use std::sync::Arc;

/// Which versions a reader sees, by sequence number and batch ticket.
pub type View<'a> = &'a dyn Fn(u64, u64) -> bool;

/// The versions of one key, newest first.
#[derive(Clone, Debug)]
pub struct Versions {
    seq: u64,
    ticket: u64,
    value: Value,
    older: Option<Arc<Versions>>,
}

impl Versions {
    fn new(seq: u64, ticket: u64, value: Value) -> Self {
        Self { seq, ticket, value, older: None }
    }

    /// These versions plus the one written at `seq`.
    fn with(&self, seq: u64, ticket: u64, value: Value) -> Self {
        if seq > self.seq {
            return Self { seq, ticket, value, older: Some(Arc::new(self.clone())) };
        }
        if seq == self.seq {
            return Self { seq, ticket, value, older: self.older.clone() };
        }
        // A writer with an earlier sequence number inserted second.
        let older = match &self.older {
            Some(o) => o.with(seq, ticket, value),
            None => Self::new(seq, ticket, value),
        };
        Self { older: Some(Arc::new(older)), ..self.clone() }
    }

    /// The newest value `view` sees.
    fn at(&self, view: View) -> Option<&Value> {
        let mut v = self;
        loop {
            if view(v.seq, v.ticket) {
                return Some(&v.value);
            }
            v = v.older.as_deref()?;
        }
    }
}

pub trait MemTable: Send + Sync {
    /// Adds the version of `key` written at `seq` by batch `ticket`.
    fn insert(&self, key: &[u8], value: Value, seq: u64, ticket: u64);
    /// The newest value of `key` that `view` sees.
    fn get(&self, key: &[u8], view: View) -> Option<Value>;
    /// Appends up to `limit` keys after `start` and below `end`, in key order,
    /// with the newest values `view` sees. Keys with no such version are
    /// skipped.
    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>);
    /// Appends up to `limit` keys at or after `start` and below `end`, in
    /// descending key order, with the newest values `view` sees. Keys with
    /// no such version are skipped.
    fn range_rev(&self, start: &[u8], end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>);
    /// Visits every key's newest value in key order. Called once writers are
    /// gone.
    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value));
    /// Number of keys.
    fn len(&self) -> usize;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemTableKind {
    /// crossbeam's lock-free skip list, as in RocksDB and fjall.
    SkipList,
    /// bbtree's concurrent B+-tree with inline keys and optimistic inner nodes.
    BBTree,
    /// std's `BTreeMap` behind a reader-writer lock.
    BTree,
    /// congee's adaptive radix tree with optimistic lock coupling (ART-OLC),
    /// the index design C++ TurtleKV uses. Keys must be exactly 8 bytes.
    Art8,
}

impl std::str::FromStr for MemTableKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "skiplist" => Ok(Self::SkipList),
            "bbtree" => Ok(Self::BBTree),
            "btree" => Ok(Self::BTree),
            "art" => Ok(Self::Art8),
            _ => Err(format!("unknown memtable kind {s}")),
        }
    }
}

pub fn new_memtable(kind: MemTableKind) -> Box<dyn MemTable> {
    match kind {
        MemTableKind::SkipList => Box::new(SkipListTable::default()),
        MemTableKind::BBTree => Box::new(BBTreeTable::default()),
        MemTableKind::BTree => Box::new(BTreeTable::default()),
        MemTableKind::Art8 => Box::new(ArtTable::default()),
    }
}

fn below(key: &[u8], end: Option<&[u8]>) -> bool {
    end.is_none_or(|e| key < e)
}

/// Whether `[start, end)` holds no keys.
fn empty_range(start: &[u8], end: Option<&[u8]>) -> bool {
    end.is_some_and(|e| e <= start)
}

#[derive(Default)]
pub struct SkipListTable {
    /// Each key's versions swap in place: replacing a skip list entry removes
    /// it before inserting its successor, and a reader between the two would
    /// find the key missing.
    map: crossbeam_skiplist::SkipMap<Bytes, ArcSwap<Versions>>,
}

impl MemTable for SkipListTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64, ticket: u64) {
        let first = Arc::new(Versions::new(seq, ticket, value.clone()));
        let entry = self.map.get_or_insert(Bytes::copy_from_slice(key), ArcSwap::new(first.clone()));
        let versions = entry.value();
        if !Arc::ptr_eq(&versions.load(), &first) {
            versions.rcu(|v| v.with(seq, ticket, value.clone()));
        }
    }

    fn get(&self, key: &[u8], view: View) -> Option<Value> {
        self.map.get(key)?.value().load().at(view).cloned()
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        let visible = self.map.range::<[u8], _>((start, hi)).filter_map(|e| Some((e.key().clone(), e.value().load().at(view)?.clone())));
        out.extend(visible.take(limit));
    }

    fn range_rev(&self, start: &[u8], end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        if empty_range(start, end) {
            return;
        }
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        let visible = self
            .map
            .range::<[u8], _>((Bound::Included(start), hi))
            .rev()
            .filter_map(|e| Some((e.key().clone(), e.value().load().at(view)?.clone())));
        out.extend(visible.take(limit));
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        for e in self.map.iter() {
            f(e.key(), &e.value().load().value);
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

#[derive(Default)]
pub struct BBTreeTable {
    map: bbtree::BBTreeMap<Vec<u8>, Versions>,
}

impl MemTable for BBTreeTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64, ticket: u64) {
        let result = self.map.update_or_insert(key.to_vec(), Versions::new(seq, ticket, value), |_, stored, new| {
            *stored = stored.with(new.seq, new.ticket, new.value);
        });
        if result.is_err() {
            panic!("memtable allocation failed");
        }
    }

    fn get(&self, key: &[u8], view: View) -> Option<Value> {
        self.map.read(key, |_, v| v.at(view).cloned()).flatten()
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        if limit == 0 {
            return;
        }
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        if let (Bound::Included(s) | Bound::Excluded(s), Bound::Excluded(e)) = (start, hi)
            && s >= e
        {
            return;
        }
        let mut n = 0;
        self.map.scan::<[u8], ()>((start, hi), |k, v| {
            if let Some(value) = v.at(view) {
                out.push((Bytes::copy_from_slice(k), value.clone()));
                n += 1;
            }
            if n >= limit { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
    }

    fn range_rev(&self, start: &[u8], end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        if limit == 0 || empty_range(start, end) {
            return;
        }
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        let mut n = 0;
        self.map.scan_rev::<[u8], ()>((Bound::Included(start), hi), |k, v| {
            if let Some(value) = v.at(view) {
                out.push((Bytes::copy_from_slice(k), value.clone()));
                n += 1;
            }
            if n >= limit { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        self.map.scan::<[u8], ()>(.., |k, v| {
            f(k, &v.value);
            ControlFlow::Continue(())
        });
    }

    fn len(&self) -> usize {
        let mut n = 0;
        self.map.scan::<[u8], ()>(.., |_, _| {
            n += 1;
            ControlFlow::Continue(())
        });
        n
    }
}

#[derive(Default)]
pub struct BTreeTable {
    map: RwLock<BTreeMap<Bytes, Versions>>,
}

impl MemTable for BTreeTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64, ticket: u64) {
        let mut map = self.map.write();
        match map.get_mut(key) {
            Some(v) => *v = v.with(seq, ticket, value),
            None => {
                map.insert(Bytes::copy_from_slice(key), Versions::new(seq, ticket, value));
            }
        }
    }

    fn get(&self, key: &[u8], view: View) -> Option<Value> {
        self.map.read().get(key)?.at(view).cloned()
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        let map = self.map.read();
        let visible = map
            .range::<[u8], _>((start, Bound::Unbounded))
            .take_while(|(k, _)| below(k, end))
            .filter_map(|(k, v)| Some((k.clone(), v.at(view)?.clone())));
        out.extend(visible.take(limit));
    }

    fn range_rev(&self, start: &[u8], end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        if empty_range(start, end) {
            return;
        }
        let map = self.map.read();
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        let visible = map.range::<[u8], _>((Bound::Included(start), hi)).rev().filter_map(|(k, v)| Some((k.clone(), v.at(view)?.clone())));
        out.extend(visible.take(limit));
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        for (k, v) in self.map.read().iter() {
            f(k, &v.value);
        }
    }

    fn len(&self) -> usize {
        self.map.read().len()
    }
}

/// An ART over 8-byte keys. The tree maps each key to an index into an
/// append-only arena of version chains, so readers never see a freed chain;
/// a replaced chain stays in the arena until the memtable drops.
#[derive(Default)]
pub struct ArtTable {
    art: congee::Congee<usize, usize>,
    arena: boxcar::Vec<Versions>,
}

fn art_key(key: &[u8]) -> usize {
    u64::from_be_bytes(key.try_into().expect("the ART memtable takes 8-byte keys")) as usize
}

/// The first eight bytes of `key` as an integer, zero-padded, and whether
/// `key` runs past them.
fn art_prefix(key: &[u8]) -> (u128, bool) {
    (crate::types::key_prefix(key) as u128, key.len() > 8)
}

/// The smallest 8-byte key at or above `start` (or above it, when
/// `exclusive`), as an integer; 2^64 when there is none.
fn art_lower(start: &[u8], exclusive: bool) -> u128 {
    let (p, longer) = art_prefix(start);
    // A shorter key sorts before its zero-padded form; a longer one after.
    if longer || (exclusive && start.len() == 8) { p + 1 } else { p }
}

/// The 8-byte keys below `end` are those below the returned integer.
fn art_upper(end: Option<&[u8]>) -> u128 {
    match end {
        None => 1 << 64,
        Some(e) => {
            let (p, longer) = art_prefix(e);
            if longer { p + 1 } else { p }
        }
    }
}

impl ArtTable {
    fn visible(&self, i: usize, view: View) -> Option<Value> {
        self.arena[i].at(view).cloned()
    }
}

impl MemTable for ArtTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64, ticket: u64) {
        let k = art_key(key);
        let guard = self.art.pin();
        // Each version adds a chain to the arena, and the key moves to it only
        // if no other writer moved the key first.
        loop {
            let current = self.art.get(&k, &guard);
            let next = match current {
                Some(c) => self.arena[c].with(seq, ticket, value.clone()),
                None => Versions::new(seq, ticket, value.clone()),
            };
            let idx = self.arena.push(next);
            let mut moved = false;
            self.art
                .compute_or_insert(
                    k,
                    |old| {
                        moved = old == current;
                        if moved { idx } else { old.expect("keys are never removed") }
                    },
                    &guard,
                )
                .expect("ART allocation failed");
            if moved {
                return;
            }
        }
    }

    fn get(&self, key: &[u8], view: View) -> Option<Value> {
        let guard = self.art.pin();
        self.visible(self.art.get(&art_key(key), &guard)?, view)
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        let mut lo = match start {
            Bound::Included(k) => art_lower(k, false),
            Bound::Excluded(k) => art_lower(k, true),
            Bound::Unbounded => 0,
        };
        let hi = art_upper(end);
        let guard = self.art.pin();
        let mut buf = vec![(0usize, 0usize); limit.clamp(1, 4096)];
        let mut taken = 0;
        while taken < limit && lo < hi {
            // `range` excludes its end, so the largest key needs its own lookup.
            let to = hi.min(u64::MAX as u128) as usize;
            let n = self.art.range(&(lo as usize), &to, &mut buf, &guard);
            for &(k, i) in &buf[..n] {
                if taken < limit
                    && let Some(value) = self.visible(i, view)
                {
                    out.push((Bytes::copy_from_slice(&(k as u64).to_be_bytes()), value));
                    taken += 1;
                }
            }
            if n < buf.len() {
                if taken < limit
                    && hi > u64::MAX as u128
                    && lo <= u64::MAX as u128
                    && let Some(i) = self.art.get(&usize::MAX, &guard)
                    && let Some(value) = self.visible(i, view)
                {
                    out.push((Bytes::copy_from_slice(&u64::MAX.to_be_bytes()), value));
                }
                return;
            }
            lo = buf[n - 1].0 as u128 + 1;
        }
    }

    /// The tree only iterates forward, so this reads ever wider windows
    /// ending at `end`, which costs about twice the entries it returns.
    fn range_rev(&self, start: &[u8], end: Option<&[u8]>, view: View, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        let lo = art_lower(start, false);
        let mut hi = art_upper(end);
        let mut span: u128 = 1 << 16;
        let mut taken = 0;
        let mut window = Vec::new();
        while taken < limit && lo < hi {
            let from = hi.saturating_sub(span).max(lo);
            let to = (hi <= u64::MAX as u128).then(|| (hi as u64).to_be_bytes());
            window.clear();
            self.range(Bound::Included(&(from as u64).to_be_bytes()), to.as_ref().map(|t| &t[..]), view, usize::MAX, &mut window);
            for e in window.drain(..).rev().take(limit - taken) {
                out.push(e);
                taken += 1;
            }
            hi = from;
            span = span.saturating_mul(2);
        }
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        let mut out = Vec::new();
        let mut start = Bound::Unbounded;
        let mut last: Option<Bytes>;
        loop {
            out.clear();
            self.range(start, None, &|_, _| true, 4096, &mut out);
            for (k, v) in &out {
                f(k, v);
            }
            if out.len() < 4096 {
                return;
            }
            last = out.last().map(|(k, _)| k.clone());
            start = Bound::Excluded(last.as_deref().unwrap());
        }
    }

    fn len(&self) -> usize {
        let mut n = 0;
        self.for_each(&mut |_, _| n += 1);
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KINDS: [MemTableKind; 4] = [MemTableKind::SkipList, MemTableKind::BBTree, MemTableKind::BTree, MemTableKind::Art8];

    fn put(v: &'static [u8]) -> Value {
        Value::Put(Bytes::from_static(v))
    }

    const ALL: View = &|_, _| true;

    #[test]
    fn newest_sequence_wins_in_every_kind() {
        for kind in KINDS {
            let t = new_memtable(kind);
            let (a, b, c) = (b"aaaaaaaa", b"bbbbbbbb", b"\xff\xff\xff\xff\xff\xff\xff\xff");
            t.insert(a, put(b"2"), 2, 2);
            t.insert(a, put(b"1"), 1, 1);
            t.insert(b, Value::Delete, 3, 3);
            t.insert(c, put(b"3"), 4, 4);
            assert_eq!(t.get(a, ALL), Some(put(b"2")), "{kind:?}");
            assert_eq!(t.get(b, ALL), Some(Value::Delete));
            assert_eq!(t.get(b"zzzzzzzz", ALL), None);
            let mut out = Vec::new();
            t.range(Bound::Excluded(a), Some(c), ALL, 10, &mut out);
            assert_eq!(out.len(), 1, "{kind:?}");
            assert_eq!(&out[0].0[..], b);
            out.clear();
            t.range(Bound::Excluded(a), None, ALL, 10, &mut out);
            assert_eq!(out.len(), 2, "{kind:?}");
            let mut keys = Vec::new();
            t.for_each(&mut |k, v| keys.push((k.to_vec(), v.clone())));
            assert_eq!(keys, vec![(a.to_vec(), put(b"2")), (b.to_vec(), Value::Delete), (c.to_vec(), put(b"3"))], "{kind:?}");
            assert_eq!(t.len(), 3);
        }
    }

    #[test]
    fn reads_see_what_the_view_accepts_in_every_kind() {
        for kind in KINDS {
            let t = new_memtable(kind);
            let (a, b) = (b"aaaaaaaa", b"bbbbbbbb");
            // Tickets run backwards, so a view by ticket differs from one by sequence number.
            for seq in [5, 1, 9, 3, 7] {
                t.insert(a, Value::Put(Bytes::from(seq.to_string())), seq, 10 - seq);
            }
            t.insert(b, put(b"b"), 6, 4);
            let at = |s: u64| t.get(a, &|seq, _| seq <= s).and_then(Value::into_put).map(|v| String::from_utf8(v.to_vec()).unwrap());
            assert_eq!(
                [at(0), at(1), at(2), at(4), at(6), at(8), at(9), at(100)],
                [
                    None,
                    Some("1".into()),
                    Some("1".into()),
                    Some("3".into()),
                    Some("5".into()),
                    Some("7".into()),
                    Some("9".into()),
                    Some("9".into())
                ],
                "{kind:?}"
            );
            let committed = |ticket: u64| ticket >= 5;
            let newest_committed = t.get(a, &|_, ticket| committed(ticket));
            assert_eq!(newest_committed, Some(Value::Put(Bytes::from("5"))), "{kind:?}");
            let mut out = Vec::new();
            t.range(Bound::Unbounded, None, &|seq, _| seq <= 4, 10, &mut out);
            assert_eq!(out, vec![(Bytes::from_static(a), put(b"3"))], "{kind:?}");
            out.clear();
            t.range(Bound::Unbounded, None, &|seq, _| seq <= 6, 1, &mut out);
            assert_eq!(out, vec![(Bytes::from_static(a), put(b"5"))], "{kind:?}");
            out.clear();
            t.range(Bound::Excluded(a), None, &|seq, _| seq <= 5, 10, &mut out);
            assert!(out.is_empty(), "{kind:?}");
        }
    }

    #[test]
    fn reverse_ranges_and_short_bounds_in_every_kind() {
        for kind in KINDS {
            let t = new_memtable(kind);
            let keys: Vec<[u8; 8]> =
                [1u64, 2, 0x6161_0000_0000_0000, 0x6161_ffff_ffff_ffff, 0x6200_0000_0000_0000, u64::MAX].map(u64::to_be_bytes).to_vec();
            for (i, k) in keys.iter().enumerate() {
                t.insert(k, put(b"v"), i as u64 + 1, 0);
            }
            let got = |out: &Vec<(Bytes, Value)>| out.iter().map(|(k, _)| k.to_vec()).collect::<Vec<_>>();
            let want = |idx: &[usize]| idx.iter().map(|&i| keys[i].to_vec()).collect::<Vec<_>>();
            let mut out = Vec::new();
            t.range_rev(b"", None, ALL, 100, &mut out);
            assert_eq!(got(&out), want(&[5, 4, 3, 2, 1, 0]), "{kind:?}");
            out.clear();
            t.range_rev(b"", None, ALL, 2, &mut out);
            assert_eq!(got(&out), want(&[5, 4]), "{kind:?}");
            // Keys starting with "aa", bounded by keys shorter than the stored ones.
            out.clear();
            t.range(Bound::Included(b"aa"), Some(b"ab"), ALL, 100, &mut out);
            assert_eq!(got(&out), want(&[2, 3]), "{kind:?}");
            out.clear();
            t.range_rev(b"aa", Some(b"ab"), ALL, 100, &mut out);
            assert_eq!(got(&out), want(&[3, 2]), "{kind:?}");
            out.clear();
            t.range_rev(&keys[1], Some(&keys[4]), &|seq, _| seq != 4, 100, &mut out);
            assert_eq!(got(&out), want(&[2, 1]), "{kind:?}");
            out.clear();
            t.range_rev(&keys[3], Some(&keys[3]), ALL, 100, &mut out);
            assert!(out.is_empty(), "{kind:?}");
        }
    }
}
