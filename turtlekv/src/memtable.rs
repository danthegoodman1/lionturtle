//! In-memory tables of recent writes.
//!
//! A memtable keeps the newest value per key, tagged with its sequence
//! number, so concurrent writers to one key may insert in any order and the
//! highest sequence number still wins. Three implementations share the
//! [`MemTable`] trait so the in-memory layer can be compared head to head.

use crate::types::Value;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::ops::{Bound, ControlFlow};

#[derive(Clone, Debug)]
pub struct MemEntry {
    pub seq: u64,
    pub value: Value,
}

pub trait MemTable: Send + Sync {
    /// Stores `value` unless the table holds a newer entry for `key`.
    fn insert(&self, key: &[u8], value: Value, seq: u64);
    fn get(&self, key: &[u8]) -> Option<Value>;
    /// Appends up to `limit` entries with keys after `start` and below `end`,
    /// in key order.
    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, limit: usize, out: &mut Vec<(Bytes, Value)>);
    /// Visits every entry in key order. Called once writers are gone.
    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value));
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
}

impl std::str::FromStr for MemTableKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "skiplist" => Ok(Self::SkipList),
            "bbtree" => Ok(Self::BBTree),
            "btree" => Ok(Self::BTree),
            _ => Err(format!("unknown memtable kind {s}")),
        }
    }
}

pub fn new_memtable(kind: MemTableKind) -> Box<dyn MemTable> {
    match kind {
        MemTableKind::SkipList => Box::new(SkipListTable::default()),
        MemTableKind::BBTree => Box::new(BBTreeTable::default()),
        MemTableKind::BTree => Box::new(BTreeTable::default()),
    }
}

fn below(key: &[u8], end: Option<&[u8]>) -> bool {
    end.is_none_or(|e| key < e)
}

#[derive(Default)]
pub struct SkipListTable {
    map: crossbeam_skiplist::SkipMap<Bytes, MemEntry>,
}

impl MemTable for SkipListTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64) {
        self.map.compare_insert(Bytes::copy_from_slice(key), MemEntry { seq, value }, |old| old.seq < seq);
    }

    fn get(&self, key: &[u8]) -> Option<Value> {
        self.map.get(key).map(|e| e.value().value.clone())
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        for e in self.map.range::<[u8], _>((start, hi)).take(limit) {
            out.push((e.key().clone(), e.value().value.clone()));
        }
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        for e in self.map.iter() {
            f(e.key(), &e.value().value);
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

#[derive(Default)]
pub struct BBTreeTable {
    map: bbtree::BBTreeMap<Vec<u8>, MemEntry>,
}

impl MemTable for BBTreeTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64) {
        let entry = MemEntry { seq, value };
        let result = self.map.update_or_insert(key.to_vec(), entry, |_, stored, new| {
            if new.seq > stored.seq {
                *stored = new;
            }
        });
        if result.is_err() {
            panic!("memtable allocation failed");
        }
    }

    fn get(&self, key: &[u8]) -> Option<Value> {
        self.map.read(key, |_, e| e.value.clone())
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, limit: usize, out: &mut Vec<(Bytes, Value)>) {
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
        self.map.scan::<[u8], ()>((start, hi), |k, e| {
            out.push((Bytes::copy_from_slice(k), e.value.clone()));
            n += 1;
            if n >= limit { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        self.map.scan::<[u8], ()>(.., |k, e| {
            f(k, &e.value);
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
    map: RwLock<BTreeMap<Bytes, MemEntry>>,
}

impl MemTable for BTreeTable {
    fn insert(&self, key: &[u8], value: Value, seq: u64) {
        let mut map = self.map.write();
        match map.get_mut(key) {
            Some(e) if e.seq > seq => {}
            Some(e) => *e = MemEntry { seq, value },
            None => {
                map.insert(Bytes::copy_from_slice(key), MemEntry { seq, value });
            }
        }
    }

    fn get(&self, key: &[u8]) -> Option<Value> {
        self.map.read().get(key).map(|e| e.value.clone())
    }

    fn range(&self, start: Bound<&[u8]>, end: Option<&[u8]>, limit: usize, out: &mut Vec<(Bytes, Value)>) {
        let map = self.map.read();
        for (k, e) in map.range::<[u8], _>((start, Bound::Unbounded)).take(limit) {
            if !below(k, end) {
                break;
            }
            out.push((k.clone(), e.value.clone()));
        }
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], &Value)) {
        for (k, e) in self.map.read().iter() {
            f(k, &e.value);
        }
    }

    fn len(&self) -> usize {
        self.map.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newest_sequence_wins_in_every_kind() {
        for kind in [MemTableKind::SkipList, MemTableKind::BBTree, MemTableKind::BTree] {
            let t = new_memtable(kind);
            t.insert(b"a", Value::Put(Bytes::from_static(b"2")), 2);
            t.insert(b"a", Value::Put(Bytes::from_static(b"1")), 1);
            t.insert(b"b", Value::Delete, 3);
            t.insert(b"c", Value::Put(Bytes::from_static(b"3")), 4);
            assert_eq!(t.get(b"a"), Some(Value::Put(Bytes::from_static(b"2"))), "{kind:?}");
            assert_eq!(t.get(b"b"), Some(Value::Delete));
            assert_eq!(t.get(b"z"), None);
            let mut out = Vec::new();
            t.range(Bound::Excluded(b"a"), Some(b"c"), 10, &mut out);
            assert_eq!(out.len(), 1, "{kind:?}");
            assert_eq!(&out[0].0[..], b"b");
            let mut keys = Vec::new();
            t.for_each(&mut |k, _| keys.push(k.to_vec()));
            assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
            assert_eq!(t.len(), 3);
        }
    }
}
