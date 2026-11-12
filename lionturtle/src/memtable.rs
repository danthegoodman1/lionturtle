//! In-memory tables of recent writes.
//!
//! A memtable keeps every version of a key, each tagged with its sequence
//! number and its batch's ticket, and a read takes the newest version its
//! [`View`] accepts. Writers insert outside the writer lock, so a version
//! may sit in the table before its batch commits; the view decides which
//! versions a reader sees. Concurrent writers to one key may insert in any
//! order; versions stay sorted by sequence number. Three implementations
//! share the [`MemTable`] trait so the in-memory layer can be compared head
//! to head.
//!
//! A memtable copies the keys and values it is given, so a caller's buffers
//! are its own again once an insert returns. The default kind, the B+-tree,
//! keeps its copies and its nodes in an [`Arena`]; the other two keep each
//! entry in blocks of the general allocator's.

use crate::arena::{Arena, ChunkPool};
use crate::types::Value;
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::RwLock;
use std::alloc::Layout;
use std::collections::BTreeMap;
use std::mem::{ManuallyDrop, size_of};
use std::ops::{Bound, ControlFlow};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering::*};

/// Which versions a reader sees, by sequence number and batch ticket.
pub type View<'a> = &'a dyn Fn(u64, u64) -> bool;

/// A key and the value to give it, or `None` for a tombstone.
pub type Entry<'a> = (&'a [u8], Option<&'a [u8]>);

/// The versions of one key, newest first.
#[derive(Clone, Debug)]
pub struct Versions {
    seq: u64,
    ticket: u64,
    value: Value,
    older: Older,
}

/// The link from a version to the next older one.
#[derive(Clone, Debug)]
struct Older(Option<Arc<Versions>>);

impl Drop for Older {
    // Free the chain iteratively: a key overwritten N times in one memtable
    // holds N versions, and dropping 100,000 recursively overflowed a
    // checkpoint thread's 2 MiB stack. An overwrite drops the old head, whose
    // chain the new head shares, so that case stays inline.
    #[inline]
    fn drop(&mut self) {
        if let Some(v) = self.0.take().and_then(Arc::into_inner) {
            free_chain(v);
        }
    }
}

/// Frees `v` and the versions behind it that no other chain shares.
fn free_chain(mut v: Versions) {
    let mut next = v.older.0.take();
    while let Some(older) = next {
        let Some(mut o) = Arc::into_inner(older) else { break };
        next = o.older.0.take();
    }
}

impl Versions {
    fn new(seq: u64, ticket: u64, value: Value) -> Self {
        Self { seq, ticket, value, older: Older(None) }
    }

    /// These versions plus the one written at `seq`.
    fn with(&self, seq: u64, ticket: u64, value: Value) -> Self {
        if seq > self.seq {
            return Self { seq, ticket, value, older: Older(Some(Arc::new(self.clone()))) };
        }
        if seq == self.seq {
            return Self { seq, ticket, value, older: self.older.clone() };
        }
        // A writer with an earlier sequence number inserted second.
        let older = match &self.older.0 {
            Some(o) => o.with(seq, ticket, value),
            None => Self::new(seq, ticket, value),
        };
        Self { older: Older(Some(Arc::new(older))), ..self.clone() }
    }

    /// The newest value `view` sees.
    fn at(&self, view: View) -> Option<&Value> {
        let mut v = self;
        loop {
            if view(v.seq, v.ticket) {
                return Some(&v.value);
            }
            v = v.older.0.as_deref()?;
        }
    }
}

pub trait MemTable: Send + Sync {
    /// Adds the version of `key` written at `seq` by batch `ticket`: a copy
    /// of `value`, or a tombstone for `None`.
    fn insert(&self, key: &[u8], value: Option<&[u8]>, seq: u64, ticket: u64);
    /// Adds a version of each key in `ops`, in order, at sequence numbers
    /// counting up from `seq`, all written by batch `ticket`.
    fn insert_many(&self, ops: &[Entry<'_>], seq: u64, ticket: u64) {
        for (i, (key, value)) in ops.iter().enumerate() {
            self.insert(key, *value, seq + i as u64, ticket);
        }
    }
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
    /// Visits every key in key order with its newest value, `None` for a
    /// tombstone. Called once writers are gone.
    fn for_each(&self, f: &mut dyn FnMut(&[u8], Option<&[u8]>));
    /// Number of keys. May visit every key to count them.
    fn len(&self) -> usize;
    /// Whether the table holds no key.
    fn is_empty(&self) -> bool;
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

/// A new memtable of `kind`. The B+-tree takes its memory from `pool`,
/// which the memtables of one store share.
pub fn new_memtable(kind: MemTableKind, pool: &Arc<ChunkPool>) -> Box<dyn MemTable> {
    match kind {
        MemTableKind::SkipList => Box::new(SkipListTable::default()),
        MemTableKind::BBTree => Box::new(BBTreeTable::new(pool.clone())),
        MemTableKind::BTree => Box::new(BTreeTable::default()),
    }
}

fn owned(value: Option<&[u8]>) -> Value {
    value.map_or(Value::Delete, |v| Value::Put(Bytes::copy_from_slice(v)))
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
    fn insert(&self, key: &[u8], value: Option<&[u8]>, seq: u64, ticket: u64) {
        let value = owned(value);
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

    fn for_each(&self, f: &mut dyn FnMut(&[u8], Option<&[u8]>)) {
        for e in self.map.iter() {
            f(e.key(), e.value().load().value.as_put().map(|v| &v[..]));
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// One version of a key in a [`BBTreeTable`]'s arena: this header, then the
/// value. Keys lie after the versions they arrived with; see
/// [`BBTreeTable::store`].
#[repr(C)]
struct Header {
    seq: u64,
    ticket: u64,
    /// The key's next older version. It changes only while the key's leaf
    /// is latched for writing, and is read only under the leaf's latch.
    older: AtomicPtr<Header>,
    /// Bytes in the value.
    len: u32,
    kind: u8,
}

/// The value's bytes follow the header.
const INLINE: u8 = 0;
/// A tombstone.
const DELETE: u8 = 1;
/// A [`SharedValue`] follows the header.
const SHARED: u8 = 2;

/// Values this long and longer keep a buffer of their own, which a read
/// shares; a read copies a shorter one out of the arena. Sharing a buffer
/// costs a reference count, and copying a page or more costs more than
/// that. So few values this long fit in a memtable that freeing their
/// buffers one by one takes a millisecond or two.
const SHARED_VALUE: usize = 4096;

/// A value in a buffer of its own, linked to the others for the drop.
struct SharedValue {
    bytes: Bytes,
    next: *mut Header,
}

/// A version in the arena of the table that hands it out, valid while that
/// table lives.
#[derive(Clone, Copy)]
struct ArenaVersion(NonNull<Header>);

// SAFETY: a version is immutable but for `older`, an atomic.
unsafe impl Send for ArenaVersion {}
unsafe impl Sync for ArenaVersion {}

impl ArenaVersion {
    fn header(&self) -> &Header {
        // SAFETY: the table that handed this version out keeps it valid.
        unsafe { self.0.as_ref() }
    }

    /// What follows the header.
    fn body(self) -> *mut u8 {
        // SAFETY: the header's allocation extends past it.
        unsafe { self.0.as_ptr().add(1).cast() }
    }

    /// The value's bytes, or `None` for a tombstone.
    ///
    /// # Safety
    ///
    /// The table this version came from outlives `'a`.
    unsafe fn value<'a>(self) -> Option<&'a [u8]> {
        let header = self.header();
        match header.kind {
            // SAFETY: `len` bytes of value follow an inline header.
            INLINE => Some(unsafe { std::slice::from_raw_parts(self.body(), header.len as usize) }),
            // SAFETY: a shared value follows a shared header.
            SHARED => Some(unsafe { &(*self.body().cast::<SharedValue>()).bytes }),
            _ => None,
        }
    }

    /// The value as its own buffer.
    fn to_value(self) -> Value {
        match self.header().kind {
            // SAFETY: a shared value follows a shared header.
            SHARED => Value::Put(unsafe { (*self.body().cast::<SharedValue>()).bytes.clone() }),
            // SAFETY: the caller holds the table.
            _ => owned(unsafe { self.value() }),
        }
    }

    fn older(self) -> Option<Self> {
        NonNull::new(self.header().older.load(Relaxed)).map(Self)
    }

    /// The newest version `view` sees, from this one back.
    fn find(self, view: View) -> Option<Self> {
        let mut v = self;
        loop {
            if view(v.header().seq, v.header().ticket) {
                return Some(v);
            }
            v = v.older()?;
        }
    }

    /// Adds this version to the chain that starts at `newest`, keeping it
    /// sorted by sequence number. Called with the chain's key latched for
    /// writing.
    fn link_into(self, newest: &mut Self) {
        let seq = self.header().seq;
        if seq >= newest.header().seq {
            // A version at an existing sequence number takes its place.
            let older = if seq == newest.header().seq { newest.older() } else { Some(*newest) };
            self.header().older.store(older.map_or(std::ptr::null_mut(), |o| o.0.as_ptr()), Relaxed);
            *newest = self;
            return;
        }
        // A writer with an earlier sequence number inserted second.
        let mut after = *newest;
        let mut next = after.older();
        while let Some(n) = next
            && n.header().seq > seq
        {
            after = n;
            next = n.older();
        }
        if let Some(n) = next
            && n.header().seq == seq
        {
            next = n.older();
        }
        self.header().older.store(next.map_or(std::ptr::null_mut(), |o| o.0.as_ptr()), Relaxed);
        after.header().older.store(self.0.as_ptr(), Relaxed);
    }
}

/// Hands the tree's nodes memory from the table's arena.
struct NodePages(NonNull<Arena>);

// SAFETY: an arena allocates from any thread.
unsafe impl Send for NodePages {}
unsafe impl Sync for NodePages {}

// SAFETY: the arena hands out zeroed blocks of the size and alignment
// asked for, each distinct from every other, valid until the arena drops,
// which is after the map's last use. Its calls abort rather than unwind.
unsafe impl bbtree::Allocator for NodePages {
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<u8>, bbtree::AllocError> {
        // SAFETY: the table keeps the arena for as long as the map.
        unsafe { self.0.as_ref() }.alloc_page(layout).ok_or(bbtree::AllocError)
    }

    /// Pages return with the arena.
    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {}
}

/// A B+-tree whose keys, values and nodes all live in one [`Arena`].
///
/// Each leaf entry is a key and its newest version, both pointers into the
/// arena, so nothing in the tree needs dropping. Dropping the table gives
/// the arena's chunks back to the pool in one step, whatever the table
/// holds, and the next table writes its entries over them.
pub struct BBTreeTable {
    /// Never dropped: the tree's pages are the arena's, and a drop would
    /// walk every node to hand each back.
    map: ManuallyDrop<bbtree::BBTreeMap<&'static [u8], ArenaVersion, bbtree::Auto, NodePages>>,
    /// The versions that hold a [`SharedValue`], newest first.
    shared: AtomicPtr<Header>,
    arena: Box<Arena>,
}

impl BBTreeTable {
    pub fn new(pool: Arc<ChunkPool>) -> Self {
        let arena = Box::new(Arena::new(pool));
        let pages = NodePages(NonNull::from(&*arena));
        Self { map: ManuallyDrop::new(bbtree::BBTreeMap::new_in(pages)), shared: AtomicPtr::new(std::ptr::null_mut()), arena }
    }
}

impl Drop for BBTreeTable {
    fn drop(&mut self) {
        let mut next = *self.shared.get_mut();
        while let Some(version) = NonNull::new(next).map(ArenaVersion) {
            let shared = version.body().cast::<SharedValue>();
            // SAFETY: each version on the list holds a shared value, which
            // nothing reads once the table drops.
            unsafe {
                next = (*shared).next;
                std::ptr::drop_in_place(shared);
            }
        }
    }
}

/// Keys a batch looks up in the tree together. Each lookup misses the
/// cache at the tree's lower levels, and the misses of a group overlap.
const LANES: usize = 8;

impl BBTreeTable {
    /// Copies `ops` into one allocation of the arena: a version of each,
    /// at sequence numbers counting up from `seq`, then their keys. Returns
    /// the allocation and the entries to give the tree.
    ///
    /// The keys come last, so that the bytes of those the tree already
    /// holds can go back to the arena; see [`Self::return_keys`].
    fn store<const N: usize>(&self, ops: &[Entry<'_>; N], seq: u64, ticket: u64) -> ((*mut u8, usize), [(&'static [u8], ArenaVersion); N]) {
        const HEADER: usize = size_of::<Header>();
        let body = |value: Option<&[u8]>| match value {
            None => 0,
            Some(v) if v.len() < SHARED_VALUE => v.len(),
            Some(_) => size_of::<SharedValue>(),
        };
        // Each header starts on a multiple of 8, as its allocation does.
        let mut at = [0; N];
        let mut len = 0usize;
        for (at, (_, value)) in at.iter_mut().zip(ops) {
            *at = len.next_multiple_of(8);
            len = *at + HEADER + body(*value);
        }
        let mut key_at = len;
        len += ops.iter().map(|(key, _)| key.len()).sum::<usize>();
        let start = self.arena.alloc(len).as_ptr();
        let entries = std::array::from_fn(|i| {
            let (key, value) = ops[i];
            // SAFETY: the arena set aside `len` bytes at `start`, and the
            // offsets computed above keep each header, each value and each
            // key within them and apart. A header's offset is a multiple of
            // 8, and a shared value follows it 32 bytes on.
            unsafe {
                let header = start.add(at[i]).cast::<Header>();
                let version = ArenaVersion(NonNull::new_unchecked(header));
                let older = AtomicPtr::new(std::ptr::null_mut());
                let kind = match value {
                    None => DELETE,
                    Some(v) if v.len() < SHARED_VALUE => INLINE,
                    Some(_) => SHARED,
                };
                header.write(Header { seq: seq + i as u64, ticket, older, len: value.map_or(0, |v| v.len() as u32), kind });
                match value {
                    Some(v) if kind == INLINE => std::ptr::copy_nonoverlapping(v.as_ptr(), version.body(), v.len()),
                    Some(v) => {
                        let shared = version.body().cast::<SharedValue>();
                        shared.write(SharedValue { bytes: Bytes::copy_from_slice(v), next: self.shared.load(Relaxed) });
                        while let Err(head) = self.shared.compare_exchange_weak((*shared).next, header, Release, Relaxed) {
                            (*shared).next = head;
                        }
                    }
                    None => {}
                }
                let stored = start.add(key_at);
                std::ptr::copy_nonoverlapping(key.as_ptr(), stored, key.len());
                key_at += key.len();
                // The arena keeps the key until the table, and with it the
                // map, drops.
                (std::slice::from_raw_parts(stored, key.len()), version)
            }
        });
        ((start, len), entries)
    }

    /// Returns to the arena the keys at the end of `stored` that the tree
    /// had already: those of the last entries, back to the last one whose
    /// key was new. An overwrite thus costs its value alone, unless a new
    /// key follows it in its group.
    fn return_keys<const N: usize>(&self, (start, len): (*mut u8, usize), ops: &[Entry<'_>; N], held: [bool; N]) {
        let unused: usize = ops.iter().zip(held).rev().take_while(|(_, held)| *held).map(|((key, _), _)| key.len()).sum();
        let (end, kept) = (len.next_multiple_of(8), (len - unused).next_multiple_of(8));
        // SAFETY: the allocation ends `end` bytes past `start`.
        self.arena.unalloc(unsafe { start.add(end) }, end - kept);
    }
}

impl MemTable for BBTreeTable {
    fn insert(&self, key: &[u8], value: Option<&[u8]>, seq: u64, ticket: u64) {
        let ops = [(key, value)];
        let (stored, [(key, version)]) = self.store(&ops, seq, ticket);
        match self.map.update_or_insert(key, version, |_, newest, new| new.link_into(newest)) {
            Ok(held) => self.return_keys(stored, &ops, [held.is_some()]),
            Err(_) => panic!("memtable allocation failed"),
        }
    }

    fn insert_many(&self, ops: &[Entry<'_>], seq: u64, ticket: u64) {
        let (groups, rest) = ops.as_chunks::<LANES>();
        for (i, group) in groups.iter().enumerate() {
            let (stored, entries) = self.store(group, seq + (i * LANES) as u64, ticket);
            let held = self.map.update_or_insert_many(entries, |_, newest, new| new.link_into(newest));
            self.return_keys(stored, group, held.map(|r| r.expect("memtable allocation failed").is_some()));
        }
        for (i, (key, value)) in rest.iter().enumerate() {
            self.insert(key, *value, seq + (groups.len() * LANES + i) as u64, ticket);
        }
    }

    fn get(&self, key: &[u8], view: View) -> Option<Value> {
        // The version stays valid past the latch it was found under.
        let found = self.map.read(key, |_, newest| newest.find(view)).flatten()?;
        Some(found.to_value())
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
        self.map.scan::<[u8], ()>((start, hi), |k, newest| {
            if let Some(v) = newest.find(view) {
                out.push((Bytes::copy_from_slice(k), v.to_value()));
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
        self.map.scan_rev::<[u8], ()>((Bound::Included(start), hi), |k, newest| {
            if let Some(v) = newest.find(view) {
                out.push((Bytes::copy_from_slice(k), v.to_value()));
                n += 1;
            }
            if n >= limit { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
    }

    fn for_each(&self, f: &mut dyn FnMut(&[u8], Option<&[u8]>)) {
        self.map.scan::<[u8], ()>(.., |k, newest| {
            // SAFETY: the table outlives the call.
            f(k, unsafe { newest.value() });
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

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[derive(Default)]
pub struct BTreeTable {
    map: RwLock<BTreeMap<Bytes, Versions>>,
}

impl MemTable for BTreeTable {
    fn insert(&self, key: &[u8], value: Option<&[u8]>, seq: u64, ticket: u64) {
        let value = owned(value);
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

    fn for_each(&self, f: &mut dyn FnMut(&[u8], Option<&[u8]>)) {
        for (k, v) in self.map.read().iter() {
            f(k, v.value.as_put().map(|v| &v[..]));
        }
    }

    fn len(&self) -> usize {
        self.map.read().len()
    }

    fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KINDS: [MemTableKind; 3] = [MemTableKind::SkipList, MemTableKind::BBTree, MemTableKind::BTree];

    fn put(v: &'static [u8]) -> Value {
        Value::Put(Bytes::from_static(v))
    }

    fn table(kind: MemTableKind) -> Box<dyn MemTable> {
        new_memtable(kind, &ChunkPool::new(1 << 20))
    }

    const ALL: View = &|_, _| true;

    /// A key overwritten N times holds N versions. Dropping the table must
    /// free them without recursing once per version, which overflows the
    /// stack of whichever thread drops it.
    #[test]
    fn long_version_chains_drop_on_a_small_stack() {
        for kind in KINDS {
            let t = table(kind);
            for seq in 1..=100_000 {
                t.insert(b"hot-key!", Some(b"v"), seq, seq);
            }
            assert_eq!(t.get(b"hot-key!", ALL), Some(put(b"v")), "{kind:?}");
            // 256 KiB holds a few thousand frames of a recursive drop.
            std::thread::Builder::new().stack_size(256 << 10).spawn(move || drop(t)).unwrap().join().unwrap();
        }
    }

    /// Values on both sides of the length past which a value keeps a
    /// buffer of its own, empty keys and values, and tombstones over each.
    #[test]
    fn values_of_every_length_read_back_in_every_kind() {
        for kind in KINDS {
            let t = table(kind);
            let lens = [0, 1, 7, 8, 100, SHARED_VALUE - 1, SHARED_VALUE, SHARED_VALUE + 1, 5 * SHARED_VALUE];
            let value = |len: usize| -> Vec<u8> { (0..len).map(|i| (i * 31 + len) as u8).collect() };
            let key = |i: usize| -> Vec<u8> { if i == 0 { Vec::new() } else { format!("key-{i:03}").into_bytes() } };
            for (i, &len) in lens.iter().enumerate() {
                t.insert(&key(i), Some(&value(len)), i as u64 + 1, 0);
            }
            for (i, &len) in lens.iter().enumerate() {
                assert_eq!(t.get(&key(i), ALL), Some(Value::Put(value(len).into())), "{kind:?}, {len} bytes");
            }
            let mut seen = Vec::new();
            t.for_each(&mut |k, v| seen.push((k.to_vec(), v.map(<[u8]>::to_vec))));
            let want: Vec<_> = lens.iter().enumerate().map(|(i, &len)| (key(i), Some(value(len)))).collect();
            assert_eq!(seen, want, "{kind:?}");
            let mut out = Vec::new();
            t.range(Bound::Unbounded, None, ALL, 100, &mut out);
            assert_eq!(out.len(), lens.len(), "{kind:?}");
            assert!(out.iter().zip(&want).all(|((k, v), (wk, wv))| k[..] == wk[..] && v.as_put().map(|b| b.to_vec()) == *wv), "{kind:?}");
            // A tombstone over each, then a long value over the first.
            for i in 0..lens.len() {
                t.insert(&key(i), None, 100 + i as u64, 0);
            }
            assert_eq!(t.get(&key(3), ALL), Some(Value::Delete), "{kind:?}");
            assert_eq!(t.get(&key(6), &|seq, _| seq < 100), Some(Value::Put(value(SHARED_VALUE).into())), "{kind:?}");
            t.insert(&key(0), Some(&value(2 * SHARED_VALUE)), 200, 0);
            assert_eq!(t.get(&key(0), ALL), Some(Value::Put(value(2 * SHARED_VALUE).into())), "{kind:?}");
            assert_eq!(t.len(), lens.len(), "{kind:?}");
            assert!(!t.is_empty() && table(kind).is_empty(), "{kind:?}");
        }
    }

    /// A batch goes in as its ops would one by one, whatever its length and
    /// however its keys repeat.
    #[test]
    fn batches_insert_like_single_ops_in_every_kind() {
        for kind in KINDS {
            let (batched, single) = (table(kind), table(kind));
            let mut seq = 1;
            for round in 0..60usize {
                let keys: Vec<Vec<u8>> = (0..round % 23).map(|i| format!("key-{:03}", (i * 7 + round * 3) % 40).into_bytes()).collect();
                let values: Vec<Option<Vec<u8>>> =
                    (0..keys.len()).map(|i| (i % 5 != 4).then(|| vec![round as u8; (i * round) % 50])).collect();
                let ops: Vec<Entry> = keys.iter().zip(&values).map(|(k, v)| (&k[..], v.as_deref())).collect();
                batched.insert_many(&ops, seq, round as u64);
                for (i, (key, value)) in ops.iter().enumerate() {
                    single.insert(key, *value, seq + i as u64, round as u64);
                }
                seq += ops.len() as u64;
            }
            for at in [seq / 3, seq / 2, seq] {
                let (mut a, mut b) = (Vec::new(), Vec::new());
                batched.range(Bound::Unbounded, None, &|s, _| s <= at, 1000, &mut a);
                single.range(Bound::Unbounded, None, &|s, _| s <= at, 1000, &mut b);
                assert_eq!(a, b, "{kind:?} at {at}");
                assert!(!a.is_empty());
            }
        }
    }

    /// A version written at a sequence number the key already has takes
    /// that version's place, wherever it sits in the chain.
    #[test]
    fn a_repeated_sequence_number_replaces_its_version_in_every_kind() {
        for kind in KINDS {
            let t = table(kind);
            for seq in [2, 4, 6] {
                t.insert(b"k", Some(b"old"), seq, 0);
            }
            for seq in [6, 2, 4] {
                t.insert(b"k", Some(format!("new{seq}").as_bytes()), seq, 0);
            }
            for seq in [2, 4, 6] {
                let got = t.get(b"k", &|s, _| s <= seq + 1);
                assert_eq!(got, Some(Value::Put(format!("new{seq}").into())), "{kind:?}");
            }
            assert_eq!(t.get(b"k", &|s, _| s < 2), None, "{kind:?}");
        }
    }

    /// The B+-tree copies a key once: a later version of the key gives its
    /// copy back, so overwrites of long keys cost only their values.
    #[test]
    fn overwrites_keep_one_copy_of_the_key() {
        let pool = ChunkPool::new(1 << 20);
        let t = BBTreeTable::new(pool.clone());
        let key = [7u8; 4000];
        for seq in 1..=10_000 {
            t.insert(&key, Some(b"12345678"), seq, 0);
        }
        // A header and a value each, where each key copy would add 4000.
        assert!(t.arena.reserved() < 10_000 * 100, "{} bytes", t.arena.reserved());
        assert_eq!(t.get(&key, &|seq, _| seq <= 5000), Some(put(b"12345678")));
        assert_eq!(t.len(), 1);
        // The same holds for a batch of overwrites.
        let before = t.arena.reserved();
        let ops = [(&key[..], Some(&b"12345678"[..])); 64];
        for round in 0..200 {
            t.insert_many(&ops, 10_001 + round * 64, 0);
        }
        assert!(t.arena.reserved() - before < 200 * 64 * 100, "{} bytes", t.arena.reserved() - before);
        assert_eq!(t.len(), 1);
    }

    /// Dropping a table leaves its memory for the next one, whatever it
    /// held.
    #[test]
    fn a_dropped_table_leaves_its_memory_to_the_next() {
        let pool = ChunkPool::new(1 << 20);
        let fill = || {
            let t = BBTreeTable::new(pool.clone());
            for seq in 1..=20_000u64 {
                let value = vec![seq as u8; if seq % 1000 == 0 { 2 * SHARED_VALUE } else { 40 }];
                t.insert(&seq.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes(), Some(&value), seq, 0);
            }
            t
        };
        let first = fill();
        let reserved = first.arena.reserved();
        drop(first);
        assert_eq!(pool.idle_bytes(), reserved);
        let second = fill();
        assert_eq!((pool.idle_bytes(), second.arena.reserved()), (0, reserved));
    }

    #[test]
    fn newest_sequence_wins_in_every_kind() {
        for kind in KINDS {
            let t = table(kind);
            let (a, b, c) = (b"aaaaaaaa", b"bbbbbbbb", b"\xff\xff\xff\xff\xff\xff\xff\xff");
            t.insert(a, Some(b"2"), 2, 2);
            t.insert(a, Some(b"1"), 1, 1);
            t.insert(b, None, 3, 3);
            t.insert(c, Some(b"3"), 4, 4);
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
            t.for_each(&mut |k, v| keys.push((k.to_vec(), owned(v))));
            assert_eq!(keys, vec![(a.to_vec(), put(b"2")), (b.to_vec(), Value::Delete), (c.to_vec(), put(b"3"))], "{kind:?}");
            assert_eq!(t.len(), 3);
        }
    }

    #[test]
    fn reads_see_what_the_view_accepts_in_every_kind() {
        for kind in KINDS {
            let t = table(kind);
            let (a, b) = (b"aaaaaaaa", b"bbbbbbbb");
            // Tickets run backwards, so a view by ticket differs from one by sequence number.
            for seq in [5, 1, 9, 3, 7] {
                t.insert(a, Some(seq.to_string().as_bytes()), seq, 10 - seq);
            }
            t.insert(b, Some(b"b"), 6, 4);
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
            let t = table(kind);
            let keys: Vec<[u8; 8]> =
                [1u64, 2, 0x6161_0000_0000_0000, 0x6161_ffff_ffff_ffff, 0x6200_0000_0000_0000, u64::MAX].map(u64::to_be_bytes).to_vec();
            for (i, k) in keys.iter().enumerate() {
                t.insert(k, Some(b"v"), i as u64 + 1, 0);
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
