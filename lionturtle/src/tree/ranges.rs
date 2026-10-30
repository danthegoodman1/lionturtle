//! Sets of keys as sorted, disjoint, non-adjacent half-open intervals.
//!
//! A segment records which of its keys are dead (flushed to a child, or owned
//! by a sibling after a node split) as a `KeyRanges`. An interval `[lo, hi)`
//! with `hi = None` extends to +∞; the empty key is the smallest key, so
//! `lo = ""` starts at -∞.

use crate::types::cmp_keys;
use bytes::Bytes;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRanges {
    ranges: Vec<(Bytes, Option<Bytes>)>,
}

/// The smallest key greater than `key`.
pub fn successor(key: &[u8]) -> Bytes {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key);
    v.push(0);
    Bytes::from(v)
}

fn hi_lt(a: &Option<Bytes>, b: &Option<Bytes>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a < b,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

fn max_hi(a: Option<Bytes>, b: Option<Bytes>) -> Option<Bytes> {
    if hi_lt(&a, &b) { b } else { a }
}

impl KeyRanges {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn as_slice(&self) -> &[(Bytes, Option<Bytes>)] {
        &self.ranges
    }

    pub fn iter(&self) -> impl Iterator<Item = &(Bytes, Option<Bytes>)> {
        self.ranges.iter()
    }

    /// Adds `[lo, hi)`.
    pub fn insert(&mut self, lo: Bytes, hi: Option<Bytes>) {
        if hi.as_ref().is_some_and(|h| *h <= lo) {
            return;
        }
        // First range that ends at or after `lo` (touching ranges merge).
        let start = self.ranges.partition_point(|(_, h)| h.as_ref().is_some_and(|h| *h < lo));
        // One past the last range that starts at or before `hi`.
        let end = match &hi {
            None => self.ranges.len(),
            Some(h) => self.ranges.partition_point(|(l, _)| l <= h),
        };
        let (mut new_lo, mut new_hi) = (lo, hi);
        if start < end {
            if self.ranges[start].0 < new_lo {
                new_lo = self.ranges[start].0.clone();
            }
            new_hi = max_hi(new_hi, self.ranges[end - 1].1.clone());
        }
        self.ranges.splice(start..end, [(new_lo, new_hi)]);
    }

    /// The interval containing `key`, if any.
    fn find(&self, key: &[u8]) -> Option<&(Bytes, Option<Bytes>)> {
        let i = self.ranges.partition_point(|(l, _)| cmp_keys(l, key).is_le());
        let r = self.ranges.get(i.checked_sub(1)?)?;
        r.1.as_ref().is_none_or(|h| cmp_keys(key, h).is_lt()).then_some(r)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.find(key).is_some()
    }

    /// The smallest key at or above `key` outside the set: `Ok(None)` if
    /// `key` is outside already, `Ok(Some(k))` to skip ahead to `k`, and
    /// `Err(())` if everything from `key` on is in the set.
    #[allow(clippy::result_unit_err)]
    pub fn skip(&self, key: &[u8]) -> Result<Option<&Bytes>, ()> {
        match self.find(key) {
            None => Ok(None),
            Some((_, Some(hi))) => Ok(Some(hi)),
            Some((_, None)) => Err(()),
        }
    }

    /// Whether the closed interval `[lo, hi]` lies inside the set.
    pub fn covers(&self, lo: &[u8], hi: &[u8]) -> bool {
        self.find(lo).is_some_and(|(_, h)| h.as_ref().is_none_or(|h| hi < &h[..]))
    }

    /// The part of the set inside `[lo, hi)`.
    pub fn clip(&self, lo: &[u8], hi: Option<&[u8]>) -> KeyRanges {
        let mut out = KeyRanges::new();
        for (l, h) in &self.ranges {
            let nl = if &l[..] < lo { Bytes::copy_from_slice(lo) } else { l.clone() };
            let nh = match (h, hi) {
                (None, None) => None,
                (Some(h), None) => Some(h.clone()),
                (None, Some(b)) => Some(Bytes::copy_from_slice(b)),
                (Some(h), Some(b)) => Some(if &h[..] < b { h.clone() } else { Bytes::copy_from_slice(b) }),
            };
            if nh.as_ref().is_none_or(|nh| nl < *nh) {
                out.ranges.push((nl, nh));
            }
        }
        out
    }

    pub fn union(&mut self, other: &KeyRanges) {
        for (l, h) in &other.ranges {
            self.insert(l.clone(), h.clone());
        }
    }

    pub fn check(&self) {
        for w in self.ranges.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            assert!(a.1.as_ref().is_some_and(|h| *h < b.0), "ranges overlap or touch: {self:?}");
        }
        for (l, h) in &self.ranges {
            assert!(h.as_ref().is_none_or(|h| l < h), "empty range in {self:?}");
        }
    }

    /// Encoded as `count: u32` then per range `lo_len: u32 | lo | has_hi: u8 | (hi_len: u32 | hi)?`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.ranges.len() as u32).to_le_bytes());
        for (l, h) in &self.ranges {
            out.extend_from_slice(&(l.len() as u32).to_le_bytes());
            out.extend_from_slice(l);
            match h {
                None => out.push(0),
                Some(h) => {
                    out.push(1);
                    out.extend_from_slice(&(h.len() as u32).to_le_bytes());
                    out.extend_from_slice(h);
                }
            }
        }
    }

    pub fn decode(r: &mut crate::tree::codec::Reader<'_>) -> Option<Self> {
        let n = r.u32()? as usize;
        let mut ranges = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let l = r.bytes()?;
            let h = match r.u8()? {
                0 => None,
                1 => Some(r.bytes()?),
                _ => return None,
            };
            ranges.push((l, h));
        }
        let out = Self { ranges };
        for w in out.ranges.windows(2) {
            if !w[0].1.as_ref().is_some_and(|h| *h < w[1].0) {
                return None;
            }
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::Rng;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn matches_bitmap_model() {
        let mut rng = Rng::new(9);
        for _ in 0..500 {
            let mut set = KeyRanges::new();
            let mut model = [false; 26];
            for _ in 0..rng.range(1, 8) {
                let lo = rng.below(26) as u8;
                let hi = rng.range(lo as u64, 26) as u8;
                let hi_key = (hi < 26).then(|| Bytes::from(vec![b'a' + hi]));
                set.insert(Bytes::from(vec![b'a' + lo]), hi_key);
                for m in model.iter_mut().take(hi as usize).skip(lo as usize) {
                    *m = true;
                }
                if hi == 26 {
                    for m in model.iter_mut().skip(lo as usize) {
                        *m = true;
                    }
                }
                set.check();
                for (i, &m) in model.iter().enumerate() {
                    assert_eq!(set.contains(&[b'a' + i as u8]), m, "{set:?} at {i}");
                }
            }
            let clipped = set.clip(b"f", Some(b"p"));
            for (i, &m) in model.iter().enumerate() {
                let k = [b'a' + i as u8];
                let inside = k[..] >= b"f"[..] && k[..] < b"p"[..];
                assert_eq!(clipped.contains(&k), m && inside);
            }
        }
    }

    #[test]
    fn skip_and_covers() {
        let mut set = KeyRanges::new();
        set.insert(b("b"), Some(b("d")));
        set.insert(b("x"), None);
        assert_eq!(set.skip(b"a"), Ok(None));
        assert_eq!(set.skip(b"c"), Ok(Some(&b("d"))));
        assert_eq!(set.skip(b"y"), Err(()));
        assert!(set.covers(b"b", b"c"));
        assert!(!set.covers(b"b", b"d"));
        assert!(set.covers(b"x", b"zzz"));
    }
}
