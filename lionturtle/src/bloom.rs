//! Split-block Bloom filter: each key sets one bit in each of the eight 32-bit
//! words of a single 256-bit block, so a probe touches one cache line.

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

const SALT: [u32; 8] = [0x47b6_137b, 0x4497_4d91, 0x8824_ad5b, 0xa2b7_289d, 0x7054_95c7, 0x2df1_424b, 0x9efc_4947, 0x5c6b_fb31];

pub const BLOCK_BYTES: usize = 32;

pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

/// Number of filter bytes for `n` keys at `bits_per_key`.
pub fn filter_len(n: usize, bits_per_key: u32) -> usize {
    if n == 0 || bits_per_key == 0 {
        return 0;
    }
    let blocks = (n * bits_per_key as usize).div_ceil(256).max(1);
    blocks * BLOCK_BYTES
}

fn block_index(hash: u64, blocks: usize) -> usize {
    (((hash >> 32) * blocks as u64) >> 32) as usize
}

fn mask(hash: u64) -> [u32; 8] {
    let key = hash as u32;
    SALT.map(|salt| 1u32 << (key.wrapping_mul(salt) >> 27))
}

/// Sets the bits for `hash` in `filter`, a buffer of [`filter_len`] bytes.
pub fn insert(filter: &mut [u8], hash: u64) {
    let blocks = filter.len() / BLOCK_BYTES;
    if blocks == 0 {
        return;
    }
    let base = block_index(hash, blocks) * BLOCK_BYTES;
    for (i, bit) in mask(hash).into_iter().enumerate() {
        let at = base + i * 4;
        let word = u32::from_le_bytes(filter[at..at + 4].try_into().unwrap()) | bit;
        filter[at..at + 4].copy_from_slice(&word.to_le_bytes());
    }
}

/// False only if no key with this hash was inserted. An empty filter admits
/// everything.
pub fn may_contain(filter: &[u8], hash: u64) -> bool {
    let blocks = filter.len() / BLOCK_BYTES;
    if blocks == 0 {
        return true;
    }
    let base = block_index(hash, blocks) * BLOCK_BYTES;
    mask(hash).into_iter().enumerate().all(|(i, bit)| {
        let at = base + i * 4;
        u32::from_le_bytes(filter[at..at + 4].try_into().unwrap()) & bit != 0
    })
}

/// A filter of the same layout that writers add to while readers probe it.
pub struct SharedFilter {
    words: Box<[AtomicU32]>,
}

impl SharedFilter {
    /// A filter of at least `bytes` bytes, in whole blocks.
    pub fn new(bytes: usize) -> Self {
        let n = bytes.div_ceil(BLOCK_BYTES).max(1) * (BLOCK_BYTES / 4);
        // Zeroed memory fresh from the OS stays untouched until a key lands
        // in it, so a new memtable's filter costs no time to clear.
        let words = Box::into_raw(vec![0u32; n].into_boxed_slice()) as *mut [AtomicU32];
        // SAFETY: `AtomicU32` has the size, alignment and bit validity of `u32`.
        Self { words: unsafe { Box::from_raw(words) } }
    }

    fn block(&self, hash: u64) -> &[AtomicU32] {
        let base = block_index(hash, self.words.len() / 8) * 8;
        &self.words[base..base + 8]
    }

    pub fn insert(&self, hash: u64) {
        for (word, bit) in self.block(hash).iter().zip(mask(hash)) {
            // Hot keys find their bits set; loading first leaves the line
            // shared in readers' caches.
            if word.load(Relaxed) & bit == 0 {
                word.fetch_or(bit, Relaxed);
            }
        }
    }

    /// False only if no key with this hash was inserted.
    pub fn may_contain(&self, hash: u64) -> bool {
        self.block(hash).iter().zip(mask(hash)).all(|(word, bit)| word.load(Relaxed) & bit != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_fpr() {
        let n = 10_000;
        let mut filter = vec![0u8; filter_len(n, 12)];
        for i in 0..n as u64 {
            insert(&mut filter, hash_key(&i.to_be_bytes()));
        }
        for i in 0..n as u64 {
            assert!(may_contain(&filter, hash_key(&i.to_be_bytes())));
        }
        let fp = (n as u64..n as u64 * 11).filter(|i| may_contain(&filter, hash_key(&i.to_be_bytes()))).count();
        let rate = fp as f64 / (n * 10) as f64;
        assert!(rate < 0.01, "false positive rate {rate}");
    }

    #[test]
    fn shared_filter_matches_layout() {
        let n = 10_000;
        let shared = SharedFilter::new(filter_len(n, 12));
        let mut plain = vec![0u8; filter_len(n, 12)];
        for i in 0..n as u64 {
            shared.insert(hash_key(&i.to_be_bytes()));
            insert(&mut plain, hash_key(&i.to_be_bytes()));
        }
        for i in 0..n as u64 * 11 {
            let h = hash_key(&i.to_be_bytes());
            assert_eq!(shared.may_contain(h), may_contain(&plain, h));
        }
    }
}
