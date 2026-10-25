//! Resident memory per memtable entry, inserted the way the write path does:
//! the value arrives as its own `Bytes`, and the memtable keeps a clone.
//! Usage: mem_footprint <bbtree|art|skiplist|btree>

use bytes::Bytes;
use turtlekv::memtable::{MemTableKind, new_memtable};
use turtlekv::types::Value;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn rss() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap();
    s.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 4096
}

fn main() {
    let n = 1_000_000u64;
    let value = [7u8; 120];
    // One kind per process, so freed memory from one never pads another.
    let arg = std::env::args().nth(1).unwrap_or_else(|| "bbtree".into());
    {
        let kind: MemTableKind = arg.parse().unwrap();
        let before = rss();
        let t = new_memtable(kind);
        for i in 0..n {
            let k = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).to_be_bytes();
            let v = Value::Put(Bytes::copy_from_slice(&value));
            if std::env::var_os("NO_CLONE").is_some() { t.insert(&k, v, i + 1, 0) } else { t.insert(&k, v.clone(), i + 1, 0) }
        }
        let used = rss() - before;
        println!("{kind:?}: {:.0} bytes per entry for 128 logical bytes", used as f64 / n as f64);
        drop(t);
    }
}
