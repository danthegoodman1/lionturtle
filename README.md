# turtlekv-rs

A Rust implementation of [TurtleKV](https://github.com/mathworks/turtle_kv)
([paper](https://arxiv.org/abs/2509.10714)), a key-value store built on the
TurtleTree, checked with VOPR-style deterministic simulation and benchmarked
against fjall and RocksDB.

## Layout

- `turtlekv/` — the store.
  - `storage.rs` — the `Storage`/`File` traits; `FsStorage` for real disks.
  - `wal.rs` — write-ahead log, one segment per memtable.
  - `memtable.rs` — memtables behind one trait: bbtree, congee ART, crossbeam
    skip list, locked `BTreeMap`.
  - `page.rs`, `bloom.rs` — immutable data pages: 4 KiB checksummed blocks, a
    sparse index, and a split-block Bloom filter.
  - `pagestore.rs` — page arenas (one sparse file per power-of-two size class)
    and the double-buffered superblock.
  - `tree/` — the TurtleTree: buffer levels, flushes, splits and merges,
    queries, scans, and checkpoint serialization.
  - `db.rs` — the store: write path, memtable rotation, the two-stage
    checkpoint pipeline (apply, then sync and commit), recovery, and
    epoch-deferred page reclamation.
  - `sim/` — deterministic simulation: a seeded PRNG, a simulated disk with
    crash and fault semantics, the VOPR driver, and a deterministic
    write/read-amplification simulator.
- `kvbench/` — YCSB driver for turtlekv, fjall, and RocksDB, plus an
  in-memory index benchmark.

## Running

```sh
cargo test --release                                  # unit, model, and concurrency tests
cargo run --release --bin vopr -- --seeds 1000        # VOPR campaign from a random seed
cargo run --release --bin vopr -- --seed N --verbose  # replay one seed
cargo run --release --bin simperf                     # deterministic WAF sweep over χ
cargo run --release -p kvbench --features rocksdb -- --engine turtlekv --records 10000000
cargo run --release -p kvbench --bin indexbench       # bbtree vs ART vs skip list
cargo run --release -p turtlekv --example mem_footprint bbtree  # resident bytes per memtable entry
```

## Design notes

The TurtleTree follows the C++ implementation: each interior node buffers
updates in up to `max_levels` levels, where level `i` holds up to `2^i`
leaf-sized batches. Inserting a batch merges it with the occupied prefix of
levels, like incrementing a binary counter. After each insert the node flushes
at most one leaf-sized batch to the child with the most buffered bytes.
Checkpoint distance χ sets the memtable size in leaf-sized batches. Each
frozen memtable becomes one checkpoint, so pages that later batches in the
same checkpoint supersede are never written.

Departures from the C++ code:

- Each buffered segment records its flushed keys as key intervals plus a
  clip range, where the C++ code keeps item-index filters. Pivot changes
  leave segment metadata alone, and flushes update it without reading pages.
- Free space lives only in memory. At open, every arena slot outside the
  recovered tree is free. Later, a slot frees once a newer checkpoint drops
  it and every reader of older versions has finished; the commit stage then
  punches a hole over the slot, returning its space to the file system.
- Bloom filters and a sparse block index stand in for quotient filters and
  the per-leaf trie.
- The log, page arenas, and superblock are fsynced, where the C++ code
  relies on O_DIRECT completion. Each page write starts device writeback at
  once, so the commit's fsync has little left to flush.
- Writers encode and checksum log records before taking the writer lock;
  each record carries its sequence number last, so the lock only appends it.
- Nodes default to 4 buffer levels: scans and point reads gain more than
  write amplification loses at this depth.
- A scan merges a leaf with every buffer level on its root-to-leaf path, so
  a cold key range can need a disk read per level. When its thread has been
  waiting on the device, a scan starts those reads together before reading
  any, and the range costs about one read latency.
- Merges run on the store's own pool of 4 threads (`merge_threads`). They
  wait on I/O more than CPU, and every worker thread keeps memory that other
  threads freed from its buffers resident, so a pool sized to the machine
  raises RSS with little gain.
