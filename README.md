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

Departures from the C++ code, chosen for simplicity:

- Flushed data is tracked per segment as key intervals plus a clip range,
  not as item-index filters. Pivot changes then never touch segment
  metadata, and flushes need no page reads to update it.
- Free space is never persisted. At open, every arena slot outside the
  recovered tree is free. Later, a slot frees only when a newer checkpoint no
  longer references it and every reader of older versions has finished.
- Bloom filters and a sparse block index replace quotient filters and the
  per-leaf trie.
- The log, page arenas, and superblock are fsynced. The C++ code relies on
  O_DIRECT completion alone.
