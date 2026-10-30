# lionturtle

An embedded key-value store in Rust, built on the TurtleTree from [TurtleKV](https://github.com/mathworks/turtle_kv) ([paper](https://arxiv.org/abs/2509.10714)). The TurtleTree is a Bε-tree that merges each full memtable into the tree as one checkpoint, and its nodes buffer several checkpoints before data reaches the leaves. lionturtle writes fewer bytes than RocksDB and fjall, outpaces both on every YCSB workload measured, and keeps read tails short while every write is durable.

## Features

**Fewer bytes written.** A key overwritten within one checkpoint reaches disk once. Loading 50M records wrote 5.8 bytes per user byte, against 7.6 for RocksDB and 8.7 for fjall.

**Short tails with durable writes.** Checkpoints pace their I/O after the [SILK](https://www.usenix.org/conference/atc19/presentation/balmau) paper: each gets only the bandwidth it needs to finish before writers would wait on it. With every update durable at 150k ops/s, read p99 was 0.35 ms against 603 ms for RocksDB.

**Shared syncs.** Synchronous writers share each log write and fsync, and the log writes over retired segments. On a drive with power-loss protection, a durable write costs one 4 KiB write: 26 µs on EC2 instance store.

**Atomic batches.** A batch survives a crash whole or not at all, and no reader sees half of one. Writers commit independently, so one slow insert delays no other writer.

**Snapshots.** `Db::snapshot` returns a handle that any number of reads and iterators can share, and every read through it sees the same batches. Each iterator is a snapshot too.

**One knob trades memory for write cost.** Checkpoint distance χ sets the memtable size. A larger memtable absorbs more overwrites per checkpoint and writes fewer pages.

**Checked by deterministic simulation.** Each VOPR seed runs a random store configuration through crashes, torn writes, read errors and bit flips on a simulated disk, and checks every answer and every recovery against a model. A failing seed replays exactly.

## Performance

These runs used an EC2 i7ie.6xlarge (24 vCPUs, one instance-store NVMe drive). [BENCHMARKS.md](BENCHMARKS.md) has every result, the setup, and the commands to reproduce them.

YCSB on 50M records of 128 bytes, with 16 client threads under a 4 GB memory cap, in thousand ops/s:

| Workload | lionturtle | RocksDB | fjall |
|---|--:|--:|--:|
| Load | 1,417 | 867 | 382 |
| A: 50% read, 50% update | 1,180 | 958 | 511 |
| B: 95% read, 5% update | 1,028 | 902 | 705 |
| C: 100% read | 1,041 | 883 | 732 |
| E: 95% scan, 5% insert | 149 | 135 | 43 |
| F: 50% read, 50% read-modify-write | 680 | 646 | 464 |

Durable writes: workload A on 20M records of 1 KB, with 128 client threads under a 3 GB memory cap, and every update durable before it returns:

| Durable ops/s | Engine | Read p99 | Read p999 | Update p99 | Update p999 |
|---|---|--:|--:|--:|--:|
| 70k | lionturtle | 0.23 ms | 1.0 ms | 0.25 ms | 1.4 ms |
| | RocksDB | 4.4 ms | 399 ms | 6.8 ms | 403 ms |
| | ScyllaDB | 9.3 ms | 31 ms | 9.3 ms | 30 ms |
| 150k | lionturtle | 0.35 ms | 7.6 ms | 0.45 ms | 8.7 ms |
| | RocksDB | 603 ms | 1.4 s | 610 ms | 1.4 s |

ScyllaDB peaked at 130k–152k ops/s, so it has no 150k row. In closed loop over 10 minutes or more, sustained throughput was 226k ops/s for lionturtle, 206k for RocksDB and 88k for ScyllaDB.

### Trade-offs

- **Memory.** RSS runs about twice RocksDB's: 3.0 GB against 1.5 GB under 128 durable writers.
- **Writes at full load.** When merging falls behind, writers wait. Sustained update p999 was 18 ms against RocksDB's 15 ms, and the slowest update took 0.92 s against 75 ms. Over its first 40 seconds, RocksDB's durable throughput was the higher one: 237k ops/s against 229k.
- **Scans.** A scan merges every buffer level on its path. At 400M records, scans ran 1.03 times RocksDB's rate, with a median of 61 µs against 45.

## Quick start

```rust
use std::sync::Arc;
use lionturtle::storage::FsStorage;
use lionturtle::{Db, Options, WriteOp};

let db = Db::open(Arc::new(FsStorage::new("data")?), Options::default())?;
db.put(b"apple", b"red")?;
// One log record: recovery keeps both ops or neither, and readers never see one without the other.
db.write(&[WriteOp::put(&b"banana"[..], &b"yellow"[..]), WriteOp::delete(&b"apple"[..])], true)?;
let value = db.get(b"banana")?;
let rows = db.scan(b"a", Some(b"c"), 100)?;
let mut it = db.iter(b"", None); // a snapshot as of this call
while let Some((key, value)) = it.next()? { /* ... */ }

// Reads through one snapshot agree with each other, whatever is written meanwhile.
let snap = db.snapshot();
let fruit = snap.prefix(b"b"); // keys starting with "b", ascending
let newest = snap.iter_rev(b"a", Some(b"c")); // [a, c), descending
db.sync()?;
```

```console
cargo run --release -p lionturtle --example quickstart
```

The API is `put`, `delete`, `write` for batches, `get`, `scan`, `iter`, `iter_rev`, `prefix`, `prefix_rev`, `snapshot`, `sync`, `checkpoint` and `stats`. A `Snapshot` offers the same reads. Keys run to 64 KiB and values to 16 MiB.

## Configuration

`Options` and `TreeConfig` fields, with defaults:

| Field | Default | Effect |
|---|---|---|
| `checkpoint_distance` | 32 | Memtable size in leaf-sized batches (χ). Larger cuts write amplification and costs memory. |
| `cache_bytes` | 1 GiB | Block and page-image cache. A checkpoint keeps its newest quarter of this in unwritten pages, and applying waits once its unwritten pages pass half. |
| `memtable` | `BBTree` | Memtable implementation: `BBTree`, `SkipList`, `BTree`, or `Art8` (8-byte keys) |
| `max_frozen` | 2 | Frozen memtables allowed to await checkpointing before writers wait |
| `wal_buffer_bytes` | 1 MiB | Log bytes buffered before a write to the file |
| `merge_threads` | 8 | Threads for parallel merges and page writes; 0 uses the caller's rayon pool. Each adds reads in flight and holds memory. |
| `background` | true | Checkpoint on background threads; with false, callers drive `checkpoint_step` |
| `pace_checkpoints` | true | Spread each background checkpoint's I/O over three quarters of the time its memtable took to fill |
| `tree.leaf_size` | 4 MiB | Leaf and flush batch size |
| `tree.max_levels` | 4 | Buffer levels per node. More levels cut write amplification and slow reads. |
| `tree.filter_bits` | 12 | Bloom filter bits per key |
| `tree.max_pivots` | 64 | Pivots per node before it splits |

## Compared with RocksDB and fjall

| | lionturtle | RocksDB | fjall 3 |
|---|---|---|---|
| Structure | Bε-tree, checkpointed once per memtable | Leveled LSM tree | Leveled LSM tree |
| Atomic batches | To recovery and to readers | `WriteBatch` | Batches across keyspaces |
| Snapshots | Explicit handles; each iterator reads one | Explicit snapshots | Explicit snapshots |
| Transactions | None | Pessimistic and optimistic | Serializable single-writer; optimistic with conflict checks |
| Namespaces | One keyspace | Column families | Keyspaces |
| Iteration | Forward, reverse, prefix | Forward, reverse, prefix | Forward, reverse, prefix |
| Compression | None | Several codecs | LZ4 |
| Large values | Inline, up to 16 MiB | Inline or BlobDB | Inline or key-value separation |
| Backpressure | Writers wait while two frozen memtables await checkpoints | Slows and stops writes on L0 files and pending compaction | Delays writes as L0 runs pile up |

## Documentation

- [BENCHMARKS.md](BENCHMARKS.md): every benchmark result, how each ran, and the commands to reproduce it.
- [DESIGN.md](DESIGN.md): the write path, the TurtleTree, reads and visibility; durability guarantees; how lionturtle differs from the C++ TurtleKV; and what the tests check.

## Development

```console
cargo test --release                                   # unit, model and threaded tests
cargo run --release --bin vopr -- --seeds 1000         # 1,000 simulation seeds from a random start
cargo run --release --bin vopr -- --seed 42 --verbose  # replay one seed
cargo run --release -p kvbench --features rocksdb -- --engine lionturtle --records 10000000
cargo run --release -p kvbench --bin indexbench        # bbtree vs ART vs skip list
cargo run --release --bin simperf                      # deterministic write amplification across χ
```
