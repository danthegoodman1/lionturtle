# Benchmarks

Each section names its machine and setup. [How the benchmarks run](#how-the-benchmarks-run) covers the method they share and the scripts that run them.

## Contents

- [YCSB at 50M records](#ycsb-at-50m-records)
- [YCSB at 400M records](#ycsb-at-400m-records)
- [Where lionturtle wins and where it pays](#where-lionturtle-wins-and-where-it-pays)
- [Durable writes under load](#durable-writes-under-load)
- [Reads during sustained writes](#reads-during-sustained-writes)
- [Synchronous writes](#synchronous-writes)
- [Clock speed](#clock-speed)
- [Compression](#compression)
- [fjall in these runs](#fjall-in-these-runs)
- [How the benchmarks run](#how-the-benchmarks-run)

## YCSB at 50M records

50M records of 8-byte keys and 120-byte values (6.4 GB of user data) on an EC2 i7ie.6xlarge (24 vCPUs, one instance-store NVMe drive under ext4), under a 4 GB memory cap that covers page cache, with the CPUs' C6 idle state disabled. Every engine got a 1 GB block cache, 128 MiB memtables and 16 client threads, and ran once on a freshly trimmed drive.

With LZ4 on every engine, on values whose second half repeats the first (`--compression-ratio 0.5`), so they compress to about half their size:

| Workload (thousand ops/s) | lionturtle | RocksDB | fjall | vs RocksDB |
|---|--:|--:|--:|--:|
| Load (inserts) | 1,710 | 1,008 | 394 | 1.70× |
| A: 50% read, 50% update | 1,473 | 1,180 | 562 | 1.25× |
| B: 95% read, 5% update | 1,496 | 1,248 | 1,066 | 1.20× |
| C: 100% read | 1,617 | 1,209 | 1,130 | 1.34× |
| E: 95% scan of up to 100 keys, 5% insert | 224 | 214 | 50 | 1.05× |
| F: 50% read, 50% read-modify-write | 917 | 841 | 578 | 1.09× |
| Write amplification during load | 4.0 | 4.3 | 5.1 | |
| Disk after load (GB) | 5.3 | 5.1 | 4.6 | |
| RSS during reads (GB) | 1.9 | 1.7 | 1.7 | |

| Read latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| A | 129 µs | 248 µs | 120 µs | 291 µs | 130 µs | 246 µs |
| B | 107 µs | 155 µs | 118 µs | 170 µs | 136 µs | 229 µs |
| C | 106 µs | 138 µs | 118 µs | 166 µs | 134 µs | 225 µs |
| E (scans) | 310 µs | 457 µs | 365 µs | 472 µs | 6.4 ms | 7.4 ms |
| F | 116 µs | 218 µs | 120 µs | 224 µs | 140 µs | 251 µs |

Reads that hit memory took 0.6 µs at the median on C for lionturtle, 1.9 µs for RocksDB and 1.9 µs for fjall. Over C, lionturtle read 17 GB from the drive, RocksDB 28 GB and fjall 46 GB. Scan p50 on E was 36, 29 and 59 µs.

| Write latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| Load (inserts) | 22 µs | 60 µs | 28 µs | 332 µs | 299 µs | 492 µs |
| A (updates) | 8 µs | 30 µs | 21 µs | 456 µs | 223 µs | 347 µs |
| B (updates) | 7 µs | 32 µs | 13 µs | 453 µs | 21 µs | 101 µs |
| E (inserts) | 6 µs | 15 µs | 18 µs | 125 µs | 18 µs | 28 µs |
| F (read-modify-writes) | 120 µs | 248 µs | 130 µs | 542 µs | 177 µs | 288 µs |

Update p99.9 on A and B was 30–32 µs for lionturtle, 453–456 µs for RocksDB and 101–347 µs for fjall. A read-modify-write includes its read.

Without compression, on random values:

| Workload (thousand ops/s) | lionturtle | RocksDB | fjall | vs RocksDB |
|---|--:|--:|--:|--:|
| Load (inserts) | 1,635 | 984 | 398 | 1.66× |
| A: 50% read, 50% update | 1,256 | 983 | 524 | 1.28× |
| B: 95% read, 5% update | 1,144 | 914 | 731 | 1.25× |
| C: 100% read | 1,192 | 901 | 746 | 1.32× |
| E: 95% scan of up to 100 keys, 5% insert | 160 | 142 | 45 | 1.13× |
| F: 50% read, 50% read-modify-write | 727 | 634 | 476 | 1.15× |
| Write amplification during load | 5.8 | 7.7 | 9.3 | |
| Disk after load (GB) | 8.2 | 9.2 | 8.1 | |
| RSS during reads (GB) | 1.9 | 1.5 | 1.8 | |

| Read latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| A | 140 µs | 304 µs | 133 µs | 423 µs | 147 µs | 463 µs |
| B | 110 µs | 193 µs | 127 µs | 235 µs | 150 µs | 251 µs |
| C | 108 µs | 156 µs | 126 µs | 229 µs | 147 µs | 245 µs |
| E (scans) | 461 µs | 664 µs | 462 µs | 585 µs | 6.4 ms | 7.7 ms |
| F | 125 µs | 272 µs | 131 µs | 265 µs | 157 µs | 303 µs |

| Write latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| Load (inserts) | 24 µs | 69 µs | 30 µs | 357 µs | 292 µs | 477 µs |
| A (updates) | 7 µs | 35 µs | 21 µs | 470 µs | 195 µs | 308 µs |
| B (updates) | 7 µs | 30 µs | 13 µs | 416 µs | 20 µs | 95 µs |
| E (inserts) | 6 µs | 15 µs | 18 µs | 102 µs | 19 µs | 30 µs |
| F (read-modify-writes) | 129 µs | 304 µs | 143 µs | 560 µs | 181 µs | 334 µs |

## YCSB at 400M records

400M records (51 GB of user data) on an EC2 i7ie.48xlarge (192 vCPUs, 16 NVMe drives in RAID 0 under XFS), under a 16 GB memory cap that covers page cache, with the CPUs' C6 idle state disabled. Every engine got a 4 GB block cache, 128 MiB memtables and 32 client threads; RocksDB loaded with 8, its fastest. RocksDB and fjall ran one after the other on one instance, and lionturtle later on another of the same type, with the drives trimmed before each. E ran 5M operations and every other workload 50M.

With LZ4 on every engine, on values that compress by half:

| Workload (thousand ops/s) | lionturtle | RocksDB | fjall | vs RocksDB |
|---|--:|--:|--:|--:|
| Load (inserts) | 927 | 475 | 297 | 1.95× |
| A: 50% read, 50% update | 1,386 | 1,129 | 391 | 1.23× |
| B: 95% read, 5% update | 1,545 | 1,370 | 1,114 | 1.13× |
| C: 100% read | 1,568 | 1,403 | 1,159 | 1.12× |
| E: 95% scan of up to 100 keys, 5% insert | 298 | 268 | 54 | 1.11× |
| F: 50% read, 50% read-modify-write | 1,020 | 871 | 453 | 1.17× |
| Write amplification during load | 5.8 | 7.9 | 9.7 | |
| Disk after load (GB) | 36.7 | 31.8 | 33.5 | |
| RSS during reads (GB) | 6.9 | 6.0 | 5.9 | |

| Read latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| A | 124 µs | 217 µs | 126 µs | 242 µs | 139 µs | 314 µs |
| B | 111 µs | 190 µs | 122 µs | 215 µs | 140 µs | 243 µs |
| C | 109 µs | 182 µs | 120 µs | 208 µs | 140 µs | 240 µs |
| E (scans) | 357 µs | 514 µs | 451 µs | 755 µs | 8.0 ms | 10.2 ms |
| F | 117 µs | 206 µs | 124 µs | 225 µs | 141 µs | 266 µs |

Most reads hit memory: median read latency on C was 0.9 µs for lionturtle, 4.0 µs for RocksDB and 4.9 µs for fjall. The rest wait on the drives. On A, lionturtle's read p99.9 was 217 µs against RocksDB's 242, and on F 206 µs against 225. Scan p99.9 on E was 514 µs against 755 µs, and scan p50 58, 49 and 187 µs.

| Write latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| Load (inserts) | 88 µs | 137 µs | 30 µs | 245 µs | 891 µs | 1.4 ms |
| A (updates) | 50 µs | 83 µs | 54 µs | 789 µs | 771 µs | 1.2 ms |
| B (updates) | 14 µs | 45 µs | 22 µs | 526 µs | 49 µs | 449 µs |
| E (inserts) | 10 µs | 20 µs | 24 µs | 352 µs | 22 µs | 35 µs |
| F (read-modify-writes) | 131 µs | 268 µs | 148 µs | 800 µs | 471 µs | 844 µs |

A buffered write appends to a log buffer that stays resident between flushes, then inserts into the memtable. lionturtle's update p99.9 was 45 µs on B against RocksDB's 526, and its insert p99.9 on E 20 µs against 352. A read-modify-write includes its read, which can wait on a drive. RocksDB loaded with 8 threads to lionturtle's 32, which shortens its load latencies.

Without compression, on random values:

| Workload (thousand ops/s) | lionturtle | RocksDB | fjall | vs RocksDB |
|---|--:|--:|--:|--:|
| Load (inserts) | 914 | 466 | 299 | 1.96× |
| A: 50% read, 50% update | 1,350 | 1,028 | 372 | 1.31× |
| B: 95% read, 5% update | 1,348 | 1,173 | 940 | 1.15× |
| C: 100% read | 1,406 | 1,172 | 946 | 1.20× |
| E: 95% scan of up to 100 keys, 5% insert | 236 | 209 | 38 | 1.13× |
| F: 50% read, 50% read-modify-write | 916 | 745 | 432 | 1.23× |
| Write amplification during load | 9.0 | 13.5 | 15.6 | |
| Disk after load (GB) | 61.9 | 59.4 | 56.1 | |
| RSS during reads (GB) | 6.9 | 5.9 | 6.1 | |

| Read latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| A | 127 µs | 243 µs | 134 µs | 307 µs | 146 µs | 344 µs |
| B | 109 µs | 192 µs | 127 µs | 222 µs | 147 µs | 300 µs |
| C | 108 µs | 185 µs | 125 µs | 226 µs | 146 µs | 396 µs |
| E (scans) | 489 µs | 689 µs | 539 µs | 855 µs | 11.7 ms | 15.6 ms |
| F | 119 µs | 223 µs | 131 µs | 320 µs | 149 µs | 322 µs |

| Write latency | lionturtle p99 | p99.9 | RocksDB p99 | p99.9 | fjall p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| Load (inserts) | 96 µs | 156 µs | 37 µs | 251 µs | 742 µs | 1.3 ms |
| A (updates) | 52 µs | 88 µs | 69 µs | 745 µs | 759 µs | 1.2 ms |
| B (updates) | 14 µs | 45 µs | 24 µs | 707 µs | 46 µs | 386 µs |
| E (inserts) | 11 µs | 20 µs | 24 µs | 187 µs | 22 µs | 34 µs |
| F (read-modify-writes) | 134 µs | 423 µs | 162 µs | 807 µs | 440 µs | 813 µs |

## Where lionturtle wins and where it pays

- **Writes.** A checkpoint merges a whole memtable into the tree at once, and buffer levels absorb several checkpoints before data reaches the leaves. At 50M records with LZ4, loads lead by 1.70×, and the mixed workloads A and F by 1.09–1.25×.
- **Point reads.** A get checks the memtables, skipping any whose Bloom filter rules the key out, then walks root to leaf. At each node it checks one Bloom filter per buffer level and reads at most one 4 KiB block. Page metadata (block index and filters) stays in memory, so a miss in the cache costs one device read.
- **Scans.** A scan merges the leaf with every buffer level on its root-to-leaf path: up to nine sorted runs with the default four levels. In a cold key range each run can need its own device read. A scan starts those reads together before reading any, along with the leaf's next block when it expects to read more than half a block, so a cold range costs about one read latency. Background threads start them, because starting a read allocates page cache, and under a memory limit each allocation can wait milliseconds to reclaim memory. An LSM tree keeps about 90% of its data in the last level, so its scans touch fewer runs. At 400M records with LZ4, lionturtle's scans ran 1.11 times RocksDB's rate, with p99.9 of 514 µs against 755 and a median of 58 µs against 49.
- **Memory.** Page metadata stays resident: about 1 GB per 400M keys, outside the block cache. Memtables keep every version of a key until the next checkpoint, a checkpoint holds the pages it creates until it writes them, up to half the cache, and each merge thread's allocator heap keeps buffers other threads freed. RSS at equal cache sizes runs above RocksDB's, which leaves less page cache under a cap: 2.8 GB against 1.3 GB under 128 durable writers in [Durable writes under load](#durable-writes-under-load).

## Durable writes under load

Workload A with uniform keys, half reads and half updates, on 20M records of 1 KB: 20 GB against a 3 GB memory cap that covers page cache, so nearly every read goes to the drive. Every update is durable before it returns: `--sync 1` for the embedded engines, and ScyllaDB's commitlog in batch mode. 128 client threads drive each engine, each engine caches 256 MB, and each run starts from a fresh copy of the same loaded store, out of the page cache. The embedded engines ran with LZ4 on; the values are random, so it saves little. The machine is an EC2 i7ie.6xlarge (24 vCPUs) with one of its 7.5 TB instance-store NVMe drives under ext4, deep idle states off, and the drive trimmed before each run. lionturtle ran version 0.5.1; the log-buffer reuse in 0.5.2 left these runs within noise. ScyllaDB's rows come from an earlier run with the same setup; kvbench's changes since then leave its path unchanged.

At a fixed offered load, five minutes each:

| Durable ops/s | Engine | Read p50 | Read p99 | Read p99.9 | Update p50 | Update p99 | Update p99.9 | Seconds with read p99 over 10 ms |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| 11k | lionturtle | 98 µs | 0.19 ms | 0.28 ms | 36 µs | 0.12 ms | 0.16 ms | 0 of 300 |
| | RocksDB | 113 µs | 0.20 ms | 0.47 ms | 185 µs | 0.42 ms | 3.4 ms | 0 |
| | ScyllaDB | 193 µs | 1.4 ms | 2.8 ms | 107 µs | 1.3 ms | 2.3 ms | 0 |
| 70k | lionturtle | 103 µs | 0.24 ms | 0.88 ms | 89 µs | 0.26 ms | 1.1 ms | 0 |
| | RocksDB | 118 µs | 3.2 ms | 14 ms | 416 µs | 5.4 ms | 17 ms | 15 |
| | ScyllaDB | 540 µs | 9.3 ms | 31 ms | 529 µs | 9.3 ms | 30 ms | 62 |
| 118k | lionturtle | 108 µs | 0.31 ms | 3.5 ms | 152 µs | 0.39 ms | 3.8 ms | 1 |
| | RocksDB | 127 µs | 12 ms | 26 ms | 489 µs | 14 ms | 27 ms | 118 |
| 150k | lionturtle | 113 µs | 0.37 ms | 6.7 ms | 163 µs | 0.47 ms | 7.8 ms | 3 |
| | RocksDB | 136 µs | 860 ms | 1.5 s | 596 µs | 863 ms | 1.5 s | 184 |

70k ops/s is half of ScyllaDB's closed-loop peak, and 118k half of RocksDB's over its first 40 seconds. In closed loop, where each client issues its next operation when the last returns:

| Closed loop, 128 clients | lionturtle | RocksDB | ScyllaDB |
|---|--:|--:|--:|
| Throughput, sustained | 253k ops/s | 205k | 88k |
| Throughput over the first 40 seconds | 258k | 238k | 130k–152k |
| Update p50, sustained | 297 µs | 907 µs | 1.25 ms |
| Update p99 | 0.93 ms | 4.6 ms | 3.1 ms |
| Update p99.9 | 11 ms | 19 ms | 12 ms |
| Update max | 0.80 s | 86 ms | 53 ms |
| Read p99 | 0.49 ms | 0.60 ms | 7.9 ms |
| Read p99.9 | 0.78 ms | 0.96 ms | 24 ms |
| Write amplification | 8.1 | 12.0 | |
| Peak RSS | 2.8 GB | 1.3 GB | |

Sustained runs lasted 9 to 17 minutes: 140M operations for the embedded engines and 90M for ScyllaDB, which slowed from 96k ops/s in its first minute to 77k in its sixteenth. RocksDB slows too. It defers compaction to keep writes moving: over 11 minutes its pending compaction grew to 8.1 GB, and its throughput fell from 223k ops/s in the first minute to 198k–207k after. The deferred compaction competes with reads: at 118k ops/s its read p99 was 12 ms, and at 150k 860 ms. lionturtle checkpoints each memtable as it fills and held 247k–256k ops/s in every minute. When merging falls behind, its writers wait instead: its slowest update took 0.80 s against RocksDB's 86 ms, while its update p99.9 was 11 ms against 19 ms.

ScyllaDB reports single-digit-millisecond read p99s while it compacts: on one i4i.4xlarge at about half its peak load, a 50/50 read/write workload held read p99 at 6.06 ms through a major compaction ([ScyllaDB 5.0 on i4i](https://www.scylladb.com/2022/09/07/benchmarking-scylladb-5-0-on-aws-i4i-4xlarge/)). `bench/scylla.sh` runs ScyllaDB 2026.3.4 as one node with 8 shards, an `iotune` profile of the drive and the same 3 GB cap, and kvbench calls it over CQL on loopback. The comparison favors the embedded engines in three ways:

- **Requests.** Every ScyllaDB request crosses loopback, is parsed as CQL, and hops to the shard that owns its key, which puts its medians near 0.5 ms at 70k ops/s.
- **Memory.** ScyllaDB recommends at least 1 GB per shard; under the cap its 8 shards share 2.5 GB.
- **Cores.** Its shards run on 8 of the 24 vCPUs with `--overprovisioned`, sharing them with the client threads.

With buffered writes from 16 client threads instead, as in the YCSB tables, the closed-loop peak was 248k–249k ops/s for lionturtle in two runs and 209k–210k for RocksDB.

The drive keeps reads fast beside heavy writes. In fio, 4 KiB reads at 72k IOPS beside 600 MB/s of writes held read p99.99 under 0.9 ms, whether the kernel wrote the data back or the writer issued it directly in 256 KiB or 4 MiB pieces. In the same test, the workstation's consumer NVMe drive held read p99.99 near 50 ms for every write pattern but 4 MiB direct writes, which held it at 6 ms, so every engine's tails run longer there.

```console
REGION=us-west-2 bench/aws/launch.sh i7ie.6xlarge <subnet> lionturtle-bench
# On the instance, with this repository at ~/lionturtle:
ONE_DRIVE=1 bench/aws/setup.sh
sudo cpupower idle-set -D 10
k=~/lionturtle/target/release/kvbench
load="--records 20000000 --value-size 1000 --cache-mb 256 --phases load --settle 1 --keep 1 --compression lz4"
systemd-run --user --scope -p MemoryMax=3G $k --engine lionturtle --threads 16 --dir /mnt/data/keep-lionturtle $load
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 systemd-run --user --scope -p MemoryMax=3G \
  $k --engine rocksdb --threads 8 --dir /mnt/data/keep-rocksdb $load
THREADS=128 bench/aws/run.sh lionturtle lt-70k 70000 21000000 --sync 1 --compression lz4    # five minutes at 70k
THREADS=128 bench/aws/run.sh rocksdb rocks-long - 140000000 --sync 1 --compression lz4   # closed loop
# ScyllaDB: load with the periodic commitlog, flush and compact, then restart it in batch mode.
sudo sysctl -w fs.aio-max-nr=1048576
bench/scylla.sh /mnt/data/scylla periodic &
run="$k --engine scylla --records 20000000 --value-size 1000 --threads 128 --dir /tmp/kvbench-scylla --keep 1"
$run --phases load
bench/scylla.sh /mnt/data/scylla &
$run --phases a --uniform 1 --rate 70000 --ops 21000000 --timeline timeline.jsonl
# Back on the workstation:
bench/aws/teardown.sh lionturtle-bench
```

## Reads during sustained writes

A checkpoint reads and writes several times its memtable: about 1.2 GB for each 128 MiB memtable of uniform 1 KB updates. On a drive with little bandwidth to spare, client reads queue behind that I/O. The [SILK](https://www.usenix.org/conference/atc19/presentation/balmau) paper (ATC '19) traced an LSM tree's latency spikes to the same contention, and to flushes stuck behind compactions until writes stop. lionturtle paces checkpoints on SILK's two lessons: background I/O gets only the bandwidth it needs to finish before writers would wait on it, and full bandwidth once they would.

Each checkpoint finishes within three quarters of the time its memtable took to fill, or sooner if the next memtable fills faster. A paced checkpoint spreads its reads and writes evenly over that time, writes pages directly to the drive in 256 KiB pieces, and skips merge read-ahead, so a client read queues behind little of its I/O. A checkpoint runs at full speed when writers wait on it or waited on the one before, when another frozen memtable waits behind it, and when a caller asks for it. Then it writes each page whole and reads ahead, which finishes sooner: on the NVMe drive above, whole-page writes raised peak throughput by 7%. Before its first checkpoint, a store expects one to read and write 12 times its memtable.

`bench/steady-writes.sh` runs SILK's experiment: workload A with uniform keys at a fixed rate for five minutes, on 5M records of 1 KB with a 256 MB cache and a 3 GB memory cap. The drive, from `bench/slow-drive.sh`, is RAM-backed and throttled to 200 MB/s with 100 µs latency, like the SATA SSD in that paper. Each latency counts from the operation's scheduled start, so an operation stuck behind another counts its wait:

| Offered load | Engine | Read mean | Read p99 | Read max | Seconds with read p99 over 10 ms |
|---|---|--:|--:|--:|--:|
| 15k ops/s | lionturtle, paced | 0.1 ms | 0.2 ms | 33 ms | 6 of 300 |
| | lionturtle, unpaced | 38 ms | 471 ms | 913 ms | 118 |
| | RocksDB, rate limiter | 18 ms | 344 ms | 709 ms | 97 |
| | RocksDB | 128 ms | 1.5 s | 2.0 s | 114 |
| 22k ops/s | lionturtle, paced | 0.3 ms | 7.3 ms | 49 ms | 16 |
| | lionturtle, unpaced | 300 ms | 1.9 s | 2.7 s | 188 |
| | RocksDB, rate limiter | 767 ms | 8.8 s | 10.6 s | 164 |
| | RocksDB | 2.4 s | 7.8 s | 8.4 s | 210 |
| 10k ops/s for 20 s, then 30k for 10 s | lionturtle, paced | 43 ms | 1.0 s | 1.6 s | 58 |
| | lionturtle, unpaced | 452 ms | 2.9 s | 3.6 s | 138 |
| | RocksDB, rate limiter | 952 ms | 12.3 s | 14.7 s | 155 |
| | RocksDB | 886 ms | 7.8 s | 8.8 s | 137 |

RocksDB ran with the benchmark's settings: 16 background jobs and subcompactions, 128 MiB memtables, a 256 MB block cache, 10-bit Bloom filters, no compression and jemalloc. "Rate limiter" adds its auto-tuned rate limiter, which caps flush and compaction writes at between 10 and 200 MB/s and moves the cap with recent demand; 200 MB/s is the drive's bandwidth.

The limiter shields reads from compaction, but it throttles flushes too. When two full memtables wait on a throttled flush, RocksDB stops all writes, and each client thread queues behind its stopped write. At 22k ops/s that left 39 seconds in which no operation finished. Without the limiter, RocksDB fell behind the 22k schedule and its median read waited 1.8 s.

lionturtle wrote 5.5–5.7 bytes per user byte in every run, paced or not; no run stalled a writer, and paced runs held median read latency at 112–121 µs. The limiter cut RocksDB's write amplification from 6.1 to 3.5 at 15k ops/s by deferring compaction, and the deferred work came due. Over 20 minutes at 15k ops/s, RocksDB with the limiter fell up to 15 s behind in minutes 8 through 10, and its median per-second read p99 stayed above 130 ms for the rest of the run.

| 20 minutes at 15k ops/s | Read mean | Read p99 | Read max | Seconds with read p99 over 10 ms | Write amplification |
|---|--:|--:|--:|--:|--:|
| lionturtle, paced | 0.3 ms | 7 ms | 212 ms | 36 of 1,200 | 5.7 |
| RocksDB, rate limiter | 756 ms | 14.1 s | 15.7 s | 850 of 1,200 | 7.7 |

Unpaced, a checkpoint writes whole pages and reads ahead on 8 merge threads, and a 200 MB/s drive serves that only by holding client reads for hundreds of milliseconds. In bursts of 30k ops/s, a second memtable fills before the first one's checkpoint finishes, so the next checkpoint runs at full speed, and those seconds hold reads up to 1.6 s. Where writers outrun checkpoints and wait on them, as in the closed-loop tables above, checkpoints run unpaced.

```console
cargo build --release -p kvbench --features rocksdb
sudo bench/slow-drive.sh /mnt/slow
bench/steady-writes.sh /mnt/slow/data
```

## Synchronous writes

`write(.., true)` returns once its batch is durable. Synchronous writers share syncs: one writer at a time writes every buffered record in one direct write and makes it durable, and the others return when a sync covers them. When every earlier log write is already durable, that write is one `pwritev2` with `RWF_DSYNC`; otherwise it is a write and an fsync. The log bypasses the page cache and writes over retired segments, so a sync has no dirty pages to write back and no file size to journal. A new store has no retired segment until its first checkpoint, so its first segments append, unless `preallocate_log` zero-fills them ahead of their writes.

Thousand durable updates per second, from `bench/sync.sh` (3M records, 128 MiB memtables, background work finished first), median of three runs:

| Client threads | lionturtle | RocksDB | RocksDB, recycled logs | fjall |
|---|--:|--:|--:|--:|
| Workstation NVMe, 1 | 0.5 | 0.3 | 0.5 | 0.5 |
| Workstation NVMe, 16 | 4.1 | 2.4 | 4.7 | 0.5 |
| Workstation NVMe, 64 | 16.0 | 7.7 | 15.1 | 0.4 |
| EC2 i7ie.6xlarge, 1 | 37.2 | 7.9 | 35.2 | 32.4 |
| EC2 i7ie.6xlarge, 16 | 191.1 | 63.1 | 179.7 | 30.4 |
| EC2 i7ie.6xlarge, 64 | 383.7 | 141.8 | 251.8 | 16.2 |

The workstation drive (ext4) has a volatile write cache, so every sync waits about 2 ms for a cache flush. The i7ie's instance-store drive (ext4) reports a write-through cache, as drives with power-loss protection do, so Linux sends no flush: a sync write costs one 4 KiB write, 27 µs on average for lionturtle on one thread.

The EC2 rows ran with the CPUs' C6 idle state disabled. C6 takes 190 µs to exit. In an earlier run with it enabled, RocksDB with recycled logs fell to 29 thousand durable updates per second at 16 threads and 54 thousand at 64, RocksDB's one-thread mean rose from 127 to 548 µs, and fjall fell to 4 thousand per second at 64 threads. lionturtle fell to 127 thousand at 16 threads and held within 4% of its C6-off rate at 1 and 64. For latency-sensitive storage, disable deep idle states: `sudo cpupower idle-set -D 10` disables those with an exit latency of 10 µs or more.

The i7ie.6xlarge has one CPU socket; the i7ie.48xlarge has two, and writers on different sockets pass the log's state across the link between them. There, binding the process to one socket's CPUs raised 64-thread durable throughput by 17% for lionturtle, from 319 to 373 thousand per second, and by 10% for RocksDB with recycled logs, at the cost of the other socket's cores for reads and background work:

```console
taskset -c "$(cat /sys/devices/system/node/node0/cpulist)" <command>
```

Power-loss protection needs no lionturtle setting. Linux reads each drive's cache mode from `/sys/block/<drive>/queue/write_cache` and skips flushes on `write through`. A drive with power-loss protection that reports `write back` still gets flushes, which such drives finish quickly; writing `write through` to that file stops them, and is safe only when the cache really survives power loss.

## Clock speed

Cached operations scale with core speed. To measure it alone, lionturtle ran on a workstation with an Intel Core Ultra 9 285 (24 cores), on 10M records in a 2 GB cache, so every read hit memory, with 16 client threads and checkpoints written to a RAM-backed `null_blk` drive. It ran at full clock (P-cores up to 5.8 GHz, E-cores up to 4.6 GHz) and with every core capped at 3.2 GHz, the i7ie's all-core turbo, three alternating runs each:

| Workload (thousand ops/s) | Full clock | 3.2 GHz | Full / capped |
|---|--:|--:|--:|
| Load (inserts) | 2,953 | 2,113 | 1.40× |
| A: 50% read, 50% update | 5,492 | 4,041 | 1.36× |
| C: 100% read | 22,346 | 18,058 | 1.24× |
| E: 95% scan of up to 100 keys, 5% insert | 1,119 | 821 | 1.36× |

Point reads gain less than the clock, because much of a 0.6 µs read waits on memory. Writes and scans gain more: capping raised mean update latency from 4.2 to 6.0 µs. On EC2 at 400M records, the reads that miss memory wait about 100 µs on a drive and make up most of the mean read latency, so there clock speed matters mainly for writes.

```console
echo 3200000 | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq
./target/release/kvbench --engine lionturtle --records 10000000 --ops 20000000 --threads 16 --cache-mb 2048 --phases load,a,c,e --settle 1
```

## Compression

50M records of 8-byte keys and 120-byte values whose second half repeats the first (`--compression-ratio 0.5`), on a workstation with an Intel Core Ultra 9 285 (24 cores), 125 GB of RAM and a 2 TB Micron consumer NVMe drive (MTFDKBA2T0TGD, ext4, volatile write cache), under a 4 GB memory cap that covers page cache. Every engine got a 1 GB block cache, 128 MiB memtables and 16 client threads, and ran without compression and with LZ4 on every data block. `bench/compression.sh` runs each configuration twice, the second pass in reverse order, and trims the drive before each run. The table gives the mean of the two passes, which agreed within 8%, except lionturtle's uncompressed load (2,117 thousand inserts per second against 1,763), lionturtle with LZ4 on B (11%) and fjall with LZ4 on E (11%).

| Workload (thousand ops/s) | lionturtle | lionturtle LZ4 | RocksDB | RocksDB LZ4 | fjall | fjall LZ4 |
|---|--:|--:|--:|--:|--:|--:|
| Load (inserts) | 1,940 | 2,108 | 1,141 | 1,384 | 427 | 446 |
| A: 50% read, 50% update | 1,170 | 1,433 | 1,007 | 1,369 | 647 | 728 |
| B: 95% read, 5% update | 1,733 | 2,298 | 944 | 1,924 | 953 | 1,212 |
| C: 100% read | 1,994 | 2,600 | 972 | 2,062 | 986 | 1,243 |
| E: 95% scan of up to 100 keys, 5% insert | 274 | 381 | 154 | 338 | 50 | 52 |
| F: 50% read, 50% read-modify-write | 816 | 1,000 | 737 | 1,113 | 566 | 702 |
| Write amplification during load | 5.8 | 3.9 | 7.4 | 4.4 | 8.8 | 5.0 |
| Disk after A (GB) | 9.1 | 5.5 | 7.7 | 4.7 | 7.8 | 5.0 |
| Read from the drive on C (GB) | 24.7 | 17.4 | 50.1 | 23.8 | 78.5 | 48.3 |
| RSS during C (GB) | 1.9 | 1.9 | 1.5 | 1.6 | 1.8 | 1.8 |

- **lionturtle.** LZ4 raised throughput by 22% on A and F to 39% on E, raised the load's by 9%, and cut the pages written during the load from 29.3 to 17.3 GB. A block that misses the cache costs one 4 KiB read either way, and with LZ4 that page holds about 1.7 times as many entries, so the page cache covers more of the store: C read 30% less from the drive.
- **RocksDB.** It gains the most. Its uncompressed blocks hold about 4 KiB plus a 5-byte trailer at unaligned offsets, so nearly every one spans two pages, and C read twice as much from the drive as lionturtle. LZ4 halves that. With LZ4 on both, lionturtle leads by 5% on A, 19% on B, 26% on C and 13% on E, and loads 1.52 times as fast; RocksDB leads by 11% on F. Checkpoint pacing accounts for F: in runs of A and F alone on copies of one loaded store, `--pace 0` raised lionturtle's F from 1,065 to 1,227 thousand ops/s, against RocksDB's 1,180, and its A by 8%. On A, lionturtle's read p99 was 164 µs against RocksDB's 86; on C, without writes, it was 59 µs against 81.
- **fjall.** With LZ4, C read 38% less from the drive and ran 26% faster.

To isolate the CPU cost, lionturtle and RocksDB ran 20M records of the same values on tmpfs with a 256 MB block cache, so a read that misses the block cache copies from memory instead of waiting on a drive. On C, one lionturtle read in five missed the block cache, and with LZ4 each of those decompressed its block: 2.2 million blocks per second. Medians of three alternating runs:

| Workload (thousand ops/s) | lionturtle | lionturtle LZ4 | Change | RocksDB | RocksDB LZ4 | Change |
|---|--:|--:|--:|--:|--:|--:|
| Load (inserts) | 2,928 | 2,896 | −1.1% | 1,402 | 1,403 | +0.1% |
| A: 50% read, 50% update | 5,160 | 5,212 | +1.0% | 2,513 | 2,515 | +0.1% |
| B: 95% read, 5% update | 9,591 | 9,808 | +2.3% | 5,354 | 5,283 | −1.3% |
| C: 100% read | 10,684 | 10,974 | +2.7% | 6,298 | 5,938 | −5.7% |
| E: 95% scan of up to 100 keys, 5% insert | 709 | 788 | +11.2% | 773 | 975 | +26.1% |
| F: 50% read, 50% read-modify-write | 4,600 | 4,461 | −3.0% | 2,471 | 2,426 | −1.8% |

LZ4 cost lionturtle at most 3.0% on any workload. Mean read latency on C fell from 1.40 to 1.36 µs, and RocksDB's rose from 2.23 to 2.35 µs.

Values that compress too little cost nothing to store with LZ4 on, since lionturtle then stores its pages as it would without compression. The same tmpfs runs with random values, and with values whose last tenth repeats their first (`--compression-ratio 0.9`), stayed within 2.4% of the runs without LZ4 on every workload, except E on random values at −5.6%, where runs of one configuration spread by 10%. Compressing every block of the 0.9 values instead cut page writes by 12% and cost 4–5% on A, B and C. On the i7ie.6xlarge under a 4 GB cap, with 50M records and two runs of each, compressing every block of those values cut page writes by 6%, read 1% more from the drive, slowed the load by 7%, and left A and C within 2%. Values that compress to 0.75 of their size clear the cutoff: lionturtle compressed them, cutting page writes by 19% and drive reads on C by 11%, and ran A 6% and C 11% faster than without LZ4.

```console
bench/compression.sh bench-data/ycsb > compression.jsonl
./target/release/kvbench --engine lionturtle --dir /dev/shm/kvbench --records 20000000 --ops 20000000 --threads 16 \
  --cache-mb 256 --compression lz4 --compression-ratio 0.5
```

## fjall in these runs

fjall trails here by more than its own benchmarks suggest, because these runs use 16–32 client threads and scan after update phases. Both stress parts of fjall's design:

- **One journal lock.** Every fjall write takes one journal mutex, so loads slow as threads grow. On the workstation at 10M records, fjall loaded 781 thousand inserts per second on one thread, ahead of RocksDB's 681, and 435 thousand on 16.
- **Version skipping.** A fjall scan steps past a key's older versions one at a time; RocksDB reseeks after eight. Zipfian updates pile versions onto hot keys, so E after A and B scans slowly: on 2M records with one thread, fjall ran 106–125 thousand scans per second after the load and 7.6 thousand after 400 thousand operations of workload A.
- **Synchronous writes.** fjall persists its journal once per writer, without grouping, so more threads add no durable throughput: on EC2 it fell from 32 thousand per second on one thread to 16 thousand on 64.

## How the benchmarks run

`kvbench` drives every engine with the same keys, values, operation mix, operation counts and client threads.

- Each run is one process in one memory cgroup, with page cache counted against the cap.
- No engine compresses data unless `--compression lz4` turns on LZ4 for all of them, and no engine fsyncs per write. RocksDB buffers its log manually, fjall persists its journal manually, and lionturtle buffers up to 1 MiB of log. `--sync 1` makes every write in the timed phases durable before it returns; `bench/sync.sh` runs that comparison.
- RocksDB runs on jemalloc, its production allocator. lionturtle and fjall run on mimalloc.
- kvbench turns off transparent huge pages for its process. mimalloc asks for them on its arenas, and under a memory cap they leave less of it for page cache: on C at 50M records with LZ4 under a 4 GB cap, lionturtle kept 1.2 GB of page cache with them and 2.3 GB without, and ran 37% faster without. Between two runs of `bench/compression.sh`, turning them off raised lionturtle's throughput after the load by 13–33% with LZ4 and 4–19% without, and moved RocksDB's by at most 7%. Reads during sustained writes, Clock speed and the workstation rows of Synchronous writes ran with them on; every other section ran with them off.
- lionturtle's Bloom filters use 12 bits per key against RocksDB's 10. The extra memory counts against its cap.
- Latency runs need a quiet drive. Each read of an NVMe drive's temperature in `/sys/class/hwmon` sends the drive a SMART log request, and the workstation's drive holds reads about 3.4 ms behind each one; monitoring agents that poll every second put a floor under every second's p99. Eight ClickHouse processes, polling together, made that floor about 40 ms.
- Runs on an SSD need it trimmed. A drive that has not been told which blocks were freed copies them during garbage collection, and on the workstation's drive, mounted without `discard` and trimmed weekly, identical runs drifted 30% apart after a few hundred GB of copies were written and deleted. Run `sudo fstrim /` before each run; after one full trim, six alternating runs of three builds landed within 2% of each other.
- On machines where RocksDB's write path collapses with many loading threads, `--load-threads` loads it with fewer, in the same process as the read phases.
- Each client thread issues its next operation when the last returns. `--rate` instead schedules operations at a fixed total rate, or a cycle such as `10000:20,30000:10`, and counts each latency from the scheduled start, so time queued behind a slow operation counts too. `--uniform 1` picks keys uniformly rather than by scrambled Zipfian, `--timeline <file>` writes each second's throughput, latency percentiles and lionturtle's and RocksDB's writer stall time, `--pace 0` turns off lionturtle's checkpoint pacing, `--preallocate-log 1` zero-fills lionturtle's log segments ahead of their writes, `--compression-ratio <r>` makes each value random for its first share `r` and repeats that share to fill the rest, as db_bench's `--compression_ratio` does, so values compress to about `r` of their size, and `--rocksdb-rate-mb <MB/s>` turns on RocksDB's auto-tuned rate limiter with that upper bound.
- `--engine scylla`, built with `--features scylla`, drives a ScyllaDB node at `--scylla-host` (default `127.0.0.1:9042`) over CQL with prepared statements, in a keyspace with one replica and an uncompressed table. `bench/scylla.sh` runs the node's commitlog in batch mode by default, so a write returns once its commitlog entry is fsynced, as `--sync 1` makes the embedded engines' writes durable.

```console
cargo build --release -p kvbench --features rocksdb
lz4="--compression lz4 --compression-ratio 0.5"   # or --compression none for the uncompressed tables
systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0 \
  ./target/release/kvbench --engine lionturtle --records 50000000 --ops 50000000 --threads 16 $lz4
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0 \
  ./target/release/kvbench --engine rocksdb --records 50000000 --ops 50000000 --threads 16 $lz4
# 400M records on an i7ie.48xlarge, after bench/aws/setup.sh:
systemd-run --user --scope -p MemoryMax=16G -p MemorySwapMax=0 ./target/release/kvbench --engine lionturtle \
  --dir /mnt/data/run --records 400000000 --ops 50000000 --threads 32 --cache-mb 4096 $lz4
```

Each phase prints one JSON line with throughput, latency percentiles, bytes read and written, write amplification, RSS and disk use. On EC2, `bench/aws/launch.sh` starts a tagged spot instance, `setup.sh` builds RAID 0 over its instance-store drives, or ext4 on one with `ONE_DRIVE=1`, and compiles the benchmarks, `exp.sh` runs one engine's YCSB sequence at 400M records under a 16 GB cap with profiles of the read phases, `run.sh` runs workload A once on a fresh copy of a loaded store, and `teardown.sh` removes every tagged resource.
