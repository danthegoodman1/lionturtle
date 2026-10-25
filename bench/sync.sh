#!/bin/bash
# Synchronous writes: each update is durable before it returns. Loads 3M
# records, enough for every engine to reuse retired logs when configured
# to, finishes the load's background work, then runs 100% updates at 1, 16
# and 64 client threads. The updates fill less than one memtable, so the
# timed phase measures the log alone.
# Usage: bench/sync.sh <data dir> [kvbench binary]
set -u
dir=$1; bin=${2:-./target/release/kvbench}
jemalloc=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
for engine in turtlekv rocksdb rocksdb-recycle fjall; do
  for threads in 1 16 64; do
    args=(--engine ${engine%-recycle} --records 3000000 --cache-mb 256 --phases load,w --sync 1 --settle 1
          --threads $threads --ops $((threads * 4000 > 40000 ? threads * 4000 : 40000)) --label sync-$engine)
    [ $engine = rocksdb-recycle ] && args+=(--rocksdb-recycle-logs 1)
    pre=(); [[ $engine == rocksdb* && -e $jemalloc ]] && pre=(env LD_PRELOAD=$jemalloc)
    rm -rf "$dir"
    "${pre[@]}" "$bin" --dir "$dir" "${args[@]}" 2>/dev/null | grep '"phase":"w"'
  done
done
rm -rf "$dir"
