#!/bin/bash
# Synchronous writes: each update is durable before it returns. Loads 1M
# records with 8 MiB memtables, so every engine rotates its log often, then
# runs 100% updates at 1, 16 and 64 client threads.
# Usage: bench/sync.sh <data dir> [kvbench binary]
set -u
dir=$1; bin=${2:-./target/release/kvbench}
jemalloc=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
for engine in turtlekv rocksdb rocksdb-recycle fjall; do
  for threads in 1 16 64; do
    args=(--engine ${engine%-recycle} --records 1000000 --chi 2 --cache-mb 256 --phases load,w --sync 1
          --threads $threads --ops $((threads * 1000 > 8000 ? threads * 1000 : 8000)) --label sync-$engine)
    [ $engine = rocksdb-recycle ] && args+=(--rocksdb-recycle-logs 1)
    pre=(); [[ $engine == rocksdb* && -e $jemalloc ]] && pre=(env LD_PRELOAD=$jemalloc)
    rm -rf "$dir"
    "${pre[@]}" "$bin" --dir "$dir" "${args[@]}" 2>/dev/null | grep '"phase":"w"'
  done
done
rm -rf "$dir"
