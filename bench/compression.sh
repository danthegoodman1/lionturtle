#!/bin/bash
# YCSB at 50M records for each engine without compression and with LZ4, on
# values that compress to half their size, under a 4 GB memory cap. Runs
# every configuration twice, the second pass in reverse order, so drift in
# the drive over the session favors none of them. Trims the drive and drops
# the page cache before each run, which needs sudo.
# Usage: bench/compression.sh <data dir> [kvbench binary] > results.jsonl
set -u
dir=$1; bin=${2:-./target/release/kvbench}
jemalloc=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
configs="lionturtle:none lionturtle:lz4 rocksdb:none rocksdb:lz4 fjall:none fjall:lz4"
reversed=$(echo $configs | tr ' ' '\n' | tac | tr '\n' ' ')
pass=0
for order in "$configs" "$reversed"; do
  pass=$((pass + 1))
  for cfg in $order; do
    engine=${cfg%%:*}; compression=${cfg##*:}
    pre=(); [[ $engine == rocksdb && -e $jemalloc ]] && pre=(env LD_PRELOAD=$jemalloc)
    rm -rf "$dir"
    sync; sudo fstrim /; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    systemd-run --user --scope --quiet -p MemoryMax=4G -p MemorySwapMax=0 "${pre[@]}" \
      "$bin" --engine $engine --dir "$dir" --records 50000000 --ops 50000000 --threads 16 \
      --compression $compression --compression-ratio 0.5 --label $pass-$engine-$compression
  done
done
rm -rf "$dir"
