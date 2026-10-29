#!/bin/bash
# Reads during sustained writes, after the SILK paper (ATC '19). Loads 5M
# records of 1 KB into turtlekv-rs and RocksDB, then runs workload A with
# uniform keys at fixed rates for five minutes each under a 3 GB memory cap:
# turtlekv-rs with checkpoint pacing on and off, and RocksDB with its
# auto-tuned rate limiter on and off. Latency counts from each operation's
# scheduled start. Run it on a drive with little bandwidth to spare, such as
# bench/slow-drive.sh makes. Each run appends its per-second percentiles to
# <data dir>/timeline.jsonl.
# Usage: bench/steady-writes.sh <data dir> [kvbench binary built with --features rocksdb]
set -u
dir=$1; bin=${2:-./target/release/kvbench}
common=(--records 5000000 --value-size 1000 --cache-mb 256 --threads 16)
# RocksDB runs on jemalloc, its production allocator.
engine() {
  if [ "$1" = rocksdb ]; then
    env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 "${@:2}"
  else
    "${@:2}"
  fi
}
for e in turtlekv rocksdb; do
  rm -rf "$dir/loaded-$e"
  engine $e "$bin" --engine $e "${common[@]}" --dir "$dir/loaded-$e" --phases load --settle 1 --keep 1 > /dev/null
done
# Rate and operation count: five minutes of each.
for run in "15000 4500000" "22000 6600000" "10000:20,30000:10 5000000"; do
  read -r rate ops <<< "$run"
  for variant in "turtlekv --pace 1" "turtlekv --pace 0" "rocksdb --rocksdb-rate-mb 200" "rocksdb --rocksdb-rate-mb 0"; do
    read -r e flag value <<< "$variant"
    rm -rf "$dir/run"
    cp -r --sparse=always "$dir/loaded-$e" "$dir/run"
    sync
    # Start with the copy out of the page cache, as after a restart.
    python3 -c 'import os, sys
for d, _, fs in os.walk(sys.argv[1]):
    for f in fs:
        fd = os.open(os.path.join(d, f), os.O_RDONLY); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED); os.close(fd)' "$dir/run"
    engine $e systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=0 \
      "$bin" --engine $e "${common[@]}" --dir "$dir/run" --phases a --uniform 1 --rate "$rate" --ops "$ops" $flag $value \
      --keep 1 --label "$e rate=$rate $flag $value" --timeline "$dir/timeline.jsonl" | grep -o '"label":"[^"]*"\|"read":{[^}]*}'
  done
done
rm -rf "$dir/run" "$dir/loaded-turtlekv" "$dir/loaded-rocksdb"
