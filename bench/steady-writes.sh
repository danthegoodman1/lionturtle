#!/bin/bash
# Reads during sustained writes, after the SILK paper (ATC '19). Loads 5M
# records of 1 KB, then runs workload A with uniform keys at fixed rates for
# five minutes each under a 3 GB memory cap, with checkpoint pacing on and
# off. Latency counts from each operation's scheduled start. Run it on a
# drive with little bandwidth to spare, such as bench/slow-drive.sh makes.
# Each run appends its per-second percentiles to <data dir>/timeline.jsonl.
# Usage: bench/steady-writes.sh <data dir> [kvbench binary]
set -u
dir=$1; bin=${2:-./target/release/kvbench}
common=(--engine turtlekv --records 5000000 --value-size 1000 --cache-mb 256 --threads 16)
rm -rf "$dir/loaded"
"$bin" "${common[@]}" --dir "$dir/loaded" --phases load --settle 1 --keep 1 > /dev/null
# Rate and operation count: five minutes of each.
for run in "15000 4500000" "22000 6600000" "10000:20,30000:10 5000000"; do
  set -- $run
  for pace in 1 0; do
    rm -rf "$dir/run"
    cp -r --sparse=always "$dir/loaded" "$dir/run"
    sync
    # Start with the copy out of the page cache, as after a restart.
    python3 -c 'import os, sys
for d, _, fs in os.walk(sys.argv[1]):
    for f in fs:
        fd = os.open(os.path.join(d, f), os.O_RDONLY); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED); os.close(fd)' "$dir/run"
    systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=0 \
      "$bin" "${common[@]}" --dir "$dir/run" --phases a --uniform 1 --rate "$1" --ops "$2" --pace $pace --keep 1 \
      --label "rate=$1 pace=$pace" --timeline "$dir/timeline.jsonl" | grep -o '"label":"[^"]*"\|"read":{[^}]*}'
  done
done
rm -rf "$dir/run" "$dir/loaded"
