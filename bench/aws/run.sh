#!/bin/bash
# One run of workload A with uniform keys on a fresh copy of a loaded store
# of 20M records of 1 KB, under a 3 GB memory cap with a 256 MB cache. The
# drive is trimmed and the copy dropped from the page cache, then the run
# waits a minute for the drive to settle. A rate of "-" runs closed-loop.
# Results append to ~/results/phases.jsonl and timeline.jsonl.
# Load a store first, for example:
#   kvbench --engine lionturtle --records 20000000 --value-size 1000 --threads 16 \
#     --cache-mb 256 --dir /mnt/data/keep-lionturtle --phases load --settle 1 --keep 1
# Usage: run.sh <engine> <label> <rate|-> <ops> [kvbench args]
set -u
B=/mnt/data; R=~/results
e=$1 label=$2 rate=$3 ops=$4; shift 4
bin=${BIN:-$HOME/lionturtle/target/release/kvbench}
mkdir -p $R
rm -rf $B/run
sudo fstrim /mnt/data
cp -r --sparse=always $B/keep-$e $B/run
sync
python3 -c 'import os, sys
for d, _, fs in os.walk(sys.argv[1]):
    for f in fs:
        fd = os.open(os.path.join(d, f), os.O_RDONLY); os.fsync(fd); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED); os.close(fd)' $B/keep-$e $B/run
sleep 60
ratearg=(--rate $rate); [ $rate = - ] && ratearg=()
pre=(); [ $e = rocksdb ] && pre=(env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2)
"${pre[@]}" systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=0 \
  "$bin" --engine $e --records 20000000 --value-size 1000 --threads "${THREADS:-16}" --cache-mb 256 --dir $B/run \
  --phases a --uniform 1 "${ratearg[@]}" --ops "$ops" --keep 1 --label "$label" --timeline $R/timeline.jsonl "$@" \
  >> $R/phases.jsonl 2> $R/$label.err
rm -rf $B/run
