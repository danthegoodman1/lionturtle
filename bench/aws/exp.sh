#!/bin/bash
# Full YCSB sequence for one engine under a 16 GB cgroup cap, profiling the
# start of phases C and E: an on-CPU profile, syscall latencies, device
# stats, and the cgroup's memory pressure.
# Usage: exp.sh <engine> <label> [kvbench args...]
set -u
engine=$1; label=$2; shift 2
cd ~/turtlekv-rs; OUT=~/results; mkdir -p $OUT
out=$OUT/ycsb-$label.jsonl; err=$OUT/ycsb-$label.err
pre=""; [ $engine = rocksdb ] && pre="env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
iostat -x -m md0 10 > $OUT/iostat-$label.txt 2>&1 &
iostat_pid=$!
(
  for ph in b c; do
    until grep -q "\"phase\":\"$ph\"" $out 2>/dev/null; do sleep 2; done
    sleep 10
    pid=$(pgrep -x kvbench | head -1)
    cg=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/$pid/cgroup)
    next=$([ $ph = b ] && echo c || echo e)
    { cat $cg/memory.pressure; grep -E "^(pgscan|pgsteal|workingset_refault_file|file|anon) " $cg/memory.stat; } > $OUT/mem-$label-$next-start.txt
    sudo perf record -g -F 499 -p $pid -o $OUT/perf-$label-$next.data -- sleep 15 > /dev/null 2>&1
    sudo timeout 10 perf trace -s -p $pid > $OUT/trace-$label-$next.txt 2>&1
    { cat $cg/memory.pressure; grep -E "^(pgscan|pgsteal|workingset_refault_file|file|anon) " $cg/memory.stat; } > $OUT/mem-$label-$next-end.txt
  done
) &
watch_pid=$!
sudo systemd-run --scope --quiet -p MemoryMax=16G -p MemorySwapMax=0 $pre \
  ${BIN:-./target/release/kvbench} --engine $engine --dir /mnt/data/$engine --records 400000000 --ops 50000000 \
  --threads 32 --cache-mb 4096 --label aws-$label "$@" >> $out 2>> $err
echo "exit $?" >> $err
kill $iostat_pid $watch_pid 2>/dev/null
sudo chown -R $USER $OUT
echo done > $OUT/DONE-$label
