#!/bin/bash
# Large-value YCSB for lionturtle and RocksDB, each with values inline and
# with values stored apart from the tree (lionturtle's blobs, RocksDB's
# BlobDB), under a 4 GB memory cap. Each configuration loads and runs A in
# one process, then C and E in a process each on the store it kept, so the
# read phases start cold and run as many operations as they need. Drops the
# page cache before each process and trims the drive before each load, which
# needs sudo.
# Usage: bench/blobs.sh <data dir> <value bytes> <records> <A ops> <C ops> <E ops> \
#          <blob KiB> <compression> "<engine:blobs ...>" [kvbench binary] > results.jsonl
# where blobs is 1 to store values apart and 0 to keep them inline, and an
# ops count of 0 skips its phase.
set -u
dir=$1; value=$2; records=$3; ops_a=$4; ops_c=$5; ops_e=$6; blob_kb=$7; compression=$8; configs=$9
bin=${10:-./target/release/kvbench}
jemalloc=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ratio=1; [[ $compression == lz4 ]] && ratio=0.5
for cfg in $configs; do
  engine=${cfg%%:*}; blobs=${cfg##*:}
  kb=0; [[ $blobs == 1 ]] && kb=$blob_kb
  pre=(); [[ $engine == rocksdb && -e $jemalloc ]] && pre=(env LD_PRELOAD=$jemalloc)
  # run <trim: 1 before a process that writes> <kvbench arguments>
  run() {
    sync; [[ $1 == 1 ]] && sudo fstrim /; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    sleep 15
    shift
    systemd-run --user --scope --quiet -p MemoryMax=4G -p MemorySwapMax=0 "${pre[@]}" \
      "$bin" --engine $engine --dir "$dir" --records $records --threads 16 --keep 1 \
      --value-size $value --blob-kb $kb --compression $compression --compression-ratio $ratio \
      --label $engine-blobs$blobs-$value-$compression "$@"
  }
  rm -rf "$dir"
  run 1 --phases load,a --ops $ops_a
  [[ $ops_c -gt 0 ]] && run 0 --phases c --ops $ops_c
  [[ $ops_e -gt 0 ]] && run 0 --phases e --ops $ops_e
done
rm -rf "$dir"
