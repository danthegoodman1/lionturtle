#!/bin/bash
# Backup and restore of a lionturtle store under a 4 GB memory cap: the
# store's own backup, which copies pages and blobs as they are stored, into
# an archive and into a store, against streaming every row of a snapshot to
# a file. Each runs on a quiet store, then under YCSB A at a fixed rate with
# the backup starting 20 s in. The drive holds both the store and the
# backups, so a backup written to /dev/null or to `null` stands for one
# written to another device. Drops the page cache before each process and
# trims the drive before each one that writes much, which needs sudo.
# Usage: bench/backup.sh <data dir> <value bytes> <records> <blob KiB> <A ops/s> <A seconds> \
#          <same-drive MiB/s> <output prefix> [kvbench binary]
# where blob KiB is 0 to keep values in the tree, A seconds is how long the
# runs with a row backup last (the others last half as long), and same-drive
# MiB/s is the rate of the page backups written beside the store: one past
# the page cache, as the store writes an archive file, and one through it.
# Writes <prefix>-load.jsonl, -quiet.jsonl, -restore.jsonl and -live.jsonl,
# and a timeline per live run.
set -u
dir=$1; value=$2; records=$3; blob_kb=$4; rate=$5; secs=$6; beside=$7; out=$8
bin=${9:-./target/release/kvbench}
src=$dir/src
# run <trim: 1 before a process that writes much> <kvbench arguments>
run() {
  sync; [[ $1 == 1 ]] && sudo fstrim /; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  sleep 15
  shift
  systemd-run --user --scope --quiet -p MemoryMax=4G -p MemorySwapMax=0 \
    "$bin" --engine lionturtle --threads 16 --keep 1 --value-size $value --records $records \
    --blob-kb $blob_kb --compression lz4 --compression-ratio 0.5 "$@"
}
rm -rf "$dir"; mkdir -p "$dir"

# A store that has been loaded, then overwritten for a while.
run 1 --dir $src --phases load,a --ops $((rate * 60)) --label load >> $out-load.jsonl
run 0 --dir $src --phases fingerprint --label source >> $out-quiet.jsonl

# Quiet backups, each kept for its restore. The page backups run unlimited.
run 1 --dir $src --phases backup --backup rows --backup-dest $dir/rows --label rows >> $out-quiet.jsonl
run 1 --dir $src --phases backup --backup archive --backup-dest $dir/archive --backup-mbps 0 --label archive >> $out-quiet.jsonl
run 1 --dir $src --phases backup --backup copy --backup-dest $dir/copy --backup-mbps 0 --label copy >> $out-quiet.jsonl
run 0 --dir $src --phases backup --backup archive --backup-dest /dev/null --backup-mbps 0 --label archive-null >> $out-quiet.jsonl

# Restores, each followed by a read of every row.
run 1 --dir $dir/restored --phases restore,fingerprint --backup-dest $dir/rows --label rows >> $out-restore.jsonl
rm -rf $dir/restored
run 1 --dir $dir/restored --phases restore-archive,fingerprint --backup-dest $dir/archive --label archive >> $out-restore.jsonl
rm -rf $dir/restored
run 0 --dir $dir/copy --phases fingerprint --label copy >> $out-restore.jsonl
rm -rf $dir/rows $dir/archive $dir/copy

# Backups under load.
live() {  # live <label> <seconds> <backup arguments>
  label=$1; ops=$((rate * $2)); shift 2
  run 1 --dir $src --phases a --ops $ops --rate $rate --backup-at 20 --timeline $out-live-$label.tl --label $label "$@" >> $out-live.jsonl
  rm -rf $dir/rows $dir/archive $dir/copy
}
half=$((secs / 2))
live base-1 $secs
live rows $secs --backup rows --backup-dest $dir/rows
live archive-null-1 $half --backup archive --backup-dest /dev/null
live archive-beside $half --backup archive --backup-dest $dir/archive --backup-mbps $beside
live archive-beside-buffered $half --backup archive-buffered --backup-dest $dir/archive --backup-mbps $beside
live archive-null-unlimited $half --backup archive --backup-dest /dev/null --backup-mbps 0
live archive-null-2 $half --backup archive --backup-dest /dev/null
live base-2 $half
rm -rf "$dir"
