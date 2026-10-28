#!/bin/bash
# Creates a RAM-backed drive throttled to 200 MB/s with 100 µs latency, like
# the SATA SSD in the SILK paper (ATC '19), formats it ext4 and mounts it.
# null_blk's throttle never passes a request larger than one millisecond's
# bytes, about 200 KiB here, so requests stay at 64 KiB and the drive takes
# no discards. Remove it with: umount <mount point>; echo 0 >
# /sys/kernel/config/nullb/slow/power; rmdir /sys/kernel/config/nullb/slow
# Usage: sudo bench/slow-drive.sh <mount point>
set -eu
mnt=$1
lsmod | grep -q '^null_blk' || modprobe null_blk nr_devices=0
c=/sys/kernel/config/nullb/slow
mkdir "$c"
set_attr() { echo "$2" > "$c/$1"; }
set_attr size 32768
set_attr memory_backed 1
set_attr discard 0
set_attr mbps 200
set_attr max_sectors 128
set_attr irqmode 2
set_attr completion_nsec 100000
set_attr queue_mode 2
set_attr submit_queues "$(nproc)"
set_attr blocksize 4096
set_attr power 1
mkfs.ext4 -q -F -E nodiscard /dev/slow
mkdir -p "$mnt"
mount /dev/slow "$mnt"
chown "${SUDO_UID:-0}:${SUDO_GID:-0}" "$mnt"
