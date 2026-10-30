#!/bin/bash
# Prepares a fresh Ubuntu instance: toolchain, a file system on its
# instance-store NVMe drives at /mnt/data, and a build of the benchmark
# binaries. By default the drives form one RAID 0 array under XFS; with
# ONE_DRIVE=1, the first drive alone holds ext4, like a single-drive server.
set -euxo pipefail
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q build-essential clang libclang-dev cmake mdadm xfsprogs fio pkg-config python3 sysstat libjemalloc2 linux-tools-common "linux-tools-$(uname -r)" >/dev/null
if ! command -v cargo >/dev/null; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.97.0 >/dev/null
fi
source "$HOME/.cargo/env"
if ! mountpoint -q /mnt/data; then
  devs=$(lsblk -d -n -o NAME,MODEL | awk '/Instance Storage/ {print "/dev/"$1}')
  if [ "${ONE_DRIVE:-0}" = 1 ]; then
    dev=$(echo "$devs" | head -1)
    # Initialize inode tables now, so no background zeroing runs during benchmarks.
    sudo mkfs.ext4 -q -F -T largefile4 -E lazy_itable_init=0,lazy_journal_init=0,nodiscard "$dev"
  else
    n=$(echo "$devs" | wc -w)
    sudo mdadm --create /dev/md0 --run --level=0 --raid-devices="$n" $devs
    dev=/dev/md0
    sudo mkfs.xfs -f -K "$dev" >/dev/null
  fi
  sudo mkdir -p /mnt/data
  sudo mount -o noatime "$dev" /mnt/data
  sudo chown "$USER" /mnt/data
fi
cd ~/turtlekv-rs
cargo build --release -p kvbench --features rocksdb,scylla --bins 2>&1 | tail -1
cargo build --release -p turtlekv --bins 2>&1 | tail -1
lsblk -d -n -o NAME,MODEL,SIZE | grep -c "Instance Storage"
df -h /mnt/data
