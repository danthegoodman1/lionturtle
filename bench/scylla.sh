#!/bin/bash
# Runs a one-node ScyllaDB on the host, under the same 3 GB memory cap as the
# embedded engines, for kvbench's scylla engine. Usage: bench/scylla.sh <dir>
#
# <dir> holds the build, its configuration, the drive's I/O profile and the
# data. The first run copies the relocatable build out of the Docker image and
# runs iotune on the drive that holds <dir>, which takes a few minutes.
# ScyllaDB needs `sudo sysctl -w fs.aio-max-nr=1048576`.
set -euo pipefail

image=scylladb/scylla@sha256:d864230da81af7f6cdcf68446a440e0e4a55a0827fcf49af00725f660e8647d3 # 2026.3.4
dir=$(realpath -m "$1")
L=$dir/scylladb

if [ ! -d "$L" ]; then
  mkdir -p "$dir"
  id=$(docker create "$image")
  docker cp "$id:/opt/scylladb" "$dir/"
  docker cp "$id:/etc/scylla" "$dir/conf"
  docker rm "$id" > /dev/null
  # Saturated runs queue requests for seconds; long timeouts count that wait
  # as latency instead of failing the request.
  sed -i -e 's/^read_request_timeout_in_ms:.*/read_request_timeout_in_ms: 60000/' \
    -e 's/^write_request_timeout_in_ms:.*/write_request_timeout_in_ms: 60000/' "$dir/conf/scylla.yaml"
fi

if [ ! -f "$dir/conf/io.yaml" ]; then
  mkdir -p "$dir/iotune"
  "$L/libreloc/ld.so" --library-path "$L/libreloc" "$L/libexec/iotune" \
    --evaluation-directory "$dir/iotune" --properties-file "$dir/conf/io.yaml"
  rm -rf "$dir/iotune"
fi

# 8 shards share 2.5 GB. --overprovisioned keeps shards from busy-polling
# the cores the client threads share, and developer mode accepts ext4 and
# less than ScyllaDB's recommended 1 GB per shard.
exec systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=0 \
  "$L/libreloc/ld.so" --library-path "$L/libreloc" "$L/libexec/scylla" \
  --options-file "$dir/conf/scylla.yaml" --workdir "$dir/data" \
  --smp 8 --memory 2560M --reserve-memory 256M --overprovisioned --developer-mode 1 \
  --io-properties-file "$dir/conf/io.yaml" \
  --api-ui-dir "$L/swagger-ui/dist/" --api-doc-dir "$L/api/api-doc/"
