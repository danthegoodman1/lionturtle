#!/bin/bash
# Raw device capability of the RAID0 array: 4 KiB random reads and writes.
set -eu
cd /mnt/data
for rw in randread randwrite; do
  fio --name=$rw --filename=/mnt/data/fio.dat --size=200G --rw=$rw --bs=4k --direct=1 \
      --ioengine=libaio --iodepth=64 --numjobs=32 --group_reporting --time_based --runtime=30 \
      --output-format=json > fio-$rw.json
  python3 -c "import json,sys; j=json.load(open('fio-$rw.json'))['jobs'][0]; k='read' if 'read' in '$rw' else 'write'; print('$rw', 'iops=%.0f'%j[k]['iops'], 'bw_MBps=%.0f'%(j[k]['bw_bytes']/1e6), 'lat_p99_us=%.0f'%(j[k]['clat_ns']['percentile']['99.000000']/1e3))"
done
rm -f /mnt/data/fio.dat
