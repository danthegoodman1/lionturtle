"""Prints one line per phase of a kvbench JSON-lines file: throughput, device
reads per operation, cache misses per operation, RSS, disk, and latency.

Usage: summarize.py <file.jsonl>
"""
import json, sys

prev = None
for l in open(sys.argv[1]):
    d = json.loads(l); s = d['engine_stats']
    mo = 'misses/op %.3f' % ((s['block_reads'] - prev['block_reads']) / d['ops']) if prev and 'block_reads' in s else ''
    lat = {k: (d[k]['mean_us'], d[k]['p50_us'], d[k]['p99_us']) for k in ('read', 'scan', 'insert', 'update', 'rmw') if k in d}
    print(d['engine'], d['phase'], 't', d['threads'], 'kops', d['kops'], 'devKB/op %.2f' % (d['read_mb'] * 1e3 / d['ops']), mo, 'rss', d['rss_mb'], 'disk', d['disk_mb'], 'waf', d['waf'], 'stage', s.get('stage_secs'), lat)
    prev = s if s else None
