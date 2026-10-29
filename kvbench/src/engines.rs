//! Engine adapters. By default no engine fsyncs per write; writes reach the
//! OS in batches, the same durability for every engine. A synchronous put is
//! durable when it returns.

use crate::Args;
use std::sync::Arc;

pub trait Engine: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8], sync: bool);
    /// Returns the value's length, if found.
    fn get(&self, key: &[u8]) -> Option<usize>;
    /// Reads up to `n` entries from `start`, touching every key and value;
    /// returns how many it read.
    fn scan(&self, start: &[u8], n: usize) -> usize;
    fn stats_json(&self) -> String {
        "{}".into()
    }
    /// Finishes background work the writes so far started: turtlekv-rs
    /// checkpoints its memtables, and the LSM engines flush and compact fully.
    fn settle(&self) {}
    /// Total seconds writers have waited for background work, where the
    /// engine counts them.
    fn stall_secs(&self) -> f64 {
        0.0
    }
}

pub fn open(args: &Args) -> Arc<dyn Engine> {
    match args.engine.as_str() {
        "turtlekv" => Arc::new(Turtle::open(args)),
        "fjall" => Arc::new(Fjall::open(args)),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => Arc::new(Rocks::open(args)),
        e => panic!("unknown engine {e}"),
    }
}

struct Turtle {
    db: turtlekv::Db,
}

impl Turtle {
    fn open(args: &Args) -> Self {
        let tree = turtlekv::TreeConfig {
            leaf_size: args.leaf_kb << 10,
            max_levels: args.max_levels,
            filter_bits: args.filter_bits,
            ..Default::default()
        };
        let opts = turtlekv::Options {
            tree,
            checkpoint_distance: args.chi,
            cache_bytes: args.cache_mb << 20,
            memtable: args.memtable.parse().unwrap(),
            wal_buffer_bytes: args.wal_buffer_kb << 10,
            max_frozen: args.max_frozen,
            background: true,
            merge_threads: args.merge_threads,
            pace_checkpoints: args.pace,
        };
        let storage = Arc::new(turtlekv::storage::FsStorage::new(&args.dir).unwrap());
        Self { db: turtlekv::Db::open(storage, opts).unwrap() }
    }
}

impl Engine for Turtle {
    fn put(&self, key: &[u8], value: &[u8], sync: bool) {
        if sync {
            self.db.write(&[turtlekv::WriteOp::put(key.to_vec(), value.to_vec())], true).unwrap();
        } else {
            self.db.put(key, value).unwrap();
        }
    }

    fn settle(&self) {
        self.db.checkpoint().unwrap();
    }

    fn stall_secs(&self) -> f64 {
        self.db.stats().stage_secs[5]
    }

    fn get(&self, key: &[u8]) -> Option<usize> {
        self.db.get(key).unwrap().map(|v| v.len())
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let rows = self.db.scan(start, None, n).unwrap();
        for (k, v) in &rows {
            std::hint::black_box(k.len() + v.len());
        }
        rows.len()
    }

    fn stats_json(&self) -> String {
        let s = self.db.stats();
        format!(
            r#"{{"checkpoints":{},"page_mb_written":{:.1},"pages_written":{},"block_reads":{},"span_reads":{},"span_read_mb":{:.1},"cache_mb":{:.1},"image_mb":{:.1},"memtable_mb":{:.1},"frozen":{},"height":{},"nodes":{},"leaves":{},"segments":{},"leaf_mb":{:.1},"buffer_mb":{:.1},"filter_mb":{:.1},"index_mb":{:.1},"stage_secs":{:?}}}"#,
            s.checkpoints,
            s.page_bytes_written as f64 / 1e6,
            s.pages_written,
            s.block_reads,
            s.span_reads,
            s.span_read_bytes as f64 / 1e6,
            s.cache_bytes as f64 / 1e6,
            s.cache_image_bytes as f64 / 1e6,
            s.memtable_bytes as f64 / 1e6,
            s.frozen_memtables,
            s.tree.height,
            s.tree.nodes,
            s.tree.leaves,
            s.tree.segments,
            s.tree.leaf_bytes as f64 / 1e6,
            s.tree.buffer_bytes as f64 / 1e6,
            s.tree.filter_bytes as f64 / 1e6,
            s.tree.index_bytes as f64 / 1e6,
            s.stage_secs.map(|x| (x * 100.0).round() / 100.0),
        )
    }
}

struct Fjall {
    db: fjall::Database,
    ks: fjall::Keyspace,
}

impl Fjall {
    fn open(args: &Args) -> Self {
        let db = fjall::Database::builder(&args.dir)
            .cache_size(args.cache_mb << 20)
            .worker_threads(args.fjall_workers)
            .manual_journal_persist(args.fjall_manual_persist)
            .open()
            .unwrap();
        let memtable = args.memtable_bytes();
        let manual = args.fjall_manual_persist;
        let ks = db
            .keyspace("bench", move || {
                fjall::KeyspaceCreateOptions::default()
                    .max_memtable_size(memtable)
                    .manual_journal_persist(manual)
                    .data_block_compression_policy(fjall::config::CompressionPolicy::all(fjall::CompressionType::None))
            })
            .unwrap();
        Self { db, ks }
    }
}

impl Engine for Fjall {
    fn put(&self, key: &[u8], value: &[u8], sync: bool) {
        self.ks.insert(key, value).unwrap();
        if sync {
            self.db.persist(fjall::PersistMode::SyncData).unwrap();
        }
    }

    fn settle(&self) {
        self.ks.rotate_memtable_and_wait().unwrap();
        self.ks.major_compact().unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<usize> {
        self.ks.get(key).unwrap().map(|v| v.len())
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut count = 0;
        for kv in self.ks.range(start..).take(n) {
            let (k, v) = kv.into_inner().unwrap();
            std::hint::black_box(k.len() + v.len());
            count += 1;
        }
        count
    }
}

#[cfg(feature = "rocksdb")]
struct Rocks {
    db: rocksdb::DB,
}

#[cfg(feature = "rocksdb")]
impl Rocks {
    fn open(args: &Args) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.increase_parallelism(args.threads as i32);
        opts.set_max_subcompactions(args.threads as u32);
        opts.set_write_buffer_size(args.memtable_bytes() as usize);
        let mut block = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache((args.cache_mb << 20) as usize);
        block.set_block_cache(&cache);
        block.set_bloom_filter(10.0, false);
        block.set_cache_index_and_filter_blocks(false);
        opts.set_block_based_table_factory(&block);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        // Buffer the log in memory like the other engines, unless writes
        // sync.
        opts.set_manual_wal_flush(!args.sync);
        if args.rocksdb_recycle_logs {
            opts.set_recycle_log_file_num(4);
        }
        if args.rocksdb_rate_mb > 0 {
            opts.set_auto_tuned_ratelimiter((args.rocksdb_rate_mb << 20) as i64, 100_000, 10);
        }
        Self { db: rocksdb::DB::open(&opts, &args.dir).unwrap() }
    }
}

#[cfg(feature = "rocksdb")]
impl Engine for Rocks {
    fn put(&self, key: &[u8], value: &[u8], sync: bool) {
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(sync);
        self.db.put_opt(key, value, &wo).unwrap();
    }

    fn settle(&self) {
        self.db.flush().unwrap();
        self.db.compact_range::<&[u8], &[u8]>(None, None);
    }

    fn get(&self, key: &[u8]) -> Option<usize> {
        self.db.get_pinned(key).unwrap().map(|v| v.len())
    }

    fn stats_json(&self) -> String {
        use rocksdb::properties::{ESTIMATE_PENDING_COMPACTION_BYTES, num_files_at_level};
        let pending = self.db.property_int_value(ESTIMATE_PENDING_COMPACTION_BYTES).unwrap();
        let l0_files = self.db.property_int_value(num_files_at_level(0)).unwrap();
        format!(
            r#"{{"pending_compaction_mb":{:.1},"l0_files":{},"stall_secs":{:.1}}}"#,
            pending.unwrap_or(0) as f64 / 1e6,
            l0_files.unwrap_or(0),
            self.stall_secs()
        )
    }

    fn stall_secs(&self) -> f64 {
        // "Cumulative stall: 00:00:1.234 H:M:S, 0.1 percent"
        let stats = self.db.property_value(rocksdb::properties::DBSTATS).unwrap().unwrap_or_default();
        let Some(line) = stats.lines().find_map(|l| l.strip_prefix("Cumulative stall: ")) else {
            return 0.0;
        };
        let hms = line.split_whitespace().next().unwrap_or_default();
        hms.split(':').fold(0.0, |t, x| t * 60.0 + x.parse::<f64>().unwrap_or(0.0))
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut it = self.db.raw_iterator();
        it.seek(start);
        let mut count = 0;
        while count < n
            && let (Some(k), Some(v)) = (it.key(), it.value())
        {
            std::hint::black_box(k.len() + v.len());
            count += 1;
            it.next();
        }
        count
    }
}
