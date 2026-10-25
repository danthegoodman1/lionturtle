//! Engine adapters. Each engine runs without a per-write fsync; writes reach
//! the OS in batches, the same durability for every engine.

use crate::Args;
use std::sync::Arc;

pub trait Engine: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]);
    /// Returns the value's length, if found.
    fn get(&self, key: &[u8]) -> Option<usize>;
    /// Reads up to `n` entries from `start`; returns how many it read.
    fn scan(&self, start: &[u8], n: usize) -> usize;
    fn stats_json(&self) -> String {
        "{}".into()
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
        };
        let storage = Arc::new(turtlekv::storage::FsStorage::new(&args.dir).unwrap());
        Self { db: turtlekv::Db::open(storage, opts).unwrap() }
    }
}

impl Engine for Turtle {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<usize> {
        self.db.get(key).unwrap().map(|v| v.len())
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        self.db.scan(start, None, n).unwrap().len()
    }

    fn stats_json(&self) -> String {
        let s = self.db.stats();
        format!(
            r#"{{"checkpoints":{},"page_mb_written":{:.1},"pages_written":{},"block_reads":{},"span_reads":{},"span_read_mb":{:.1},"cache_hits":{},"cache_mb":{:.1},"memtable_mb":{:.1},"frozen":{},"height":{},"nodes":{},"leaves":{},"segments":{},"leaf_mb":{:.1},"buffer_mb":{:.1},"filter_mb":{:.1},"index_mb":{:.1},"stage_secs":{:?}}}"#,
            s.checkpoints,
            s.page_bytes_written as f64 / 1e6,
            s.pages_written,
            s.block_reads,
            s.span_reads,
            s.span_read_bytes as f64 / 1e6,
            s.cache_hits,
            s.cache_bytes as f64 / 1e6,
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
    _db: fjall::Database,
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
        let memtable = args.fjall_memtable_mb << 20;
        let manual = args.fjall_manual_persist;
        let ks = db
            .keyspace("bench", move || {
                fjall::KeyspaceCreateOptions::default().max_memtable_size(memtable).manual_journal_persist(manual)
            })
            .unwrap();
        Self { _db: db, ks }
    }
}

impl Engine for Fjall {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.ks.insert(key, value).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<usize> {
        self.ks.get(key).unwrap().map(|v| v.len())
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut count = 0;
        for kv in self.ks.range(start..).take(n) {
            let _ = kv.into_inner().unwrap();
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
        opts.set_write_buffer_size(64 << 20);
        let mut block = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache((args.cache_mb << 20) as usize);
        block.set_block_cache(&cache);
        block.set_bloom_filter(10.0, false);
        block.set_cache_index_and_filter_blocks(false);
        opts.set_block_based_table_factory(&block);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        Self { db: rocksdb::DB::open(&opts, &args.dir).unwrap() }
    }
}

#[cfg(feature = "rocksdb")]
impl Engine for Rocks {
    fn put(&self, key: &[u8], value: &[u8]) {
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(false);
        self.db.put_opt(key, value, &wo).unwrap();
    }

    fn get(&self, key: &[u8]) -> Option<usize> {
        self.db.get_pinned(key).unwrap().map(|v| v.len())
    }

    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut it = self.db.raw_iterator();
        it.seek(start);
        let mut count = 0;
        while it.valid() && count < n {
            count += 1;
            it.next();
        }
        count
    }
}
