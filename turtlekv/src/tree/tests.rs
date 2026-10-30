use super::*;
use crate::pagestore::PageStore;
use crate::sim::{FaultConfig, Rng, SimStorage};
use std::collections::BTreeMap;

struct Harness {
    cfg: TreeConfig,
    io: PageIo,
    ids: AtomicU64,
    root: Option<Subtree>,
    model: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Harness {
    fn new(cfg: TreeConfig) -> Self {
        let disk = SimStorage::new(1, FaultConfig::default());
        let store = PageStore::open(Arc::new(disk), &[]).unwrap();
        Self { io: PageIo::new(store, 1 << 20, 1 << 16), cfg, ids: AtomicU64::new(1), root: None, model: BTreeMap::new() }
    }

    fn ctx(&self) -> Ctx<'_> {
        Ctx { cfg: &self.cfg, io: &self.io, next_page_id: &self.ids }
    }

    fn apply(&mut self, ops: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) {
        let entries: Vec<EntryRef> = ops.iter().map(|(k, v)| EntryRef { key: k, value: v.as_deref() }).collect();
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let page = Arc::new(DataPage::unwritten(page_from_entries(id, entries, 10)));
        let root = apply_batch(&self.ctx(), self.root.as_ref(), page).unwrap();
        self.root = root;
        for (k, v) in ops {
            match v {
                Some(v) => self.model.insert(k.clone(), v.clone()),
                None => self.model.remove(k),
            };
        }
    }

    fn verify(&self, rng: &mut Rng) {
        let Some(root) = &self.root else {
            assert!(self.model.is_empty());
            return;
        };
        check_invariants(&self.io, &self.cfg, root).unwrap();
        let mut scan = TreeScan::new(Some(root.clone()), b"", None);
        let mut got = BTreeMap::new();
        while let Some((k, v)) = scan.next(&self.io).unwrap() {
            assert!(got.insert(k.to_vec(), v.to_vec()).is_none(), "scan repeated a key");
        }
        assert_eq!(got.len(), self.model.len());
        assert!(got == self.model, "scan differs from model");
        for _ in 0..50 {
            let k = key(rng.below(2000));
            let v = get(&self.io, root, &k, crate::bloom::hash_key(&k)).unwrap().and_then(Value::into_put).map(|b| b.to_vec());
            assert_eq!(v.as_ref(), self.model.get(&k), "get {:?}", String::from_utf8_lossy(&k));
        }
        let start = key(rng.below(2000));
        let mut scan = TreeScan::new(Some(root.clone()), &start, None);
        for (mk, mv) in self.model.range(start.clone()..).take(20) {
            let (k, v) = scan.next(&self.io).unwrap().expect("scan ended early");
            assert_eq!((&k[..], &v[..]), (&mk[..], &mv[..]));
        }
    }

    fn checkpoint_and_reload(&mut self) {
        let Some(root) = &self.root else { return };
        write_tree(&self.ctx(), root).unwrap();
        let ptr = root_ptr(root).unwrap();
        self.root = Some(load_tree(&self.io, ptr).unwrap());
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn run(seed: u64, cfg: TreeConfig, batches: usize) {
    let mut rng = Rng::new(seed);
    let mut h = Harness::new(cfg);
    let keyspace = rng.range(50, 2000);
    for b in 0..batches {
        let mut ops = BTreeMap::new();
        let n = rng.range(1, 120);
        let delete_p = [0.0, 0.1, 0.5, 0.9][rng.below(4) as usize];
        for _ in 0..n {
            let k = key(rng.below(keyspace));
            let v = if rng.chance(delete_p) {
                None
            } else {
                let len = rng.range(0, 60) as usize;
                let mut v = vec![0u8; len];
                rng.fill(&mut v);
                Some(v)
            };
            ops.insert(k, v);
        }
        h.apply(&ops);
        if b % 7 == 0 || rng.chance(0.2) {
            h.verify(&mut rng);
        }
        if rng.chance(0.1) {
            h.checkpoint_and_reload();
            h.verify(&mut rng);
        }
    }
    h.verify(&mut rng);
    if let Some(root) = &h.root {
        eprintln!("  final: {:?} keys={}", stats(root), h.model.len());
    }
}

fn small_cfg(rng: &mut Rng) -> TreeConfig {
    let max_pivots = rng.range(4, 12) as usize;
    TreeConfig {
        leaf_size: rng.range(300, 4000),
        max_pivots,
        min_pivots: rng.range(2, (max_pivots / 2) as u64) as usize,
        max_segments: rng.range(3, 16) as usize,
        max_levels: rng.range(1, 5) as usize,
        filter_bits: 8,
        merge_readahead: rng.range(4096, 20000),
        parallel: rng.chance(0.5),
    }
}

#[test]
fn random_batches_match_model() {
    for seed in 0..40 {
        let mut rng = Rng::new(seed * 7919);
        let cfg = small_cfg(&mut rng);
        eprintln!("seed {seed}: {cfg:?}");
        run(seed, cfg, 400);
    }
}
