//! A key overwritten many times within one memtable, as a counter or a
//! commit timestamp written in every batch is.

use lionturtle::sim::{FaultConfig, SimStorage};
use lionturtle::{Db, Options};
use std::sync::Arc;

/// Each overwrite adds a version to the key's chain in the memtable, and the
/// checkpoint frees the whole chain on a background thread. Under the
/// default 128 MiB memtable, this many overwrites fit in one memtable.
#[test]
fn hot_key_survives_checkpoint_and_reopen() {
    const N: u64 = 300_000;
    let disk = SimStorage::new(1, FaultConfig::default());
    let newest = (N - 1).to_le_bytes();
    let db = Db::open(Arc::new(disk.clone()), Options::default()).unwrap();
    for i in 0..N {
        db.put(b"hot", &i.to_le_bytes()).unwrap();
    }
    db.checkpoint().unwrap();
    assert_eq!(db.get(b"hot").unwrap().as_deref(), Some(&newest[..]));
    drop(db);
    let db = Db::open(Arc::new(disk), Options::default()).unwrap();
    assert_eq!(db.get(b"hot").unwrap().as_deref(), Some(&newest[..]));
}
