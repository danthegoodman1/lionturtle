//! The largest key and value the store accepts, and what a write past them
//! does to its batch.

use lionturtle::sim::{FaultConfig, SimStorage};
use lionturtle::{Db, Error, Options, WriteOp};
use std::sync::Arc;

const MAX_KEY: usize = 65_535;
const MAX_VALUE: usize = 16 << 20;

/// A key of 65,535 bytes and a value of 16 MiB go in, survive a checkpoint
/// into the tree and a reopen; one byte more is an invalid argument.
#[test]
fn largest_key_and_value_round_trip() {
    let disk = SimStorage::new(1, FaultConfig::default());
    let db = Db::open(Arc::new(disk.clone()), Options::default()).unwrap();
    let key = vec![b'k'; MAX_KEY];
    let value = vec![b'v'; MAX_VALUE];
    db.put(&key, b"small").unwrap();
    db.put(b"big", &value).unwrap();
    assert!(matches!(db.put(&vec![b'k'; MAX_KEY + 1], b"x"), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.put(b"big", &vec![b'v'; MAX_VALUE + 1]), Err(Error::InvalidArgument(_))));
    db.checkpoint().unwrap();
    drop(db);
    let db = Db::open(Arc::new(disk), Options::default()).unwrap();
    assert_eq!(db.get(&key).unwrap().as_deref(), Some(&b"small"[..]));
    assert_eq!(db.get(b"big").unwrap().map(|v| v.len()), Some(MAX_VALUE));
}

/// A batch with one op past a limit fails whole: none of its ops land, and
/// the store keeps taking writes.
#[test]
fn oversized_op_rejects_its_whole_batch() {
    let db = Db::open(Arc::new(SimStorage::new(2, FaultConfig::default())), Options::default()).unwrap();
    db.put(b"a", b"1").unwrap();
    let seq = db.last_seq();
    let batch = [WriteOp::put(&b"a"[..], &b"2"[..]), WriteOp::put(vec![b'k'; MAX_KEY + 1], &b"x"[..]), WriteOp::delete(&b"b"[..])];
    assert!(matches!(db.write(&batch, true), Err(Error::InvalidArgument(_))));
    assert_eq!(db.last_seq(), seq);
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert!(!db.is_poisoned());
    db.put(b"a", b"3").unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"3"[..]));
}
