//! Opens a store, writes a key and an atomic batch, then reads them back
//! through iterators and a snapshot.
//!
//! `cargo run --release -p lionturtle --example quickstart`

use lionturtle::storage::FsStorage;
use lionturtle::{Db, Options, WriteOp};
use std::sync::Arc;

fn main() -> lionturtle::Result<()> {
    let dir = std::env::temp_dir().join("lionturtle-quickstart");
    let db = Db::open(Arc::new(FsStorage::new(&dir)?), Options::default())?;

    db.put(b"apple", b"red")?;
    // One log record: recovery keeps both ops or neither, and readers never
    // see one without the other. `true` returns after an fsync.
    db.write(&[WriteOp::put(&b"banana"[..], &b"yellow"[..]), WriteOp::delete(&b"apple"[..])], true)?;

    assert_eq!(db.get(b"apple")?, None);
    for (key, value) in db.scan(b"a", Some(b"c"), 100)? {
        println!("{} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
    }

    // An iterator reads one snapshot, taken when it is created.
    let mut it = db.iter(b"", None);
    db.put(b"cherry", b"dark red")?;
    while let Some((key, _)) = it.next()? {
        assert_ne!(&key[..], b"cherry");
    }

    // Reads through one snapshot agree with each other, whatever is written
    // meanwhile.
    let snap = db.snapshot();
    db.put(b"blueberry", b"blue")?;
    let mut fruit = snap.prefix(b"b"); // keys starting with "b", ascending
    while let Some((key, _)) = fruit.next()? {
        assert_eq!(&key[..], b"banana");
    }
    let mut newest = snap.iter_rev(b"", None); // every key, descending
    assert_eq!(newest.next()?.map(|(k, _)| k), Some("cherry".into()));
    assert_eq!(snap.get(b"blueberry")?, None);

    db.sync()?;
    drop(db);
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
