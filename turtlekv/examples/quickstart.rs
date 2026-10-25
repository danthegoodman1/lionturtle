//! Opens a store, writes a key and an atomic batch, then reads them back.
//!
//! `cargo run --release -p turtlekv --example quickstart`

use std::sync::Arc;
use turtlekv::storage::FsStorage;
use turtlekv::{Db, Options, WriteOp};

fn main() -> turtlekv::Result<()> {
    let dir = std::env::temp_dir().join("turtlekv-quickstart");
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

    db.sync()?;
    drop(db);
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
