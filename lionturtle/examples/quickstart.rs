//! Opens a store, writes a key and an atomic batch, reads them back through
//! iterators and a snapshot, deletes them by range, then backs the store up
//! and restores it.
//!
//! `cargo run --release -p lionturtle --example quickstart`

use lionturtle::storage::FsStorage;
use lionturtle::{BackupOptions, Db, Options, WriteOp};
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

    // Deletes every key in [a, c), at a cost that does not depend on how
    // many there are, and every key starting with "ch". The snapshot still
    // holds them.
    db.delete_range(b"a", Some(b"c"))?;
    db.delete_prefix(b"ch")?;
    assert!(db.iter(b"", None).next()?.is_none());
    assert_eq!(snap.get(b"banana")?.as_deref(), Some(&b"yellow"[..]));

    db.sync()?;

    // A backup holds every write completed before it began, and writes go
    // on while it is copied. This one goes to an archive file, which
    // restores into an empty directory that then opens as a store.
    let (backups, restored) = (dir.with_extension("backups"), dir.with_extension("restored"));
    let _ = std::fs::remove_dir_all(&restored);
    db.put(b"date", b"brown")?;
    let backup = db.backup(BackupOptions::default())?;
    db.put(b"elderberry", b"black")?;
    backup.archive_to(&FsStorage::new(&backups)?, "fruit.backup")?;
    let archive = std::fs::File::open(backups.join("fruit.backup"))?;
    lionturtle::backup::restore(archive, Arc::new(FsStorage::new(&restored)?))?;
    let copy = Db::open(Arc::new(FsStorage::new(&restored)?), Options::default())?;
    assert_eq!(copy.get(b"date")?.as_deref(), Some(&b"brown"[..]));
    assert_eq!(copy.get(b"elderberry")?, None);

    drop((db, copy));
    std::fs::remove_dir_all(&dir)?;
    std::fs::remove_dir_all(&restored)?;
    std::fs::remove_dir_all(&backups)?;
    Ok(())
}
