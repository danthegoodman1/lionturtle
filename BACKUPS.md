# Backups

`Db::backup` backs up a store while it serves reads and writes. A backup holds every write completed before the call and none made after it returns, with each batch whole. It copies one checkpoint's pages and blobs as they are stored, verifies each checksum on the way, and reads at a set rate.

[DESIGN.md](DESIGN.md#backups) describes the mechanism and [BENCHMARKS.md](BENCHMARKS.md#backups) what it costs. This page covers how to use it.

## Take a backup

A `Backup` yields its checkpoint in one of two forms. Use each backup once.

**An archive** holds the pages and blobs end to end. It is the form to send to another machine or to object storage.

```rust
use lionturtle::storage::FsStorage;
use lionturtle::{BackupOptions, backup};

// To a file, written past the page cache and durable when the call returns.
let info = db.backup(BackupOptions::default())?.archive_to(&FsStorage::new("/mnt/backups")?, "store.backup")?;
println!("backed up through write {} in {} bytes", info.seq, info.bytes);

// To any writer: a socket, a child process's input, an upload.
db.backup(BackupOptions::default())?.write_to(&mut upload)?;

// Or read it: a Backup is a std::io::Read.
let mut backup = db.backup(BackupOptions::default())?;
std::io::copy(&mut backup, &mut upload)?;
```

**A copy** is a directory that opens as a store, with nothing to restore. It suits a second disk on the same machine. Its files keep the source's layout, free space included, so they are sparse: move one with a tool that preserves holes, or send an archive.

```rust
db.backup(BackupOptions::default())?.copy_to(Arc::new(FsStorage::new("/mnt/standby/store")?))?;
```

The destination of a copy must be an empty directory.

## Restore

An archive restores into an empty directory, which then opens as a store. Opening it takes as long as opening the store it came from.

```rust
let archive = std::fs::File::open("/mnt/backups/store.backup")?;
let info = backup::restore(archive, Arc::new(FsStorage::new("/var/lib/store")?))?;
let db = Db::open(Arc::new(FsStorage::new("/var/lib/store")?), Options::default())?;
assert_eq!(db.last_seq(), info.seq);
```

A restore verifies every page and blob as it writes it. If a restore or a copy fails part-way, its destination refuses to open; empty the directory and run it again.

## Verify

`backup::verify` reads an archive from end to end and checks that it is complete and that every page and blob passes its checksums. Run it on an archive after moving it, and before you rely on it.

```rust
backup::verify(std::fs::File::open("/mnt/backups/store.backup")?)?;
```

A backup that finishes without error has already read and verified every page and blob of its checkpoint, so each backup also checks the store itself. A damaged page fails the backup with `Error::Corruption`.

## What a backup costs the store

- **Reads at a rate.** A backup reads the store at `BackupOptions::rate_limit` bytes per second: 256 MiB/s by default, or `None` for as fast as one thread reads, which was 2 to 3 GB/s on an NVMe drive. At the default, a terabyte takes about an hour. On a store of 100M small rows serving 500,000 operations a second, the default rate moved the read p99 of a typical second from 5 to 5.5 ms.

  ```rust
  let slower = BackupOptions { rate_limit: Some(64 << 20) };
  ```

- **A wait at the start.** `Db::backup` first brings the writes before it into the tree, at the pace of an ordinary checkpoint. That took 2 to 3 s on the store above and takes no time on an idle one.
- **Disk space until it finishes.** The store keeps a page or blob that clients supersede until the backup has read it, and frees it then. The space held is bounded by what the backup has yet to read. Finish a backup or drop it; a `Backup` left in hand holds its space.
- **Memory.** About 100 bytes per page or blob until the backup has read it.
- **The backup's own writes.** Send the backup to another drive or another machine where you can. Written to the store's drive, an archive at 100 MiB/s moved that typical second's p99 to 8 ms. `archive_to` and `copy_to` write past the page cache. If you write an archive to a local file yourself, do the same: written through the page cache under a memory limit, the same archive held client p99 at 445 ms.

A `Backup` can move to another thread, so a backup can run beside the threads that serve clients. Drop every backup before you reopen the store.

## Plan for recovery

- **Each backup is whole.** A backup holds the entire store and depends on no earlier one. Keep as many as your recovery window needs, and delete the rest.
- **What you can lose.** Restoring returns the store to the moment a backup began, so writes made since the newest backup are lost with the store. Back up as often as that allows.
- **A backup counts once it has arrived.** `Db::backup` returns when the backup starts; the archive exists when `archive_to`, `write_to` or your copy of the stream finishes. Verify it where it lands.
- **Versions.** An archive restores with the lionturtle version that wrote it or a later one, as a store opens.

## From C and C++

```c
lt_backup_options options = lt_backup_options_default();
options.rate_limit = 64u << 20; /* bytes per second; 0 for no limit */
lt_backup_info info;
if (lt_backup_to_file(db, "/mnt/backups/store.backup", &options, &info) != LT_OK) {
  fprintf(stderr, "backup failed: %s\n", lt_last_error());
}

/* Later, on the machine that recovers. */
lt_verify_backup_file("/mnt/backups/store.backup", NULL);
lt_restore_from_file("/mnt/backups/store.backup", "/var/lib/store", NULL);
lt_open("/var/lib/store", NULL, &db);
```

`lt_backup_to_dir` writes a copy that opens as a store. The C++ wrapper offers `Db::backup_to_file`, `Db::backup_to_dir`, `lionturtle::restore` and `lionturtle::verify_backup`. Backups to a stream are Rust-only.
