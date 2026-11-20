/* Exercises the C API against a store in the directory argv[1]. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lionturtle.h"

#define CHECK(cond)                                                                    \
  do {                                                                                 \
    if (!(cond)) {                                                                     \
      fprintf(stderr, "%s:%d: check failed: %s (%s)\n", __FILE__, __LINE__, #cond,      \
              lt_last_error());                                                        \
      exit(1);                                                                         \
    }                                                                                  \
  } while (0)
#define OK(call) CHECK((call) == LT_OK)

static int slice_is(lt_slice s, const char *want) {
  return s.len == strlen(want) && memcmp(s.data, want, s.len) == 0;
}

/* Reads `key` through `db`, or through `snap` when it is non-null, and
 * compares the value with `want`, null meaning absent. */
static void expect(lt_db *db, lt_snapshot *snap, const char *key, const char *want) {
  lt_value *v = NULL;
  if (snap) {
    OK(lt_snapshot_get(snap, key, strlen(key), &v));
  } else {
    OK(lt_get(db, key, strlen(key), &v));
  }
  if (!want) {
    CHECK(v == NULL);
    return;
  }
  CHECK(v != NULL);
  CHECK(slice_is(lt_value_bytes(v), want));
  lt_value_free(v);
}

/* Drains `it`, checking that its keys join to `want`. */
static void expect_keys(lt_iter *it, const char *want) {
  char got[256] = "";
  bool has_entry;
  lt_slice key, value;
  for (;;) {
    OK(lt_iter_next(it, &has_entry, &key, &value));
    if (!has_entry) break;
    CHECK(strlen(got) + key.len < sizeof got);
    strncat(got, key.data, key.len);
  }
  if (strcmp(got, want) != 0) {
    fprintf(stderr, "iterated %s, want %s\n", got, want);
    exit(1);
  }
  lt_iter_free(it);
}

int main(int argc, char **argv) {
  CHECK(argc == 2);
  lt_options opts = lt_options_default();
  opts.cache_bytes = 64 << 20;
  opts.compression = LT_COMPRESSION_LZ4;
  lt_db *db = NULL;
  OK(lt_open(argv[1], &opts, &db));

  OK(lt_put(db, "a", 1, "1", 1, false));
  OK(lt_put(db, "b", 1, "2", 1, true));
  OK(lt_put(db, "empty", 5, NULL, 0, false));
  expect(db, NULL, "a", "1");
  expect(db, NULL, "empty", "");
  expect(db, NULL, "missing", NULL);

  lt_snapshot *snap = lt_snapshot_new(db);
  CHECK(snap != NULL);

  lt_batch *batch = lt_batch_new();
  OK(lt_batch_put(batch, "c", 1, "3", 1));
  OK(lt_batch_put(batch, "ca", 2, "4", 1));
  OK(lt_batch_delete(batch, "a", 1));
  OK(lt_write(db, batch, true));
  lt_batch_free(batch);
  expect(db, NULL, "a", NULL);
  expect(db, NULL, "ca", "4");

  /* The snapshot predates the batch. */
  expect(db, snap, "a", "1");
  expect(db, snap, "c", NULL);
  lt_iter *it = NULL;
  OK(lt_snapshot_iter_new(snap, "", 0, NULL, 0, false, &it));
  expect_keys(it, "abempty");
  lt_snapshot_free(snap);

  OK(lt_iter_new(db, "b", 1, "d", 1, false, &it));
  expect_keys(it, "bcca");
  OK(lt_iter_new(db, "", 0, NULL, 0, true, &it));
  expect_keys(it, "emptycacb");
  OK(lt_iter_prefix(db, "c", 1, false, &it));
  expect_keys(it, "cca");
  OK(lt_iter_prefix(db, "c", 1, true, &it));
  expect_keys(it, "cac");
  /* A non-null empty end bounds the range to nothing. */
  OK(lt_iter_new(db, "", 0, "", 0, false, &it));
  expect_keys(it, "");

  /* Failures return a status and leave a message. */
  CHECK(lt_put(db, NULL, 1, "x", 1, false) == LT_INVALID_ARGUMENT);
  CHECK(strlen(lt_last_error()) > 0);
  size_t big = 1 << 17;
  char *key = calloc(big, 1);
  CHECK(lt_put(db, key, big, "x", 1, false) == LT_INVALID_ARGUMENT);
  free(key);
  lt_db *none = NULL;
  CHECK(lt_open(NULL, NULL, &none) == LT_INVALID_ARGUMENT && none == NULL);
  opts.memtable = 99;
  CHECK(lt_open(argv[1], &opts, &none) == LT_INVALID_ARGUMENT && none == NULL);
  opts.memtable = LT_MEMTABLE_BBTREE;
  opts.compression = 99;
  CHECK(lt_open(argv[1], &opts, &none) == LT_INVALID_ARGUMENT && none == NULL);

  OK(lt_checkpoint(db));
  OK(lt_sync(db));
  lt_close(db);

  /* Every write survives a reopen with default options, which read the
     pages the checkpoint stored compressed. */
  OK(lt_open(argv[1], NULL, &db));
  expect(db, NULL, "a", NULL);
  expect(db, NULL, "b", "2");
  expect(db, NULL, "ca", "4");
  OK(lt_iter_new(db, "", 0, NULL, 0, false, &it));
  expect_keys(it, "bccaempty");

  /* Range deletes: [b, cb) takes b, c and ca; a batch's writes apply in
     order, so cz, put after its range delete, survives it. */
  OK(lt_put(db, "d", 1, "5", 1, false));
  OK(lt_put(db, "dd", 2, "6", 1, false));
  OK(lt_delete_range(db, "b", 1, "cb", 2, false));
  OK(lt_iter_new(db, "", 0, NULL, 0, false, &it));
  expect_keys(it, "dddempty");
  batch = lt_batch_new();
  OK(lt_batch_put(batch, "ex", 2, "7", 1));
  OK(lt_batch_delete_prefix(batch, "e", 1));
  OK(lt_batch_put(batch, "cz", 2, "8", 1));
  OK(lt_batch_delete_range(batch, "z", 1, NULL, 0));
  OK(lt_write(db, batch, true));
  lt_batch_free(batch);
  expect(db, NULL, "ex", NULL);
  expect(db, NULL, "empty", NULL);
  expect(db, NULL, "cz", "8");
  OK(lt_delete_prefix(db, "d", 1, true));
  OK(lt_iter_new(db, "", 0, NULL, 0, true, &it));
  expect_keys(it, "cz");
  CHECK(lt_delete_range(db, NULL, 1, NULL, 0, false) == LT_INVALID_ARGUMENT);
  OK(lt_checkpoint(db));
  lt_close(db);
  OK(lt_open(argv[1], NULL, &db));
  OK(lt_iter_new(db, "", 0, NULL, 0, false, &it));
  expect_keys(it, "cz");

  /* Backups: a copy that opens as a store, and an archive that restores to
     one. Each holds the writes made before it and none after. */
  char copy[4096], archive[4096], restored[4096], not_archive[4200];
  snprintf(copy, sizeof copy, "%s-copy", argv[1]);
  snprintf(archive, sizeof archive, "%s-archive", argv[1]);
  snprintf(restored, sizeof restored, "%s-restored", argv[1]);
  snprintf(not_archive, sizeof not_archive, "%s/superblock", copy);
  /* A value this long is stored as a blob. */
  static char blob[100000];
  memset(blob, 'x', sizeof blob);
  OK(lt_put(db, "big", 3, blob, sizeof blob, false));
  lt_backup_options fast = lt_backup_options_default();
  CHECK(fast.rate_limit == 256u << 20);
  fast.rate_limit = 0;
  lt_backup_info info, again;
  OK(lt_backup_to_dir(db, copy, &fast, &info));
  CHECK(info.pages > 0 && info.blobs == 1 && info.bytes > 0);
  OK(lt_backup_to_file(db, archive, NULL, &again));
  CHECK(again.seq == info.seq && again.bytes == info.bytes);
  OK(lt_put(db, "later", 5, "6", 1, false));
  CHECK(lt_backup_to_dir(db, copy, NULL, NULL) == LT_INVALID_ARGUMENT);
  CHECK(lt_backup_to_dir(NULL, restored, NULL, NULL) == LT_INVALID_ARGUMENT);
  OK(lt_verify_backup_file(archive, &again));
  CHECK(again.seq == info.seq && again.pages == info.pages);
  OK(lt_restore_from_file(archive, restored, NULL));
  CHECK(lt_restore_from_file(archive, restored, NULL) == LT_INVALID_ARGUMENT);
  CHECK(lt_verify_backup_file(not_archive, NULL) == LT_CORRUPTION);
  CHECK(lt_verify_backup_file(NULL, NULL) == LT_INVALID_ARGUMENT);
  lt_close(db);
  const char *stores[] = {copy, restored};
  for (int i = 0; i < 2; i++) {
    OK(lt_open(stores[i], NULL, &db));
    expect(db, NULL, "cz", "8");
    expect(db, NULL, "later", NULL);
    lt_value *v = NULL;
    OK(lt_get(db, "big", 3, &v));
    CHECK(v != NULL && lt_value_bytes(v).len == sizeof blob);
    lt_value_free(v);
    OK(lt_iter_new(db, "", 0, NULL, 0, false, &it));
    expect_keys(it, "bigcz");
    lt_close(db);
  }

  puts("c_api: ok");
  return 0;
}
