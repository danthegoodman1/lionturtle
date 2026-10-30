// C++17 wrapper for lionturtle.h: handles free themselves, arguments are
// std::string_view, and failures throw lionturtle::Error.
#ifndef LIONTURTLE_HPP
#define LIONTURTLE_HPP

#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "lionturtle.h"

namespace lionturtle {

// A failed call. status() is the C status, and what() describes it.
class Error : public std::runtime_error {
 public:
  Error(lt_status status, const char *message) : std::runtime_error(message), status_(status) {}
  lt_status status() const noexcept { return status_; }

 private:
  lt_status status_;
};

namespace detail {

inline void check(lt_status status) {
  if (status != LT_OK) throw Error(status, lt_last_error());
}

inline std::string_view view(lt_slice s) noexcept { return {s.data, s.len}; }

// An upper bound for the C API, where null means none. An empty bound must
// stay non-null, and std::string_view{} has a null data().
inline const char *bound(const std::optional<std::string_view> &end) noexcept {
  if (!end) return nullptr;
  return end->data() ? end->data() : "";
}

template <auto Fn>
struct Free {
  template <typename T>
  void operator()(T *p) const noexcept {
    Fn(p);
  }
};

}  // namespace detail

// A value read from the store. It shares the store's bytes without copying
// them.
class Value {
 public:
  explicit Value(lt_value *v) noexcept : v_(v) {}
  std::string_view view() const noexcept { return detail::view(lt_value_bytes(v_.get())); }
  operator std::string_view() const noexcept { return view(); }
  std::string str() const { return std::string(view()); }

 private:
  std::unique_ptr<lt_value, detail::Free<lt_value_free>> v_;
};

// Writes that Db::write applies atomically. Use one from one thread at a
// time.
class Batch {
 public:
  Batch() : b_(lt_batch_new()) {}
  Batch &put(std::string_view key, std::string_view value) {
    detail::check(lt_batch_put(b_.get(), key.data(), key.size(), value.data(), value.size()));
    return *this;
  }
  Batch &erase(std::string_view key) {
    detail::check(lt_batch_delete(b_.get(), key.data(), key.size()));
    return *this;
  }
  void clear() noexcept { lt_batch_clear(b_.get()); }
  const lt_batch *get() const noexcept { return b_.get(); }

 private:
  std::unique_ptr<lt_batch, detail::Free<lt_batch_free>> b_;
};

// Entries of one snapshot, in ascending or descending key order. Use one
// from one thread at a time:
//
//   for (auto it = db.iter("a", "c"); it.next();) use(it.key(), it.value());
class Iterator {
 public:
  explicit Iterator(lt_iter *it) noexcept : it_(it) {}
  // Moves to the next entry, returning false past the end. key() and
  // value() view the entry until the next call.
  bool next() {
    bool has_entry = false;
    detail::check(lt_iter_next(it_.get(), &has_entry, &key_, &value_));
    return has_entry;
  }
  std::string_view key() const noexcept { return detail::view(key_); }
  std::string_view value() const noexcept { return detail::view(value_); }

 private:
  std::unique_ptr<lt_iter, detail::Free<lt_iter_free>> it_;
  lt_slice key_{};
  lt_slice value_{};
};

// A view of the store as of one moment: every read through it sees the
// writes completed before Db::snapshot and none after. Any number of
// threads may share one.
class Snapshot {
 public:
  explicit Snapshot(lt_snapshot *s) noexcept : s_(s) {}

  std::optional<Value> get(std::string_view key) const {
    lt_value *v = nullptr;
    detail::check(lt_snapshot_get(s_.get(), key.data(), key.size(), &v));
    if (!v) return std::nullopt;
    return Value(v);
  }
  // Entries with keys in [start, end); no end means no upper bound.
  Iterator iter(std::string_view start = {}, std::optional<std::string_view> end = std::nullopt) const {
    return range(start, end, false);
  }
  Iterator iter_rev(std::string_view start = {}, std::optional<std::string_view> end = std::nullopt) const {
    return range(start, end, true);
  }
  Iterator prefix(std::string_view p) const { return with_prefix(p, false); }
  Iterator prefix_rev(std::string_view p) const { return with_prefix(p, true); }

 private:
  Iterator range(std::string_view start, const std::optional<std::string_view> &end, bool reverse) const {
    lt_iter *it = nullptr;
    detail::check(lt_snapshot_iter_new(s_.get(), start.data(), start.size(), detail::bound(end),
                                       end ? end->size() : 0, reverse, &it));
    return Iterator(it);
  }
  Iterator with_prefix(std::string_view p, bool reverse) const {
    lt_iter *it = nullptr;
    detail::check(lt_snapshot_iter_prefix(s_.get(), p.data(), p.size(), reverse, &it));
    return Iterator(it);
  }

  std::unique_ptr<lt_snapshot, detail::Free<lt_snapshot_free>> s_;
};

// An open store. Any number of threads may share one. The destructor stops
// background work and syncs the log.
class Db {
 public:
  // Opens the store in the directory `path`, creating both if needed.
  explicit Db(const std::string &path, const lt_options &options = lt_options_default()) {
    lt_db *db = nullptr;
    detail::check(lt_open(path.c_str(), &options, &db));
    db_.reset(db);
  }

  // With `sync`, each write returns once it is durable.
  void put(std::string_view key, std::string_view value, bool sync = false) {
    detail::check(lt_put(db_.get(), key.data(), key.size(), value.data(), value.size(), sync));
  }
  void erase(std::string_view key, bool sync = false) {
    detail::check(lt_delete(db_.get(), key.data(), key.size(), sync));
  }
  // Applies every write in `batch` as one: a crash keeps all or none, and
  // no read sees some without the rest.
  void write(const Batch &batch, bool sync = false) { detail::check(lt_write(db_.get(), batch.get(), sync)); }

  std::optional<Value> get(std::string_view key) const {
    lt_value *v = nullptr;
    detail::check(lt_get(db_.get(), key.data(), key.size(), &v));
    if (!v) return std::nullopt;
    return Value(v);
  }
  // Each iterator reads a snapshot taken when it is created.
  Iterator iter(std::string_view start = {}, std::optional<std::string_view> end = std::nullopt) const {
    return range(start, end, false);
  }
  Iterator iter_rev(std::string_view start = {}, std::optional<std::string_view> end = std::nullopt) const {
    return range(start, end, true);
  }
  Iterator prefix(std::string_view p) const { return with_prefix(p, false); }
  Iterator prefix_rev(std::string_view p) const { return with_prefix(p, true); }
  Snapshot snapshot() const { return Snapshot(lt_snapshot_new(db_.get())); }

  // Makes every completed write durable.
  void sync() { detail::check(lt_sync(db_.get())); }
  // Waits until every write so far is checkpointed into the tree.
  void checkpoint() { detail::check(lt_checkpoint(db_.get())); }
  // Advances checkpointing by one stage, for a store opened with background
  // false. Returns false when there was nothing to do.
  bool checkpoint_step() {
    bool progressed = false;
    detail::check(lt_checkpoint_step(db_.get(), &progressed));
    return progressed;
  }

 private:
  Iterator range(std::string_view start, const std::optional<std::string_view> &end, bool reverse) const {
    lt_iter *it = nullptr;
    detail::check(
        lt_iter_new(db_.get(), start.data(), start.size(), detail::bound(end), end ? end->size() : 0, reverse, &it));
    return Iterator(it);
  }
  Iterator with_prefix(std::string_view p, bool reverse) const {
    lt_iter *it = nullptr;
    detail::check(lt_iter_prefix(db_.get(), p.data(), p.size(), reverse, &it));
    return Iterator(it);
  }

  std::unique_ptr<lt_db, detail::Free<lt_close>> db_;
};

}  // namespace lionturtle

#endif  // LIONTURTLE_HPP
