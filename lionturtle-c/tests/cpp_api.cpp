// Exercises the C++ wrapper against a store in the directory argv[1].
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "lionturtle.hpp"

#define CHECK(cond)                                                              \
  do {                                                                           \
    if (!(cond)) {                                                               \
      std::fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #cond); \
      std::exit(1);                                                              \
    }                                                                            \
  } while (0)

static std::string keys(lionturtle::Iterator it) {
  std::string got;
  while (it.next()) got += it.key();
  return got;
}

int main(int argc, char **argv) {
  CHECK(argc == 2);
  using lionturtle::Db;
  {
    Db db(argv[1]);
    db.put("a", "1");
    db.put("b", "2", true);
    CHECK(db.get("a")->view() == "1");
    CHECK(!db.get("missing"));

    auto snap = db.snapshot();
    lionturtle::Batch batch;
    batch.put("c", "3").put("ca", "4").erase("a");
    db.write(batch, true);
    CHECK(!db.get("a"));
    CHECK(snap.get("a")->str() == "1");
    CHECK(keys(snap.iter()) == "ab");
    CHECK(keys(db.iter("b", "d")) == "bcca");
    CHECK(keys(db.iter_rev()) == "cacb");
    CHECK(keys(db.prefix("c")) == "cca");
    CHECK(keys(db.prefix_rev("c")) == "cac");
    CHECK(keys(db.iter("", std::string_view{})) == "");

    auto it = db.iter("ca");
    CHECK(it.next() && it.key() == "ca" && it.value() == "4");
    CHECK(!it.next());

    // Writers on several threads share one Db, each write durable.
    std::atomic<int> failures{0};
    std::vector<std::thread> writers;
    for (int t = 0; t < 8; t++) {
      writers.emplace_back([&db, &failures, t] {
        try {
          for (int i = 0; i < 200; i++) db.put("t" + std::to_string(t) + "-" + std::to_string(i), "v", true);
        } catch (const lionturtle::Error &) {
          failures++;
        }
      });
    }
    for (auto &w : writers) w.join();
    CHECK(failures == 0);
    int n = 0;
    for (auto it = db.prefix("t"); it.next();) n++;
    CHECK(n == 8 * 200);

    bool threw = false;
    try {
      db.put(std::string(1 << 17, 'k'), "v");
    } catch (const lionturtle::Error &e) {
      threw = e.status() == LT_INVALID_ARGUMENT && std::string(e.what()).size() > 0;
    }
    CHECK(threw);
  }
  {
    Db db(argv[1]);
    CHECK(db.get("b")->str() == "2");
    CHECK(db.get("t7-199"));
  }
  std::puts("cpp_api: ok");
  return 0;
}
