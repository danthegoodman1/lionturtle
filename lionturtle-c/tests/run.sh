#!/bin/sh
# Builds lionturtle-c, then builds the C and C++ tests against its static and
# shared libraries and runs each on a new store.
set -eu
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
cargo build --release -p lionturtle-c
lib=$root/target/release
inc=$root/lionturtle-c/include
tests=$root/lionturtle-c/tests
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

# System libraries the static library needs beyond the compiler's defaults,
# from `rustc --print native-static-libs`.
case $(uname) in
  Linux) native="-lutil -lrt -lpthread -lm -ldl" ;;
  *) native="" ;;
esac
if [ "$(uname)" = Darwin ]; then
  otool -D "$lib/liblionturtle_c.dylib" | grep -q '^@rpath/liblionturtle_c.dylib$'
fi

# $native is unquoted so that it splits into separate flags.
cc -std=c11 -Wall -Wextra -Werror -I"$inc" "$tests/c_api.c" "$lib/liblionturtle_c.a" $native -o "$out/c_static"
c++ -std=c++17 -Wall -Wextra -Werror -I"$inc" "$tests/cpp_api.cpp" "$lib/liblionturtle_c.a" $native -o "$out/cpp_static"
cc -std=c11 -Wall -Wextra -Werror -I"$inc" "$tests/c_api.c" -L"$lib" -llionturtle_c -Wl,-rpath,"$lib" -o "$out/c_shared"
for t in c_static cpp_static c_shared; do
  "$out/$t" "$out/$t-store"
done
