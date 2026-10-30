#!/bin/sh
# Builds the C library for this machine, packages it with its headers as
# lionturtle-c-<version>-<target>.tar.gz in <dir>, and prints the archive's
# path. Usage: lionturtle-c/package.sh <version> <dir>
set -eu
version=$1
dir=$(cd "$2" && pwd)
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
cargo build --release -p lionturtle-c >&2
name=lionturtle-c-$version-$(rustc -vV | sed -n 's/^host: //p')
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/$name/include" "$stage/$name/lib"
cp lionturtle-c/include/lionturtle.h lionturtle-c/include/lionturtle.hpp "$stage/$name/include/"
for f in liblionturtle_c.a liblionturtle_c.so liblionturtle_c.dylib; do
  if [ -f "target/release/$f" ]; then
    cp "target/release/$f" "$stage/$name/lib/"
    strip -S "$stage/$name/lib/$f"
  fi
done
tar -C "$stage" -czf "$dir/$name.tar.gz" "$name"
echo "$dir/$name.tar.gz"
