#!/bin/sh
set -eu

ROOT=${TMPDIR:-/tmp}/msc-resume-suite.$$
SRC=$ROOT/source
DST=$ROOT/destination
CP=$ROOT/tree.checkpoint
cleanup() { rm -rf "$ROOT"; }
trap cleanup EXIT HUP INT TERM

expect_code()
{
   expected=$1
   shift
   set +e
   "$@"
   actual=$?
   set -e
   if test "$actual" -ne "$expected"; then
      echo "expected exit $expected, got $actual: $*" >&2
      exit 1
   fi
}

mkdir -p "$ROOT"
dd if=/dev/urandom of="$ROOT/single source.bin" bs=1048576 count=8 status=none
./build/resume_test "$ROOT/single source.bin" "$ROOT/single destination.bin"

mkdir -p "$SRC/empty dir" "$SRC/sub"
dd if=/dev/urandom of="$SRC/sub/data file" bs=1048576 count=8 status=none
: > "$SRC/empty file"
ln "$SRC/sub/data file" "$SRC/hard link"
ln -s '../empty file' "$SRC/sub/symbolic link"
truncate -s 33554432 "$SRC/sparse file"
dd if=/dev/urandom of="$SRC/sparse file" bs=4096 count=1 seek=2048 conv=notrunc status=none
chmod 0751 "$SRC/sub"
chmod 0640 "$SRC/sub/data file"
touch -m -d '2024-01-02 03:04:05 UTC' "$SRC" "$SRC/sub" "$SRC/sub/data file"

# Manifest-phase interruption leaves an empty but valid checkpoint; resume
# rebuilds the idempotent manifest and completes.
if MSC_TEST_CHECKPOINT="$CP" MSC_TEST_FORCE=1 MSC_TEST_INTERRUPT_PHASE=manifest \
   ./build/recursive_test "$SRC" "$DST" 4 65536; then
   echo "manifest interruption unexpectedly succeeded" >&2; exit 1
fi
MSC_TEST_CHECKPOINT="$CP" MSC_TEST_RESUME=1 ./build/recursive_test "$SRC" "$DST" 4 65536
diff -r --no-dereference "$SRC" "$DST"

# Same root inode is insufficient destination identity: damage every possible
# data-bearing file and require content verification to reject the tree.
rm -rf "$DST" "$CP"
if MSC_TEST_CHECKPOINT="$CP" MSC_TEST_FORCE=1 MSC_TEST_INTERRUPT_AFTER_SEGMENTS=7 \
   ./build/recursive_test "$SRC" "$DST" 4 65536; then
   echo "corruption setup transfer unexpectedly succeeded" >&2; exit 1
fi
# Overwrite, don't truncate: resume re-extends a truncated file with zeros, so
# when the durable ranges fall in the sparse file's all-zero region (the
# manifest follows readdir order, which varies by filesystem -- tmpfs lists the
# sparse file first) a truncation is not damage at all and resume correctly
# succeeds.  Random bytes at full size differ from the source in every range.
dd if=/dev/urandom of="$DST/sub/data file" bs=1048576 count=8 \
   conv=notrunc status=none
dd if=/dev/urandom of="$DST/sparse file" bs=1048576 count=32 \
   conv=notrunc status=none
expect_code 7 env MSC_TEST_CHECKPOINT="$CP" MSC_TEST_RESUME=1 \
   ./build/recursive_test "$SRC" "$DST" 4 65536

# Data-phase interruption and clean resume.
rm -rf "$DST" "$CP"
if MSC_TEST_CHECKPOINT="$CP" MSC_TEST_FORCE=1 MSC_TEST_INTERRUPT_AFTER_SEGMENTS=7 \
   ./build/recursive_test "$SRC" "$DST" 4 65536; then
   echo "data interruption unexpectedly succeeded" >&2; exit 1
fi
MSC_TEST_CHECKPOINT="$CP" MSC_TEST_RESUME=1 ./build/recursive_test "$SRC" "$DST" 4 65536
diff -r --no-dereference "$SRC" "$DST"
test "$(stat -c %i "$DST/sub/data file")" = "$(stat -c %i "$DST/hard link")"
test "$(stat -c %b "$DST/sparse file")" -lt 1000
test "$(stat -c %a "$SRC/sub/data file")" = "$(stat -c %a "$DST/sub/data file")"
test "$(stat -c %Y "$SRC/sub/data file")" = "$(stat -c %Y "$DST/sub/data file")"

# Final-metadata interruption: all file bytes are already durable/reused.
rm -rf "$DST" "$CP"
if MSC_TEST_CHECKPOINT="$CP" MSC_TEST_FORCE=1 MSC_TEST_INTERRUPT_PHASE=metadata \
   ./build/recursive_test "$SRC" "$DST" 4 65536; then
   echo "metadata interruption unexpectedly succeeded" >&2; exit 1
fi
MSC_TEST_CHECKPOINT="$CP" MSC_TEST_RESUME=1 ./build/recursive_test "$SRC" "$DST" 4 65536
diff -r --no-dereference "$SRC" "$DST"

# A changed source manifest is permanent and must not silently restart.
rm -rf "$DST" "$CP"
if MSC_TEST_CHECKPOINT="$CP" MSC_TEST_FORCE=1 MSC_TEST_INTERRUPT_AFTER_SEGMENTS=5 \
   ./build/recursive_test "$SRC" "$DST" 4 65536; then exit 1; fi
printf x >> "$SRC/sub/data file"
if MSC_TEST_CHECKPOINT="$CP" MSC_TEST_RESUME=1 ./build/recursive_test "$SRC" "$DST" 4 65536; then
   echo "mutated recursive source was not rejected" >&2; exit 1
fi

echo "recursive resume tests passed"
