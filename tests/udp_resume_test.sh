#!/bin/sh
set -eu

ROOT=${TMPDIR:-/tmp}/msc-udp-resume.$$
CHUNK=524288
SRC=$ROOT/source.bin
STORAGE_SHIM=./build/storage_fault_shim.so
trap 'rm -rf "$ROOT"' EXIT HUP INT TERM
mkdir -p "$ROOT"
dd if=/dev/urandom of="$SRC" bs=1M count=4 status=none

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

interrupt_one()
{
   name=$1
   dst=$ROOT/$name.bin
   cp=$ROOT/$name.checkpoint
   expect_code 6 env MSC_TEST_CHECKPOINT="$cp" \
      MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
      ./build/udp_test "$SRC" "$dst" 4
   test -e "$cp"
   test -e "$dst.msc-part"
   test ! -e "$dst"
   test "$(stat -c %s "$dst.msc-part")" -eq "$CHUNK"
}

# Fresh success, interruption, stable partial/checkpoint, exact resume, and
# accounting that proves the durable first chunk was not sent again.
fresh=$ROOT/fresh.bin
freshcp=$ROOT/fresh.checkpoint
env MSC_TEST_CHECKPOINT="$freshcp" MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$fresh" 4
cmp "$SRC" "$fresh"
test ! -e "$freshcp"
test ! -e "$fresh.msc-part"

interrupt_one basic
env MSC_TEST_CHECKPOINT="$ROOT/basic.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/basic.bin" 4 \
   > "$ROOT/basic.resume.log"
cmp "$SRC" "$ROOT/basic.bin"
grep -q 'udp transfer of 3670016 bytes complete' "$ROOT/basic.resume.log"

# A same-inode, same-size edit inside a claimed range must be detected from
# content, not accepted on the strength of stat metadata.
interrupt_one content
dd if=/dev/zero of="$ROOT/content.bin.msc-part" bs=$CHUNK count=1 \
   conv=notrunc status=none
expect_code 7 env MSC_TEST_CHECKPOINT="$ROOT/content.checkpoint" \
   MSC_TEST_RESUME=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/content.bin" 4
test ! -e "$ROOT/content.bin"

# A failed first chunk retains a zero-prefix checkpoint and rolls back to zero.
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/zero.checkpoint" \
   MSC_TEST_STALL_TIMEOUT_MS=200 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/zero.bin" 4 100
test -e "$ROOT/zero.checkpoint"
test "$(stat -c %s "$ROOT/zero.bin.msc-part")" -eq 0
test ! -e "$ROOT/zero.bin"
env MSC_TEST_CHECKPOINT="$ROOT/zero.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/zero.bin" 4
cmp "$SRC" "$ROOT/zero.bin"

# Abort while MSC UDP is writing the second chunk.  Only the first chunk may remain
# after rollback, and the next session must overwrite the discarded suffix.
expect_code 6 env MSC_UDP_PACE_RATE_MBIT=10 MSC_UDP_PACE_BURST=1 \
   MSC_TEST_CHECKPOINT="$ROOT/midchunk.checkpoint" \
   MSC_TEST_FAIL_DURING_CHUNK_MS=50 \
   MSC_TEST_FAIL_DURING_CHUNK_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   MSC_TEST_STALL_TIMEOUT_MS=500 \
   ./build/udp_test "$SRC" "$ROOT/midchunk.bin" 4
test -e "$ROOT/midchunk.checkpoint"
test "$(stat -c %s "$ROOT/midchunk.bin.msc-part")" -eq "$CHUNK"
test ! -e "$ROOT/midchunk.bin"
env MSC_TEST_CHECKPOINT="$ROOT/midchunk.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/midchunk.bin" 4
cmp "$SRC" "$ROOT/midchunk.bin"

# SIGKILL bypasses cleanup entirely.  The checkpoint still owns only the first
# durable chunk; the pre-sized suffix is ignored and the next session resumes.
expect_code 6 env MSC_UDP_PACE_RATE_MBIT=10 MSC_UDP_PACE_BURST=1 \
   MSC_TEST_CHECKPOINT="$ROOT/killed.checkpoint" \
   MSC_TEST_CHECKPOINT_MARKER="$ROOT/killed.marker" \
   MSC_TEST_KILL_RECEIVER_AFTER_MARKER="$ROOT/killed.marker" \
   MSC_TEST_KILL_RECEIVER_DELAY_MS=50 \
   MSC_TEST_STALL_TIMEOUT_MS=500 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/killed.bin" 4
test -e "$ROOT/killed.checkpoint"
test -e "$ROOT/killed.bin.msc-part"
test "$(stat -c %s "$ROOT/killed.bin.msc-part")" -gt "$CHUNK"
test ! -e "$ROOT/killed.bin"
env MSC_TEST_CHECKPOINT="$ROOT/killed.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/killed.bin" 4
cmp "$SRC" "$ROOT/killed.bin"

# Source identity and stable-partial identity are permanent failures.
cp "$SRC" "$ROOT/mutated-source.bin"
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/mutated.checkpoint" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$ROOT/mutated-source.bin" "$ROOT/mutated.bin" 4
printf x >> "$ROOT/mutated-source.bin"
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/mutated.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$ROOT/mutated-source.bin" "$ROOT/mutated.bin" 4
test ! -e "$ROOT/mutated.bin"

interrupt_one truncated
truncate -s $((CHUNK - 1)) "$ROOT/truncated.bin.msc-part"
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/truncated.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/truncated.bin" 4

interrupt_one replaced
mv "$ROOT/replaced.bin.msc-part" "$ROOT/replaced.old"
dd if=/dev/zero of="$ROOT/replaced.bin.msc-part" bs=$CHUNK count=1 status=none
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/replaced.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/replaced.bin" 4

# Missing, corrupt, and unsupported checkpoints never silently restart, even
# though udp_test enables --force by default.
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/missing.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/missing.bin" 4

interrupt_one corrupt
printf X | dd of="$ROOT/corrupt.checkpoint" bs=1 seek=20 conv=notrunc status=none
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/corrupt.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/corrupt.bin" 4

interrupt_one version
printf '\000\000\000\003' | dd of="$ROOT/version.checkpoint" bs=1 seek=4 conv=notrunc status=none
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/version.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/version.bin" 4

# Fresh stale state requires --force; force may restart fresh state but cannot
# rescue an explicit invalid resume.
interrupt_one stale
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/stale.checkpoint" MSC_TEST_NO_FORCE=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/stale.bin" 4
env MSC_TEST_CHECKPOINT="$ROOT/stale.checkpoint" MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/stale.bin" 4
cmp "$SRC" "$ROOT/stale.bin"

# Checksum failure is permanent integrity failure and cannot publish bytes.
expect_code 7 env MSC_UDP_RX_NOWRITE=1 MSC_TEST_CHECKPOINT="$ROOT/integrity.checkpoint" \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/integrity.bin" 4
test -e "$ROOT/integrity.checkpoint"
test "$(stat -c %s "$ROOT/integrity.bin.msc-part")" -eq 0
test ! -e "$ROOT/integrity.bin"

# A receiver-local storage failure is a destination error, never a network
# error or success. Rollback must expose only the last durable checkpoint, and
# the retained state must complete exactly when storage becomes available.
# Exercise the typed failure record over every socket-based control transport.
test -r "$STORAGE_SHIM"
for ctl in tcp many one; do
   expect_code 4 env MSC_UDP_CTL="$ctl" LD_PRELOAD="$STORAGE_SHIM" \
      MSC_TEST_ENOSPC_AFTER_BYTES=1310720 MSC_UDP_NO_MMAP=1 \
      MSC_TEST_STALL_TIMEOUT_MS=500 \
      MSC_TEST_CHECKPOINT="$ROOT/enospc-$ctl.checkpoint" \
      MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
      ./build/udp_test "$SRC" "$ROOT/enospc-$ctl.bin" 4
   test -e "$ROOT/enospc-$ctl.checkpoint"
   test "$(stat -c %s "$ROOT/enospc-$ctl.bin.msc-part")" -eq $((2 * CHUNK))
   test ! -e "$ROOT/enospc-$ctl.bin"
   env MSC_UDP_CTL="$ctl" \
      MSC_TEST_CHECKPOINT="$ROOT/enospc-$ctl.checkpoint" MSC_TEST_RESUME=1 \
      MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
      ./build/udp_test "$SRC" "$ROOT/enospc-$ctl.bin" 4
   cmp "$SRC" "$ROOT/enospc-$ctl.bin"
done

# Retention and cancellation preserve a resumable durable prefix.
env MSC_TEST_CHECKPOINT="$ROOT/keep.checkpoint" MSC_TEST_KEEP_CHECKPOINT=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/keep.bin" 4
cmp "$SRC" "$ROOT/keep.bin"
test -e "$ROOT/keep.checkpoint"

expect_code 130 env MSC_TEST_CHECKPOINT="$ROOT/sigint.checkpoint" \
   MSC_TEST_SIGNAL_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/sigint.bin" 4
test -e "$ROOT/sigint.checkpoint"
test ! -e "$ROOT/sigint.bin"
env MSC_TEST_CHECKPOINT="$ROOT/sigint.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/sigint.bin" 4
cmp "$SRC" "$ROOT/sigint.bin"

expect_code 143 env MSC_TEST_CHECKPOINT="$ROOT/sigterm.checkpoint" \
   MSC_TEST_SIGNAL_AFTER_CHECKPOINTS=1 MSC_TEST_SIGNAL_TERM=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/sigterm.bin" 4
test -e "$ROOT/sigterm.checkpoint"
test ! -e "$ROOT/sigterm.bin"
env MSC_TEST_CHECKPOINT="$ROOT/sigterm.checkpoint" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/sigterm.bin" 4
cmp "$SRC" "$ROOT/sigterm.bin"

# Ambiguous checkpointed slices remain an explicit CLI error.
expect_code 2 env MSC_TEST_CHECKPOINT="$ROOT/slice.checkpoint" \
   MSC_TEST_SRC_OFFSET=1 ./build/udp_test "$SRC" "$ROOT/slice.bin" 2

echo "MSC UDP checkpoint/resume tests passed"
