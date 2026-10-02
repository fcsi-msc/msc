#!/bin/sh
set -eu

ROOT=${TMPDIR:-/tmp}/msc-udp-recursive-resume.$$
CHUNK=524288
STORAGE_SHIM=./build/storage_fault_shim.so
trap 'rm -rf "$ROOT"' EXIT HUP INT TERM
mkdir -p "$ROOT"

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

make_tree()
{
   tree=$1
   mkdir -p "$tree/sub/emptydir"
   dd if=/dev/urandom of="$tree/a" bs=1 count=300000 status=none
   dd if=/dev/urandom of="$tree/sub/b" bs=1 count=400000 status=none
   truncate -s 2097152 "$tree/sparse"
   dd if=/dev/urandom of="$tree/sparse" bs=4096 count=1 seek=3 \
      conv=notrunc status=none
   dd if=/dev/urandom of="$tree/sparse" bs=4096 count=1 seek=400 \
      conv=notrunc status=none
   : > "$tree/empty"
   ln "$tree/a" "$tree/a-hard"
   ln -s sub/b "$tree/b-link"
   chmod 0751 "$tree/sub"
   chmod 0640 "$tree/a"
   touch -t 202001020304.05 "$tree/a" "$tree/sub"
}

assert_tree()
{
   src=$1
   dst=$2
   diff -r "$src" "$dst"
   test "$(stat -c %i "$dst/a")" = "$(stat -c %i "$dst/a-hard")"
   test "$(stat -c %a "$src/a")" = "$(stat -c %a "$dst/a")"
   test "$(stat -c %a "$src/sub")" = "$(stat -c %a "$dst/sub")"
   test "$(stat -c %Y "$src/a")" = "$(stat -c %Y "$dst/a")"
   test "$(stat -c %Y "$src/sub")" = "$(stat -c %Y "$dst/sub")"
   test "$(stat -c %b "$dst/sparse")" -lt \
      "$(( $(stat -c %s "$dst/sparse") / 512 ))"
}

SRC=$ROOT/source
make_tree "$SRC"
TOTAL=$((300000 + 400000 + 2097152))

# Fresh success removes the checkpoint. CHUNK falls in a file on the first
# boundary, and the second chunk crosses from b into sparse.
MSC_TEST_CHECKPOINT="$ROOT/fresh.cp" MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/fresh" 4
assert_tree "$SRC" "$ROOT/fresh"
test ! -e "$ROOT/fresh.cp"

# Interrupt only after a durable logical chunk. Resume must reuse exactly that
# chunk and transmit only the complement.
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/basic.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/basic" 4
test -e "$ROOT/basic.cp"
MSC_TEST_CHECKPOINT="$ROOT/basic.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/basic" 4 \
   > "$ROOT/basic.log"
grep -q "udp transfer of $((TOTAL - CHUNK)) bytes complete" "$ROOT/basic.log"
assert_tree "$SRC" "$ROOT/basic"

# Claimed destination corruption is content-verified and permanent.
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/content.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/content" 4
dd if=/dev/zero of="$ROOT/content/a" bs=4096 count=1 \
   conv=notrunc status=none
expect_code 7 env MSC_TEST_CHECKPOINT="$ROOT/content.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/content" 4

# New chunks are content-checked before their logical range can become durable.
expect_code 7 env MSC_UDP_RX_NOWRITE=1 \
   MSC_TEST_CHECKPOINT="$ROOT/write-integrity.cp" \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/write-integrity" 3
test -e "$ROOT/write-integrity.cp"
MSC_TEST_CHECKPOINT="$ROOT/write-integrity.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/write-integrity" 3
assert_tree "$SRC" "$ROOT/write-integrity"

# Receiver ENOSPC is a destination failure for a recursive table transfer too.
# The last two durable chunks remain reusable after storage is restored. Cover
# the typed error over TCP control and both reliable UDP-control modes.
test -r "$STORAGE_SHIM"
for ctl in tcp many one; do
   expect_code 4 env MSC_UDP_CTL="$ctl" LD_PRELOAD="$STORAGE_SHIM" \
      MSC_TEST_ENOSPC_AFTER_BYTES=1310720 MSC_UDP_NO_MMAP=1 \
      MSC_TEST_STALL_TIMEOUT_MS=500 \
      MSC_TEST_CHECKPOINT="$ROOT/enospc-$ctl.cp" \
      MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
      ./build/udp_test "$SRC" "$ROOT/enospc-$ctl" 3
   test -e "$ROOT/enospc-$ctl.cp"
   env MSC_UDP_CTL="$ctl" \
      MSC_TEST_CHECKPOINT="$ROOT/enospc-$ctl.cp" MSC_TEST_RESUME=1 \
      MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
      ./build/udp_test "$SRC" "$ROOT/enospc-$ctl" 3 \
      > "$ROOT/enospc-$ctl.log"
   grep -q "udp transfer of $((TOTAL - 2 * CHUNK)) bytes complete" \
      "$ROOT/enospc-$ctl.log"
   assert_tree "$SRC" "$ROOT/enospc-$ctl"
done

# Manifest, data, and metadata interruptions all retain recoverable state.
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/manifest.cp" \
   MSC_TEST_INTERRUPT_PHASE=manifest MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/manifest" 3
test -e "$ROOT/manifest.cp"
MSC_TEST_CHECKPOINT="$ROOT/manifest.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/manifest" 3
assert_tree "$SRC" "$ROOT/manifest"

expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/data.cp" \
   MSC_TEST_INTERRUPT_PHASE=data MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/data" 3
MSC_TEST_CHECKPOINT="$ROOT/data.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/data" 3
assert_tree "$SRC" "$ROOT/data"

# Abort while a later chunk is active. The checkpoint still owns only the
# prior durable range; any partial writes are overwritten on resume.
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/midchunk.cp" \
   MSC_TEST_FAIL_DURING_CHUNK_MS=30 \
   MSC_TEST_FAIL_DURING_CHUNK_AFTER_CHECKPOINTS=1 \
   MSC_UDP_PACE=1 MSC_UDP_PACE_BURST=1 MSC_UDP_PACE_RATE_MBIT=10 \
   MSC_TEST_STALL_TIMEOUT_MS=500 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/midchunk" 3
MSC_TEST_CHECKPOINT="$ROOT/midchunk.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/midchunk" 3
assert_tree "$SRC" "$ROOT/midchunk"

expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/metadata.cp" \
   MSC_TEST_INTERRUPT_PHASE=metadata MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/metadata" 3
test -e "$ROOT/metadata.cp"
MSC_TEST_CHECKPOINT="$ROOT/metadata.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/metadata" 3 \
   > "$ROOT/metadata.log"
grep -q 'udp transfer of 0 bytes complete' "$ROOT/metadata.log"
assert_tree "$SRC" "$ROOT/metadata"

# A source manifest/content identity change cannot resume.
cp -a "$SRC" "$ROOT/mutated-source"
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/mutated.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$ROOT/mutated-source" "$ROOT/mutated" 3
printf x >> "$ROOT/mutated-source/sub/b"
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/mutated.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$ROOT/mutated-source" "$ROOT/mutated" 3

# Missing, corrupt, unsupported, and stale checkpoints never silently restart.
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/missing.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/missing" 2

expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/corrupt.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/corrupt" 2
printf X | dd of="$ROOT/corrupt.cp" bs=1 seek=20 conv=notrunc status=none
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/corrupt.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/corrupt" 2

expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/version.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/version" 2
printf '\000\000\000\003' | dd of="$ROOT/version.cp" bs=1 seek=4 \
   conv=notrunc status=none
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/version.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/version" 2

expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/stale.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/stale" 2
expect_code 8 env MSC_TEST_CHECKPOINT="$ROOT/stale.cp" MSC_TEST_NO_FORCE=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/stale" 2
MSC_TEST_CHECKPOINT="$ROOT/stale.cp" MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/stale" 2
assert_tree "$SRC" "$ROOT/stale"

# Retention, cancellation, and abrupt receiver death leave resumable trees.
MSC_TEST_CHECKPOINT="$ROOT/keep.cp" MSC_TEST_KEEP_CHECKPOINT=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/keep" 3
assert_tree "$SRC" "$ROOT/keep"
test -e "$ROOT/keep.cp"

expect_code 130 env MSC_TEST_CHECKPOINT="$ROOT/sigint.cp" \
   MSC_TEST_SIGNAL_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$SRC" "$ROOT/sigint" 3
MSC_TEST_CHECKPOINT="$ROOT/sigint.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/sigint" 3
assert_tree "$SRC" "$ROOT/sigint"

expect_code 143 env MSC_TEST_CHECKPOINT="$ROOT/sigterm.cp" \
   MSC_TEST_SIGNAL_AFTER_CHECKPOINTS=1 MSC_TEST_SIGNAL_TERM=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/sigterm" 3
MSC_TEST_CHECKPOINT="$ROOT/sigterm.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/sigterm" 3
assert_tree "$SRC" "$ROOT/sigterm"

expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/killed.cp" \
   MSC_TEST_CHECKPOINT_MARKER="$ROOT/killed.marker" \
   MSC_TEST_KILL_RECEIVER_AFTER_MARKER="$ROOT/killed.marker" \
   MSC_TEST_KILL_RECEIVER_DELAY_MS=20 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   MSC_UDP_PACE_RATE_MBIT=10 MSC_TEST_STALL_TIMEOUT_MS=500 \
   ./build/udp_test "$SRC" "$ROOT/killed" 3
test -e "$ROOT/killed.cp"
MSC_TEST_CHECKPOINT="$ROOT/killed.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK ./build/udp_test "$SRC" "$ROOT/killed" 3
assert_tree "$SRC" "$ROOT/killed"

# Payload/PMTU geometry can change across sessions, and loss recovery remains
# independent from logical checkpoint chunks.
expect_code 6 env MSC_TEST_CHECKPOINT="$ROOT/payload.cp" \
   MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   MSC_UDP_PAYLOAD=900 ./build/udp_test "$SRC" "$ROOT/payload" 4
MSC_TEST_CHECKPOINT="$ROOT/payload.cp" MSC_TEST_RESUME=1 \
   MSC_UDP_CHECKPOINT_BYTES=$CHUNK MSC_UDP_PAYLOAD=1200 \
   ./build/udp_test "$SRC" "$ROOT/payload" 4 2
assert_tree "$SRC" "$ROOT/payload"

# Checkpoint/control records use the transport-aware stream in tcp, many, and
# one modes. Exercise repeated table transfers in each session.
for mode in tcp many one; do
   MSC_UDP_CTL=$mode MSC_TEST_CHECKPOINT="$ROOT/$mode.cp" \
      MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
      ./build/udp_test "$SRC" "$ROOT/$mode" 3
   assert_tree "$SRC" "$ROOT/$mode"
done

# A tree with no logical file bytes still checkpoints manifest durability and
# final metadata, then removes the checkpoint.
mkdir -p "$ROOT/zero-source/dir"
: > "$ROOT/zero-source/empty"
ln -s empty "$ROOT/zero-source/link"
MSC_TEST_CHECKPOINT="$ROOT/zero.cp" MSC_UDP_CHECKPOINT_BYTES=$CHUNK \
   ./build/udp_test "$ROOT/zero-source" "$ROOT/zero" 2 > "$ROOT/zero.log"
grep -q 'udp transfer of 0 bytes complete' "$ROOT/zero.log"
diff -r "$ROOT/zero-source" "$ROOT/zero"
test ! -e "$ROOT/zero.cp"

echo "MSC recursive UDP checkpoint/resume tests passed"
