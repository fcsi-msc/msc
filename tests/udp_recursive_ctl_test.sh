#!/bin/sh
# Recursive (-R) transfers over the UDP control channel (MSC_UDP_CTL=many).
#
# Guards a past regression: the recursive sender sent MSC_UDP_CTL_TABLE_BEGIN
# right before handing off to the (shim-silent) data-flow threads WITHOUT
# draining it, so a single lost TABLE_BEGIN datagram was never retransmitted and
# the receiver blocked forever in its control-channel recv -- past every
# watchdog.
#
# Two properties, both of which the pre-fix code fails:
#   1. a TABLE_BEGIN lost on first transmission is retransmitted by the sender's
#      post-send drain, so the tree still transfers and byte-verifies;
#   2. if the sender vanishes at the handoff, the receiver trips its idle bound
#      and exits 6 (network, retryable) instead of orphaning.
set -eu

ROOT=${TMPDIR:-/tmp}/msc-udp-recursive-ctl.$$
trap 'rm -rf "$ROOT"' EXIT HUP INT TERM
mkdir -p "$ROOT"

# A modest tree: a few subdirs, an empty file, a hard link, a symlink -- enough
# structure that the manifest is non-trivial but the run stays fast under a
# bounded control channel.
SRC=$ROOT/src
mkdir -p "$SRC/sub/deep" "$SRC/empty_dir"
i=1
while [ $i -le 40 ]; do
   dd if=/dev/urandom of="$SRC/f$i" bs=1024 count=6 status=none
   i=$((i + 1))
done
dd if=/dev/urandom of="$SRC/sub/g" bs=1024 count=32 status=none
dd if=/dev/urandom of="$SRC/sub/deep/h" bs=1024 count=8 status=none
: > "$SRC/empty"
ln "$SRC/f1" "$SRC/f1-hard"
ln -s sub/g "$SRC/g-link"

verify_tree()
{
   diff -r "$SRC" "$1"
   test "$(stat -c %i "$1/f1")" = "$(stat -c %i "$1/f1-hard")"
}

# 1. Baseline: recursive over the UDP control channel, no impairment. This
#    combination had no gate before; it must simply work and byte-verify.
echo "recursive+many: clean"
rm -rf "$ROOT/clean"
timeout 60 env MSC_UDP_CTL=many MSC_UDP_PORTS=2 \
   ./build/udp_test "$SRC" "$ROOT/clean" 4
verify_tree "$ROOT/clean"

# 2. Deterministic defect-1 guard: drop the TABLE_BEGIN record on its first
#    transmission. Only the sender's post-send drain retransmits it. Fixed code
#    delivers it and completes; the un-drained regression never resends it and
#    the receiver wedges (killed by timeout -> nonzero -> this test fails).
echo "recursive+many: TABLE_BEGIN dropped once, drain must recover"
rm -rf "$ROOT/dropped"
timeout 60 env MSC_UDP_CTL=many MSC_UDP_PORTS=2 \
   MSC_TEST_DROP_TABLE_BEGIN=1 MSC_TEST_STALL_TIMEOUT_MS=8000 \
   ./build/udp_test "$SRC" "$ROOT/dropped" 4
verify_tree "$ROOT/dropped"

# 3. Defect-2 guard (the orphan watchdog): the sender vanishes at the handoff
#    with no control-channel FIN, so the receiver's peer is silent-but-open.
#    The receiver must trip its idle bound and exit 6 well within the wrapper,
#    never hang. Pre-fix, the receiver's unbounded control recv orphans here and
#    `timeout` kills it (124) -> this test fails.
echo "recursive+many: silent sender at handoff must exit 6, not orphan"
rm -rf "$ROOT/orphan"
# Output goes to a log, not this script's stdout: a regressed (orphaning)
# receiver would otherwise inherit and hold the pipe open past `timeout`.
set +e
timeout 30 env MSC_UDP_CTL=many MSC_UDP_PORTS=2 \
   MSC_TEST_SILENT_AFTER_MANIFEST=1 MSC_TEST_STALL_TIMEOUT_MS=3000 \
   ./build/udp_test "$SRC" "$ROOT/orphan" 4 > "$ROOT/orphan.log" 2>&1
code=$?
set -e
if [ "$code" -ne 6 ]; then
   echo "expected exit 6 (network) from the handoff watchdog, got $code" >&2
   echo "(124 = timed out = the receiver orphaned: the watchdog did not fire)" >&2
   sed 's/^/  orphan.log: /' "$ROOT/orphan.log" >&2 || true
   exit 1
fi

echo "udp_recursive_ctl_test: all recursive+many control-channel checks passed"
