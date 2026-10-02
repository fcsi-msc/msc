#!/bin/sh
set -eu

ROOT=${TMPDIR:-/tmp}/msc-udp-parity.$$
trap 'rm -rf "$ROOT"' EXIT HUP INT TERM
mkdir -p "$ROOT"
dd if=/dev/urandom of="$ROOT/input.bin" bs=1M count=2 status=none

run_file()
{
   name=$1
   shift
   env "$@" ./build/udp_test "$ROOT/input.bin" "$ROOT/$name.bin" 4
   cmp "$ROOT/input.bin" "$ROOT/$name.bin"
}

run_file clean
./build/udp_test "$ROOT/input.bin" "$ROOT/loss.bin" 4 10
cmp "$ROOT/input.bin" "$ROOT/loss.bin"

if MSC_TEST_STALL_TIMEOUT_MS=200 timeout 5 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/stalled.bin" 4 100; then
   echo "100% UDP loss unexpectedly outlived the stall timeout" >&2
   exit 1
fi

MSC_TEST_SRC_OFFSET=262144 MSC_TEST_DST_OFFSET=524288 MSC_TEST_LENGTH=524288 \
./build/udp_test "$ROOT/input.bin" "$ROOT/offset.out" 4
dd if="$ROOT/input.bin" of="$ROOT/offset.src" bs=1 skip=262144 count=524288 status=none
dd if="$ROOT/offset.out" of="$ROOT/offset.dst" bs=1 skip=524288 count=524288 status=none
cmp "$ROOT/offset.src" "$ROOT/offset.dst"
test "$(od -An -tu1 -N1 "$ROOT/offset.out")" -eq 0

mkdir -p "$ROOT/tree/a/b" "$ROOT/tree/emptydir"
dd if=/dev/urandom of="$ROOT/tree/root.bin" bs=64K count=4 status=none
dd if=/dev/urandom of="$ROOT/tree/a/b/deep.bin" bs=32K count=3 status=none
: > "$ROOT/tree/a/empty"
chmod 640 "$ROOT/tree/root.bin"
chmod 750 "$ROOT/tree/a"
./build/udp_test "$ROOT/tree" "$ROOT/tree.out" 4
diff -r "$ROOT/tree" "$ROOT/tree.out"

# A manifest bigger than the soft fd limit: msc must raise its own soft
# limit toward the hard cap (MSC UDP's table API opens every file up front;
# a 1024 soft default kills 12k-file trees with EMFILE).
mkdir -p "$ROOT/manytree"
i=1; while [ $i -le 300 ]; do printf x > "$ROOT/manytree/f$i"; i=$((i+1)); done
# shellcheck disable=SC3045 # ulimit -S: supported by dash, bash, and busybox sh
( ulimit -S -n 128 && exec ./build/udp_test "$ROOT/manytree" "$ROOT/manytree.out" 2 )
diff -r "$ROOT/manytree" "$ROOT/manytree.out"

dd if=/dev/zero of="$ROOT/slice.out" bs=1M count=4 status=none
MSC_TEST_MULTI=1 MSC_TEST_SRC_OFFSET=524288 MSC_TEST_DST_OFFSET=2097152 \
MSC_TEST_LENGTH=1048576 ./build/udp_test "$ROOT/input.bin" "$ROOT/slice.out" 4
dd if="$ROOT/input.bin" of="$ROOT/slice.src" bs=1 skip=524288 count=1048576 status=none
dd if="$ROOT/slice.out" of="$ROOT/slice.dst" bs=1 skip=2097152 count=1048576 status=none
cmp "$ROOT/slice.src" "$ROOT/slice.dst"
test "$(od -An -tu1 -N1 "$ROOT/slice.out")" -eq 0
test "$(od -An -tu1 -j 3145728 -N1 "$ROOT/slice.out")" -eq 0

printf keep > "$ROOT/existing.bin"
if MSC_TEST_NO_FORCE=1 ./build/udp_test "$ROOT/input.bin" "$ROOT/existing.bin" 2; then
   echo "no-force publication unexpectedly succeeded" >&2
   exit 1
fi
test "$(cat "$ROOT/existing.bin")" = keep
if MSC_UDP_RX_NOWRITE=1 ./build/udp_test "$ROOT/input.bin" "$ROOT/corrupt.bin" 2; then
   echo "checksum mismatch unexpectedly published" >&2
   exit 1
fi
test ! -e "$ROOT/corrupt.bin"
test -z "$(find "$ROOT" -name '*.msc-udptmp.*' -print -quit)"

run_file rate MSC_UDP_CC=rate MSC_UDP_STATS=1
run_file cubic MSC_UDP_CC=cubic MSC_UDP_STATS=1
run_file nopmtud MSC_UDP_PMTUD=0
run_file nooffload MSC_TEST_NO_GSO=1 MSC_TEST_NO_GRO=1
run_file pwrite MSC_UDP_NO_MMAP=1
run_file mmap MSC_UDP_FORCE_MMAP=1
run_file nochecksum MSC_TEST_NO_CHECKSUM=1

# Implicit -s must auto-raise the payload to the PMTUD-probed ceiling (msc's
# MSC_UDP_PMTUD_CAP default of 9000 confirms on loopback, collapsing per-flow unit
# counts to ~59 for 512 KiB/flow); an explicit payload and a lowered cap must
# both be honored instead (~375 units/flow at 1400). The two ends' stats lines
# can interleave mid-line on shared stderr, so a strict per-line match plus a
# minimum match count keeps this robust.
flow_units()
{
   name=$1
   shift
   env "$@" MSC_UDP_STATS=1 ./build/udp_test "$ROOT/input.bin" "$ROOT/$name.bin" 4 \
      2> "$ROOT/$name.stats" > /dev/null
   cmp "$ROOT/input.bin" "$ROOT/$name.bin"
   sed -n 's/.*flow [0-9][0-9]*: units \([0-9][0-9]*\) .*/\1/p' \
      "$ROOT/$name.stats" | sort -n > "$ROOT/$name.units"
   test "$(wc -l < "$ROOT/$name.units")" -ge 2
}
flow_units autoraise
test "$(head -1 "$ROOT/autoraise.units")" -lt 100
flow_units explicit1400 MSC_UDP_PAYLOAD=1400
test "$(head -1 "$ROOT/explicit1400.units")" -gt 300
flow_units lowcap MSC_UDP_PMTUD_CAP=1400
test "$(head -1 "$ROOT/lowcap.units")" -gt 300

# Resume with an implicit -s across sessions whose path ceilings differ: the
# first session picks a jumbo payload (default cap), is interrupted after one
# durable 512 KiB checkpoint, and the retry runs under a 1400-byte cap -- each
# session must re-pick its own payload with the checkpoint still reconciling.
# The receiver's test interruption strands the sender mid-chunk, so a watchdog
# bounds the test; the checkpoint is already durable either way.
if MSC_TEST_CHECKPOINT="$ROOT/resume.ckpt" MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS=1 \
   MSC_UDP_CHECKPOINT_BYTES=524288 \
   timeout 30 ./build/udp_test "$ROOT/input.bin" "$ROOT/resume.bin" 4; then
   echo "interrupted resumable transfer unexpectedly succeeded" >&2
   exit 1
fi
test -e "$ROOT/resume.ckpt"
MSC_TEST_CHECKPOINT="$ROOT/resume.ckpt" MSC_TEST_RESUME=1 MSC_UDP_PMTUD_CAP=1400 \
MSC_UDP_CHECKPOINT_BYTES=524288 ./build/udp_test "$ROOT/input.bin" "$ROOT/resume.bin" 4
cmp "$ROOT/input.bin" "$ROOT/resume.bin"
test ! -e "$ROOT/resume.ckpt"
test ! -e "$ROOT/resume.bin.msc-part"

# A kernel without UDP_SEGMENT/UDP_GRO (the shim fails both syscalls): GSO
# must log a single fallback and finish via sendmmsg, GRO must silently
# no-op, and the bytes must still be correct.
LD_PRELOAD="$PWD/build/udp_offload_shim.so" \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/noffload.bin" 4 2> "$ROOT/noffload.stats"
cmp "$ROOT/input.bin" "$ROOT/noffload.bin"
grep -q "falling back to sendmmsg" "$ROOT/noffload.stats"
test "$(grep -c "falling back to sendmmsg" "$ROOT/noffload.stats")" -eq 1

# --- data-port selection ---------------------------------------------------
# Every transfer names the ports it is actually using, on stderr from both ends
# (stdout may be the transferred file itself). The receiver picks; the sender
# verifies the advertised list against its own copy of the request.
expand_ports()
{
   tr ',' '\n' | while read -r item; do
      case $item in
         *-*) seq "${item%-*}" "${item#*-}" ;;
         *)   echo "$item" ;;
      esac
   done
}

# reported_ports <errfile> <sender|receiver> -> one port per line
reported_ports()
{
   sed -n "s/^MSC UDP $2: .*: \([0-9,-]*\) (.*/\1/p" "$1" | head -1 | expand_ports
}

# in_window <first> <last> < portlist
in_window()
{
   while read -r p; do
      if [ "$p" -lt "$1" ] || [ "$p" -gt "$2" ]; then
         echo "port $p outside the expected window $1-$2" >&2
         exit 1
      fi
   done
}

squat()   # squat <port>... ; sets SQUAT_PID
{
   : > "$ROOT/squat.out"
   ./build/port_squat "$@" > "$ROOT/squat.out" &
   SQUAT_PID=$!
   tries=0
   while [ ! -s "$ROOT/squat.out" ] && [ $tries -lt 500 ]; do
      tries=$((tries + 1)); sleep 0.01
   done
   if ! grep -q ready "$ROOT/squat.out" 2>/dev/null; then
      kill "$SQUAT_PID" 2>/dev/null || true
      echo "port_squat could not take $*; pick a different test window" >&2
      exit 1
   fi
}
unsquat() { kill "$SQUAT_PID" 2>/dev/null || true; wait "$SQUAT_PID" 2>/dev/null || true; }

# Default: K = min(flows, 8) ports scanned from 17400, window 4*K.
./build/udp_test "$ROOT/input.bin" "$ROOT/ports.bin" 4 \
   2> "$ROOT/ports.err" > "$ROOT/ports.out"
cmp "$ROOT/input.bin" "$ROOT/ports.bin"
grep -q "^MSC UDP receiver: 4 data ports: " "$ROOT/ports.err"
grep -q "^MSC UDP sender: 4 data ports on 127.0.0.1: " "$ROOT/ports.err"
reported_ports "$ROOT/ports.err" receiver | in_window 17400 17415
reported_ports "$ROOT/ports.err" sender | in_window 17400 17415
test "$(reported_ports "$ROOT/ports.err" receiver | wc -l)" -eq 4
# stderr, never stdout: stdout may be the transferred file itself (-o -)
! grep -q "data port" "$ROOT/ports.out"
# both ends must name the SAME ports -- the sender's line is what a user reads
# to write a firewall rule for the receiver
reported_ports "$ROOT/ports.err" receiver > "$ROOT/ports.rx"
reported_ports "$ROOT/ports.err" sender > "$ROOT/ports.tx"
cmp "$ROOT/ports.rx" "$ROOT/ports.tx"

# K below the flow count: 8 flows over 2 sockets, with the mapping spelled out.
MSC_UDP_PORTS=2 ./build/udp_test "$ROOT/input.bin" "$ROOT/k2.bin" 8 2> "$ROOT/k2.err"
cmp "$ROOT/input.bin" "$ROOT/k2.bin"
grep -q "^MSC UDP receiver: 2 data ports: .* (8 flows, flow f -> socket f mod 2)" "$ROOT/k2.err"
test "$(reported_ports "$ROOT/k2.err" receiver | wc -l)" -eq 2

# Exact ports, honored verbatim and in order.
MSC_UDP_PORT_LIST=21500,21502-21504 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/list.bin" 4 2> "$ROOT/list.err"
cmp "$ROOT/input.bin" "$ROOT/list.bin"
test "$(reported_ports "$ROOT/list.err" receiver | tr '\n' ' ')" = "21500 21502 21503 21504 "

# Exact ports, one of them busy: refuse to start, name the busy port and only
# it, and leave nothing bound.
squat 21502
if MSC_UDP_PORT_LIST=21500,21502-21504 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/busy.bin" 4 2> "$ROOT/busy.err"; then
   unsquat
   echo "a busy exact port list unexpectedly transferred" >&2
   exit 1
fi
unsquat
grep -q "requested data port 21502 is in use by another transfer" "$ROOT/busy.err"
! grep -q "21500" "$ROOT/busy.err"
test ! -e "$ROOT/busy.bin"

# Scanning steps over a busy port instead of failing -- this is what lets two
# users transfer at once without agreeing on anything.
squat 21600 21601
MSC_UDP_PORT_BASE=21600 ./build/udp_test "$ROOT/input.bin" "$ROOT/scan.bin" 4 \
   2> "$ROOT/scan.err"
unsquat
cmp "$ROOT/input.bin" "$ROOT/scan.bin"
test "$(reported_ports "$ROOT/scan.err" receiver | tr '\n' ' ')" = "21602 21603 21604 21605 "

# ... but a window with too few free ports fails, naming the window.
squat 21701
if MSC_UDP_PORT_BASE=21700 MSC_UDP_PORT_SPAN=4 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/narrow.bin" 4 2> "$ROOT/narrow.err"; then
   unsquat
   echo "an exhausted port window unexpectedly transferred" >&2
   exit 1
fi
unsquat
grep -q "could not find 4 free data ports in 21700-21703" "$ROOT/narrow.err"

# span 0 is the exact-block escape hatch (the pre-scan --udp-port-base contract).
MSC_UDP_PORT_BASE=21800 MSC_UDP_PORT_SPAN=0 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/block.bin" 4 2> "$ROOT/block.err"
cmp "$ROOT/input.bin" "$ROOT/block.bin"
test "$(reported_ports "$ROOT/block.err" receiver | tr '\n' ' ')" = "21800 21801 21802 21803 "
squat 21802
if MSC_UDP_PORT_BASE=21800 MSC_UDP_PORT_SPAN=0 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/block2.bin" 4 2> "$ROOT/block2.err"; then
   unsquat
   echo "a busy exact block unexpectedly transferred" >&2
   exit 1
fi
unsquat
grep -q "requested data port 21802 is in use by another transfer" "$ROOT/block2.err"

# A malformed request is refused, not silently defaulted.
if MSC_UDP_PORT_LIST=21900,21900 \
   ./build/udp_test "$ROOT/input.bin" "$ROOT/dup.bin" 2 2> "$ROOT/dup.err"; then
   echo "a duplicated port list unexpectedly transferred" >&2
   exit 1
fi
grep -q "port 21900 appears twice" "$ROOT/dup.err"

# Concurrent transfers in one window must not collide: exclusive UDP binds are
# the whole isolation mechanism, so three at once must take disjoint ports and
# all complete.
for i in 1 2 3; do
   ( ./build/udp_test "$ROOT/input.bin" "$ROOT/conc$i.bin" 4 2> "$ROOT/conc$i.err" ) &
done
wait
for i in 1 2 3; do
   cmp "$ROOT/input.bin" "$ROOT/conc$i.bin"
   reported_ports "$ROOT/conc$i.err" receiver > "$ROOT/conc$i.ports"
   test "$(wc -l < "$ROOT/conc$i.ports")" -eq 4
done
cat "$ROOT/conc1.ports" "$ROOT/conc2.ports" "$ROOT/conc3.ports" > "$ROOT/conc.all"
test "$(sort "$ROOT/conc.all" | wc -l)" -eq "$(sort -u "$ROOT/conc.all" | wc -l)"

# Once on the default control mode, once on tcp: the version check itself is
# mode-independent, but the rendezvous it rides is not, and everything above
# this line now runs on the default only.
./build/udp_protocol_test
MSC_UDP_CTL=tcp ./build/udp_protocol_test

echo "MSC UDP parity tests passed"
