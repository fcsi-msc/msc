#!/bin/bash
# msc-slurm-smoke.sh - two-node smoke test of msc on a Slurm cluster, with the
# remote side started by srun (msc-srun, beside this script) instead of ssh, so
# it also works where compute nodes cannot ssh to each other.
#
# Build msc first (make -j all in the repository root), then run this from
# inside a two-node allocation; the shell lands on the first node, which sends:
#
#   salloc -N2 --exclusive -t 1:00:00          # add -p/-w/--mem as your site needs
#   bash contrib/slurm/msc-slurm-smoke.sh
#
# It creates test data in /dev/shm on both nodes (about 6x SIZE_MB on the
# receiver at its peak), runs a dozen transfers -- UDP and TCP, single files and
# trees, interrupt and resume, stdin and -c pipes, exit codes -- verifies each
# by checksum on both nodes, prints PASS/FAIL and throughput, and removes the
# test data.  The report and per-test logs are kept under $OUT.
#
# Settings (environment):
#   MSC_DIR   directory holding the built msc  (default: the repository root)
#   SIZE_MB   size of the large test file       (default: 2048)
#   B_IP      the receiver's data address       (default: its InfiniBand IPv4
#             address if it has one, else its node name)
#   OUT       where to keep the results         (default: ~/msc-results/<time>)
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
MSC_DIR=${MSC_DIR:-$(cd "$HERE/../.." && pwd)}
MSC=$MSC_DIR/msc
SIZE_MB=${SIZE_MB:-2048}
OUT=${OUT:-$HOME/msc-results/$(date +%Y%m%d-%H%M%S)}
SRC=/dev/shm/msc-src      # on the sending node (this one)
DST=/dev/shm/msc-dst      # on the receiving node
mkdir -p "$OUT"
REPORT=$OUT/report.txt
: > "$REPORT"

say() { echo "$*" | tee -a "$REPORT"; }
die() { say "STOP: $*"; exit 1; }

[ -n "${SLURM_JOB_NODELIST:-}" ] || die "run this inside a two-node salloc allocation"
[ -x "$MSC" ] || die "no msc binary at $MSC -- build it first: cd $MSC_DIR && make -j all"

A=${SLURMD_NODENAME:-$(hostname -s)}
B=""
for n in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
   [ "$n" != "$A" ] && B=$n
done
[ -n "$B" ] || die "no second node in allocation $SLURM_JOB_NODELIST"

onB() { srun --overlap -N1 -n1 -w "$B" "$@"; }   # run one command on node B

if [ -z "${B_IP:-}" ]; then
   B_IP=$(onB sh -c "ip -4 -o addr show | awk '\$2 ~ /^ib/ {split(\$4, a, \"/\"); print a[1]; exit}'")
fi
[ -n "$B_IP" ] || B_IP=$B   # no InfiniBand address: dial the node by name

# msc dials B_IP; msc-srun starts the receiver on node B under srun
[ -x "$HERE/msc-srun" ] || die "msc-srun is missing from $HERE"
export MSC_SSH=$HERE/msc-srun MSC_SRUN_NODE=$B

cleanup() { rm -rf "$SRC"; onB rm -rf "$DST" 2>/dev/null; }
trap cleanup EXIT

say "== environment"
say "sender:    $A ($(hostname -I 2>/dev/null))"
say "receiver:  $B (data address $B_IP)"
say "msc:       $("$MSC" --version 2>&1)   kernel $(uname -r)"
say "socket buffer max (rmem/wmem) on $A: $(cat /proc/sys/net/core/rmem_max) / $(cat /proc/sys/net/core/wmem_max)"
say "socket buffer max (rmem/wmem) on $B: $(onB cat /proc/sys/net/core/rmem_max /proc/sys/net/core/wmem_max | tr '\n' ' ')"

# The stand-in must run a command on B with stdin forwarded, or nothing works.
got=$(printf 'ping\n' | "$MSC_SSH" -o BatchMode=yes "$B_IP" 'cat; hostname -s' 2>&1)
say "srun stand-in check: $(echo "$got" | tr '\n' ' ')"
case "$got" in
   ping*) ;;
   *) die "the srun stand-in could not run a command on $B with stdin forwarded" ;;
esac

# ---- test data ----------------------------------------------------------------
say "== creating ${SIZE_MB} MiB file and a small directory tree in $SRC"
rm -rf "$SRC"
mkdir -p "$SRC/tree/sub/deep" "$SRC/tree/emptydir"
head -c $((SIZE_MB * 1048576)) /dev/urandom > "$SRC/big.bin"
head -c 1048576 /dev/urandom > "$SRC/small.bin"
for i in $(seq 1 60); do
   head -c $(( (RANDOM % 4096 + 1) * 1024 )) /dev/urandom > "$SRC/tree/f$i"
done
for i in $(seq 1 20); do
   head -c $(( RANDOM * 8 )) /dev/urandom > "$SRC/tree/sub/deep/g$i"
done
: > "$SRC/tree/empty"
ln -s ../f1 "$SRC/tree/sub/link"
ln "$SRC/tree/f2" "$SRC/tree/sub/hardlink"
BIG_SUM=$(sha256sum "$SRC/big.bin" | cut -d' ' -f1)
SMALL_SUM=$(sha256sum "$SRC/small.bin" | cut -d' ' -f1)
onB rm -rf "$DST"
onB mkdir -p "$DST"

# A tree fingerprint: file contents, symlink targets, and directory names.
MANIFEST='cd "$1" && { find . -type f -print0 | sort -z | xargs -0 -r sha256sum;
          find . -type l -printf "%p -> %l\n" | sort; find . -type d | sort; }'
tree_sum_here() { sh -c "$MANIFEST" _ "$1" | sha256sum | cut -d' ' -f1; }
tree_sum_B()    { onB sh -c "$MANIFEST" _ "$1" | sha256sum | cut -d' ' -f1; }
file_sum_B()    { onB sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }
TREE_SUM=$(tree_sum_here "$SRC/tree")

# ---- helpers --------------------------------------------------------------------
PASS=0; FAIL=0; SKIP=0
record() {  # name, PASS|FAIL|SKIP, detail
   say "$(printf '  %-22s %-4s  %s' "$1" "$2" "$3")"
   case $2 in PASS) PASS=$((PASS + 1)) ;; FAIL) FAIL=$((FAIL + 1)) ;; *) SKIP=$((SKIP + 1)) ;; esac
}
ENVS=()     # extra environment for the next msc_run
msc_run() { # name [msc args...]; stdin passes through; sets RC and RATE
   local name=$1; shift
   env ${ENVS[@]+"${ENVS[@]}"} timeout 900 "$MSC" -l localhost -r "$B_IP" -B "$MSC" "$@" \
      > "$OUT/$name.out" 2> "$OUT/$name.err"
   RC=$?
   RATE=$(awk '/^msc transfer/ {printf "%.2f Gbit/s", $7 * 8 / 1e9}' "$OUT/$name.out")
   ENVS=()
}
verify() {  # name, wanted exit, actual checksum, expected checksum, note
   if [ "$RC" -ne "$2" ]; then
      record "$1" FAIL "exit $RC, want $2: $(tail -2 "$OUT/$1.err" | tr '\n' ' ')"
   elif [ -n "$4" ] && [ "$3" != "$4" ]; then
      record "$1" FAIL "exit $RC but the data differs from the source"
   else
      record "$1" PASS "${RATE:-} $5"
   fi
}

# ---- tests ------------------------------------------------------------------------
say "== tests (logs: $OUT/<name>.out and .err)"

ENVS=(MSC_UDP_STATS=1)
msc_run udp_file -i "$SRC/big.bin" -o "$DST/udp.bin"
verify udp_file 0 "$(file_sum_B "$DST/udp.bin")" "$BIG_SUM" \
   "$(grep -m1 -o 'profile=[a-z-]* rtt_ms=[0-9.]*' "$OUT/udp_file.err")"

msc_run tcp_file -T -i "$SRC/big.bin" -o "$DST/tcp.bin"
verify tcp_file 0 "$(file_sum_B "$DST/tcp.bin")" "$BIG_SUM" ""

msc_run udp_tree -R -i "$SRC/tree" -o "$DST/tree_udp"
verify udp_tree 0 "$(tree_sum_B "$DST/tree_udp")" "$TREE_SUM" ""

msc_run tcp_tree -T -R -i "$SRC/tree" -o "$DST/tree_tcp"
verify tcp_tree 0 "$(tree_sum_B "$DST/tree_tcp")" "$TREE_SUM" ""

# Checkpoint and resume: pace the first run to about 20 s so it can be
# interrupted part way, then resume it at full speed.
PACE=$(( SIZE_MB * 8 / 20 )); [ "$PACE" -ge 1 ] || PACE=1
env MSC_UDP_PACE_RATE_MBIT=$PACE "$MSC" -l localhost -r "$B_IP" -B "$MSC" \
   --checkpoint "$DST/resume.cp" -i "$SRC/big.bin" -o "$DST/resume.bin" \
   > "$OUT/resume_interrupt.out" 2> "$OUT/resume_interrupt.err" &
pid=$!
sleep 6
kill -TERM "$pid" 2>/dev/null
wait "$pid"; RC=$?; RATE=""
sleep 3   # let the receiver side notice and exit
if [ "$RC" -eq 0 ]; then
   record resume_interrupt SKIP "the paced transfer finished before it could be interrupted"
   record resume_finish SKIP "nothing to resume"
else
   verify resume_interrupt 143 "" "" "(SIGTERM -> 143, checkpoint kept)"
   msc_run resume_finish --resume --checkpoint "$DST/resume.cp" -i "$SRC/big.bin" -o "$DST/resume.bin"
   verify resume_finish 0 "$(file_sum_B "$DST/resume.bin")" "$BIG_SUM" \
      "$(grep -m1 -oiE '[0-9]+ reused|reused [0-9]+' "$OUT/resume_finish.out" "$OUT/resume_finish.err" | head -1)"
fi

msc_run pipe_stdin -c "cat > $DST/pipe.bin" < "$SRC/big.bin"
verify pipe_stdin 0 "$(file_sum_B "$DST/pipe.bin")" "$BIG_SUM" "(stdin -> -c, TCP)"

tar cf - -C "$SRC" tree | {
   msc_run tar_pipe -c "mkdir -p $DST/tarout && tar xf - -C $DST/tarout"
   verify tar_pipe 0 "$(tree_sum_B "$DST/tarout/tree")" "$TREE_SUM" "(tar | msc -c 'tar x')"
   echo "$PASS $FAIL $SKIP" > "$OUT/.counts"
}
read -r PASS FAIL SKIP < "$OUT/.counts"

msc_run cmd_fail -i "$SRC/small.bin" -c "cat > /dev/null; exit 3"
verify cmd_fail 4 "" "" "(a failing -c command -> exit 4)"

msc_run exists_refused -i "$SRC/small.bin" -o "$DST/udp.bin"
verify exists_refused 4 "" "" "(existing destination without --force -> exit 4)"
msc_run exists_force --force -i "$SRC/small.bin" -o "$DST/udp.bin"
verify exists_force 0 "$(file_sum_B "$DST/udp.bin")" "$SMALL_SUM" "(--force replaces it)"

ENVS=(MSC_UDP_CTL=tcp)
msc_run udp_ctl_tcp -i "$SRC/big.bin" -o "$DST/ctltcp.bin"
verify udp_ctl_tcp 0 "$(file_sum_B "$DST/ctltcp.bin")" "$BIG_SUM" "(UDP data, TCP control channel)"

# ---- throughput (information only) -------------------------------------------------
say "== throughput, ${SIZE_MB} MiB memory to memory"
for args in "-n 1" "-n 8" "" "-T -n 8" "-T"; do
   onB rm -f "$DST/perf.bin"
   # shellcheck disable=SC2086 # $args is a list of msc options
   msc_run perf $args -i "$SRC/big.bin" -o "$DST/perf.bin"
   say "$(printf '  %-12s exit %-3s %s' "${args:-default}" "$RC" "${RATE:-?}")"
done

left=$(onB sh -c 'pgrep -u "$(id -u)" -x msc || true' | tr '\n' ' ')
[ -n "$left" ] && say "NOTE: msc processes still running on $B afterwards: $left"
say "== $PASS passed, $FAIL failed, $SKIP skipped; full logs in $OUT"
[ "$FAIL" -eq 0 ]
