#!/bin/sh
# WAN-emulated gates for MSC's UDP engine via shims/wan_shim.so (privilege-free
# LD_PRELOAD emulator; delays/drops UDP only, TCP control passes clean).
#
#   smoke  4 MiB at 15ms/2% loss: byte-verify, and prove the shim actually
#          intercepted the data path (non-zero delayed and dropped).
#   mtu    the pre-data RTT probe on a path whose MTU is BELOW the PMTUD cap.
#          That combination is the precondition for a ~90 ms measurement
#          artifact this test guards against: the ladder
#          always probes the cap, the ceiling datagram is black-holed remotely,
#          so the receiver can never take its `max_seen >= highest_sent` exit
#          and waits out the full grace -- during which the sender's probe ack
#          sits unread. Drives the msc CLI, not udp_test, because only the CLI
#          applies the 9000-byte PMTUD cap setup this needs.
#   wan    64 MiB at 50ms/0.1% loss: additionally assert a throughput floor and
#          a retransmit ceiling. Those are the two signals that move when
#          loss detection is broken (rtx ~50% of units, goodput 1/100th of
#          the path); byte-verification alone sees neither, so such defects
#          can ship behind an otherwise green suite.
#          Calibration: a healthy build moves 64 MiB in ~5s (~0.11 Gbit/s)
#          with rtx 0.3-0.8% of tx.
#
# MSC_WANSHIM_SO overrides the shim path, so a current shim can gate an old
# engine build (the shim is the instrument, not the thing under test).
set -eu

profile=${1:?usage: wanshim_test.sh smoke|wan|mtu}
SHIM=${MSC_WANSHIM_SO:-./shims/wan_shim.so}

ROOT=${TMPDIR:-/tmp}/msc-wanshim.$$
trap 'rm -rf "$ROOT"' EXIT HUP INT TERM
mkdir -p "$ROOT"

# The shim summary and, for wan, the per-flow stats lines land in $1.
check_shim_counters()
{
   log=$1
   summary=$(grep -c 'wan_shim: sent=' "$log" || true)
   if [ "$summary" -ne 1 ]; then
      echo "FAIL: expected exactly 1 wan_shim summary, got $summary" >&2
      exit 1
   fi
   d=$(sed -n 's/.*delayed=\([0-9]*\).*/\1/p' "$log")
   x=$(sed -n 's/.*dropped=\([0-9]*\).*/\1/p' "$log")
   if [ "${d:-0}" -eq 0 ]; then
      echo "FAIL: shim delayed 0 datagrams -- it did not intercept the data path," >&2
      echo "      so this gate proved nothing about WAN behaviour." >&2
      exit 1
   fi
   if [ "${x:-0}" -eq 0 ]; then
      echo "FAIL: shim dropped 0 datagrams -- loss recovery was untested." >&2
      exit 1
   fi
}

case $profile in
smoke)
   head -c 4M /dev/urandom > "$ROOT/src"
   LD_PRELOAD=$SHIM MSC_UDP_WANSHIM_DELAY_MS=15 \
      MSC_UDP_WANSHIM_JITTER_MS=3 MSC_UDP_WANSHIM_LOSS_PCT=2 \
      MSC_UDP_WANSHIM_SEED=0x5eed1234 MSC_UDP_WANSHIM_TRACE=1 \
      ./udp_test "$ROOT/src" "$ROOT/out" 4 2>"$ROOT/shim.log"
   cat "$ROOT/shim.log" >&2
   cmp "$ROOT/src" "$ROOT/out"
   check_shim_counters "$ROOT/shim.log"
   d=$(sed -n 's/.*delayed=\([0-9]*\).*/\1/p' "$ROOT/shim.log")
   x=$(sed -n 's/.*dropped=\([0-9]*\).*/\1/p' "$ROOT/shim.log")
   echo "msc wanshim smoke OK (delayed=$d dropped=$x, byte-verified)"
   ;;
wan)
   # Floor/ceiling rationale: 0.02 Gbit/s is 5x below the slowest healthy run
   # observed and >4x above the broken build; 5% rtx is ~10x above healthy and
   # ~10x below broken. The 90s timeout is the backstop for a crawl so slow the
   # floor check would otherwise wait minutes to report it.
   floor_gbit=0.02
   rtx_ceiling_pct=5
   bytes=67108864
   head -c 64M /dev/urandom > "$ROOT/src"
   start=$(date +%s.%N)
   rc=0
   LD_PRELOAD=$SHIM MSC_UDP_WANSHIM_DELAY_MS=50 \
      MSC_UDP_WANSHIM_JITTER_MS=5 MSC_UDP_WANSHIM_LOSS_PCT=0.1 \
      MSC_UDP_WANSHIM_SEED=0x5eed1234 MSC_UDP_WANSHIM_TRACE=1 \
      MSC_UDP_STATS=1 \
      timeout 90 ./udp_test "$ROOT/src" "$ROOT/out" 8 2>"$ROOT/shim.log" || rc=$?
   end=$(date +%s.%N)
   grep -E 'wan_shim:|sender flow' "$ROOT/shim.log" >&2 || true
   if [ "$rc" -eq 124 ]; then
      echo "FAIL: 64 MiB at 50ms/0.1% loss did not finish inside 90s --" >&2
      echo "      the engine is crawling on a plain WAN path." >&2
      exit 1
   fi
   if [ "$rc" -ne 0 ]; then
      echo "FAIL: udp_test exited $rc" >&2
      exit 1
   fi
   cmp "$ROOT/src" "$ROOT/out"
   check_shim_counters "$ROOT/shim.log"
   gbit=$(awk -v b=$bytes -v s="$start" -v e="$end" \
      'BEGIN { printf "%.4f", b * 8 / (e - s) / 1e9 }')
   ok=$(awk -v g="$gbit" -v f=$floor_gbit 'BEGIN { print (g >= f) ? 1 : 0 }')
   if [ "$ok" -ne 1 ]; then
      echo "FAIL: goodput $gbit Gbit/s is under the $floor_gbit floor --" >&2
      echo "      a healthy engine does ~0.11 Gbit/s on this profile." >&2
      exit 1
   fi
   rtx=$(awk '/MSC UDP sender flow/ {
            for (i = 1; i < NF; i++) {
               if ($i == "tx") tx += $(i+1)
               if ($i == "retransmitted") rtx += $(i+1)
            }
         } END { print rtx+0, tx+0 }' "$ROOT/shim.log")
   rtx_units=${rtx% *}
   tx_units=${rtx#* }
   if [ "$tx_units" -eq 0 ]; then
      echo "FAIL: no per-flow stats parsed -- MSC_UDP_STATS output changed shape?" >&2
      exit 1
   fi
   over=$(awk -v r="$rtx_units" -v t="$tx_units" -v c=$rtx_ceiling_pct \
      'BEGIN { print (r * 100 > t * c) ? 1 : 0 }')
   if [ "$over" -eq 1 ]; then
      echo "FAIL: $rtx_units retransmits of $tx_units units exceeds ${rtx_ceiling_pct}% --" >&2
      echo "      the engine is misreading this path (reordering-as-loss or RTO storm)." >&2
      exit 1
   fi
   echo "msc wanshim wan OK ($gbit Gbit/s, rtx $rtx_units/$tx_units, byte-verified)"
   ;;
mtu)
   # A 25ms one-way delay (50ms RTT) on a path that black-holes anything over
   # 4000 bytes, with the PMTUD cap left at its 9000 default. The ceiling
   # candidate therefore leaves the sender and never lands -- a REMOTE loss,
   # which is the case a local EMSGSIZE check structurally cannot see.
   #
   # Asserts, in order of what they protect:
   #   1. the reported data RTT is the PATH, not our own startup. Pre-fix this
   #      reads ~90ms + delay because the probe's ack is not read until after
   #      the ladder and the receiver's grace.
   #   2. the negotiated payload still converges on the real ceiling. This is
   #      what stops a too-eager receiver exit from silently costing throughput.
   #   3. the bytes arrive.
   # 4100 sits just above the 4068 ladder rung, so the correct outcome is
   # unambiguous: 4068 fits and 8972 does not, giving payload 4020. A rung-
   # straddling MTU (e.g. 4000) would legitimately settle on 2016 and make the
   # ceiling assertion untestable.
   shim_mtu=4100
   expect_ceiling_min=3000    # an early receiver exit lands on 1968 or lower
   delay_ms=25
   rtt_ceiling_ms=70          # 50ms path + headroom; the artifact lands ~90+
   MSC=${MSC:-./msc}
   mkdir -p "$ROOT/w"
   cat > "$ROOT/w/fakessh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
   chmod +x "$ROOT/w/fakessh"
   head -c 8M /dev/urandom > "$ROOT/src"
   rc=0
   LD_PRELOAD=$SHIM MSC_UDP_WANSHIM_DELAY_MS=$delay_ms \
      MSC_UDP_WANSHIM_MTU=$shim_mtu MSC_UDP_WANSHIM_TRACE=1 \
      MSC_UDP_PMTUD_CAP=9000 MSC_UDP_STATS=1 MSC_SSH="$ROOT/w/fakessh" \
      timeout 120 "$MSC" -U -n4 --force -l localhost -r localhost -B "$MSC" \
      -i "$ROOT/src" -o "$ROOT/out" >/dev/null 2>"$ROOT/mtu.log" || rc=$?
   grep -E 'wan_shim:|initial data RTT|MSC-UDP-STATS profile=|payload ceiling' \
      "$ROOT/mtu.log" >&2 || true
   if [ "$rc" -ne 0 ]; then
      echo "FAIL: msc exited $rc on the below-cap-MTU path" >&2
      exit 1
   fi
   cmp "$ROOT/src" "$ROOT/out"

   # The shim must actually have swallowed the oversized ladder probes, or the
   # precondition never existed and the rest of this proves nothing.
   md=$(sed -n 's/.*mtu_drops=\([0-9]*\).*/\1/p' "$ROOT/mtu.log" | tail -1)
   if [ "${md:-0}" -eq 0 ]; then
      echo "FAIL: shim black-holed 0 oversized datagrams -- the below-cap-MTU" >&2
      echo "      precondition was not reproduced, so this gate is vacuous." >&2
      exit 1
   fi

   rtt=$(sed -n 's/.*initial data RTT \([0-9.]*\)ms.*/\1/p' "$ROOT/mtu.log" | tail -1)
   if [ -z "$rtt" ]; then
      echo "FAIL: no 'initial data RTT' line -- the probe never produced a sample" >&2
      exit 1
   fi
   over=$(awk -v r="$rtt" -v c=$rtt_ceiling_ms 'BEGIN { print (r > c) ? 1 : 0 }')
   if [ "$over" -eq 1 ]; then
      echo "FAIL: initial data RTT ${rtt}ms exceeds ${rtt_ceiling_ms}ms on a" >&2
      echo "      ${delay_ms}ms-each-way path. The sample is measuring msc's own" >&2
      echo "      pre-data pipeline (PMTUD ladder + receiver grace), not the" >&2
      echo "      network." >&2
      exit 1
   fi

   # The ceiling must still be discovered: below the black hole, but not so low
   # that an early receiver exit cost us the real MTU.
   ceil=$(sed -n 's/.*payload ceiling \([0-9]*\).*/\1/p' "$ROOT/mtu.log" | tail -1)
   if [ -z "$ceil" ]; then
      echo "FAIL: no 'payload ceiling' line -- PMTUD did not complete" >&2
      exit 1
   fi
   bad=$(awk -v c="$ceil" -v m=$shim_mtu -v lo=$expect_ceiling_min \
      'BEGIN { print (c > m || c < lo) ? 1 : 0 }')
   if [ "$bad" -eq 1 ]; then
      echo "FAIL: negotiated payload ceiling $ceil is not consistent with a" >&2
      echo "      ${shim_mtu}-byte path (expected >= $expect_ceiling_min). A ceiling near 1968" >&2
      echo "      means the receiver stopped draining before the 4068-byte rung" >&2
      echo "      landed, which silently costs payload on every transfer." >&2
      exit 1
   fi
   echo "msc wanshim mtu OK (data RTT ${rtt}ms <= ${rtt_ceiling_ms}ms, ceiling ${ceil:-n/a}, mtu_drops=$md, byte-verified)"
   ;;
*)
   echo "usage: wanshim_test.sh smoke|wan|mtu" >&2
   exit 2
   ;;
esac
