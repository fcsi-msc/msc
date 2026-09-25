#!/bin/sh
# env_preset_test.sh - the MSC_UDP_PROFILE / --env preset contract.
#
# Pins the measured preset VALUES so a later edit to apply_profile_def() cannot silently change what
# ships. Asserts against the real msc binary and its MSC_UDP_STATS output rather
# than a reimplementation, so it also exercises the env-forwarding and RTT-band
# selection the engine actually uses.
#
# wan_shim delays UDP only, which is what lets a loopback run reach a chosen RTT
# band without a second host. No shim -> skip (announced), like wanshim_test.sh.
set -eu

MSC="${MSC:-./msc}"
SHIM="${MSC_WANSHIM_SO:-./shims/wan_shim.so}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [ ! -f "$SHIM" ]; then
   echo "env preset test: SKIP (no $SHIM; make shims/wan_shim.so)"
   exit 0
fi

cat > "$work/fakessh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
chmod +x "$work/fakessh"
head -c 4194304 /dev/urandom > "$work/src"

# Run one transfer and echo the guard line the engine prints under
# MSC_UDP_STATS ("<preset> guard init_cwnd N pace gain G burst B ...") plus the
# selected "profile=NAME" line. $1 is extra env; the rest is delay ms.
run() {  # env-assignments delay_ms
   env_pre="$1"; delay="$2"
   rm -f "$work/out"
   # shellcheck disable=SC2086
   env MSC_SSH="$work/fakessh" MSC_UDP_STATS=1 \
       LD_PRELOAD="$SHIM" MSC_UDP_WANSHIM_DELAY_MS="$delay" \
       $env_pre \
       timeout 120 "$MSC" -U -n4 --force -l localhost -r localhost -B "$MSC" \
       -i "$work/src" -o "$work/out" >/dev/null 2>"$work/log" || true
   cmp -s "$work/src" "$work/out" || { echo "FAIL: transfer corrupted ($env_pre d=$delay)"; exit 1; }
}

# field N of the last "<preset> guard ..." line (key value pairs)
guard_val() { sed -n 's/.* guard //p' "$work/log" | tail -1 | \
   sed -n "s/.*$1 \([0-9][0-9.]*\).*/\1/p"; }
selected() { sed -n 's/.*MSC-UDP-STATS profile=\([a-z-]*\) .*/\1/p' "$work/log" | tail -1; }

fail=0
expect() {  # label actual expected
   if [ "$2" != "$3" ]; then echo "FAIL: $1 = '$2', want '$3'"; fail=1
   else echo "ok: $1 = $2"; fi
}

# --- the two shipped tables (FINDINGS.md sections 7-8) --------------------------
# wan: burst 64, init_cwnd 4. Forced, so the value is asserted regardless of RTT.
run "MSC_UDP_PROFILE=wan" 25
expect "wan burst"      "$(guard_val burst)"     "64"
expect "wan init_cwnd"  "$(guard_val init_cwnd)" "4"

# geo: burst 256, init_cwnd 4.
run "MSC_UDP_PROFILE=geo" 25
expect "geo burst"      "$(guard_val burst)"     "256"

# wan-long: burst 256, init_cwnd 32, and the delay-based startup exit ON.
# init_cwnd is the point of this
# table -- the WAN presets ship 4, which is BELOW the 16-unit LAN default on
# exactly the paths with the largest bandwidth-delay product.
run "MSC_UDP_PROFILE=wan-long" 25
expect "wan-long burst"      "$(guard_val burst)"              "256"
expect "wan-long init_cwnd"  "$(guard_val init_cwnd)"          "32"
expect "wan-long startupq"   "$(guard_val startup_queue_mult)" "1.50"

# The delay exit is overridable OFF, like MSC_UDP_PACE=0 pins pacing off.
run "MSC_UDP_PROFILE=wan-long MSC_UDP_STARTUP_QUEUE_MULT=0" 25
expect "startup_queue_mult pinned off" "$(guard_val startup_queue_mult)" "0.00"

# STARTUP re-entry is ON for every long-path preset and OFF for LAN. That split
# is load-bearing, not cosmetic: measured on IPoIB (0.15 ms, ~28 Gbit/s)
# the mechanism fired ~25 times per transfer, bought nothing -- PROBE_BW's
# recovery there takes about a millisecond -- and widened the low tail.
run "MSC_UDP_PROFILE=wan" 25
expect "wan bw_restart"      "$(guard_val bw_restart)" "1"
run "MSC_UDP_PROFILE=wan-long" 25
expect "wan-long bw_restart" "$(guard_val bw_restart)" "1"
run "MSC_UDP_PROFILE=geo" 25
expect "geo bw_restart"      "$(guard_val bw_restart)" "1"
run "MSC_UDP_PROFILE=wan-long MSC_UDP_BW_RESTART=0" 25
expect "bw_restart pinned off" "$(guard_val bw_restart)" "0"

# --env is just MSC_UDP_PROFILE by another name; fiber shares the wan table.
run "" 25   # baseline (no env) reaches wan by RTT below; here assert the flag path
rm -f "$work/out"
env MSC_SSH="$work/fakessh" MSC_UDP_STATS=1 LD_PRELOAD="$SHIM" \
    MSC_UDP_WANSHIM_DELAY_MS=25 timeout 120 "$MSC" -U -n4 --env fiber --force \
    -l localhost -r localhost -B "$MSC" -i "$work/src" -o "$work/out" \
    >/dev/null 2>"$work/log" || true
cmp -s "$work/src" "$work/out" || { echo "FAIL: --env fiber transfer corrupted"; exit 1; }
expect "--env fiber -> wan burst" "$(guard_val burst)" "64"

# --- auto selects by RTT band --------------------------------------------------
# 25 ms one-way ~= 50 ms path -> wan; 300 ms ~= 600 ms path -> geo.
run "MSC_UDP_PROFILE=auto" 25
expect "auto @50ms -> profile"  "$(selected)" "wan"
run "MSC_UDP_PROFILE=auto" 300
expect "auto @600ms -> profile" "$(selected)" "geo"
# 75 ms one-way ~= 150 ms path -> wan-long.  The geo threshold is 450 ms, so a
# 300 ms path is terrestrial and must NOT land on the satellite preset.
run "MSC_UDP_PROFILE=auto" 75
expect "auto @150ms -> profile" "$(selected)" "wan-long"
run "MSC_UDP_PROFILE=auto" 150
expect "auto @300ms -> profile" "$(selected)" "wan-long"

# --- explicit knob still beats the preset (the A/B-harness contract) -----------
run "MSC_UDP_PROFILE=geo MSC_UDP_PACE_BURST=8" 25
expect "explicit burst wins over geo" "$(guard_val burst)" "8"

# --- a bad name is a CLI error, not a silent fallback --------------------------
if MSC_SSH="$work/fakessh" "$MSC" -U -n4 --env bogus -l localhost -r localhost \
      -i "$work/src" -o "$work/x" >/dev/null 2>&1; then
   echo "FAIL: --env bogus was accepted"; fail=1
else
   echo "ok: --env bogus rejected"
fi

[ "$fail" -eq 0 ] || { echo "env preset tests FAILED"; exit 1; }
echo "env preset tests passed"
