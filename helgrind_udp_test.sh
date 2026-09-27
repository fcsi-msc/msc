#!/bin/sh
# Observe telemetry and shared-socket fallback with a race detector.
set -eu
work=$(mktemp -d "${TMPDIR:-/tmp}/msc-helgrind.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM
head -c 2097152 /dev/urandom > "$work/source"
for mode in direct shared; do
   if [ "$mode" = shared ]; then
      export MSC_UDP_PORTS=1 LD_PRELOAD="$PWD/udp_offload_shim.so"
   fi
   MSC_UDP_STATS=1 MSC_TEST_STALL_TIMEOUT_MS=120000 \
      timeout 180 "${VALGRIND:-valgrind}" --tool=helgrind --error-exitcode=99 \
      --suppressions="$PWD/valgrind.supp" \
      --log-file="$work/$mode.%p.log" ./udp_test "$work/source" "$work/$mode" 2 \
      >"$work/$mode.transfer" 2>&1 && rc=0 || rc=$?
   unset LD_PRELOAD MSC_UDP_PORTS
   cat "$work/$mode.transfer"
   cat "$work/$mode."*.log
   test "$rc" -eq 0
   cmp "$work/source" "$work/$mode"
done
