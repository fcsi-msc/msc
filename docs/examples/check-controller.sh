#!/bin/sh
# Run from the repository root after `make udp_test`.
set -eu
controller=${1:?usage: sh docs/examples/check-controller.sh CONTROLLER}
case "$controller" in
  *[!a-zA-Z0-9_-]*|'') echo "invalid controller name" >&2; exit 2 ;;
esac
test -x ./udp_test || { echo 'Build the harness with: make udp_test' >&2; exit 2; }
work=$(mktemp -d "${TMPDIR:-/tmp}/msc-controller.XXXXXX")
printf 'Controller fixtures and logs: %s\n' "$work"
head -c 2M /dev/urandom > "$work/input.bin"
for loss in 0 5; do
  log="$work/loss-$loss.log"
  if ! timeout 60 env -u MSC_UDP_NO_CC -u MSC_UDP_PORT_LIST \
    -u MSC_UDP_PORT_BASE -u MSC_UDP_PORT_SPAN \
    MSC_UDP_CC="$controller" MSC_UDP_CTL=stdio MSC_UDP_PORTS=2 \
    MSC_UDP_PROFILE=lan MSC_UDP_STATS=1 MSC_UDP_PAYLOAD=1400 \
    ./udp_test "$work/input.bin" "$work/output-$loss.bin" 2 "$loss" \
    >"$work/loss-$loss.stdout" 2>"$log"; then
    printf 'FAIL: transfer with %s%% loss; see %s\n' "$loss" "$log" >&2
    exit 1
  fi
  cmp "$work/input.bin" "$work/output-$loss.bin"
  if ! grep -F " cc $controller " "$log" >/dev/null; then
    printf 'FAIL: telemetry did not confirm controller %s; see %s\n' \
      "$controller" "$log" >&2
    exit 1
  fi
  printf 'PASS: %s, %s%% loss, matching bytes and controller selection\n' \
    "$controller" "$loss"
done
