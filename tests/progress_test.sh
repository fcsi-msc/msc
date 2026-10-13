#!/bin/sh
# progress_test.sh - progress reporting must not delay a finished transfer.
#
# The progress reporter wakes once per --progress-interval. When a transfer
# finishes, it must stop at once rather than sleep out the rest of the interval,
# which would add up to one interval to every transfer with progress enabled
# (including --progress=auto on a terminal). Each transfer here takes well under
# a second with a ten-second interval, and must still print its final summary.
# Nothing here needs an sshd.
set -e

MSC="${MSC:-./msc}"
MSC="$(cd "$(dirname "$MSC")" && pwd)/$(basename "$MSC")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cat > "$work/fakessh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
chmod +x "$work/fakessh"
MSC_SSH="$work/fakessh"; export MSC_SSH

head -c 1048576 /dev/urandom > "$work/src"
mkdir "$work/tree"
cp "$work/src" "$work/tree/a"
cp "$work/src" "$work/tree/b"

check()   # label, then msc options
{
   label="$1"; shift
   rm -rf "$work/out"
   start=$(date +%s)
   timeout 60 "$MSC" --progress=always --progress-interval 10s -n 4 \
      -l localhost -r localhost -B "$MSC" "$@" >/dev/null 2>"$work/log" \
      && rc=0 || rc=$?
   elapsed=$(( $(date +%s) - start ))
   if [ "$rc" -ne 0 ]; then
      echo "FAIL: $label exited $rc"
      sed -n '1,5p' "$work/log"
      exit 1
   fi
   grep -q '^msc summary: success' "$work/log" \
      || { echo "FAIL: $label printed no final summary"; exit 1; }
   if [ "$elapsed" -ge 6 ]; then
      echo "FAIL: $label took ${elapsed}s with a 10s progress interval"
      exit 1
   fi
   echo "ok: $label finished in ${elapsed}s with a 10s progress interval"
}

check "TCP file" -T -i "$work/src" -o "$work/out"
check "UDP file" -U -i "$work/src" -o "$work/out"
check "TCP tree" -T -R -i "$work/tree" -o "$work/out"
