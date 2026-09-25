#!/bin/sh
# stdio_ctl_test.sh - the MSC_UDP_CTL=stdio|stdio1 contract.
#
# These arms exist to transfer a file while opening NO listening TCP port, so
# this asserts two things per arm: the bytes arrive intact, and nothing in the
# msc process tree ever calls listen() on a TCP socket (recorded by
# shims/listen_count_shim.so, which catches even a listener that lives for a
# few milliseconds -- polling `ss` cannot).  An explicit tcp
# arm is included as a control -- it MUST show a listener, otherwise the probe
# is broken and the stdio result would be vacuously true.  Select tcp by SETTING
# MSC_UDP_CTL, never by unsetting it: stdio is the default now, so an unset
# variable is a second stdio arm and the control proves nothing.
#
# There is no sshd on a build host, so MSC_SSH substitutes a stand-in that runs
# the remote command locally with ssh's stdin/stdout wiring.
set -e

MSC="${MSC:-./msc}"
LISTEN_SHIM="${MSC_LISTEN_SHIM_SO:-./shims/listen_count_shim.so}"
test -r "$LISTEN_SHIM" || { echo "FAIL: no $LISTEN_SHIM; make shims/listen_count_shim.so"; exit 1; }
LISTEN_SHIM="$(cd "$(dirname "$LISTEN_SHIM")" && pwd)/$(basename "$LISTEN_SHIM")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Mimics ssh's own argument shape: [-o option]... user@host command.  msc passes
# keepalive options on every launch, so a stand-in that skipped only the
# destination would hand the option values to the shell as part of the command.
cat > "$work/fakessh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
chmod +x "$work/fakessh"

head -c 8388608 /dev/urandom > "$work/src"

# Count the TCP listen() calls made anywhere in the msc process tree while $@
# runs. The fake ssh runs the receiver locally with this environment, so both
# ends are covered.
count_listeners() {
   log="$work/listens"
   : > "$log"
   LD_PRELOAD="$LISTEN_SHIM" MSC_TEST_LISTEN_LOG="$log" "$@" >/dev/null 2>&1 || true
   wc -l < "$log" | tr -d ' '
}

run_arm() {
   arm="$1"; out="$work/out.$1"
   rm -f "$out"
   MSC_UDP_CTL="$arm"; export MSC_UDP_CTL
   MSC_SSH="$work/fakessh" export MSC_SSH
   peak=$(count_listeners "$MSC" -U -n4 -l localhost -r localhost -B "$MSC" \
                         -i "$work/src" -o "$out")
   cmp -s "$work/src" "$out" || { echo "FAIL: $arm corrupted or produced no output"; exit 1; }
   echo "$arm: bytes ok, msc TCP listen() calls = ${peak:-0}"
   echo "${peak:-0}"> "$work/peak.$arm"
}

for arm in tcp stdio stdio1; do run_arm "$arm"; done

# The control arm proves the probe can see a listener at all.
if [ "$(cat "$work/peak.tcp")" -lt 1 ]; then
   echo "FAIL: explicit tcp arm showed no TCP listener -- the probe is broken,"
   echo "      so the stdio results below prove nothing"
   exit 1
fi
for arm in stdio stdio1; do
   if [ "$(cat "$work/peak.$arm")" -ne 0 ]; then
      echo "FAIL: MSC_UDP_CTL=$arm opened a listening TCP port"
      exit 1
   fi
done

# Recursive checkpoint records share the same stdio abstraction as manifests.
# Use more than one logical chunk so repeated offset-aware table transfers and
# their durable acknowledgments cross the SSH pipe in both stdio modes.
mkdir -p "$work/tree/sub"
head -c 350000 /dev/urandom > "$work/tree/a"
head -c 420000 /dev/urandom > "$work/tree/sub/b"
: > "$work/tree/empty"
ln -s sub/b "$work/tree/link"
for arm in stdio stdio1; do
   out="$work/tree.$arm"
   cp="$work/tree.$arm.cp"
   MSC_UDP_CTL="$arm" MSC_UDP_CHECKPOINT_BYTES=262144 \
      MSC_SSH="$work/fakessh" \
      "$MSC" -U -R -n4 -l localhost -r localhost -B "$MSC" \
      --checkpoint "$cp" -i "$work/tree" -o "$out"
   diff -r "$work/tree" "$out"
   test ! -e "$cp"
   echo "$arm: recursive checkpoint control records ok"
done

# Both misuse paths must be refused, not silently ignored.  The first arm needs
# -T to mean anything: reliable UDP is msc's default transport, so a command
# line with no transport flag is a UDP transfer and stdio1 is legal on it.
if MSC_UDP_CTL=stdio1 "$MSC" -T -n4 -l localhost -r localhost \
      -i "$work/src" -o "$work/x" >/dev/null 2>&1; then
   echo "FAIL: MSC_UDP_CTL=stdio1 on a -T transfer was accepted"
   exit 1
fi
if MSC_UDP_CTL=stdio1 "$MSC" -U -n4 --udp-data-ports 2 -l localhost -r localhost \
      -i "$work/src" -o "$work/x" >/dev/null 2>&1; then
   echo "FAIL: --udp-data-ports under stdio1 was accepted"
   exit 1
fi

# The stdio control channel must not fool the path profiler.
#
# stdio's control channel is a pipe to the local ssh process, so its handshake
# round trip measures that pipe -- 0.24 ms on a 50 ms netem WAN (48 runs).
# Without a correction msc would classify every such transfer as a
# LAN and silently apply LAN defaults, so selecting SSH control would change
# the transport's behaviour. The engine therefore treats the handshake value
# as provisional and re-decides from the first real data-path RTT sample.
#
# wan_shim delays UDP only and leaves the pipe alone, which reproduces exactly
# that asymmetry: without the correction this run reports profile=lan and
# nothing else.
SHIM=${MSC_WANSHIM_SO:-./shims/wan_shim.so}
if [ -f "$SHIM" ]; then
   MSC_SSH="$work/fakessh" MSC_UDP_CTL=stdio MSC_UDP_STATS=1 \
   LD_PRELOAD="$SHIM" MSC_UDP_WANSHIM_DELAY_MS=25 MSC_UDP_WANSHIM_JITTER_MS=2 \
      timeout 120 "$MSC" -U -n4 --force -l localhost -r localhost -B "$MSC" \
      -i "$work/src" -o "$work/profile_out" >/dev/null 2>"$work/profile.log" || true
   unset MSC_UDP_CTL
   cmp -s "$work/src" "$work/profile_out" \
      || { echo "FAIL: profile arm corrupted or produced no output"; exit 1; }
   if ! grep -q 'profile=wan .*reason=data-rtt' "$work/profile.log"; then
      echo "FAIL: stdio control on a 50ms UDP path did not reach the WAN profile"
      echo "      from a data-path RTT sample. The handshake measures the ssh"
      echo "      pipe, so without that correction msc runs a WAN as a LAN."
      grep 'MSC-UDP-STATS profile=' "$work/profile.log" || echo "      (no profile line at all)"
      exit 1
   fi
   echo "stdio: path profile corrected from the data path$(
      sed -n 's/.*profile=wan rtt_ms=\([0-9.]*\).*handshake said \([0-9.]*\) ms.*/ (\2 ms handshake -> \1 ms path)/p' \
         "$work/profile.log" | head -1)"
else
   echo "stdio: SKIP profile check (no $SHIM; build it with make shims/wan_shim.so)"
fi

# A remote that dies during launch must be REPORTED, not waited on forever.
# This used to hang: the parent kept the child's pipe ends open, so the read
# could never see EOF -- and in the stdio modes that pipe is the control
# channel, which carries no socket timeout to break the wait.
for arm in tcp stdio1; do
   MSC_UDP_CTL="$arm"; export MSC_UDP_CTL   # explicit, for the same reason as above
   started=$(date +%s)
   # `&& rc=0 || rc=$?` keeps the expected non-zero exit from tripping set -e
   MSC_SSH="$work/fakessh" timeout 30 "$MSC" -U -n4 -l localhost -r localhost \
      -B /nonexistent/msc -i "$work/src" -o "$work/late" >/dev/null 2>&1 \
      && rc=0 || rc=$?
   elapsed=$(( $(date +%s) - started ))
   if [ "$rc" -eq 124 ]; then
      echo "FAIL: $arm hung on a remote that failed to launch"
      exit 1
   fi
   if [ "$elapsed" -gt 15 ]; then
      echo "FAIL: $arm took ${elapsed}s to report a failed remote launch"
      exit 1
   fi
   if [ "$rc" -ne 6 ]; then
      echo "FAIL: $arm exited $rc on a failed remote launch, want 6 (network)"
      exit 1
   fi
   echo "$arm: failed remote launch reported in ${elapsed}s, exit 6"
done
unset MSC_UDP_CTL

echo "stdio control-channel tests passed"
