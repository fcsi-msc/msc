#!/bin/sh
# launcher_test.sh - msc's peer launcher must not outlive msc.
#
# A sender that gives up exits without reaping the launcher it started (ssh,
# or a substitute such as srun). A launcher left running would hold msc's
# stdout and stderr for as long as its peer runs, so anything reading msc's
# output through a pipe would wait for a peer that may be stuck for good.
# The stand-in launcher here behaves like srun: it runs the peer in its own
# session and relays the peer's stdio through pipes of its own, so only the
# launcher's exit releases msc's descriptors. Nothing here needs an sshd.
set -e

MSC="${MSC:-./msc}"
MSC="$(cd "$(dirname "$MSC")" && pwd)/$(basename "$MSC")"
shim="$(dirname "$MSC")/build/fsync_fault_shim.so"
work="$(mktemp -d)"
# The stuck peer runs in its own session, beyond msc's reach as a remote peer
# would be; this test owns it, so kill its process group before cleaning up.
stop_peer()
{
   if [ -s "$work/peer.pid" ]; then
      kill -s KILL -- "-$(cat "$work/peer.pid")" 2>/dev/null || true
   fi
}
trap 'stop_peer; rm -rf "$work"' EXIT

cat > "$work/launcher" <<'EOF'
#!/usr/bin/env python3
import os, subprocess, sys, threading
args = sys.argv[1:]
while args and args[0] == "-o":
    args = args[2:]
args = args[1:]                      # the host
with open(os.environ["LAUNCHER_PIDFILE"], "w") as f:
    f.write(str(os.getpid()))
peer = subprocess.Popen(["/bin/sh", "-c", " ".join(args)], bufsize=0,
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, start_new_session=True)
with open(os.environ["PEER_PIDFILE"], "w") as f:
    f.write(str(peer.pid))           # also its process group: a new session
def pump(src, dst):
    while True:
        data = os.read(src, 65536)
        if not data:
            break
        os.write(dst, data)
def feed():
    pump(0, peer.stdin.fileno())
    peer.stdin.close()
for job in ((peer.stdout.fileno(), 1), (peer.stderr.fileno(), 2)):
    threading.Thread(target=pump, args=job, daemon=True).start()
threading.Thread(target=feed, daemon=True).start()
sys.exit(peer.wait())
EOF
chmod +x "$work/launcher"
head -c 1048576 /dev/urandom > "$work/src"

# The receiver's flush sticks for 20 s, far past the sender's limits, so the
# sender gives up within a few seconds while its peer stays stuck.
start=$(date +%s)
{ timeout 60 env LD_PRELOAD="$shim" MSC_TEST_FSYNC_DELAY_MS=20000 \
     MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576 MSC_SSH="$work/launcher" \
     LAUNCHER_PIDFILE="$work/launcher.pid" PEER_PIDFILE="$work/peer.pid" \
     "$MSC" -T -n 4 --stall-timeout 1s -l localhost -r localhost -B "$MSC" \
     -i "$work/src" -o "$work/out" 2>&1 && status=0 || status=$?
  echo "$status" > "$work/rc"; } | cat > "$work/log"
elapsed=$(( $(date +%s) - start ))
rc=$(cat "$work/rc")
if [ "$rc" -eq 0 ]; then
   echo "FAIL: a sender whose peer stayed stuck exited 0"
   sed -n '1,8p' "$work/log"
   exit 1
fi
if [ "$elapsed" -ge 10 ]; then
   echo "FAIL: msc's output stayed open ${elapsed}s after the sender gave up (rc $rc)"
   sed -n '1,8p' "$work/log"
   exit 1
fi
pid=$(cat "$work/launcher.pid")
if kill -0 "$pid" 2>/dev/null; then
   echo "FAIL: the launcher (pid $pid) outlived msc"
   kill -9 "$pid" 2>/dev/null || true
   exit 1
fi
# Leave nothing behind for the suites that follow: no peer still holding a
# port or writing into this work directory.
stop_peer
waited=0
while pgrep -f "$work/out" >/dev/null 2>&1 && [ "$waited" -lt 50 ]; do
   sleep 0.1
   waited=$((waited + 1))
done
if pgrep -f "$work/out" >/dev/null 2>&1; then
   echo "FAIL: a receiver from this test is still running"
   exit 1
fi
echo "ok: a sender that gives up takes its launcher with it (rc $rc, ${elapsed}s)"
