#!/bin/sh
# tcp_destination_test.sh - how a TCP single-file receiver writes its destination.
#
# A destination the transfer creates or truncates on a local filesystem (tmpfs,
# ext2/3/4, XFS, Btrfs) is received straight into shared mappings of the file.
# Other filesystems, offsets and MSC_TCP_NO_MMAP=1 keep pwrite. Both
# paths must deliver identical bytes for any segment size, including segments
# that cross mapping regions (MSC_TEST_TCP_MAP_REGION_MB=1 shrinks a region to
# 1 MiB), and a destination that cannot be written must name the reason. A
# slow flush before the acknowledgement must not fail a finished transfer.
# Nothing here needs an sshd. The full-filesystem case runs only where
# unprivileged user and mount namespaces are available.
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

# The path a fresh destination in $work should take.
case "$(stat -f -c %t "$work")" in
   1021994|ef53|58465342|9123683e) maps=mmap ;;
   *) maps=pwrite
      echo "note: $work is not on a local filesystem MSC maps; expecting pwrite" ;;
esac

head -c 5242881 /dev/urandom > "$work/src"    # five regions and one byte
: > "$work/empty"

# label, expected write path, environment assignment (or ""), source, msc options
transfer()
{
   label="$1"; want="$2"; assign="$3"; src="$4"; shift 4
   timeout 120 env ${assign:+"$assign"} "$MSC" -T --stats -l localhost -r localhost \
      -B "$MSC" "$@" -i "$src" -o "$work/out" >/dev/null 2>"$work/log" \
      && rc=0 || rc=$?
   if [ "$rc" -ne 0 ]; then
      echo "FAIL: $label exited $rc"
      sed -n '1,5p' "$work/log"
      exit 1
   fi
   got="$(sed -n 's/^MSC TCP receiver write-path: //p' "$work/log")"
   if [ "$got" != "$want" ]; then
      echo "FAIL: $label used write path '$got', want '$want'"
      exit 1
   fi
   cmp -s "$src" "$work/out" || { echo "FAIL: $label corrupted the file"; exit 1; }
}

for streams in 1 8; do
   for segment in 1048576 100000 4097; do
      transfer "mapped -n$streams -s$segment" "$maps" MSC_TEST_TCP_MAP_REGION_MB=1 \
         "$work/src" -n "$streams" -s "$segment"
      transfer "pwrite -n$streams -s$segment" pwrite MSC_TCP_NO_MMAP=1 \
         "$work/src" -n "$streams" -s "$segment"
   done
done
echo "ok: mapped and pwrite destinations match for 1 and 8 streams and odd segment sizes"

transfer "default region" "$maps" "" "$work/src" -n 8
transfer "empty source" "$maps" "" "$work/empty" -n 4
head -c 9437184 /dev/urandom > "$work/out"
transfer "longer existing destination" "$maps" "" "$work/src" -n 8
echo "ok: an empty source and a longer existing destination end at the source length"

# A destination offset continues an existing file, so it keeps pwrite and the
# bytes before the offset survive.
head -c 4096 /dev/zero | tr '\0' X > "$work/out"
timeout 120 "$MSC" -T --stats -n 4 -b 4096 -l localhost -r localhost -B "$MSC" \
   -i "$work/src" -o "$work/out" >/dev/null 2>"$work/log"
grep -q '^MSC TCP receiver write-path: pwrite$' "$work/log" \
   || { echo "FAIL: an offset transfer did not keep pwrite"; exit 1; }
{ head -c 4096 /dev/zero | tr '\0' X; cat "$work/src"; } | cmp -s - "$work/out" \
   || { echo "FAIL: an offset transfer changed the bytes around it"; exit 1; }
echo "ok: a destination offset keeps pwrite and the bytes before it"

# A file-size limit stops either write path partway; the receiver must say
# where and why rather than only that the transfer failed.
python3 - "$MSC" "$work" <<'PY'
import os
import resource
import signal
import subprocess
import sys

binary, work = sys.argv[1], sys.argv[2]

def limit():
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    resource.setrlimit(resource.RLIMIT_FSIZE, (3 << 20, 3 << 20))

for name, assign in (("mapped", {}), ("pwrite", {"MSC_TCP_NO_MMAP": "1"})):
    result = subprocess.run([binary, "-T", "-n", "4", "-l", "localhost",
                             "-r", "localhost", "-B", binary,
                             "-i", work + "/src", "-o", work + "/limited"],
                            env=dict(os.environ, **assign), preexec_fn=limit,
                            capture_output=True, text=True, timeout=120)
    assert result.returncode != 0, name + " transfer passed a file-size limit"
    assert "MSC could not write the destination at byte" in result.stderr and \
        "File too large" in result.stderr, result.stderr
    print("ok: the " + name + " path names a file-size limit")
PY

# A real full filesystem: the mapped path receives into pages the kernel cannot
# allocate, and must report the filesystem's reason instead of dying of SIGBUS.
mkdir "$work/full"
if unshare -Urm true 2>/dev/null; then
   for assign in "" MSC_TCP_NO_MMAP=1; do
      rc=0
      timeout 120 unshare -Urm sh -c '
         mount -t tmpfs -o size=1m msc-full "$1/full" || exit 99
         env ${3:+"$3"} "$2" -T --stats -n 4 -l localhost -r localhost -B "$2" \
            -i "$1/src" -o "$1/full/out"' sh "$work" "$MSC" "$assign" \
         >/dev/null 2>"$work/log" || rc=$?
      if [ "$rc" -eq 99 ]; then
         echo "skip: tmpfs cannot be mounted in a user namespace here"
         break
      fi
      if [ "$rc" -eq 0 ] || [ "$rc" -gt 128 ] ||
         ! grep -q 'MSC could not write the destination at byte .*: No space left on device' "$work/log"; then
         echo "FAIL: a full filesystem (${assign:-mapped}) exited $rc without naming ENOSPC"
         sed -n '1,8p' "$work/log"
         exit 1
      fi
      echo "ok: a full filesystem fails with ENOSPC named (${assign:-mapped})"
   done
else
   echo "skip: unprivileged user namespaces are unavailable; full-filesystem case not run"
fi

# The receiver flushes the destination before acknowledging it, and the
# sender waits for that acknowledgement under the stall timeout. A flush that
# keeps advancing must not fail a finished transfer however long it takes; one
# that stops must still fail it. The fsync shim slows writeback steps.
shim="$(dirname "$MSC")/build/fsync_fault_shim.so"
flush()   # label, expected exit, then environment assignments
{
   label="$1"; want="$2"; shift 2
   rm -f "$work/out"
   timeout 60 env LD_PRELOAD="$shim" "$@" "$MSC" -T -n 4 --stall-timeout 1s \
      -l localhost -r localhost -B "$MSC" -i "$work/src" -o "$work/out" \
      >/dev/null 2>"$work/log" && rc=0 || rc=$?
   if [ "$rc" -ne "$want" ]; then
      echo "FAIL: $label exited $rc, want $want"
      sed -n '1,5p' "$work/log"
      exit 1
   fi
   cmp -s "$work/src" "$work/out" || { echo "FAIL: $label left a different file"; exit 1; }
}
flush "writeback advancing for 3.5 s" 0 MSC_TEST_FLUSH_CHUNK_MB=1 MSC_TEST_SYNC_RANGE_DELAY_MS=600
flush "final fsync of 1.2 s" 0 MSC_TEST_FSYNC_DELAY_MS=1200
flush "writeback stuck for 4 s" 6 MSC_TEST_FLUSH_CHUNK_MB=8 MSC_TEST_SYNC_RANGE_DELAY_MS=4000
grep -q "did not acknowledge" "$work/log" \
   || { echo "FAIL: a stuck flush did not report the missing acknowledgement"; exit 1; }
# A sender from before FLUSHING records never receives one: a short flush
# still succeeds, and a long one fails as it always did.
flush "pre-heartbeat sender, short flush" 0 MSC_TEST_LEGACY_SENDER=1 MSC_TEST_FSYNC_DELAY_MS=600
flush "pre-heartbeat sender, long flush" 6 MSC_TEST_LEGACY_SENDER=1 MSC_TEST_FSYNC_DELAY_MS=2500
echo "ok: a slow but advancing flush completes; a stuck one fails; older senders see no new records"

# A checkpointed transfer flushes before its final checkpoint update.
rm -f "$work/out" "$work/out.ckpt"
timeout 60 env LD_PRELOAD="$shim" MSC_TEST_FLUSH_CHUNK_MB=1 MSC_TEST_SYNC_RANGE_DELAY_MS=600 \
   "$MSC" -T -n 4 --stall-timeout 1s --checkpoint "$work/out.ckpt" \
   -l localhost -r localhost -B "$MSC" -i "$work/src" -o "$work/out" \
   >/dev/null 2>"$work/log" || { echo "FAIL: a checkpointed slow flush failed"; sed -n '1,5p' "$work/log"; exit 1; }
cmp -s "$work/src" "$work/out" || { echo "FAIL: a checkpointed slow flush corrupted the file"; exit 1; }
test ! -e "$work/out.ckpt" || { echo "FAIL: a completed checkpointed transfer kept its checkpoint"; exit 1; }
echo "ok: a checkpointed transfer survives a slow flush and removes its checkpoint"

# Recursive TCP: the receiver syncs every file, every directory and the root
# before acknowledging the tree, telling the sender while that advances.
mkdir -p "$work/tree/a/b"
head -c 1048576 /dev/urandom > "$work/tree/a/x"
head -c 2097152 /dev/urandom > "$work/tree/a/b/y"
cp "$work/src" "$work/tree/z"
tree()   # label, expected exit, then environment assignments
{
   label="$1"; want="$2"; shift 2
   rm -rf "$work/copy"
   timeout 60 env LD_PRELOAD="$shim" "$@" "$MSC" -T -R -n 4 --stall-timeout 1s \
      -l localhost -r localhost -B "$MSC" -i "$work/tree" -o "$work/copy" \
      >/dev/null 2>"$work/log" && rc=0 || rc=$?
   if { [ "$want" -eq 0 ] && [ "$rc" -ne 0 ]; } || { [ "$want" -ne 0 ] && [ "$rc" -eq 0 ]; }; then
      echo "FAIL: recursive $label exited $rc"
      sed -n '1,5p' "$work/log"
      exit 1
   fi
   diff -r "$work/tree" "$work/copy" >/dev/null \
      || { echo "FAIL: recursive $label left a different tree"; exit 1; }
}
rm -f "$work/fsync.log"
tree "durability" 0 MSC_TEST_FSYNC_LOG="$work/fsync.log"
for path in a/x a/b/y z; do
   grep -qx "F $work/copy/$path" "$work/fsync.log" \
      || { echo "FAIL: recursive TCP acknowledged before syncing $path"; exit 1; }
done
for path in a a/b; do
   grep -qx "D $work/copy/$path" "$work/fsync.log" \
      || { echo "FAIL: recursive TCP acknowledged before syncing directory $path"; exit 1; }
done
grep -qx "D $work/copy" "$work/fsync.log" \
   || { echo "FAIL: recursive TCP acknowledged before syncing the tree root"; exit 1; }
echo "ok: recursive TCP syncs every file, directory and the root before acknowledging"
tree "sync advancing for about 1.6 s" 0 MSC_TEST_FSYNC_DELAY_MS=400
tree "sync stuck for 4 s" 1 MSC_TEST_FLUSH_CHUNK_MB=8 MSC_TEST_SYNC_RANGE_DELAY_MS=4000
tree "pre-heartbeat sender, short sync" 0 MSC_TEST_LEGACY_SENDER=1 MSC_TEST_FSYNC_DELAY_MS=150
tree "pre-heartbeat sender, long sync" 1 MSC_TEST_LEGACY_SENDER=1 MSC_TEST_FSYNC_DELAY_MS=1500
echo "ok: a slow but advancing recursive sync completes; a stuck one fails; older senders see no new records"
