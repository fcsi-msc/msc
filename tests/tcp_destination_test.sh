#!/bin/sh
# tcp_destination_test.sh - how a TCP single-file receiver writes its destination.
#
# A destination the transfer creates or truncates on a local filesystem (tmpfs,
# ext2/3/4, XFS, Btrfs) is received straight into shared mappings of the file.
# Other filesystems, offsets and MSC_TCP_NO_MMAP=1 keep pwrite. Both
# paths must deliver identical bytes for any segment size, including segments
# that cross mapping regions (MSC_TEST_TCP_MAP_REGION_MB=1 shrinks a region to
# 1 MiB), and a destination that cannot be written must name the reason. A
# slow flush before the acknowledgement must not fail a finished transfer, for
# a single file or a recursive tree. Nothing here needs an sshd. The
# full-filesystem case runs only where unprivileged user and mount namespaces
# are available.
set -e

MSC="${MSC:-./msc}"
MSC="$(cd "$(dirname "$MSC")" && pwd)/$(basename "$MSC")"
work="$(mktemp -d)"
trap 'chmod -R u+w "$work" 2>/dev/null; rm -rf "$work"' EXIT

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

# A file-size limit stops either write path partway; the transfer must fail
# as the destination's (exit 4) and the sender must say where and why.
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
    assert result.returncode == 4, \
        "%s transfer past a file-size limit exited %d" % (name, result.returncode)
    assert "MSC the destination failed while writing at byte" in result.stderr and \
        "File too large" in result.stderr, result.stderr
    print("ok: the " + name + " path names a file-size limit")
PY

# A real full filesystem: the mapped path receives into pages the kernel cannot
# allocate, and must report the filesystem's reason instead of dying of SIGBUS.
# The receiver tells the sender in a FAILED record, then closes its streams,
# resetting them; the runs repeat so the record is seen to arrive every time.
mkdir "$work/full"
if unshare -Urm true 2>/dev/null; then
   full=yes
   for assign in "" MSC_TCP_NO_MMAP=1; do
      run=0
      while [ "$run" -lt 10 ]; do
         run=$((run + 1))
         rc=0
         timeout 120 unshare -Urm sh -c '
            mount -t tmpfs -o size=1m msc-full "$1/full" || exit 99
            env ${3:+"$3"} "$2" -T --stats -n 4 -l localhost -r localhost -B "$2" \
               -i "$1/src" -o "$1/full/out"' sh "$work" "$MSC" "$assign" \
            >/dev/null 2>"$work/log" || rc=$?
         if [ "$rc" -eq 99 ]; then
            echo "skip: tmpfs cannot be mounted in a user namespace here"
            full=no
            break 2
         fi
         if [ "$rc" -ne 4 ] ||
            ! grep -q 'MSC could not write the destination at byte .*: No space left on device' "$work/log" ||
            ! grep -q 'MSC the destination failed while writing at byte .*: No space left on device' "$work/log"; then
            echo "FAIL: a full filesystem (${assign:-mapped}, run $run) exited $rc without the sender naming ENOSPC"
            sed -n '1,8p' "$work/log"
            exit 1
         fi
      done
      echo "ok: a full filesystem exits 4 with ENOSPC named by the sender, 10 of 10 (${assign:-mapped})"
   done
   # Checkpointed: the partial file and checkpoint stay, and --resume finishes
   # once there is room.
   if [ "$full" = yes ]; then
      rc=0
      timeout 120 unshare -Urm sh -c '
         mount -t tmpfs -o size=8m msc-full "$1/full" || exit 99
         head -c 6291456 /dev/zero > "$1/full/filler"
         "$2" -T -n 4 --checkpoint "$1/full.ckpt" -l localhost -r localhost -B "$2" \
            -i "$1/src" -o "$1/full/out" 2>"$1/log" && first=0 || first=$?
         [ "$first" -eq 4 ] || exit 90
         grep -q "rerun with --resume" "$1/log" || exit 91
         test -e "$1/full/out.msc-part" && test -e "$1/full.ckpt" || exit 92
         : > "$1/full/filler"
         "$2" -T -n 4 --resume --checkpoint "$1/full.ckpt" -l localhost -r localhost \
            -B "$2" -i "$1/src" -o "$1/full/out" 2>>"$1/log" || exit 93
         cmp -s "$1/src" "$1/full/out" || exit 94' sh "$work" "$MSC" || rc=$?
      if [ "$rc" -ne 0 ]; then
         echo "FAIL: a checkpointed transfer to a full filesystem (step $rc)"
         sed -n '1,10p' "$work/log"
         exit 1
      fi
      echo "ok: a checkpointed transfer to a full filesystem exits 4, keeps its checkpoint, and --resume finishes"
      # Far more than the socket buffers hold, so the sender is still sending
      # when the destination fills, and the checkpoint update that follows
      # outlasts the stall timeout: the sender must hear FAILED first, not
      # stall into a reconnect.
      head -c 268435456 /dev/urandom > "$work/big"
      rc=0
      start=$(date +%s)
      timeout 120 unshare -Urm sh -c '
         mount -t tmpfs -o size=8m msc-full "$1/full" || exit 99
         head -c 6291456 /dev/zero > "$1/full/filler"
         env LD_PRELOAD="$3" MSC_TEST_FSYNC_DELAY_MS=3000 \
            MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576 \
            "$2" -T -n 4 --stall-timeout 1s --checkpoint "$1/big.ckpt" \
            -l localhost -r localhost -B "$2" -i "$1/big" -o "$1/full/out"' \
         sh "$work" "$MSC" "$(dirname "$MSC")/build/fsync_fault_shim.so" \
         >/dev/null 2>"$work/log" || rc=$?
      elapsed=$(( $(date +%s) - start ))
      if [ "$rc" -ne 4 ] || [ "$elapsed" -ge 10 ] || grep -q "msc reconnect" "$work/log"; then
         echo "FAIL: a large checkpointed transfer to a full filesystem exited $rc after ${elapsed}s"
         sed -n '1,10p' "$work/log"
         exit 1
      fi
      waited=0
      while pgrep -f "$work/full/out" >/dev/null 2>&1 && [ "$waited" -lt 100 ]; do
         sleep 0.1
         waited=$((waited + 1))
      done
      echo "ok: a large checkpointed transfer to a full filesystem exits 4 before its checkpoint update (${elapsed}s)"
   fi
else
   echo "skip: unprivileged user namespaces are unavailable; full-filesystem case not run"
fi

# The receiver flushes the destination before acknowledging it. Meanwhile it
# says it is alive, so a flush longer than the stall timeout -- one that shows
# no progress at all, as on a network filesystem -- does not fail a transfer
# whose data has arrived. The sender bounds the finish at the larger of
# --flush-timeout and 1 s per 2 MiB transferred, and reports a destination that
# never confirms its data with exit 4. The fsync shim makes every fsync of a
# regular file take MSC_TEST_FSYNC_DELAY_MS.
shim="$(dirname "$MSC")/build/fsync_fault_shim.so"
head -c 1048576 /dev/urandom > "$work/mib"
head -c 20971520 /dev/urandom > "$work/twenty"
flush()   # label, expected exit, source, sender binary, receiver binary, msc
{         # options..., then -- and environment assignments
   label="$1"; want="$2"; src="$3"; sender="$4"; receiver="$5"; shift 5
   options=""
   while [ "$1" != "--" ]; do options="$options $1"; shift; done
   shift
   rm -f "$work/out"
   # shellcheck disable=SC2086 # options are single words
   timeout 60 env LD_PRELOAD="$shim" "$@" "$sender" -T -n 4 --stall-timeout 1s $options \
      -l localhost -r localhost -B "$receiver" -i "$src" -o "$work/out" \
      >/dev/null 2>"$work/log" && rc=0 || rc=$?
   if [ "$rc" -ne "$want" ]; then
      echo "FAIL: $label exited $rc, want $want"
      sed -n '1,5p' "$work/log"
      exit 1
   fi
   cmp -s "$src" "$work/out" || { echo "FAIL: $label left a different file"; exit 1; }
}
flush "a 3 s final fsync" 0 "$work/src" "$MSC" "$MSC" -- MSC_TEST_FSYNC_DELAY_MS=3000
grep -q "all data delivered; waiting for the destination to confirm it is durable (limit 30m00s" \
   "$work/log" || { echo "FAIL: a long flush was not announced"; sed -n '1,5p' "$work/log"; exit 1; }
# 1 MiB: the ceiling is max(1 s, 1 MiB at 2 MiB/s) = 1 s; the fsync takes 4 s.
flush "a stuck fsync past the ceiling" 4 "$work/mib" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_FSYNC_DELAY_MS=4000
grep -q "did not confirm it was durable within" "$work/log" \
   || { echo "FAIL: a stuck fsync did not report the ceiling"; sed -n '1,5p' "$work/log"; exit 1; }
# 20 MiB: the size term (10 s at 2 MiB/s) outlasts --flush-timeout 1s.
flush "a 3 s fsync inside the size term" 0 "$work/twenty" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_FSYNC_DELAY_MS=3000
echo "ok: a slow flush completes; one past --flush-timeout and the size term fails with exit 4"

# A flush that fails is the destination's failure: exit 4, named by the sender.
flush "a failed fsync" 4 "$work/src" "$MSC" "$MSC" -- MSC_TEST_FAIL_FSYNC=files
grep -q "MSC the destination failed while flushing: Input/output error" "$work/log" \
   || { echo "FAIL: the sender did not name a failed fsync"; sed -n '1,5p' "$work/log"; exit 1; }
echo "ok: a failed fsync exits 4 and the sender names it"

# A checkpointed transfer flushes and publishes before its ACK.
rm -f "$work/out" "$work/out.ckpt"
timeout 60 env LD_PRELOAD="$shim" MSC_TEST_FSYNC_DELAY_MS=2500 \
   MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576 \
   "$MSC" -T -n 4 --stall-timeout 1s --checkpoint "$work/out.ckpt" \
   -l localhost -r localhost -B "$MSC" -i "$work/src" -o "$work/out" \
   >/dev/null 2>"$work/log" || { echo "FAIL: a checkpointed slow flush failed"; sed -n '1,5p' "$work/log"; exit 1; }
cmp -s "$work/src" "$work/out" || { echo "FAIL: a checkpointed slow flush corrupted the file"; exit 1; }
test ! -e "$work/out.ckpt" || { echo "FAIL: a completed checkpointed transfer kept its checkpoint"; exit 1; }
if grep -q "msc reconnect" "$work/log"; then
   echo "FAIL: the checkpointed slow flush only succeeded by reconnecting"
   sed -n '1,8p' "$work/log"
   exit 1
fi
echo "ok: a checkpointed transfer survives a slow flush and removes its checkpoint"

# When the ceiling trips on a checkpointed transfer, the receiver must not
# publish behind the sender's failure: it keeps the partial file and the
# checkpoint, and --resume then finishes the transfer. (The shim's delay can
# end early when the exiting sender's SSH stand-in is signalled, unlike an
# uninterruptible fsync on a stuck network filesystem; the outcome is the same.)
rm -f "$work/out" "$work/out.ckpt"
timeout 60 env LD_PRELOAD="$shim" MSC_TEST_FSYNC_DELAY_MS=3000 \
   MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576 \
   "$MSC" -T -n 4 --stall-timeout 1s --flush-timeout 1s --checkpoint "$work/out.ckpt" \
   -l localhost -r localhost -B "$MSC" -i "$work/mib" -o "$work/out" \
   >/dev/null 2>"$work/log" && rc=0 || rc=$?
[ "$rc" -eq 4 ] || { echo "FAIL: a checkpointed transfer past the ceiling exited $rc, want 4"; sed -n '1,8p' "$work/log"; exit 1; }
grep -q "rerun with --resume" "$work/log" \
   || { echo "FAIL: a checkpointed transfer past the ceiling did not suggest --resume"; exit 1; }
waited=0
until grep -q "keeping .* for --resume" "$work/log" || [ "$waited" -ge 20 ]; do
   sleep 1
   waited=$((waited + 1))
done
grep -q "keeping .* for --resume" "$work/log" \
   || { echo "FAIL: the receiver did not keep its checkpoint after the sender gave up"; sed -n '1,8p' "$work/log"; exit 1; }
test ! -e "$work/out" || { echo "FAIL: the receiver published after the sender gave up"; exit 1; }
test -e "$work/out.ckpt" || { echo "FAIL: the receiver dropped the checkpoint after the sender gave up"; exit 1; }
timeout 60 "$MSC" -T -n 4 --stall-timeout 1s --resume --checkpoint "$work/out.ckpt" \
   -l localhost -r localhost -B "$MSC" -i "$work/mib" -o "$work/out" >/dev/null 2>"$work/log" \
   || { echo "FAIL: --resume after the ceiling failed"; sed -n '1,8p' "$work/log"; exit 1; }
cmp -s "$work/mib" "$work/out" || { echo "FAIL: --resume after the ceiling left a different file"; exit 1; }
echo "ok: past the ceiling a checkpointed receiver keeps its checkpoint, and --resume finishes"

# A recursive receiver syncs every file, every directory, the root and the
# directory holding it before it acknowledges the tree, and says it is alive
# meanwhile, as a single-file receiver does. The sender bounds the finish at
# the larger of --flush-timeout and 1 s per 2 MiB sent plus 50 ms per entry.
make_tree()   # directory, "readonly" for read-only files and directories
{
   mkdir -p "$1/sub/deep"
   head -c 1572864 /dev/urandom > "$1/a"
   head -c 1258291 /dev/urandom > "$1/sub/b"
   head -c 1000 /dev/urandom > "$1/sub/deep/c"
   : > "$1/empty"
   ln "$1/a" "$1/sub/a-hard"
   ln -s sub/b "$1/b-link"
   if [ -n "$2" ]; then
      chmod 0444 "$1/a" "$1/sub/b" "$1/sub/deep/c" "$1/empty"
      chmod 0555 "$1/sub/deep" "$1/sub"
   fi
}
make_tree "$work/tree" ""
reset_out()
{
   chmod -R u+w "$work/rout" 2>/dev/null || true
   rm -rf "$work/rout" "$work/rout.ckpt"
}
# Wait (up to 30 s) for a receiver left behind by a sender that gave up.
receiver_done()
{
   waited=0
   while pgrep -f "$work/rout" >/dev/null 2>&1 && [ "$waited" -lt 300 ]; do
      sleep 0.1
      waited=$((waited + 1))
   done
}
tree()    # label, expected exit, source, sender binary, receiver binary, msc
{         # options..., then -- and environment assignments; sets elapsed (s)
   label="$1"; want="$2"; src="$3"; sender="$4"; receiver="$5"; shift 5
   options=""
   while [ "$1" != "--" ]; do options="$options $1"; shift; done
   shift
   start=$(date +%s)
   # shellcheck disable=SC2086 # options are single words
   timeout 60 env LD_PRELOAD="$shim" "$@" "$sender" -T -R -n 4 --stall-timeout 1s $options \
      -l localhost -r localhost -B "$receiver" -i "$src" -o "$work/rout" \
      >/dev/null 2>"$work/log" && rc=0 || rc=$?
   elapsed=$(( $(date +%s) - start ))
   if [ "$rc" -ne "$want" ]; then
      echo "FAIL: $label exited $rc, want $want"
      sed -n '1,8p' "$work/log"
      exit 1
   fi
   if [ "$want" -eq 0 ]; then
      diff -r "$src" "$work/rout" >/dev/null || { echo "FAIL: $label left a different tree"; exit 1; }
   fi
}
# Whether the shim's log $1 shows an fsync of every regular file's inode in
# $2, every directory under it, $2 itself and the directory holding it.
synced_all()
{
   python3 - "$1" "$2" <<'PY'
import os, sys
log, root = sys.argv[1], os.path.realpath(sys.argv[2])
files, dirs = set(), set()
for line in open(log):
    kind, path = line.rstrip("\n").split(" ", 1)
    if kind == "F" and os.path.exists(path):
        files.add(os.stat(path).st_ino)
    elif kind == "D":
        dirs.add(path)
missing = [root + " (root)"] if root not in dirs else []
if os.path.dirname(root) not in dirs:
    missing.append(os.path.dirname(root) + " (parent)")
for top, subdirs, names in os.walk(root):
    for d in subdirs:
        if os.path.join(top, d) not in dirs:
            missing.append(os.path.join(top, d))
    for n in names:
        path = os.path.join(top, n)
        if not os.path.islink(path) and os.stat(path).st_ino not in files:
            missing.append(path)
if missing:
    print("not synced: " + ", ".join(missing))
    sys.exit(1)
PY
}

reset_out
rm -f "$work/fsync.log"
tree "a recursive transfer" 0 "$work/tree" "$MSC" "$MSC" -- MSC_TEST_FSYNC_LOG="$work/fsync.log"
synced_all "$work/fsync.log" "$work/rout" || { echo "FAIL: a recursive transfer left entries unsynced"; exit 1; }
# A resumed tree is synced whole too, including files the first run finished.
reset_out
rm -f "$work/fsync.log"
tree "an interrupted checkpointed tree" 6 "$work/tree" "$MSC" "$MSC" \
   --checkpoint "$work/rout.ckpt" --reconnect-interval 0 -s 65536 -- \
   MSC_TEST_INTERRUPT_AFTER_SEGMENTS=8
tree "its resume" 0 "$work/tree" "$MSC" "$MSC" --resume --checkpoint "$work/rout.ckpt" -s 65536 -- \
   MSC_TEST_FSYNC_LOG="$work/fsync.log"
synced_all "$work/fsync.log" "$work/rout" || { echo "FAIL: a resumed tree left entries unsynced"; exit 1; }
echo "ok: a recursive transfer, fresh or resumed, syncs every file, directory, the root and its parent"

# Each data file's fsync blocks for 3 s and shows no progress, as a commit on a
# network filesystem can; the receiver's beats keep the sender waiting.
reset_out
tree "a tree with 3 s fsyncs" 0 "$work/tree" "$MSC" "$MSC" -- \
   MSC_TEST_FSYNC_DELAY_MS=3000 MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576
grep -q "all data delivered; waiting for the destination to confirm it is durable" "$work/log" \
   || { echo "FAIL: a tree's long sync was not announced"; sed -n '1,5p' "$work/log"; exit 1; }
# About 2.7 MiB and 8 entries: the ceiling is max(1 s, 1.3 s + 0.4 s), long
# before a 10 s fsync ends.
reset_out
tree "a tree past the ceiling" 4 "$work/tree" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_FSYNC_DELAY_MS=10000 MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576
[ "$elapsed" -lt 6 ] || { echo "FAIL: a tree past the ceiling took ${elapsed}s to fail"; exit 1; }
grep -q "did not confirm it was durable within .* plus 50 ms per entry" "$work/log" \
   || { echo "FAIL: a tree past the ceiling did not report it"; sed -n '1,5p' "$work/log"; exit 1; }
receiver_done
echo "ok: a tree's slow sync completes, and one past the ceiling exits 4 (${elapsed}s)"

# The entry term: 200 entries allow 10 s, and the sync takes about 3 s (a 1 s
# fsync of the subdirectory, then the root, then its parent), far past
# --flush-timeout 1s. Three entries allow only the 1 s.
mkdir -p "$work/many/d" "$work/few/d"
i=0
while [ "$i" -lt 199 ]; do : > "$work/many/d/f$i"; i=$((i + 1)); done
: > "$work/few/d/f"
reset_out
tree "200 entries with 1 s directory fsyncs" 0 "$work/many" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_DIR_FSYNC_DELAY_MS=1000
reset_out
tree "3 entries with 1 s directory fsyncs" 4 "$work/few" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_DIR_FSYNC_DELAY_MS=1000
receiver_done
echo "ok: each entry adds 50 ms to the ceiling"

# A failed sync fails the tree at once, naming the path, rather than leaving
# the sender waiting on beats until its ceiling.
reset_out
tree "a tree whose directory fsyncs fail" 70 "$work/tree" "$MSC" "$MSC" -- \
   MSC_TEST_FAIL_FSYNC=directories
[ "$elapsed" -lt 5 ] || { echo "FAIL: a failed tree sync took ${elapsed}s to fail"; exit 1; }
grep -q "MSC could not sync .*: Input/output error" "$work/log" \
   || { echo "FAIL: a failed tree sync was not named"; sed -n '1,5p' "$work/log"; exit 1; }
if grep -q "did not confirm" "$work/log"; then
   echo "FAIL: a failed tree sync waited for the ceiling"
   exit 1
fi
echo "ok: a failed tree sync exits promptly and names the path"

# Checkpointed: the final checkpoint update syncs the tree under the beat. Past
# the ceiling the receiver keeps the checkpoint, deciding before it applies
# modes so that --resume can still reopen read-only files for writing, and
# --resume then finishes the tree.
reset_out
tree "a checkpointed tree with 2.5 s fsyncs" 0 "$work/tree" "$MSC" "$MSC" \
   --checkpoint "$work/rout.ckpt" -- \
   MSC_TEST_FSYNC_DELAY_MS=2500 MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576
test ! -e "$work/rout.ckpt" || { echo "FAIL: a completed checkpointed tree kept its checkpoint"; exit 1; }
if grep -q "msc reconnect" "$work/log"; then
   echo "FAIL: a checkpointed slow tree only succeeded by reconnecting"
   exit 1
fi
make_tree "$work/rotree" readonly
# --reconnect-interval 0: a sender that can reconnect runs each attempt in its
# own process group and reaps that group when the attempt fails, which here,
# with the receiver a local process, would kill it; a remote receiver is not
# in that group.
for kind in tree rotree; do
   reset_out
   tree "a checkpointed $kind past the ceiling" 4 "$work/$kind" "$MSC" "$MSC" \
      --flush-timeout 1s --checkpoint "$work/rout.ckpt" --reconnect-interval 0 -- \
      MSC_TEST_FSYNC_DELAY_MS=4000 MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576
   grep -q "rerun with --resume" "$work/log" \
      || { echo "FAIL: a checkpointed $kind past the ceiling did not suggest --resume"; exit 1; }
   receiver_done
   grep -q "keeping checkpoint .* for --resume" "$work/log" \
      || { echo "FAIL: the receiver of $kind did not keep its checkpoint"; sed -n '1,8p' "$work/log"; exit 1; }
   test -e "$work/rout.ckpt" || { echo "FAIL: the checkpoint of $kind is gone"; exit 1; }
   tree "--resume of $kind" 0 "$work/$kind" "$MSC" "$MSC" --resume --checkpoint "$work/rout.ckpt" --
   test ! -e "$work/rout.ckpt" || { echo "FAIL: --resume of $kind kept the checkpoint"; exit 1; }
   [ "$(stat -c %a "$work/rout/sub/b")" = "$(stat -c %a "$work/$kind/sub/b")" ] &&
   [ "$(stat -c %a "$work/rout/sub")" = "$(stat -c %a "$work/$kind/sub")" ] \
      || { echo "FAIL: --resume of $kind did not apply modes"; exit 1; }
done
echo "ok: a checkpointed tree survives a slow sync; past the ceiling it keeps its checkpoint, and --resume finishes it, read-only modes included"

# A receiver that stops altogether sends no beats, so the sender gives up
# after its stall timeout.
reset_out
python3 - "$MSC" "$work" "$shim" <<'PY'
import os, signal, subprocess, sys, time
binary, work, shim = sys.argv[1:4]
env = dict(os.environ, LD_PRELOAD=shim, MSC_TEST_FSYNC_DELAY_MS="5000",
           MSC_TEST_FSYNC_DELAY_MIN_BYTES="1048576")
log = open(work + "/log", "w")
sender = subprocess.Popen([binary, "-T", "-R", "-n", "4", "--stall-timeout", "1s",
                           "-l", "localhost", "-r", "localhost", "-B", binary,
                           "-i", work + "/tree", "-o", work + "/rout"],
                          env=env, stdout=subprocess.DEVNULL, stderr=log)
def descendants(pid):
    found = []
    for task in os.listdir("/proc"):
        if task.isdigit():
            try:
                fields = open("/proc/%s/stat" % task).read().rsplit(")", 1)[1].split()
            except OSError:
                continue
            if int(fields[1]) == pid:
                found += [int(task)] + descendants(int(task))
    return found
deadline = time.time() + 20
while "waiting for the destination" not in open(work + "/log").read():
    assert time.time() < deadline and sender.poll() is None, "the sync never started"
    time.sleep(0.05)
receivers = [p for p in descendants(sender.pid)
             if open("/proc/%d/comm" % p).read().strip() == "msc"]
assert receivers, "no receiver found"
for p in receivers:
    os.kill(p, signal.SIGSTOP)
stopped = time.time()
try:
    rc = sender.wait(timeout=10)
finally:
    for p in receivers:
        os.kill(p, signal.SIGCONT)
waited = time.time() - stopped
assert rc == 70, "the sender exited %d, want 70" % rc
assert waited < 4, "the sender took %.1f s to notice" % waited
assert "sent nothing for" in open(work + "/log").read()
print("ok: a stopped receiver fails the tree after the stall timeout (%.1f s)" % waited)
PY
receiver_done

# Mixed versions, against MSC 1.1.0 built from its release tag: neither side
# sees records it does not know, so a flush inside the stall timeout succeeds
# either way, and an older receiver's long flush fails as it always did.
root="$(dirname "$MSC")"
old="$work/v1.1.0"
if git -C "$root" rev-parse -q --verify 'v1.1.0^{commit}' >/dev/null 2>&1 &&
   mkdir "$old" && git -C "$root" archive v1.1.0 | tar -x -C "$old" &&
   make -C "$old" msc >"$work/old-build.log" 2>&1; then
   flush "1.1.0 sender, new receiver" 0 "$work/src" "$old/msc" "$MSC" -- \
      MSC_TEST_FSYNC_DELAY_MS=500
   flush "new sender, 1.1.0 receiver" 0 "$work/src" "$MSC" "$old/msc" -- \
      MSC_TEST_FSYNC_DELAY_MS=500
   flush "new sender, 1.1.0 receiver, long flush" 6 "$work/src" "$MSC" "$old/msc" -- \
      MSC_TEST_FSYNC_DELAY_MS=2500
   # Neither sends nor expects a FAILED record: a failed fsync exits 6 as before.
   flush "new sender, 1.1.0 receiver, failed fsync" 6 "$work/src" "$MSC" "$old/msc" -- \
      MSC_TEST_FAIL_FSYNC=files
   flush "1.1.0 sender, new receiver, failed fsync" 6 "$work/src" "$old/msc" "$MSC" -- \
      MSC_TEST_FAIL_FSYNC=files
   echo "ok: 1.1.0 peers interoperate; with one, a long or failed flush fails as before"
   # Trees: a 1.1.0 receiver never beats, so a new sender waits as an old one
   # did. A 1.1.0 sender gets no beats either, and the new receiver still syncs
   # the whole tree, so a long sync fails a 1.1.0 sender (exit 70) even where
   # the old receiver's partial sync would have finished in time.
   reset_out
   tree "1.1.0 sender, new receiver, tree" 0 "$work/tree" "$old/msc" "$MSC" -- \
      MSC_TEST_FSYNC_DELAY_MS=500
   reset_out
   tree "new sender, 1.1.0 receiver, tree" 0 "$work/tree" "$MSC" "$old/msc" -- \
      MSC_TEST_FSYNC_DELAY_MS=500
   reset_out
   tree "new sender, 1.1.0 receiver, tree, long fsync" 70 "$work/tree" "$MSC" "$old/msc" -- \
      MSC_TEST_FSYNC_DELAY_MS=2500 MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576
   receiver_done
   reset_out
   tree "1.1.0 sender, new receiver, tree, long sync" 70 "$work/tree" "$old/msc" "$MSC" -- \
      MSC_TEST_FSYNC_DELAY_MS=2500 MSC_TEST_FSYNC_DELAY_MIN_BYTES=1048576
   receiver_done
   echo "ok: 1.1.0 peers interoperate on trees; with one, a long sync fails as before"
else
   echo "skip: cannot build v1.1.0 from a release tag here; mixed-version cases not run"
fi
