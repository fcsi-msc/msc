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

# The receiver flushes the destination before acknowledging it. Meanwhile it
# says it is alive, so a flush longer than the stall timeout -- one that shows
# no progress at all, as on a network filesystem -- does not fail a transfer
# whose data has arrived. The sender bounds the finish at the larger of
# --flush-timeout and 1 s per MiB transferred, and reports a destination that
# never confirms its data with exit 4. The fsync shim makes every fsync of a
# regular file take MSC_TEST_FSYNC_DELAY_MS.
shim="$(dirname "$MSC")/build/fsync_fault_shim.so"
head -c 1048576 /dev/urandom > "$work/mib"
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
# 1 MiB: the ceiling is max(1 s, 1 MiB at 1 MiB/s) = 1 s; the fsync takes 4 s.
flush "a stuck fsync past the ceiling" 4 "$work/mib" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_FSYNC_DELAY_MS=4000
grep -q "did not confirm it was durable within" "$work/log" \
   || { echo "FAIL: a stuck fsync did not report the ceiling"; sed -n '1,5p' "$work/log"; exit 1; }
# 5 MiB and a byte: the size term (about 5 s) outlasts --flush-timeout 1s.
flush "a 3 s fsync inside the size term" 0 "$work/src" "$MSC" "$MSC" --flush-timeout 1s -- \
   MSC_TEST_FSYNC_DELAY_MS=3000
echo "ok: a slow flush completes; one past --flush-timeout and the size term fails with exit 4"

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
   echo "ok: 1.1.0 peers interoperate; an older receiver's long flush fails as before"
else
   echo "skip: cannot build v1.1.0 from a release tag here; mixed-version cases not run"
fi
