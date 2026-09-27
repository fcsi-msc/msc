#!/bin/sh
# Optional real full-filesystem test. Only the private mount namespace needs root;
# both MSC endpoints run as the invoking user. No host filesystem is filled.
set -eu
if [ "${1:-}" != --inside ]; then
   test "$(id -u)" -ne 0 || { echo 'Run as an ordinary user with sudo access.' >&2; exit 2; }
   work=$(mktemp -d "${TMPDIR:-/tmp}/msc-fullfs.XXXXXX")
   trap 'rm -rf "$work"' EXIT
   mkdir "$work/full"
   head -c 2097152 /dev/urandom > "$work/source"
   cat > "$work/ssh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
   chmod +x "$work/ssh"
   sudo -n timeout -k 2 30 unshare --mount --propagation private \
      sh "$PWD/mapped_enospc_test.sh" --inside "$work" "$PWD" "$(id -un)"
   exit
fi

test "$(id -u)" -eq 0
work=$2
source_dir=$3
test_user=$4
mount -t tmpfs -o size=1m,mode=1777,nodev,nosuid,noexec msc-enospc "$work/full"
trap 'umount "$work/full"' EXIT
rc=0
runuser -u "$test_user" -- env MSC_SSH="$work/ssh" \
   "$source_dir/msc" -n2 --stats --stall-timeout 2s \
   -l localhost -r 127.0.0.1 -B "$source_dir/msc" \
   -i "$work/source" -o "$work/full/destination" >"$work/transfer.log" 2>&1 || rc=$?
cat "$work/transfer.log"
test "$rc" -eq 4
test ! -e "$work/full/destination"
grep -q 'cannot reserve destination storage: No space left on device' "$work/transfer.log"
echo 'ok: real 1 MiB tmpfs rejects a 2 MiB mapped transfer with exit 4 and no published file'
