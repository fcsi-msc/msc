#!/bin/sh
# lustre_dest_stripe_test.sh - the --dest-stripe-count cases that only a real
# Lustre filesystem can answer.
#
# dest_stripe_test.sh covers the CLI, the refused shapes, and the fact that the
# request reaches whichever process creates the destination.  What it cannot
# check anywhere is the thing the option is actually for: that the destination
# file comes out striped over the requested number of OSTs, whatever the source
# was striped over, and that the bytes are still identical.  That needs OSTs.
#
# Run it from the repository root on a cluster node whose /scratch is Lustre,
# with a peer it can ssh to:
#
#   make LUSTRE=1 all
#   REMOTE=otherhost sh tests/lustre_dest_stripe_test.sh
#
# Environment:
#   REMOTE  peer host for the transfer (default: this host, i.e. loopback ssh)
#   BASE    Lustre working directory (default: /scratch/$USER/msc-deststripe)
#   MSC     msc binary, must be visible at the same path on both ends
set -e

MSC="${MSC:-$PWD/msc}"
REMOTE="${REMOTE:-$(hostname -s)}"
LOCAL="$(hostname -s)"
BASE="${BASE:-/scratch/$USER/msc-deststripe}"
SIZE_MB="${SIZE_MB:-64}"

case "$MSC" in /*) ;; *) MSC="$PWD/$MSC" ;; esac
[ -x "$MSC" ] || { echo "no msc binary at $MSC (build with LUSTRE=1)"; exit 1; }

fstype=$(df -T "$(dirname "$BASE")" 2>/dev/null | tail -1 | awk '{print $2}')
if [ "$fstype" != lustre ]; then
   echo "SKIP: $(dirname "$BASE") is $fstype, not lustre -- nothing here is meaningful"
   exit 0
fi
OSTS=$(lfs df "$(dirname "$BASE")" | grep -c OST)
echo "== Lustre --dest-stripe-count tests: $LOCAL -> $REMOTE, $OSTS OSTs, ${SIZE_MB}MB files"
echo "== msc: $MSC"

rm -rf "$BASE"; mkdir -p "$BASE"
trap 'rm -rf "$BASE"' EXIT
# What a file created here inherits when nobody chooses a layout.
DIRDEF=$(lfs getstripe -c "$BASE")
echo "== destination directory default: c=$DIRDEF"

fail=0
casefail=0
note() { echo "   $*"; }
bad()  { echo "FAIL: $*"; fail=$((fail + 1)); casefail=$((casefail + 1)); }

# A source file with an exact layout: setstripe creates it empty with that
# layout, and dd's O_TRUNC keeps the inode, so the layout survives the write.
make_source()   # path, stripe count
{
   rm -f "$1"
   lfs setstripe -c "$2" "$1"
   dd if=/dev/urandom of="$1" bs=1M count="$SIZE_MB" status=none
}

stripes() { lfs getstripe -c "$1" 2>/dev/null; }

# One transfer that is expected to succeed, then everything that must be true
# about it: identical bytes, the requested destination width, and a source whose
# own layout was not touched.
ok_case()    # label, source stripe count, wanted dest count ("" = no flag), extra msc args...
{
   label="$1"; src_c="$2"; want="$3"; shift 3
   casefail=0
   src="$BASE/src.$$"; dst="$BASE/dst.$$"
   rm -f "$dst"
   make_source "$src" "$src_c"
   # Only the UDP receiver carries the source-layout heuristic; msc on TCP has
   # never created the destination with a Lustre layout at all, so with no flag
   # a TCP destination simply inherits the directory default. Verified against
   # an unpatched build -- it is the behaviour this option had to work around,
   # not something it changed.
   tcp=no
   for a in "$@"; do [ "$a" = "-T" ] && tcp=yes; done
   if [ -n "$want" ]; then set -- "$@" --dest-stripe-count "$want"; fi
   if ! "$MSC" -l "$LOCAL" -r "$REMOTE" -B "$MSC" "$@" \
        -i "$src" -o "$dst" >"$BASE/out.log" 2>&1; then
      bad "$label: transfer failed"
      sed -n '1,6p' "$BASE/out.log"
      rm -f "$src" "$dst"; return
   fi
   got=$(stripes "$dst")
   case "$want" in
      "")  [ "$tcp" = yes ] && expect="$DIRDEF" || expect="$src_c" ;;
      -1)  expect="$OSTS" ;;
      # Lustre clamps a request above the OST total rather than refusing it,
      # exactly as `lfs setstripe -c N` does; msc does not second-guess that.
      *)   [ "$want" -gt "$OSTS" ] && expect="$OSTS" || expect="$want" ;;
   esac
   [ "$got" = "$expect" ] || bad "$label: destination striped over $got OSTs, want $expect"
   cmp -s "$src" "$dst" || bad "$label: destination bytes differ from the source"
   still=$(stripes "$src")
   [ "$still" = "$src_c" ] \
      || bad "$label: the SOURCE layout changed ($src_c -> $still)"
   [ "$casefail" -eq 0 ] && note "ok: $label (src c=$src_c -> dst c=$got, bytes identical)"
   rm -f "$src" "$dst"
}

# A transfer that must be refused, leaving no destination behind for someone to
# mistake for a good one.
refuse_case()   # label, destination path, source stripe count, requested count, extra msc args...
{
   label="$1"; dst="$2"; src_c="$3"; want="$4"; shift 4
   src="$BASE/src.$$"
   rm -f "$dst"
   make_source "$src" "$src_c"
   if "$MSC" -l "$LOCAL" -r "$REMOTE" -B "$MSC" "$@" --dest-stripe-count "$want" \
        -i "$src" -o "$dst" >"$BASE/out.log" 2>&1; then
      bad "$label: transfer SUCCEEDED, but the layout could not be applied"
      note "destination came out at c=$(stripes "$dst")"
   elif ! grep -qi 'stripe count' "$BASE/out.log"; then
      bad "$label: refused, but the message never mentions the stripe count"
      sed -n '1,6p' "$BASE/out.log"
   elif [ -e "$dst" ]; then
      bad "$label: refused, but left a destination file behind"
   else
      note "ok: $label refused, no destination left behind"
   fi
   rm -f "$src" "$dst"
}

echo
echo "-- the two mismatch directions, which is the whole point"
ok_case "UDP narrow source, wide destination" 1 8
ok_case "UDP wide source, narrow destination" 8 1

echo
echo "-- matched widths, and no flag at all, must behave exactly as before"
ok_case "UDP matched request"        4 4
ok_case "UDP no flag (inherits src)" 8 ""
ok_case "UDP no flag (inherits src)" 1 ""

echo
echo "-- every OST, and the other transport"
ok_case "UDP all OSTs (-1)"  1 -1
ok_case "TCP wide from narrow" 1 8 -T
ok_case "TCP no flag (inherits dir default)" 4 "" -T

echo
echo "-- more OSTs than exist: Lustre clamps, same as lfs setstripe"
ok_case "over-request clamps to all OSTs" 1 $((OSTS + 56))

echo
echo "-- a destination that is not on Lustre cannot honour the request at all"
# The one refusal a Lustre build can still produce: no layout exists to apply,
# so the transfer must stop rather than quietly deliver an unstriped file.
NONLUSTRE="${NONLUSTRE:-$HOME/msc-deststripe-nonlustre}"
nl_fs=$(df -T "$(dirname "$NONLUSTRE")" 2>/dev/null | tail -1 | awk '{print $2}')
if [ "$nl_fs" = lustre ]; then
   note "skip: $(dirname "$NONLUSTRE") is also Lustre, nowhere to test the refusal"
else
   refuse_case "non-Lustre destination ($nl_fs)" "$NONLUSTRE" 1 8
fi

echo
echo "-- checkpointed transfer applies the layout to the partial it publishes"
src="$BASE/src.cp"; dst="$BASE/dst.cp"
make_source "$src" 1
if "$MSC" -l "$LOCAL" -r "$REMOTE" -B "$MSC" --dest-stripe-count 8 --force \
     --checkpoint "$BASE/cp.state" -i "$src" -o "$dst" >"$BASE/out.log" 2>&1; then
   got=$(stripes "$dst")
   [ "$got" = 8 ] || bad "checkpointed: destination striped over $got OSTs, want 8"
   cmp -s "$src" "$dst" || bad "checkpointed: bytes differ"
   [ "$got" = 8 ] && note "ok: checkpointed transfer published a c=8 destination"
else
   bad "checkpointed transfer failed"
   sed -n '1,6p' "$BASE/out.log"
fi
rm -f "$src" "$dst" "$BASE/cp.state"

echo
echo "-- multimachine: findzero creates the shared destination, so it owns the layout"
# Two sets against the same peer is a legitimate K=2 config. The shared file is
# created once by `msc -x` (findzero) and then opened in place by every worker,
# and its inode number is handed to them -- so the layout has to be right at
# creation and must never be re-made underneath them.
mm_src="$BASE/src.mm"; mm_dst="$BASE/dst.mm"
make_source "$mm_src" 1
rm -f "$mm_dst"
casefail=0
if "$MSC" -l "$LOCAL,$LOCAL" -r "$REMOTE,$REMOTE" -B "$MSC" --dest-stripe-count 8 \
     -i "$mm_src" -o "$mm_dst" >"$BASE/out.log" 2>&1; then
   got=$(stripes "$mm_dst")
   [ "$got" = 8 ] || bad "multimachine fresh: shared destination c=$got, want 8"
   cmp -s "$mm_src" "$mm_dst" || bad "multimachine fresh: bytes differ"
   [ "$casefail" -eq 0 ] && note "ok: multimachine created a c=8 shared destination, bytes identical"
else
   bad "multimachine fresh: transfer failed"
   sed -n '1,8p' "$BASE/out.log"
fi

# An existing shared destination cannot be relaid out -- its layout was fixed
# when it was created and workers are about to open that exact inode. Matching
# request: fine. Mismatched: must be refused, not silently written wide.
rm -f "$mm_dst"; lfs setstripe -c 8 "$mm_dst"
if "$MSC" -l "$LOCAL,$LOCAL" -r "$REMOTE,$REMOTE" -B "$MSC" --dest-stripe-count 8 \
     --force -i "$mm_src" -o "$mm_dst" >"$BASE/out.log" 2>&1; then
   note "ok: multimachine accepted an existing destination whose layout already matches"
else
   bad "multimachine matching-existing: refused a destination that does match"
   sed -n '1,8p' "$BASE/out.log"
fi

rm -f "$mm_dst"; lfs setstripe -c 1 "$mm_dst"
if "$MSC" -l "$LOCAL,$LOCAL" -r "$REMOTE,$REMOTE" -B "$MSC" --dest-stripe-count 8 \
     --force -i "$mm_src" -o "$mm_dst" >"$BASE/out.log" 2>&1; then
   bad "multimachine mismatched-existing: SUCCEEDED against a c=$(stripes "$mm_dst") destination"
elif ! grep -qi 'stripe count' "$BASE/out.log"; then
   bad "multimachine mismatched-existing: refused without naming the stripe count"
   sed -n '1,8p' "$BASE/out.log"
else
   note "ok: multimachine refused an existing destination whose layout cannot be changed"
fi
rm -f "$mm_src" "$mm_dst"

echo
echo "-- publication must not widen the destination's permissions"
src="$BASE/src.mode"
make_source "$src" 1
"$MSC" -l "$LOCAL" -r "$REMOTE" -B "$MSC" -i "$src" -o "$BASE/plain" >/dev/null 2>&1
"$MSC" -l "$LOCAL" -r "$REMOTE" -B "$MSC" --dest-stripe-count 8 \
   -i "$src" -o "$BASE/striped" >/dev/null 2>&1
m_plain=$(stat -c %a "$BASE/plain" 2>/dev/null)
m_striped=$(stat -c %a "$BASE/striped" 2>/dev/null)
if [ "$m_plain" = "$m_striped" ]; then
   note "ok: requested-layout destination has the same mode as an ordinary one ($m_striped)"
else
   bad "mode differs: ordinary $m_plain vs requested-layout $m_striped"
fi
rm -f "$src" "$BASE/plain" "$BASE/striped"

echo
if [ "$fail" -eq 0 ]; then
   echo "Lustre dest-stripe-count tests passed"
else
   echo "Lustre dest-stripe-count tests FAILED ($fail)"
   exit 1
fi
