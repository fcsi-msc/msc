#!/bin/sh
# dest_stripe_test.sh - everything about --dest-stripe-count that does NOT need
# a Lustre filesystem.
#
# The option chooses how many OSTs the DESTINATION file is striped over,
# independently of the source's layout.  Actual OST placement can only be
# checked on Lustre (see lustre_dest_stripe_test.sh), but everything around it is testable
# here, and most of it is exactly what silently breaks:
#
#   * the CLI bounds, so a typo fails before an ssh launch rather than after;
#   * the shapes that create no single destination file (-R, -c);
#   * that the request actually REACHES the far side -- on a build without
#     HAVE_LUSTRE the receiver must refuse the transfer, and a refusal from the
#     receiver is proof the value crossed the launch, whichever carrier took it
#     (UDP greeting, forwarded environment, -x prefix, per-pair child flag);
#   * that it never degrades into "transferred fine, wrong layout", which is
#     the failure the whole feature exists to prevent.
#
# Nothing here needs an sshd: MSC_SSH substitutes a stand-in that runs the
# remote command locally with ssh's stdin/stdout wiring, so the real remote
# command string -- environment prefix included -- is what gets executed.
set -e

MSC="${MSC:-./msc}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

head -c 262144 /dev/urandom > "$work/src"

cat > "$work/fakessh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
chmod +x "$work/fakessh"
MSC_SSH="$work/fakessh"; export MSC_SSH

# msc built with LUSTRE=1 can really apply a layout, so the "no Lustre here"
# assertions below would be testing nothing.  Skip them rather than pass them.
have_lustre=no
if [ -n "$MSC_TEST_HAVE_LUSTRE" ]; then have_lustre=yes; fi

# ---- CLI bounds ------------------------------------------------------------
# -x prints restart command lines without transferring anything, so it exercises
# the parser alone.  It must not be argv[1]: a leading -x routes to findzero.
parses()      # label, then msc args
{
   label="$1"; shift
   "$MSC" "$@" -l h1 -r r1 -i "$work/src" -o /dst/out -x >/dev/null 2>&1 \
      || { echo "FAIL: $label was rejected, but is valid"; exit 1; }
   echo "ok: $label accepted"
}
rejects()     # label, expected message fragment, then msc args
{
   label="$1"; want="$2"; shift 2
   "$MSC" "$@" -l h1 -r r1 -i "$work/src" -o /dst/out -x \
      >/dev/null 2>"$work/err" && rc=0 || rc=$?
   if [ "$rc" -ne 2 ]; then
      echo "FAIL: $label exited $rc, want 2 (CLI)"; cat "$work/err"; exit 1
   fi
   if ! grep -q "$want" "$work/err"; then
      echo "FAIL: $label was rejected, but not for '$want':"; cat "$work/err"
      exit 1
   fi
   echo "ok: $label refused with exit 2"
}

parses "--dest-stripe-count 1"  --dest-stripe-count 1
parses "--dest-stripe-count 8"  --dest-stripe-count 8
parses "--dest-stripe-count -1" --dest-stripe-count -1
rejects "--dest-stripe-count 0"       "must be an OST count" --dest-stripe-count 0
rejects "--dest-stripe-count -2"      "must be an OST count" --dest-stripe-count -2
rejects "--dest-stripe-count abc"     "must be an OST count" --dest-stripe-count abc
rejects "--dest-stripe-count 8x"      "must be an OST count" --dest-stripe-count 8x
rejects "--dest-stripe-count 99999"   "must be an OST count" --dest-stripe-count 99999

# ---- shapes with no single destination file --------------------------------
"$MSC" --dest-stripe-count 4 -R -l h1 -r r1 -i "$work" -o /dst/out -x \
   >/dev/null 2>"$work/err" && rc=0 || rc=$?
[ "$rc" -eq 2 ] || { echo "FAIL: -R with the option exited $rc, want 2"; exit 1; }
grep -q 'creates a whole tree' "$work/err" \
   || { echo "FAIL: -R rejection did not explain itself:"; cat "$work/err"; exit 1; }
echo "ok: -R refused, and says to stripe the directory instead"

cat "$work/src" | "$MSC" --dest-stripe-count 4 -n2 -l localhost -r localhost \
   -B "$MSC" -c "cat > /dev/null" >/dev/null 2>"$work/err" && rc=0 || rc=$?
[ "$rc" -eq 2 ] || { echo "FAIL: -c with the option exited $rc, want 2"; exit 1; }
grep -q 'goes to a -c program' "$work/err" \
   || { echo "FAIL: -c rejection did not explain itself:"; cat "$work/err"; exit 1; }
echo "ok: -c destination refused (there is no destination file to lay out)"

# ---- the environment carrier -----------------------------------------------
# A hand-set MSC_DEST_STRIPE_COUNT is a request too, so it gets the same bounds
# and the same shape rules -- silently ignoring it would strand a benchmark arm
# on a default-striped destination.
MSC_DEST_STRIPE_COUNT=nonsense "$MSC" -l h1 -r r1 -i "$work/src" -o /dst/out -x \
   >/dev/null 2>"$work/err" && rc=0 || rc=$?
[ "$rc" -eq 2 ] || { echo "FAIL: bad MSC_DEST_STRIPE_COUNT exited $rc, want 2"; exit 1; }
grep -q 'MSC_DEST_STRIPE_COUNT must be' "$work/err" \
   || { echo "FAIL: bad MSC_DEST_STRIPE_COUNT rejected for the wrong reason:";
        cat "$work/err"; exit 1; }
echo "ok: a malformed MSC_DEST_STRIPE_COUNT fails loudly"

MSC_DEST_STRIPE_COUNT=4 "$MSC" -R -l h1 -r r1 -i "$work" -o /dst/out -x \
   >/dev/null 2>"$work/err" && rc=0 || rc=$?
[ "$rc" -eq 2 ] || { echo "FAIL: env request with -R exited $rc, want 2"; exit 1; }
echo "ok: an environment-only request obeys the same shape rules as the flag"

# Precedence: the flag wins over an inherited value, and the restart command
# lines -x prints are where the resolved value becomes observable.
got=$(MSC_DEST_STRIPE_COUNT=4 "$MSC" --dest-stripe-count 8 -l h1 -r r1 \
        -i "$work/src" -o /dst/out -x | sed -n '1s/.*--dest-stripe-count \([-0-9]*\).*/\1/p')
[ "$got" = "8" ] \
   || { echo "FAIL: flag 8 with MSC_DEST_STRIPE_COUNT=4 resolved to '$got'"; exit 1; }
echo "ok: --dest-stripe-count wins over an inherited MSC_DEST_STRIPE_COUNT"

got=$(MSC_DEST_STRIPE_COUNT=4 "$MSC" -l h1 -r r1 -i "$work/src" -o /dst/out -x \
        | sed -n '1s/.*--dest-stripe-count \([-0-9]*\).*/\1/p')
[ "$got" = "4" ] \
   || { echo "FAIL: inherited MSC_DEST_STRIPE_COUNT=4 resolved to '$got'"; exit 1; }
echo "ok: without the flag, an inherited MSC_DEST_STRIPE_COUNT still applies"

if [ "$have_lustre" = yes ]; then
   echo "dest stripe count tests passed (Lustre build: end-to-end refusal cases"
   echo "  skipped; run lustre_dest_stripe_test.sh on Lustre instead)"
   exit 0
fi

# ---- it reaches the far side, and never degrades quietly -------------------
# Without HAVE_LUSTRE no layout can be applied at all, so every one of these
# must fail as a destination error (4).  Success here would mean the request was
# dropped somewhere between the CLI and the process that creates the file --
# exactly the silent-wrong-layout outcome the feature must not have.
#
# The exit code the INITIATOR reports differs by transport, and that difference
# is pre-existing rather than anything to do with this option: UDP carries a
# typed receiver-failure record, so the destination error (4) arrives intact,
# while a TCP receiver that dies during launch only ever surfaced to the
# initiator as a network error (6) -- an unwritable -o path has always behaved
# this way.  Either way the receiver's own message names the real cause, so
# that is what both cases assert.
end_to_end()   # label, expected initiator exit code, then msc args
{
   label="$1"; want="$2"; shift 2
   rm -f "$work/out"
   timeout 120 "$MSC" -n2 -l localhost -r localhost -B "$MSC" "$@" \
      -i "$work/src" -o "$work/out" >/dev/null 2>"$work/log" && rc=0 || rc=$?
   if [ "$rc" -eq 0 ]; then
      echo "FAIL: $label reported success, but this build cannot apply any"
      echo "      Lustre layout -- the request was silently dropped"
      exit 1
   fi
   if [ "$rc" -ne "$want" ]; then
      echo "FAIL: $label exited $rc, want $want"
      sed -n '1,6p' "$work/log"
      exit 1
   fi
   if ! grep -q 'stripe count' "$work/log"; then
      echo "FAIL: $label failed without naming the stripe count:"
      sed -n '1,6p' "$work/log"
      exit 1
   fi
   echo "ok: $label refused ($rc), naming the layout it could not apply"
}
end_to_end "UDP transfer"     4 -U --dest-stripe-count 8
end_to_end "TCP transfer"     6 -T --dest-stripe-count 8
end_to_end "UDP, all OSTs"    4 -U --dest-stripe-count -1
end_to_end "UDP checkpointed" 4 -U --dest-stripe-count 4 \
   --checkpoint "$work/cp" --force

# The same, through the paths that carry the value without a greeting: the
# environment prefix on the -x findzero command, and the per-pair child flag.
#
# The exact code matters here, not just "nonzero": findzero refusing the layout
# is the one probe reply that carries no file state, and the initiator has to
# turn that into exit 4 rather than blocking on a pipe whose write end it also
# holds.  A timeout (124) would satisfy "nonzero" while hanging for two minutes.
rm -f "$work/out"
timeout 120 "$MSC" -n2 -q 2 -l localhost,localhost -r localhost,localhost \
   -B "$MSC" --dest-stripe-count 8 -i "$work/src" -o "$work/out" \
   >/dev/null 2>"$work/log" && rc=0 || rc=$?
if [ "$rc" -ne 4 ]; then
   echo "FAIL: the multimachine transfer exited $rc, want 4 (destination)"
   [ "$rc" -eq 124 ] && echo "      (124 = it hung: the -x probe failure did not reach the initiator)"
   sed -n '1,6p' "$work/log"
   exit 1
fi
if ! grep -q 'stripe count' "$work/log"; then
   echo "FAIL: the multimachine transfer failed without naming the stripe count:"
   sed -n '1,6p' "$work/log"
   exit 1
fi
echo "ok: the multimachine -x/child path carries the request too"

# And the shape it must not break: multimachine with no request at all.
# MSC_UDP_STATS reports each receiver's write path.  The pairs share one
# destination, so none may take the mmap path: it ftruncate()s the file to its
# own slice, cutting off whatever the other pair has already written -- a race
# that loses data only sometimes, which is why the check is on the path taken.
rm -f "$work/out"
MSC_UDP_STATS=1 timeout 120 "$MSC" -n2 -q 2 -l localhost,localhost \
   -r localhost,localhost -B "$MSC" -i "$work/src" -o "$work/out" \
   >/dev/null 2>"$work/log" \
   || { echo "FAIL: a multimachine transfer without the option stopped working"
        sed -n '1,6p' "$work/log"; exit 1; }
cmp -s "$work/src" "$work/out" \
   || { echo "FAIL: a multimachine transfer without the option corrupted the file"
        exit 1; }
if grep -q 'write-path: mmap [1-9]' "$work/log"; then
   echo "FAIL: a multimachine receiver mapped the shared destination:"
   grep 'write-path' "$work/log"
   exit 1
fi
if ! grep -q 'write-path: mmap 0' "$work/log"; then
   echo "FAIL: no receiver reported its write path, so the check above proves nothing"
   exit 1
fi
echo "ok: multimachine without the option transfers intact without mapping the shared file"

# A transfer with no request must be completely unaffected by any of this.
rm -f "$work/out"
timeout 120 "$MSC" -n2 -l localhost -r localhost -B "$MSC" \
   -i "$work/src" -o "$work/out" >/dev/null 2>"$work/log" \
   || { echo "FAIL: a transfer without the option stopped working"
        sed -n '1,6p' "$work/log"; exit 1; }
cmp -s "$work/src" "$work/out" \
   || { echo "FAIL: a transfer without the option corrupted the file"; exit 1; }
echo "ok: without the option nothing changes -- the file transfers intact"

echo "dest stripe count tests passed"
