#!/bin/sh
# transport_default_test.sh - which transport an msc command line actually gets.
#
# msc's default transport is reliable UDP.  -T selects TCP; -U names the default
# explicitly and stays valid for compatibility.  Because the UDP engine moves a
# file to a file, a transfer that reads stdin or writes to a -c program is TCP's:
# an implicit transport falls back so the documented `tar cf - | msc ... -c`
# pipeline keeps working, and an explicit -U on that shape is a CLI error rather
# than a silent change of transport.
#
# This needs its own suite because none of the C harnesses can see any of it:
# udp_test, segmented_test, resume_test, retry_test and recursive_test build
# `struct argdata` directly and never call parseargs(), so the entire default
# lives in code they do not execute.  Nothing here needs an sshd.
set -e

MSC="${MSC:-./msc}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

head -c 1048576 /dev/urandom > "$work/src"

# ---- the flag -x prints, which is the contract for restart command lines ----
expect_flag()
{
   want="$1"; shift
   got=$("$MSC" "$@" -l h1,h2 -r r1,r2 -i "$work/src" -o /dst/out -x \
         | awk 'NR==1 {print $2}')
   if [ "$got" != "$want" ]; then
      echo "FAIL: 'msc $* ... -x' printed transport '$got', want '$want'"
      exit 1
   fi
   echo "ok: msc ${*:-(no transport flag)} ... -> restart commands say $want"
}
expect_flag -U
expect_flag -T -T
expect_flag -U -U
# One setting, two spellings: the last one wins, as with every other option.
expect_flag -T -U -T
expect_flag -U -T -U

# A leading -x is the public partition printer when followed by normal CLI
# options; the private findzero invocation has a short positional argument
# list. Dynamic words in printed commands must survive shell tokenization.
python3 - "$MSC" "$work" <<'PY'
from pathlib import Path
import shlex
import subprocess
import sys

binary, work = sys.argv[1], Path(sys.argv[2])
source = work / "a'b $(id) source.bin"
destination = work / "copy 'here' file.bin"
remote_binary = "/tmp/msc's peer"
source.write_bytes(b"partition command quoting test")
result = subprocess.run([binary, "-x", "-n3", "-l", "localhost",
                         "-r", "127.0.0.1", "-u", "test'user",
                         "-B", remote_binary, "-i", str(source),
                         "-o", str(destination)], capture_output=True, text=True)
assert result.returncode == 0, result.stderr
lines = result.stdout.splitlines()
assert len(lines) == 1, lines
words = shlex.split(lines[0])
assert words[:2] == ["msc", "-U"], words
for flag, value in (("-l", "localhost"), ("-r", "127.0.0.1"),
                    ("-q", "1"), ("-u", "test'user"),
                    ("-B", remote_binary), ("-n", "3"),
                    ("-i", str(source)), ("-o", str(destination))):
    assert words[words.index(flag) + 1] == value, (flag, words)
print("ok: leading -x prints shell-safe restart commands with account and peer binary")
PY
test "$("$MSC" -x 1 "$work/nonexistent-findzero-destination" 0)" = '0,0'
echo 'ok: private positional -x findzero invocation still works'

# ---- end to end.  There is no sshd on a build host, so MSC_SSH substitutes a
# stand-in that runs the remote command locally with ssh's stdin/stdout wiring.
cat > "$work/fakessh" <<'EOF'
#!/bin/sh
while [ "$1" = "-o" ]; do shift 2; done
shift
exec /bin/sh -c "$*"
EOF
chmod +x "$work/fakessh"
MSC_SSH="$work/fakessh"; export MSC_SSH

python3 - "$MSC" "$work" <<'PY'
from pathlib import Path
import os
import subprocess
import sys

binary, work = sys.argv[1], Path(sys.argv[2])
source = work / "copy's input file.bin"
destination = work / "copy's destination file.bin"
source.write_bytes(b"restart command executes with quoted paths")
printed = subprocess.check_output([binary, "-x", "-n4", "-l", "localhost",
                                   "-r", "localhost", "-B", str(Path(binary).resolve()),
                                   "-i", str(source), "-o", str(destination)], text=True)
env = os.environ.copy()
env["PATH"] = str(Path(binary).resolve().parent) + os.pathsep + env.get("PATH", "")
result = subprocess.run(["sh", "-c", printed], env=env, capture_output=True,
                        text=True, timeout=120)
assert result.returncode == 0, result.stderr
assert destination.read_bytes() == source.read_bytes()
print("ok: a printed restart command executes and preserves quoted file bytes")
PY

transfer()      # label, then the transport flag(s) under test
{
   label="$1"; shift
   timeout 120 "$MSC" -n4 -l localhost -r localhost -B "$MSC" "$@" \
      -i "$work/src" -o "$work/out.$label" >/dev/null 2>"$work/log.$label" \
      && rc=0 || rc=$?
   if [ "$rc" -ne 0 ]; then
      echo "FAIL: $label transfer exited $rc"
      sed -n '1,5p' "$work/log.$label"
      exit 1
   fi
   cmp -s "$work/src" "$work/out.$label" \
      || { echo "FAIL: $label transfer corrupted the file"; exit 1; }
}
transfer default
transfer tcp -T
transfer udp -U
echo "ok: implicit, -T and -U all deliver the file intact"

# ---- stdin and -c belong to TCP -------------------------------------------
# The implicit case must fall back rather than break the documented pipeline.
rc=0
cat "$work/src" | timeout 120 "$MSC" -n4 -l localhost -r localhost -B "$MSC" \
   -c "cat > '$work/out.pipe'" >/dev/null 2>"$work/log.pipe" || rc=$?
if ! cmp -s "$work/src" "$work/out.pipe"; then
   echo "FAIL: piped -c transfer corrupted the file or produced nothing"
   echo "      (msc exited $rc; wrote $(wc -c < "$work/out.pipe" 2>/dev/null || echo no) of $(wc -c < "$work/src") bytes); msc reported:"
   tail -20 "$work/log.pipe" | sed 's/^/   /'
   exit 1
fi
echo "ok: stdin | msc ... -c CMD still transfers under the UDP default"

# ---- the pipe path itself --------------------------------------------------
# Larger than one 2 MiB receive buffer, from stdin and from a file.  By the time
# msc returns, the command must have finished: seen end of input and run what
# follows it.
head -c 8388608 /dev/urandom > "$work/big"
for shape in stdin file; do
   rm -f "$work/out.big" "$work/done.big"
   rc=0
   if [ "$shape" = stdin ]; then
      timeout 120 "$MSC" -n4 -l localhost -r localhost -B "$MSC" \
         -c "cat > '$work/out.big'; echo done > '$work/done.big'" \
         < "$work/big" >/dev/null 2>"$work/log.big" || rc=$?
   else
      timeout 120 "$MSC" -n4 -l localhost -r localhost -B "$MSC" -i "$work/big" \
         -c "cat > '$work/out.big'; echo done > '$work/done.big'" \
         >/dev/null 2>"$work/log.big" || rc=$?
   fi
   if [ "$rc" -ne 0 ] || ! cmp -s "$work/big" "$work/out.big" ||
      [ ! -f "$work/done.big" ]; then
      echo "FAIL: 8 MiB $shape -> -c: msc exited $rc, $(wc -c < "$work/out.big" 2>/dev/null || echo no) of 8388608 bytes arrived, and the command"
      if [ -f "$work/done.big" ]; then echo "      finished"; else echo "      had not finished (never saw end of input?)"; fi
      echo "      msc reported:"
      tail -20 "$work/log.big" | sed 's/^/   /'
      exit 1
   fi
   echo "ok: 8 MiB $shape -> -c arrives intact and the command has finished"
done

# A command that fails is a destination failure, not a successful transfer.
rc=0
"$MSC" -n4 -l localhost -r localhost -B "$MSC" -i "$work/src" \
   -c "cat > /dev/null; exit 3" >/dev/null 2>"$work/log.cmdfail" || rc=$?
if [ "$rc" -ne 4 ]; then
   echo "FAIL: a -c command that exited 3 made msc exit $rc, want 4 (destination)"
   tail -5 "$work/log.cmdfail" | sed 's/^/   /'
   exit 1
fi
echo "ok: a failing -c command makes msc exit 4"

# Proof that it fell back, rather than UDP quietly coping: a UDP-only control
# topology is refused on this shape, and the refusal names the reason.
if MSC_UDP_CTL=stdio1 "$MSC" -n4 -l localhost -r localhost -B "$MSC" \
      -i "$work/src" -c "cat > /dev/null" </dev/null >/dev/null 2>"$work/ctl.err"; then
   echo "FAIL: MSC_UDP_CTL=stdio1 was accepted on a -c transfer, so it never"
   echo "      left the UDP path"
   exit 1
fi
if ! grep -q 'runs on TCP (the output goes to a -c program)' "$work/ctl.err"; then
   echo "FAIL: the -c transfer was refused for the wrong reason:"
   cat "$work/ctl.err"
   exit 1
fi
echo "ok: the -c fallback really is on TCP, and says so"

# An explicit -U on either shape is a CLI error (exit 2), not a fallback.
expect_cli_error()  # label, expected message fragment, then msc args
{
   label="$1"; want="$2"; shift 2
   "$MSC" "$@" -n4 -l localhost -r localhost -B "$MSC" \
      </dev/null >/dev/null 2>"$work/err.$label" && rc=0 || rc=$?
   if [ "$rc" -ne 2 ]; then
      echo "FAIL: $label exited $rc, want 2 (CLI)"
      cat "$work/err.$label"
      exit 1
   fi
   if ! grep -q "$want" "$work/err.$label"; then
      echo "FAIL: $label was rejected, but not for '$want':"
      cat "$work/err.$label"
      exit 1
   fi
   echo "ok: $label refused with exit 2"
}
expect_cli_error "-U with a stdin source" "reading the source from stdin" \
   -U -o "$work/x"
expect_cli_error "-U with a -c destination" "sending the output to a -c program" \
   -U -i "$work/src" -c "cat"

echo "transport default tests passed"
