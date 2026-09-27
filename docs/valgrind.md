# Memory checking MSC with Valgrind

How to run repeatable memory and thread-race checks on MSC. The checks target the
**in-process test harnesses** (`*_test.c`), which exercise the sender and
receiver over socketpairs/loopback — no SSH — so Memcheck sees the whole data path
in one process tree.

The harnesses make local coverage repeatable. Valgrind follows `fork` children,
but following `exec` requires `--trace-children=yes`; even that does not
instrument a process running on another host through SSH. A real remote run
requires arranging instrumentation on each endpoint separately.

## Install

```bash
sudo apt install valgrind     # Debian/Ubuntu
valgrind --version
```

## Quick path: `make valgrind`

From the repository root. Build clean first so the harnesses are compiled unoptimised
(`-O0 -g`) for accurate line numbers:

```bash
make clean && make valgrind
```

This builds the six harnesses, generates fixtures under `/tmp/mscvg`, and runs
Memcheck over the main file-transfer paths:

| Harness | Path exercised |
|---|---|
| `segmented_test` | single-file TCP sender + receiver |
| `udp_test` (clean) | reliable-UDP control channel + data |
| `udp_test … 25` | reliable UDP with 25% injected loss → ACK/SACK retransmission recovery |
| `recursive_test` | recursive directory transfer + manifest |
| `resume_test` | checkpoint atomicity/corruption, mutation rejection, interruption/resume, progress, and cancellation |
| `retry_test` | transient recovery, permanent failure filtering, and cancellation during backoff |
| `child_status_test` | fork / child-exit handling |

It also `diff -r`s the recursive output (correctness) and **exits non-zero on any
leak or error** (`--error-exitcode=1`), so it works as a pass/fail gate.

The legacy stdin/remote-command pipe path is not represented by these
in-process harnesses; it needs an SSH-level integration run.

Thread-race pass (separate, because Helgrind is noisy):

```bash
make helgrind
make helgrind_udp
```

`helgrind_udp` enables receiver statistics and checks both dedicated data sockets
and a shared socket with forced GSO/GRO fallback. It fails on detected races and
compares the resulting bytes. This is also a CI gate; it uses unique temporary
fixtures and a bounded timeout.
The suppression file excludes glibc's internal timed-wait replacement signal,
which Helgrind 3.22 reports as an unlocked application signal. It matches the
immediate libc frames only; application data races remain errors.

Overridable make variables: `VALGRIND` (binary/wrapper), `VG_DIR` (fixture dir,
default `/tmp/mscvg`).

## Manual path

If you want to run one harness or tweak arguments:

```bash
make clean && make CFLAGS="-O0 -g" segmented_test udp_test recursive_test resume_test retry_test child_status_test

mkdir -p /tmp/mscvg
head -c 16M /dev/urandom > /tmp/mscvg/in.bin

valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes \
         --num-callers=40 --error-exitcode=1 \
         ./segmented_test /tmp/mscvg/in.bin /tmp/mscvg/out.bin 8 1048576 1
```

Harness arguments:

| Harness | Arguments |
|---|---|
| `segmented_test` | `SRC DST streams segsize expect_success` |
| `udp_test` | `SRC DST streams [drop_pct]` |
| `recursive_test` | `SRC_DIR DST_DIR streams segsize` |
| `resume_test` | `SRC DST` |
| `retry_test` | (none) |
| `child_status_test` | (none) |

The recursive fixture should have nested subdirs and an empty file to cover the
manifest edge cases:

```bash
mkdir -p /tmp/mscvg/rsrc/sub1 /tmp/mscvg/rsrc/sub2/deep
head -c 5M /dev/urandom > /tmp/mscvg/rsrc/a.bin
head -c 1M /dev/urandom > /tmp/mscvg/rsrc/sub1/b.bin
head -c 3M /dev/urandom > /tmp/mscvg/rsrc/sub2/deep/c.bin
: > /tmp/mscvg/rsrc/empty.bin
valgrind --leak-check=full --track-origins=yes ./recursive_test /tmp/mscvg/rsrc /tmp/mscvg/rdst 8 1048576
diff -r /tmp/mscvg/rsrc /tmp/mscvg/rdst && echo MATCH
```

### Flag reference

- `--leak-check=full` / `--show-leak-kinds=all` — report every leak with its
  allocation stack.
- `--track-origins=yes` — trace where an uninitialised value came from (slower).
- `--num-callers=40` — deeper stack traces.
- `--error-exitcode=1` — return non-zero if any error is found (CI/pass-fail).

## Reading the output

- The harnesses **`fork`**, so you get one report per process (parent + sender +
  receiver). Valgrind follows `fork` children automatically.
- Clean result per process: **`All heap blocks were freed`** and
  **`ERROR SUMMARY: 0 errors`**.
- Leak kinds: **`definitely lost`** is a real leak — fix it; **`still reachable`**
  is allocated-but-still-pointed-to at exit, usually benign.
- **Crucial distinction — whose leak is it?** Look at the allocation stack:
  - stack in `*_test.c` → a **harness** leak (benign to the real program, but fix
    it so it doesn't mask a real one);
  - stack in library code (`recursive.c`, `udp_transport.c`, `local_sockets.c`,
    `remote.c`, `fork.c`, `netutil.c`) → a **real MSC leak**.

Keeping the harnesses leak-clean matters: a known harness leak hides the next real
one. (Example: a `udp_test.c` `make_argdata` leak was found and fixed for exactly
this reason — it was test-only, but it polluted the baseline.)

## Thread races (Helgrind)

Memcheck does not detect data races. MSC's workers are threaded (the atomic-cursor
sender in `local_sockets.c`, the `pwrite` receiver workers in `remote.c`), so do a
race pass once Memcheck is clean:

```bash
make helgrind
# or manually:
valgrind --tool=helgrind --num-callers=40 ./segmented_test /tmp/mscvg/in.bin /tmp/mscvg/out.bin 8 1048576 1
```

Inspect full stacks to distinguish runtime-library reports from MSC ownership
bugs. This target covers the segmented TCP harness, not all UDP controller,
demultiplexer, or checkpoint concurrency. Reports involving other MSC source
files should also be investigated when they occur.

## Notes

- Keep inputs small (16 MiB / 8 streams here). Valgrind runs ~10–30× slower and
  serializes threads.
- `-O0 -g` is recommended for clean line numbers; the default `-O2 -g` also works
  but can give fuzzier locations and occasional optimiser-induced noise.
