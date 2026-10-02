# Contributing to MSC

Use a Linux development machine with GCC or Clang, GNU make, Python 3, and a
POSIX shell. OpenSSH is needed for real peers; local tests use a stand-in.
Optional tools are ShellCheck and Valgrind. Lustre builds require its development
headers and library (`make LUSTRE=1`). See [supported configurations](docs/support.md).

## Layout

| Path | Contents |
| --- | --- |
| `src/` | The msc program (see the [source map](docs/architecture.md#source-map)). |
| `tests/` | C test harnesses, shell and Python suites, Valgrind suppressions. |
| `tests/shims/` | Test-only `LD_PRELOAD` shims (WAN emulator, fault injectors). |
| `build/` | Every build product except `msc` itself (ignored by git). |
| `benchmarks/`, `contrib/`, `docs/` | Benchmark runners, Slurm launcher, documentation. |

Run the suites and harnesses from the repository root, for example
`make udp_test` and then `./build/udp_test`.

## Development

Describe the failure or workload a change addresses. Preserve the stable exit
classes and wire-format checks. Add a regression that observes the failed
behavior, such as incorrect bytes, missing durability operations, or lost
metadata. Compare received bytes independently of MSC's own checksums.

```sh
make clean
make -j8 CFLAGS='-O2 -g -Werror' all
make check
shellcheck -s sh -S warning tests/*.sh contrib/slurm/msc-srun
shellcheck -S warning contrib/slurm/msc-slurm-smoke.sh
```

`make check` runs its socket-using suites serially even under `make -j`.
Build sanitizer and alternative-compiler configurations in separate directories
or clean between them. See [testing](docs/testing.md) and
[race and memory checks](docs/valgrind.md). Do not suppress application races to
make a gate pass.

For performance changes, record revision and executable hashes, both endpoint
configurations, flow/socket counts, payload, storage, and kernel socket limits.
Interleave old/new builds over repeated transfers. Report end-to-end completion,
CPU, memory, and independent integrity checks. Avoid timing compilation or
verification as transfer work. Use [the benchmark runners](benchmarks/README.md).

Protocol-layout changes require an explicit version decision and updates to
[the wire reference](docs/wire-protocol.md). New remote environment settings must
be added to the allowlist. Explain compatibility and update user documentation
when changing defaults or completion guarantees.

## Preparing a release

1. Resolve release-blocking failures and update `CHANGELOG.md`. Choose the
   release version in `src/msc.h`; the uncommitted working tree is not a release.
2. Pass GCC/Clang warning builds, `make check`, sanitizers, and `make helgrind_udp`.
   CI also builds a Git source archive and verifies staged installation.
3. Test the release candidate on two hosts. Verify bytes, interruption/resume,
   the intended storage, and impairment recovery. When the wire and checkpoint
   versions are unchanged, test old/new peers in both sender directions,
   including small files, trees, and a resumed transfer. Retain logs and hashes.
4. Review [SECURITY.md](SECURITY.md) and confirm the supported configurations
   and known limits. After approval to make the repository public, enable
   GitHub private vulnerability reporting and verify that the link in
   `SECURITY.md` opens the reporting form before publishing the release.
5. Tag the reviewed commit and create the source archive with `git archive`.
   Check that it contains the changelog and security policy and excludes local
   engineering reports and raw benchmark results. Publish the archive's SHA-256
   alongside release notes and compatibility information. Publishing and signing
   are maintainer actions; CI does not automatically publish a release.

Contributions use the repository's [MIT license](LICENSE). Send security issues
through the process in [SECURITY.md](SECURITY.md), without public exploit details.
