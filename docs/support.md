# Supported configurations and limits

| Configuration | Validation / support scope |
| --- | --- |
| Linux x86-64, GCC and Clang, glibc | Primary development platform; Ubuntu 24.04 CI builds and tests. |
| IPv4 UDP, SSH stdio control | Default. Requires reachable advertised UDP data ports and return ACKs. |
| IPv4 TCP (`-T`) | Regression-tested file, tree, pipe, and command paths. |
| GSO/GRO available or unavailable | Attempted by default; fallback paths have regression tests. |
| Linux with Lustre client APIs | Optional `LUSTRE=1`; needs separate real-Lustre validation. Ordinary CI does not prove OST placement or performance. |
| Other architectures / libc implementations | Not covered by the current CI matrix. |
| Windows, macOS, IPv6 data endpoints | Not currently supported by this implementation. |

Runtime uses Linux batching syscalls and pthreads. Offload support is detected at
runtime; it is not a prerequisite. Tests additionally need Python 3, GNU coreutils,
and a shell. ShellCheck and Valgrind are development tools rather than runtime
dependencies. The executable requires no third-party transport library.

Both peers must use compatible wire-protocol versions. The current protocol is
version 6, and checkpoint version 2 stores byte ranges; version 1 checkpoints
are rejected. An unchanged application version string alone does not prove
matching wire versions in development snapshots. Builds with unchanged version
6 messages and checkpoint version 2 were checked in both sender directions,
including empty/small files, recursive metadata, and interrupted resume.

Data connections have the trust boundary described in [SECURITY.md](../SECURITY.md).
The default rate controller has not been established as fair to competing TCP
flows on arbitrary Internet paths; its ECN response needs further development.
The aggregate pacing cap currently counts fresh payload, not all wire traffic.

Recursive transfers currently open the file table for the transfer. Very large
trees can exceed descriptor or mapping limits; the retransmission budget does
not bound these resources. MTU is discovered at setup; path changes during a
session can require a restart, optionally from a byte-based checkpoint. Initial
mapped-I/O allocation failure is tested; power failures, late device errors, and
thin-provisioning exhaustion still need storage-specific checks.

See [performance](performance.md) and [correctness](correctness-and-resume.md)
for deployment limits and measurement guidance.
