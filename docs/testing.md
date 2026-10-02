# Testing MSC

The local harnesses exercise real transfer code with loopback endpoints. CLI
suites substitute a local command runner for SSH. They require no second host,
SSH daemon, or root, but the environment must permit local sockets and process
creation. A sandbox that forbids UDP binds cannot run these integration tests.

## Fast path for a new congestion controller

```sh
make udp_test
sh docs/examples/check-controller.sh rate
```

Replace `rate` with your registered controller name. The helper checks clean
delivery, synthetic loss, byte equality, and the selected controller's telemetry.
[Adding a controller](adding-a-controller.md) includes a working example and
an impaired-WAN command. This is the shortest route from an algorithm edit to
a real file-transfer experiment.

## Regression suites

From the repository root:

```sh
make clean all
make check
```

| Target | Coverage |
| --- | --- |
| `udp_parity` | Clean/loss transfers, byte comparisons, offsets, recursive manifests, payload/PMTUD policy, offload fallback, ports, checksums, and protocol rejection. Includes recursive control-mode tests. |
| `udp_resume_suite` | Single-file/tree checkpoint interruption, content mutation, storage failure, signals, packet-size changes, and metadata recovery. |
| `resume_suite` | TCP checkpoints, recursive resume, retry filtering, exit classes, and stall timeouts; includes UDP resume, stdio, transport-default, and stripe suites. |
| `stdio_ctl_suite` | CLI stdio modes, content checks, listener instrumentation, and environment presets. |
| `transport_suite` | UDP default, explicit `-T`/`-U`, stdin/command TCP fallback, and invalid combinations. |
| `dest_stripe_suite` | Destination-stripe option parsing, restrictions, and forwarding. |
| `wanshim` | Synthetic WAN smoke, a WAN profile with throughput/retransmission gates, and MTU black-hole behavior. |
| `shim_check` | Emulator passthrough, delay, loss, and seeded behavior; GSO/sendmmsg datagram equivalence, MTU, scatter/gather and TOS preservation. |
| `udp_regressions` | Small MTUs, exhausted PMTUD, bounded ring wraparound under loss, fsync worker failures and syscall coverage, publication errors, and growing hardlink indexes. |
| `valgrind`, `helgrind`, `helgrind_udp` | Memory checks, TCP worker races, and UDP telemetry/shared-socket/offload-fallback races; see [Valgrind](valgrind.md). |

Avoid `make clean` in a checkout where another process is building or testing.
Use an independent copy/worktree when changing compiler flags or running
multiple development experiments.

With noninteractive sudo and mount-namespace support, `sh tests/mapped_enospc_test.sh`
tests real ENOSPC on a private 1 MiB tmpfs. MSC runs as the invoking user; only
namespace creation/mounting uses root. The mount never propagates to the host,
is bounded by a timeout, and is removed on exit. This optional test is separate
from `make check`, which needs no administrative access.

## Direct harnesses

| Harness | Invocation |
| --- | --- |
| `udp_test` | `./build/udp_test SRC DST FLOWS [DROP_PERCENT]` |
| `segmented_test` | `./build/segmented_test SRC DST STREAMS SEGMENT_BYTES EXPECT_SUCCESS` |
| `recursive_test` | `./build/recursive_test SOURCE_DIR DEST_DIR STREAMS SEGMENT_BYTES` |
| `resume_test` | `./build/resume_test SRC DST` |
| `retry_test`, `exit_code_test`, `stall_timeout_test`, `child_status_test` | No arguments. |

The harnesses construct `argdata` directly and bypass `parseargs`. For example,
`udp_test` defaults to checksum verification and accepts `MSC_UDP_PAYLOAD` as
an explicit-payload override; the normal CLI defaults to no extra checksum and
uses `-s`. Harness success does not establish CLI default/forwarding behavior.
Always compare outputs even when an internal checksum is enabled.

## Fault-injection tools

| Mechanism | Use |
| --- | --- |
| `MSC_UDP_DROP` or `udp_test` drop argument | Integer-percent synthetic sender packet loss; disables GSO to preserve packet-level injection. |
| `MSC_UDP_CONTROL_DROP` | Loss in the optional UDP control shim, including handshake/ACK/FIN frames. |
| [wan_shim.c](../tests/shims/wan_shim.c) | Preload-based UDP delay, jitter, independent loss, duplication, initial ACK/FIN drops, and MTU black holes. |
| [storage_fault_shim.c](../tests/shims/storage_fault_shim.c) | Destination write failures after a byte threshold; used with the non-mapped write path. |
| [fsync_fault_shim.c](../tests/shims/fsync_fault_shim.c) | Worker creation failures, fsync recording, and forced file/directory sync errors. |
| [udp_offload_shim.c](../tests/shims/udp_offload_shim.c) | Unsupported GSO/GRO fallback paths. |
| [listen_count_shim.c](../tests/shims/listen_count_shim.c) | Records TCP `listen()` calls to verify the stdio port footprint. |

Build the WAN shim with `make build/wan_shim.so`. Its controls are:

| Variable | Meaning |
| --- | --- |
| `MSC_UDP_WANSHIM_DELAY_MS` | One-way delay in ms. |
| `MSC_UDP_WANSHIM_JITTER_MS` | Uniform jitter magnitude around that delay. |
| `MSC_UDP_WANSHIM_LOSS_PCT` | Independent per-datagram loss percentage. |
| `MSC_UDP_WANSHIM_DUP_PCT` | Per-datagram duplication percentage. |
| `MSC_UDP_WANSHIM_DROP_ACKS` | Drop first N MSC ACK datagrams. |
| `MSC_UDP_WANSHIM_DROP_FINS` | Drop first N MSC FIN datagrams. |
| `MSC_UDP_WANSHIM_MTU` | Datagram-byte threshold above which packets are swallowed. This is the shim's datagram limit, not an IP-header-inclusive MTU. |
| `MSC_UDP_WANSHIM_SEED` | Seed for controlled experiments. |
| `MSC_UDP_WANSHIM_TRACE` | Print applied configuration and activity counters. |

The shim intercepts UDP only. It does not impair TCP data or the default SSH
stdio control stream. A fixed seed aids repeatability but is not a promise of
identical thread scheduling or throughput. It is not a competing-traffic or
router queue model. Inspect trace counters to confirm impairment actually ran.

Storage fault controls include `MSC_TEST_ENOSPC_AFTER_BYTES` and
`MSC_TEST_ENOSPC_ERRNO`; tests force `MSC_UDP_NO_MMAP=1` because intercepted
write syscalls do not see mapped-memory stores. Other `MSC_TEST_*` hooks are
internal to their owning tests. They are not forwarded configuration for
ordinary remote use.

## Real-system validation

[lustre_dest_stripe_test.sh](../tests/lustre_dest_stripe_test.sh) requires an actual
Lustre filesystem and tools; read its setup before running. Local tests cannot
validate NIC offload performance, firewall/NAT policy, real SSH authentication,
multi-host shared storage, or fairness against competing traffic. Verify these
on the intended environment when a change depends on them.

For a controller change, add scenarios that test the proposed response to loss,
delay, ECN, and receiver backpressure. Keep throughput experiments separate from
byte-correctness checks and record the effective settings described in
[performance](performance.md).
