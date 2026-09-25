# MSC documentation

Start with the [project README](../README.md) for installation and examples.
These documents describe the implementation shipped in this repository.
Protocol details and defaults should be updated alongside the code that changes
them; historical benchmark results are not guarantees for another path.

## Build your own congestion controller

MSC's controller interface supports source-level plug-and-play experiments:
implement callbacks, register a name, and reuse the transport and local test
harness. [Implement and test your own controller](adding-a-controller.md) walks
through a working example and clean/loss/WAN tests, with no SSH or root required
for the local experiments.

## Operating MSC

| Document | What it covers |
| --- | --- |
| [Configuration](configuration.md) | CLI options, environment variables, defaults, precedence, and path presets. |
| [Sockets and control](sockets-and-control.md) | SSH stdio control, UDP ports, logical flows, and shared sockets. |
| [Correctness and resume](correctness-and-resume.md) | Publication, checksums, durability, checkpoints, interruption, and trust boundaries. |
| [Performance](performance.md) | PMTUD, offloads, batching, memory, storage, and Lustre. |
| [Troubleshooting](troubleshooting.md) | Transport statistics, failure diagnosis, and exit status. |

## Understanding and changing MSC

Read [architecture](architecture.md), then [reliability](reliability.md) and
[congestion control](congestion-control.md). Use the [wire protocol](wire-protocol.md)
when changing messages or interoperability. The [testing guide](testing.md) maps
behavior to the existing suites; [Valgrind](valgrind.md) covers memory and race
checks.
