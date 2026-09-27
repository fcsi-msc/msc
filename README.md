# MSC

MSC (Multi-Socket Copy) transfers large files and directory trees between Linux
hosts using parallel flows. It launches its peer over SSH and provides reliable
UDP, congestion control, pacing, path-MTU discovery, checkpoint/resume, and
optional Lustre support. `-T` selects the TCP data transport.

**Reliable UDP with SSH stdio control is the default.** Control uses the
launching SSH session's stdin/stdout; file data uses separate UDP sockets.
MSC adds no TCP listener in this mode. The receiver needs inbound access to
the selected UDP ports, and the sender needs return ACK traffic. The SSH
service retains its normal access requirements.

MSC provides a small, source-level **plug-and-play congestion-controller
interface**. Implement callbacks, register an algorithm, and test real file
transfers with the existing harness and fault-injection tools.
[Implement and test your own controller](docs/adding-a-controller.md) includes
a working example; no second host, SSH setup, or root is needed for local tests.

MSC is released under the [MIT License](LICENSE).

## Build and install

Requirements: Linux, GCC or Clang, GNU make, pthreads, and libm. The UDP engine
uses `sendmmsg`/`recvmmsg` and uses GSO/GRO when available.

```sh
make clean all
sudo make install
```

Installation defaults to `/usr/local/bin/msc` and honors `PREFIX` and `DESTDIR`.
For a user installation, use `make install PREFIX="$HOME/.local"`. Install MSC
on both hosts. Use `-B /absolute/path/to/msc` when the remote executable is not
on the non-interactive SSH PATH.

For optional Lustre integration, install its development headers/library and
build with `make clean all LUSTRE=1`. The ordinary build has no Lustre dependency.
An explicit destination stripe request requires Lustre support and a compatible
filesystem.

## Basic use

Ensure SSH can launch the remote program non-interactively. The destination's
parent directory must exist.

```sh
# Default reliable UDP and SSH stdio control.
msc -l localhost -r receiver.example -i source.bin -o /data/source.bin

# Eight logical flows.
msc -n 8 -l localhost -r receiver.example -i source.bin -o /data/source.bin

# Directory tree, including regular files, symlinks, and hard links.
msc -R -l localhost -r receiver.example -i source-tree -o /data/source-tree

# TCP data transport.
msc -T -l localhost -r receiver.example -i source.bin -o /data/source.bin

# Explicit remote executable and account.
msc -u datamover -B /opt/msc/msc -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin

# Replace an existing single-file UDP destination after completion.
msc --force -l localhost -r receiver.example -i source.bin -o /data/source.bin

# Optional extra UDP checksum verification; off by default.
msc --checksum=true -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin

# stdin to a remote command uses TCP automatically.
tar cf - . | msc -l localhost -r receiver.example -c 'cd /data && tar xf -'
```

`-U` explicitly selects the default UDP transport. Combining it with stdin or
`-c` is an error because those shapes require TCP. Run `msc -h` for options
or `msc man` for the embedded manual.

UDP defaults to eight flows for files and trees, matching the default socket
count. TCP uses 64 streams for a tree, eight for a file, and 30 for a pipe.
Explicit `-n` overrides these values; storage-heavy Lustre workloads may benefit
from `-n30` or `-R -n64`. Logical flows and UDP sockets are separate settings.

## Path profiles and congestion control

The default controller is `rate`, MSC's delivery-rate/minimum-RTT controller.
It always enables pacing. `MSC_UDP_CC=reno` and `MSC_UDP_CC=cubic` select the
loss-based alternatives. These are MSC's UDP controllers; `-T` uses kernel TCP.

`--env` chooses a path preset. The default `auto` starts from the control RTT
and can refine its initial decision using a UDP setup probe or data ACK.
Naming the environment provides an explicit preset from setup:

| Preset | Intended path / auto RTT band |
| --- | --- |
| `lan` | Local fabric, below 5 ms |
| `wan` | WAN, 5–120 ms; aliases `fiber`, `fiber-shared` |
| `wan-long` | Long-haul, 120–450 ms; aliases `fiber-long`, `longhaul` |
| `geo` | Satellite-like latency, at least 450 ms; alias `satellite`; provisional preset |

```sh
msc --env wan --stats -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin

MSC_UDP_CC=cubic msc --env wan -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin
```

The current base retransmission-timeout floor is **200 ms**. Profiles adjust
initial windows, burst limits, recovery thresholds, and other policy; their RTT
multipliers can raise that floor. The default rate controller paces even under
`lan`, and ignores `MSC_UDP_PACE=0` with a warning.

An existing `MSC_UDP_PROFILE` overrides `--env`. Other explicitly set controller
knobs generally override preset values. See [configuration](docs/configuration.md)
for the exact table and precedence, and [congestion control](docs/congestion-control.md)
for algorithm behavior, ECN response, and evaluation limits.

## Ports and control channels

By default, K = `min(N,8)` data sockets carry N flows, and flow f uses socket
`f mod K`. The receiver scans for K free UDP ports starting at 17400 within a
window of `4*K` ports. It advertises the selected ports to the sender.

```sh
# Four sockets selected within 20000–20015.
msc --udp-data-ports 4 --udp-port-base 20000 --udp-port-span 16 \
  -l localhost -r receiver.example -i source.bin -o /data/source.bin

# Exact ports; fail if one is unavailable.
msc --udp-ports 20000-20003 -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin

# One exact UDP data port, retaining default SSH stdio control.
msc --udp-data-ports 1 --udp-port-base 20000 --udp-port-span 0 \
  -l localhost -r receiver.example -i source.bin -o /data/source.bin
```

`--udp-port-base` alone starts a scan; `--udp-port-span 0` requests an exact
block. The data-port flags apply to default `stdio` and optional `tcp` control.

`MSC_UDP_CTL` also offers `stdio1` (SSH control, one data socket), `tcp`
(separate TCP control), `many` (separate reliable UDP control), and `one`
(shared UDP control/data socket). `MSC_UDP_CTL=tcp` still transfers file data
through UDP. Use `-T` to choose TCP data. See the
[topology and firewall reference](docs/sockets-and-control.md).

## Reliability, publication, and resume

UDP flows use cumulative ACKs, SACK, DSACK, adaptive retransmission, receiver
backpressure, and congestion control. PMTUD selects an implicit payload size;
explicit `-s BYTES` is never raised and can be clamped to the confirmed path
limit. The CLI's default PMTUD payload cap is 9,000 bytes. GSO/GRO are enabled
by default with fallback; `--no-gso` and `--no-gro` disable them.

An ordinary one-pair single-file UDP transfer writes a sibling temporary file
and publishes the destination atomically on completion. Existing destinations
require `--force`. The additional noncryptographic checksum handshake is off
by default. Recursive transfers update trees in place and can leave partial
trees after interruption; ordinary recursive transfer does not add a per-file
checksum pass. See [correctness and resume](docs/correctness-and-resume.md) for
guarantees by mode and the distinction between ACKs, syncing, and publication.

```sh
# Record durable progress on the receiver.
msc --checkpoint /data/source.cp -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin

# Continue from that checkpoint.
msc --resume --checkpoint /data/source.cp -l localhost -r receiver.example \
  -i source.bin -o /data/source.bin
```

Resume validates identity and compares SHA-256 over claimed source/destination
ranges before skipping them. Byte-based UDP checkpoints support a changed
payload size on a later session. Checkpointed UDP offsets/slices remain
unsupported. Successful completion removes the checkpoint unless
`--keep-checkpoint` is set.

The default stall timeout is 30 s (`--stall-timeout 0` disables it).
`--retries N` retries network failures. Checkpointed transfers also reconnect by
default, probing over SSH every 10 s after bounded retries are exhausted; use
`--reconnect-interval 0` to disable that loop. SIGINT/SIGTERM return 130/143.
See [exit codes and diagnosis](docs/troubleshooting.md#exit-status).

SSH protects the launch and default control stream. The separate UDP/TCP data
connections are not encrypted or cryptographically authenticated by MSC.

## Lustre destinations

With Lustre support, MSC can use the source's layout when creating a destination.
Override destination width for one file with `--dest-stripe-count N`, or `-1`
for all available OSTs:

```sh
msc --dest-stripe-count 8 -l localhost -r receiver.example \
  -i source.bin -o /scratch/source.bin
```

This changes destination creation; source geometry still controls transport
partitioning. The option rejects recursive/command shapes and fails when the
requested layout cannot be applied. `MSC_DEST_STRIPE_COUNT` is its environment
form; the explicit flag wins. See [performance and storage](docs/performance.md).

## Implementing and testing a controller

`msc_udp_cc_ops` separates algorithm policy from transfer machinery. Supply
callbacks for events such as ACKs, RTT samples, loss, ECN, and RTO; register the
name and rebuild. The core handles reliability, file I/O, receiver windows,
and packet parsing. Controllers compile into MSC and are selected through
`MSC_UDP_CC`.

```sh
make udp_test
sh docs/examples/check-controller.sh rate
# After following the guide and registering the example:
# sh docs/examples/check-controller.sh mycc
```

The [step-by-step guide](docs/adding-a-controller.md) provides a complete sample
controller, callback contracts, pacing integration, and local loss/WAN
experiments. The helper verifies byte equality and controller selection.

## Tests and contributing

Build warning-free with `-Wall -Wextra`, then run:

```sh
make check
```

The [testing guide](docs/testing.md) explains coverage, direct harnesses,
privilege-free WAN/storage shims, and real-Lustre checks. See
[Valgrind](docs/valgrind.md) for memory/race checks.

Keep exit classes stable, forward new remote settings through the explicit
allowlist, and pass `-T` in tests that intend TCP. Update the relevant docs with
changes to defaults, protocol records, controller callbacks, or guarantees.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and release steps,
[SECURITY.md](SECURITY.md) for the security boundary and reporting process, and
[CHANGELOG.md](CHANGELOG.md) for changes pending release. Supported platforms
and validation limits are in [docs/support.md](docs/support.md).

## Documentation

- [Implement and test your own controller](docs/adding-a-controller.md)
- [Architecture and source map](docs/architecture.md)
- [Reliability protocol](docs/reliability.md)
- [Congestion-control algorithms](docs/congestion-control.md)
- [Sockets, ports, and control](docs/sockets-and-control.md)
- [Correctness, durability, and resume](docs/correctness-and-resume.md)
- [Wire protocol](docs/wire-protocol.md)
- [Configuration reference](docs/configuration.md)
- [Performance and storage](docs/performance.md)
- [Benchmarking](benchmarks/README.md)
- [Troubleshooting and telemetry](docs/troubleshooting.md)
- [Testing](docs/testing.md)

The [documentation index](docs/README.md) suggests reading paths for users and
contributors.
