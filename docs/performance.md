# Performance and storage

Measure end-to-end completion separately from the data phase. SSH launch,
negotiation, path probes, manifest work, syncing, and optional checksums all
contribute to elapsed time. A controller's peak data rate alone does not predict
how quickly a short file or a large directory finishes.

## Flows, sockets, and memory

`-n` changes the number of logical flows and workers. `--udp-data-ports` changes
the number of physical sockets draining the receiver's kernel queues. More
flows can increase concurrency and outstanding data; more sockets can distribute
packet processing across demultiplexer threads. Neither improves a saturated
storage device or link indefinitely.

The CLI defaults are eight UDP flows for files and trees, 64 streams for a
TCP tree, and eight streams for a regular TCP file. Explicit `-n` overrides those
defaults. Default UDP sockets are `min(N,8)`. A direct session-API caller can
choose different values, so report the effective configuration in benchmarks.

Sender retransmission caches and their ring metadata share a 512 MiB budget
(`MSC_UDP_RETRANSMIT_MB`). Each flow gets a power-of-two ring of at most 16,384
slots, reduced to fit its share of the budget and its data. One slot stays empty
to prevent overwriting outstanding data. A smaller budget can limit throughput
on paths with a large bandwidth-delay product. Receiver queues, I/O and socket
buffers, file mappings, and recursive descriptors add separate costs.

The eight-flow UDP default avoids shared-socket demux work on ordinary transfers.
Storage-heavy Lustre workloads can benefit from additional flows; try explicit
`-n30` / `-R -n64` and measure on the actual storage system.

## Path MTU discovery

MSC probes candidate datagram sizes at session setup, with fragmentation
disabled, and uses receiver confirmation of arrival. Local send success alone
does not prove that a downstream link carried the datagram.

An implicit payload uses the confirmed ceiling. The probe ladder includes
548-byte UDP datagrams (500 file bytes) for paths below Ethernet MTU. If no
usable size is confirmed, setup fails rather than sending oversized file data.
With probing disabled, the fallback is 1,400 file bytes. Explicit `-s` is never
raised and can be clamped down. The
MSC CLI sets `MSC_UDP_PMTUD_CAP=9000` when absent; the underlying engine's
compiled cap is 2,016. These are file-payload limits: MSC's 48-byte packet
header and UDP/IP headers also consume MTU space.

At an IPv4 MTU of 1,500 bytes, a full datagram carries at most 1,424 file
bytes after the IP, UDP, and MSC headers. Including standard Ethernet framing
and interframe overhead, the ideal file-payload ceiling on a 1 GbE link is
about 926 Mbit/s, before ACKs or retransmissions. A saturated NIC can
therefore report a rate near 1,000 Mbit/s while file throughput is lower.
Longer transfers amortize setup costs but do not remove this packet overhead.
Larger MTUs raise the ceiling only when the entire path supports them. They do
not guarantee higher application throughput; measure the whole transfer and
the sender NIC rate after any MTU change.

The probe runs at session opening, not continuously for every data packet. A
changed path during a long session may require restarting with a smaller
explicit payload. Byte-based checkpoints allow resuming with new packet
geometry. Pre-data RTT probing shares the setup phase but is negotiated
separately from MTU discovery.

## Batching and offloads

MSC uses `sendmmsg`/`recvmmsg` to amortize syscall overhead. GSO uses
`UDP_SEGMENT` to submit multiple datagrams together; GRO coalesces received
datagrams, which MSC splits before per-flow processing. Neither feature changes
sequence numbers or reliability semantics.

The CLI enables both by default. Unsupported GSO falls back to ordinary
batched sends; unsupported GRO leaves ordinary receives. `--no-gso` and
`--no-gro` select those paths explicitly. Sender-side `MSC_UDP_DROP` disables
GSO so its loss injection remains per packet.

Receive batches default to 64 slots and can be increased up to 1,024. Increasing
batching can reduce overhead but also increase burst size, memory, and the
amount of work before another flow runs.

## Socket buffers and backpressure

MSC requests socket buffers and, with autotuning enabled, seeds receive buffers
toward the available ceiling and can adjust buffers at runtime. Explicit
`MSC_UDP_SOCK_BUFFER` disables automatic initial receive seeding. Kernel
`net.core.rmem_max` and `net.core.wmem_max` can clamp requests; the reported
granted size matters more than the requested value. Linux's reported socket
buffer accounting includes its own overhead.

Kernel drops, drops at the buffer clamp, and demultiplexer queue drops indicate
different bottlenecks. The receiver window also responds to write stalls, so
storage backpressure need not first become packet loss. See
[troubleshooting](troubleshooting.md) before changing limits.

## Destination write path

For eligible whole-file writes, MSC normally uses a shared mapping on non-Lustre
filesystems and coalesced `pwritev` on Lustre. Consecutive units for the same file
can share a vectored write; reordering and file boundaries break runs.
MSC reserves destination storage with `fallocate` before mapping, so an initial
ENOSPC is reported as a destination error rather than a write-time SIGBUS.
Filesystems that do not support allocation use the vectored path. Late device
errors, external truncation, and thin-provisioned storage still need filesystem
and operational safeguards.
Offsets, supplied file descriptors, and mapping eligibility can select the
vectored path. `MSC_UDP_NO_MMAP=1` forces it; `MSC_UDP_FORCE_MMAP=1` requests
mapping on eligible paths even on Lustre.

Recursive completion can sync multiple files concurrently with
`MSC_UDP_FSYNC_THREADS` (default 64). This setting is independent of data-flow
count. A shorter data phase can still have a long filesystem flush tail.

## Lustre layout

Build with `LUSTRE=1` for Lustre API integration. Without that build, the normal
destination-creation path falls back to ordinary files; an explicit requested
stripe count that cannot be applied fails with a destination error.

When available, source layout is used for destination creation and transport
partitioning. `--dest-stripe-count N` overrides the destination width alone;
`-1` requests all available OSTs. A request larger than the filesystem's available
OST count may be clamped by Lustre. Inspect the resulting layout rather than
assuming the requested count was available.

Destination geometry can therefore differ from source geometry. This can
change receiver locality without changing packet offsets or the source-derived
mapping. Existing/resumed files cannot simply be relaid out; the requested
layout must be compatible. Recursive transfers reject this flag; use the
destination directory's layout policy instead.

## Reproducible comparisons

Record the code revision, compiler, kernel, host/NIC configuration, source and
destination filesystems, cache state, file/tree shape, RTT/loss conditions,
controller, preset, payload, N, K, offloads, checksum setting, and socket limits.
Repeat configurations in interleaved order and report spread as well as the
median. Verify bytes independently with `cmp` for files or appropriate tree
checks. Keep checksum and durability work consistent between compared runs.

Sender-side independent drop injection establishes behavior under that synthetic
loss model. It does not locate loss on a real path. The WAN shim also does not
model arbitrary competing traffic, a NIC, or a complete router queue. Avoid
turning an isolated throughput result into a general fairness claim.

## Implementation and validation

- [udp_session.c](../src/udp_session.c): `pick_payload`, PMTUD probes, socket buffer
  setup, `fire_fresh`, `flush_run`, `receiver_run_flows`, and `fsync_table`.
- [udp_io.c](../src/udp_io.c): Lustre queries and destination creation.
- [udp_parity_test.sh](../tests/udp_parity_test.sh): payload policy and offload fallback.
- [dest_stripe_test.sh](../tests/dest_stripe_test.sh) and
  [lustre_dest_stripe_test.sh](../tests/lustre_dest_stripe_test.sh): layout forwarding
  and actual Lustre placement respectively.
