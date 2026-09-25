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

The CLI defaults are 30 flows for a single UDP file, 64 streams/flows for a
tree, and eight streams for a regular TCP file. Explicit `-n` overrides those
defaults. Default UDP sockets are `min(N,8)`. A direct session-API caller can
choose different values, so report the effective configuration in benchmarks.

Each active sender flow has a retransmission payload cache proportional to
`16384 * payload_bytes`, plus metadata and I/O buffers. At a 1,400-byte payload,
that cache is about 21.9 MiB per flow; at 9,000 bytes it is about 140.6 MiB.
Actual payload is constrained by the path probe. Receiver bitmaps, socket/GRO
buffers, file mappings, and recursive descriptors add separate costs.

## Path MTU discovery

MSC probes candidate datagram sizes at session setup, with fragmentation
disabled, and uses receiver confirmation of arrival. Local send success alone
does not prove that a downstream link carried the datagram.

An implicit payload starts from 1,400 file-data bytes and can be raised to the
confirmed ceiling. Explicit `-s` is never raised and can be clamped down. The
MSC CLI sets `MSC_UDP_PMTUD_CAP=9000` when absent; the underlying engine's
compiled cap is 2,016. These are file-payload limits: MSC's 48-byte packet
header and UDP/IP headers also consume MTU space.

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

- [udp_session.c](../udp_session.c): `pick_payload`, PMTUD probes, socket buffer
  setup, `fire_fresh`, `flush_run`, `receiver_run_flows`, and `fsync_table`.
- [udp_io.c](../udp_io.c): Lustre queries and destination creation.
- [udp_parity_test.sh](../udp_parity_test.sh): payload policy and offload fallback.
- [dest_stripe_test.sh](../dest_stripe_test.sh) and
  [lustre_dest_stripe_test.sh](../lustre_dest_stripe_test.sh): layout forwarding
  and actual Lustre placement respectively.
