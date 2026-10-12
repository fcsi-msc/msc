# Changelog

Notable user-facing changes are recorded here.

## Unreleased

### Added

- With `MSC_UDP_STATS=1`, each side prints a phase-timing line when its UDP
  session closes, including after a failure, showing how its wall time divided
  between launch, handshake, path probing, destination preparation, data,
  completion and teardown.
- `contrib/slurm`: `B_NODE` and `MSC_SRUN_EXTRA` let the smoke test and
  `msc-srun` run in heterogeneous jobs, for partitions that allow only one
  node per job.

### Fixed

- UDP path-MTU discovery no longer overestimates the path when the kernel's
  UDP GRO delivers two probes in one receive buffer. The overestimate could
  make transfers fragment heavily or fail.
- A UDP sender now waits until the receiver has created and reserved the
  destination before sending data, when both ends support it. Preparing a large
  destination on slow storage no longer trips the stall timeout as long as the
  preparation keeps advancing, and no data is sent and dropped while the
  receiver cannot take it. Preparation that stops advancing for a stall timeout
  still fails the transfer, and the receiver then abandons the destination
  instead of finishing its preparation.
- The `MSC_UDP_STATS` receiver summary now reports the bytes actually received
  and an average computed from them. A failed transfer no longer reports its
  full planned size.
- `contrib/slurm/msc-slurm-smoke.sh` keeps a job step running on the receiving
  node. Where systemd-logind removes a user's `/dev/shm` files between job
  steps, transfers and checksums no longer fail.
- `contrib/slurm/msc-slurm-smoke.sh` keeps a separate log for each throughput
  run instead of overwriting one shared log.

## 1.0.0 - 2026-09-26

### Added

- Initial public release of MSC for Linux, with reliable parallel UDP transfers,
  optional TCP data transport, SSH-launched peers, directory trees, checkpoint
  and resume support, and optional Lustre integration.
- `MSC_UDP_RETRANSMIT_MB` sets an aggregate memory budget for sender
  retransmission caches and ring metadata. The default is 512 MiB.
- UDP path probing now tests smaller datagrams for paths below standard Ethernet
  MTU and reports an error when no usable size is confirmed.
- Contribution, security, platform support, and benchmarking guides.

### Changed

- UDP file and directory transfers default to eight flows. Explicit `-n`
  continues to control the flow count. TCP defaults are unchanged.
- Hard-link discovery scales with the number of multiply linked files rather
  than rescanning the directory manifest for every file.
- Sender retransmission rings shrink for small transfers and respect the
  configured aggregate budget.

### Fixed

- Receiver shutdown wakes blocked UDP demultiplexers without accumulating a
  per-socket timeout.
- A failed fsync worker creation no longer leaves files unsynced. Ordinary UDP
  single-file publication also syncs its parent directory.
- Initial mapped destination allocation failures report a destination error
  instead of risking a process crash; unsupported allocation uses vectored I/O.
- Receiver statistics, offload fallback, profile updates, and demultiplexer
  peer state are synchronized across workers.
- `-x` restart commands now quote paths and preserve the remote account and
  peer executable. The public `-x` form also works as the first option.

The wire protocol and checkpoint format are unchanged.
