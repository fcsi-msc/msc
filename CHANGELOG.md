# Changelog

Notable user-facing changes are recorded here.

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
