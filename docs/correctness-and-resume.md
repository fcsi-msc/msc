# Correctness, publication, and resume

MSC separates transport delivery, filesystem durability, and publication of a
destination name. A UDP ACK records successful receipt/write processing. It
does not certify stable storage. Checkpoints claim only ranges that have passed
the receiver's durability barrier.

## Guarantees by transfer mode

| Mode | Destination handling | Content verification and recovery |
| --- | --- | --- |
| UDP single file, one pair, no checkpoint | Write a sibling temporary file; publish after completion. Existing final name requires `--force`. | Reliable per-flow delivery and final byte accounting. Optional whole-transfer checksum, off by default. |
| UDP single file with checkpoint | Retain `.msc-part` partial data and durable checkpoint; publish the completed file. | SHA-256 verification of reused ranges on resume; optional checksums of newly transferred chunks. |
| TCP segmented single file without checkpoint | Write the destination directly. | Kernel stream reliability and application segment/byte accounting; interruption can leave a partial destination. |
| TCP single file with checkpoint | Retain partial file and checkpoint; publish on completion. | Validate checkpoint identity and SHA-256 of ranges before reuse. |
| Recursive TCP or UDP without checkpoint | Materialize and update the tree in place. | Manifest/data accounting; no atomic tree replacement or automatic per-file end-to-end checksum pass. |
| Recursive TCP or UDP with checkpoint | Update the tree in place and record durable logical ranges. | Verify reused ranges with SHA-256; retry missing data and final metadata. |
| Multiple machine pairs writing one file | Workers update assigned portions of a shared destination. | Do not apply the one-pair temporary-file publication guarantee. CLI checkpoint/resume requires one pair. |

## Single-file publication

Ordinary one-pair UDP receives use a sibling name ending in
`.msc-udptmp.<pid>`. After reliable delivery, the receiver syncs the file and
reports its byte count. If `--checksum=true` is enabled, it also reports a
64-bit FNV-style checksum and waits for the sender's publish approval.

Publication uses `rename` for `--force`, or `link` followed by removal of the
temporary name when replacing an existing file is forbidden. This makes the
final name appear atomically. Handled failures remove the ordinary temporary
file; abrupt process termination may leave one for later inspection.

Atomic name visibility is distinct from power-failure durability. Ordinary and
checkpointed single-file UDP publication sync the destination's parent directory
after publishing. Checkpointed completion also syncs checkpoint removal.
A failure after publication can report an
error even though the final file already exists; inspect the final name and
retained state before restarting.

An offset transfer into a newly created temporary destination does not preserve
untransferred bytes from an older destination. The shared-file worker path is
different: it writes into an already-open destination at assigned offsets.

## Checksums and source identity

`--checksum=true` enables the additional UDP checksum handshake; the CLI default
is false. Disabling it does not disable ACK/SACK reliability or single-file
atomic publication. It also does not disable SHA-256 verification when reusing
checkpoint ranges.

The ordinary UDP checksum is a noncryptographic 64-bit FNV-style hash. It is not
authentication. Recursive table transfers use byte accounting without an
automatic whole-file checksum for every entry; enabling the flag does not add
such a pass to ordinary recursive copies. Checkpointed UDP can checksum new
chunks when requested.

Resume compares source and destination identity with checkpoint metadata. The
metadata binds paths, file identity, sizes/times, transfer geometry, and, for a
tree, the manifest. SHA-256 then compares current source bytes with claimed
destination bytes before they can be skipped. Same-size edits to reused bytes
are therefore detected even when an inode/size check alone would pass.

These mechanisms are not a filesystem snapshot. Keep the source stable during
a transfer; metadata checks cannot turn concurrent edits into a coherent
point-in-time copy.

## Checkpoint ordering

Checkpoint format version 2 records byte ranges rather than UDP sequence
numbers. Its metadata checksum detects damaged/torn checkpoint contents. Old
version-1 checkpoints are rejected.

The receiver's ordering is:

```text
write range
  -> sync its data and required directory entries
  -> atomically write and sync checkpoint
  -> tell the sender the range is durable
```

The sender never treats an ordinary data ACK as that durable acknowledgement.
On a failed checkpoint update the receiver must retain the last valid on-disk
range set. The next session overwrites every missing range, including bytes
that were written but never checkpointed.

For a single file, retained data uses `<destination>.msc-part`. With `--resume`
and no explicit checkpoint path, MSC selects its default destination-derived
checkpoint name. Use an explicit `--checkpoint PATH` for predictable scripting;
the path is on the receiver. It must match on subsequent attempts.

```sh
msc --checkpoint /data/archive.cp --reconnect-interval 0 \
  -l localhost -r receiver.example -i archive.bin -o /data/archive.bin

msc --resume --checkpoint /data/archive.cp --reconnect-interval 0 \
  -l localhost -r receiver.example -i archive.bin -o /data/archive.bin
```

Successful completion removes the checkpoint unless `--keep-checkpoint` is set.
Retaining it is useful for inspection; it is not a promise that a completed
checkpoint remains resumable after the partial file has been published.

## Recursive resume

Regular-file bytes are concatenated in manifest order into one logical byte
space. A durable range may cross file boundaries. Empty files, directories,
symlinks, and hard links are materialized from the manifest rather than being
data units. The durability barrier includes directory entries needed to reach
the files.

An interrupted recursive transfer leaves a partial tree. Resume verifies claimed
ranges, retransfers missing ranges in full, and retries final modes/timestamps.
A complete data checkpoint is retained if final metadata is interrupted. Resume
then skips the verified data and retries metadata. This is not an atomic tree
swap or a mirroring operation that deletes unrelated destination entries.

Recursive transfers reject offsets and length slices. Checkpointed UDP also
rejects offsets/slices for a single file. Byte-based UDP checkpoints allow a new
session to choose a different payload size or path MTU.

## Cancellation, retries, and stalled peers

SIGINT and SIGTERM retain exit statuses 130 and 143. The default no-progress
timeout is 30 s; `--stall-timeout 0` disables it. It is a liveness bound, not a
limit on total transfer duration.

Only network/transport exit 6 is retried. `--retries` controls bounded retries
with backoff. Checkpointed transfers also enable reconnect probing by default
every 10 s after retries are exhausted; `--reconnect-interval 0` disables that
loop. A finite `--retries` value alone therefore does not bound a checkpointed
run's total recovery time. Integrity, destination, and protocol errors need
their cause resolved before restarting. See [troubleshooting](troubleshooting.md).

## Protection boundaries

SSH authenticates the remote launch and protects the default stdio control
stream. The separate UDP and TCP data sockets are not encrypted or
cryptographically authenticated by MSC. Alternate TCP/UDP control sockets also
do not inherit SSH encryption. Packet identity checks and checksums are not
protection against an active network attacker. Use an appropriately protected
network path when confidentiality or adversarial integrity is required.

## Implementation and validation

- [udp_transport.c](../udp_transport.c): checkpoint handshakes, chunk durability,
  recursive resume, and CLI-to-session integration.
- [udp_session.c](../udp_session.c): data completion, checksums, ordinary UDP publication.
- [resume.c](../resume.c): format v2, atomic checkpoint writes, and SHA-256 range verification.
- [remote.c](../remote.c) and [recursive.c](../recursive.c): TCP resume and tree materialization.
- [udp_resume_test.sh](../udp_resume_test.sh),
  [udp_recursive_resume_test.sh](../udp_recursive_resume_test.sh), and
  [resume_test.sh](../resume_test.sh): interruption, mutation, storage errors,
  changed UDP packet geometry, signals, and metadata recovery.

Fault injection verifies software ordering and error handling. It does not
certify a filesystem or device's behavior under actual power loss.
