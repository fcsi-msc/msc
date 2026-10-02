# Reliable UDP data transfer

MSC implements reliability separately for each logical UDP flow. The reliable
control channel handles setup and transfer-level results; the data plane handles
packet delivery, reordering, duplicates, and loss. By default control travels
over the launching SSH session's stdin/stdout. See [sockets and control](sockets-and-control.md)
for the optional UDP control shim, whose reliability algorithm is different.

## Units and state

A transfer unit contains up to one negotiated payload's worth of file data.
Its header identifies the transfer, flow, sequence number, destination offset,
length, and sender timestamp. Sequence numbers count units within a flow, not
bytes or positions in the entire file. Both endpoints derive the expected file,
offset, and length from the same partition and file table.

| State | Meaning |
| --- | --- |
| `snd_una` | Oldest unit not cumulatively acknowledged. |
| `snd_nxt` | Next new unit to send. |
| `rcv_nxt` | First unit missing from the receiver's contiguous prefix. |
| `cwnd` | Controller's limit on outstanding units. |
| `rwnd` | Receiver-advertised capacity, in units. |
| SACK state | Units received above the cumulative ACK. |

Outstanding units lie in `[snd_una, snd_nxt)`. Fresh transmission must fit the
congestion window, receiver window, and local ring capacity, and must have a
pacing grant when pacing is active. Sender rings have at most 16,384 slots and
shrink to fit the transfer and `MSC_UDP_RETRANSMIT_MB` budget. At most
`ring_slots - 1` units may be outstanding. They store payload copies, sequence identities,
timestamps, and recovery flags. Retransmission uses the RAM copy without
rereading the source file.

The receiver tracks receipt with a bitmap and writes units at their file
offsets. It does not assemble a single ordered byte stream before writing.
The receiver bitmap covers its assigned units; do not infer that all receiver
metadata has the sender ring's fixed memory bound.

## Receiver processing

1. Validate the datagram size, signature, transfer ID, flow ID, and sequence.
2. Derive the expected destination file, offset, and payload length and reject
   a packet that disagrees.
3. Skip an already-recorded unit and remember a duplicate range for DSACK.
4. Write new data, using the selected mapped or vectored write path, then mark
   successful units in the bitmap.
5. Advance `rcv_nxt` across the contiguous received prefix and produce feedback.

The bitmap update follows a successful write/copy. A data ACK does **not** imply
that an `fsync` or a checkpoint has completed; those are separate barriers in
[correctness and resume](correctness-and-resume.md).

An ACK is a snapshot containing `cum_ack = rcv_nxt`, up to 16 SACK blocks,
`rwnd`, an echoed sender timestamp, the cumulative delivered-unit count, and
the cumulative ECN-CE count. SACK blocks use half-open unit ranges `[start,end)`.
Later snapshots replace lost ACKs.

The clean-path stretch-ACK interval defaults to 16 units, with a 1 ms feedback
timer. Gaps prompt feedback. Unsolicited idle feedback without a gap is limited
to a 20 ms cadence and carries no RTT timestamp. These are scheduling targets,
not deadlines guaranteed by the operating system.

## Example: one gap and its repair

Suppose units 0 through 3 are sent and unit 1 is lost. The table shows logical
feedback snapshots; actual datagrams may combine several arrivals into a batch.

| Event | Cumulative ACK | SACK | Sender interpretation |
| --- | ---: | --- | --- |
| Receive 0 | 1 | none | Unit 0 can be retired. |
| Receive 2, then 3 | 1 | `[2,4)` | Unit 1 is a hole below received data. |
| Reordering guard expires | 1 | `[2,4)` | Retransmit unit 1 from the cache. |
| Receive repaired 1 | 4 | none | Retire units 1 through 3. |
| A delayed original 1 arrives | 4 | DSACK `[1,2)` | Evidence that the retransmission was spurious. |

If that final event occurs, the original was delayed rather than permanently
lost. DSACK allows the sender to adjust its interpretation.

## Sender feedback and loss detection

A valid advancing cumulative ACK retires units, updates congestion control,
clears duplicate-ACK state, and removes timeout backoff when an RTT estimate is
available. RTT samples from retransmitted units or ambiguous reordered recovery
are excluded. Repeated cumulative ACKs count toward fast retransmission only
when the delivered-unit counter also advances. Idle feedback alone cannot
manufacture a duplicate-ACK loss episode.

MSC has three complementary repair paths:

| Trigger | Repair |
| --- | --- |
| A SACK range above a missing unit | Repair the exposed hole after its reordering guard. |
| Repeated cumulative ACKs with new delivery and no useful higher SACK coverage | Repair the oldest outstanding unit after the duplicate-ACK threshold and guard. |
| Oldest outstanding unit exceeds its RTO | Retransmit it, notify the controller, and back off the timer. |

SACK processing records received units in the sender ring and tracks the highest
SACKed point. Sequence identities and retransmission timestamps prevent stale
ring entries and repeated ACKs from repeatedly repairing the same hole.

## RTT, timeout, and reordering

The estimator uses smoothed RTT and RTT variance. The base timeout calculation is

```text
RTO = clamp(SRTT + max(sender_tick, 4 * RTTVAR), configured_floor, 2 seconds)
```

The initial RTO and base floor are both 200 ms. A path profile can raise the
floor using its RTT multiplier; `MSC_UDP_MIN_RTO_MS` supplies an explicit floor.
Timeouts back off up to the 2 s ceiling. The floor protects against scheduling
and queueing tails that a small RTT estimate may miss.

Reordering tolerance is a separate mechanism. Normally it starts at `SRTT/2`,
plus a DSACK-learned allowance, capped at 100 ms. An explicit
`MSC_UDP_REORDER_WAIT_MS` pins the hold instead (accepted up to 2 s). The RTO
formula does not include this hold; a per-unit check prevents a timeout from
racing a known hole's reordering hold. See [configuration](configuration.md)
for overrides and the legacy `MSC_UDP_WAN_GUARD` bootstrap.

## DSACK and recovery undo

DSACK is enabled by default and negotiated through the feature bits. A first
SACK block below the cumulative ACK, or contained within a later SACK block,
can describe duplicate delivery. The sender checks that the corresponding ring
entry still names that sequence and that it actually retransmitted the unit.

Proven spurious retransmissions increment a counter and can widen the adaptive
reordering allowance by approximately `SRTT/4`, at most once per RTT, up to the
cap. A duplicate-only block is excluded from forward SACK coverage. When all
retransmissions in an undoable loss episode are proven spurious, the saved
congestion state can be restored. The rate controller also subtracts
DSACK-proven retransmissions in its round loss accounting.

Disabling DSACK removes this evidence and adaptation; cumulative ACK, SACK,
and timeout recovery still operate.

## Completion and receiver backpressure

The sender sends several best-effort FIN copies only after all units in a flow
are cumulatively acknowledged. A complete receiver repeats its final ACK while
lingering, and can leave early after FIN. The normal linger constant is 300 ms.
Transfer-level byte accounting and optional checksum approval then run over the
reliable control channel.

The advertised receive window accounts for outstanding out-of-order units and
write-stall feedback. It is clamped to at least one unit and at most the sender
ring limit; this implementation does not use TCP's zero-window persist protocol.
Slow storage can therefore reduce fresh sending independently of congestion
control.

## Implementation and validation

- [udp_session.c](../src/udp_session.c): `sender_take_acknowledgment`,
  `flow_reorder_wait_ns`, `flow_rto_ns`, `flush_run`, sender/receiver workers.
- [udp_session.h](../src/udp_session.h): packet records and timing constants.
- [udp_parity_test.sh](../tests/udp_parity_test.sh): delivery under loss, slices,
  publication, PMTUD, and offload fallback.
- [wanshim_test.sh](../tests/wanshim_test.sh): impaired-path regression coverage.

These suites check concrete delivery cases. They do not constitute an exhaustive
proof of all packet interleavings or congestion behavior.
