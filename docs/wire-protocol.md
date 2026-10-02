# UDP wire protocol

This reference describes the current UDP session framing. The serializers and
parsers in [udp_session.c](../src/udp_session.c) and record definitions in
[udp_session.h](../src/udp_session.h) are the implementation reference. Shared
manifest and resume records also use [msc.h](../src/msc.h),
[recursive.c](../src/recursive.c), and [resume.c](../src/resume.c).

## Versions and carriers

The session protocol version is **6** (`MSC_UDP_PROTO_VERSION`). Version 5 added
the requested destination stripe count to the greeting; version 6 adds a random
initial transfer ID to the sender's HELLO. Both peers require the
same session version; HELLO rejects a mismatch before file data. Feature bits
are intersected only after version agreement. They do not make incompatible
record layouts interoperable.

Default control records travel over SSH stdio, without UDP-shim framing.
Optional `tcp` control carries the same records over a TCP stream. `many` and
`one` wrap that ordered stream in the UDP control shim, which has its own
version **1**. Checkpoint/restore records use their own format version **2**.
These version numbers describe different layers.

All integer fields described below use network byte order. Data records use
the explicit-width C layouts shown in the header; control scalars are sent with
the u32/u64 helpers. Do not add native pointers, `long`, or implicit variable
padding to the wire format.

## Session and transfer sequence

1. Establish the chosen control carrier after SSH rendezvous.
2. Advertise receiver data ports as `PORTS`, a u32 count, and that many u32
   ports. In `one` mode, the control socket already supplies the data endpoint.
3. The sender sends `HELLO`, version, and feature mask (three u32 fields), then
   the initial transfer ID (u64). The receiver replies with `HELLO`, version,
   and its feature mask (three u32 fields). Transfer IDs advance per transfer
   within this session; the random starting value rejects stale data from an
   earlier session. It is not cryptographic authentication.
4. Run negotiated MTU and/or pre-data RTT probes and the control `MTU` exchange.
5. Exchange single-file greeting, or the manifest/readiness/table records for
   a tree. Checkpointed transfers also perform their resume negotiation.
6. Transfer DATA and ACK packets; finish each flow with final ACK/FIN behavior.
7. Report transfer byte count using `DONE`, and perform the optional checksum
   approval. Checkpointed modes have additional durable-range acknowledgements.

Control teardown and SSH child status remain part of successful orchestration.
`DONE` alone is not a universal final-name durability guarantee; see
[correctness and resume](correctness-and-resume.md).

## Data-plane header

DATA uses a 48-byte `msc_udp_packet_header` followed by `length` bytes.
Its C definition is packed so consecutive packets at odd payload strides do not
require naturally aligned header addresses; the fixed wire size is asserted.

| Byte offset | Type | Field | Meaning |
| ---: | --- | --- | --- |
| 0 | u32 | `magic` | `0x4d534331` (MSC1). |
| 4 | u32 | `type` | Packet type below. |
| 8 | u32 | `flow_id` | Global logical flow identifier. |
| 12 | u32 | `length` | File-data bytes after header for DATA. |
| 16 | u64 | `xfer_id` | Transfer identity within the session. |
| 24 | u64 | `seq` | Per-flow unit sequence; probe-specific use during setup. |
| 32 | u64 | `offset` | Expected offset within the file's transferred region; receiver adds its configured base. |
| 40 | u64 | `send_tsc` | Sender timestamp in nanoseconds for echoing. |

| Type value | Name | Payload/meaning |
| ---: | --- | --- |
| 1 | DATA | File payload. |
| 2 | ACK | Uses the separate ACK header below. |
| 3 | FIN | Header only; a fully acknowledged flow may stop lingering. |
| 4 | PROBE | Padded MTU probe; transfer ID zero and sequence identifies total UDP payload size including the MSC header. |
| 5 | RTT_PROBE | Pre-data probe, transfer ID zero, stable probe identity and transmit timestamp. |
| 6 | RTT_ACK | Echoed RTT probe identity/timestamp. |

Sequence numbers map through the agreed flow partition and file table. They
cannot generally be converted to file offsets with `seq * payload` alone:
multiple flows, file tails, stripe tails, and destination bases also matter.

## ACK and SACK

ACK uses a 64-byte `msc_udp_acknowledgment_header`:

| Byte offset | Type | Field |
| ---: | --- | --- |
| 0 | u32 | `magic` |
| 4 | u32 | `type = 2` |
| 8 | u32 | `flow_id` |
| 12 | u32 | `sack_count` |
| 16 | u64 | `xfer_id` |
| 24 | u64 | `cum_ack`: first missing contiguous unit |
| 32 | u64 | `rwnd_units`: receive-window capacity |
| 40 | u64 | `echo_tsc`: echoed sender timestamp |
| 48 | u64 | `delivered`: cumulative newly written units |
| 56 | u64 | `ce_units`: cumulative CE-marked received units |

Up to 16 SACK records follow. Each is `u64 start, u64 end` and describes the
half-open per-flow range `[start,end)`. With negotiated DSACK, the first block
can describe duplicate delivery. See [reliability](reliability.md) for its
interpretation and validation against active sender state.

## Control tags and features

Control tags are u32 values. Record lengths are determined by tag and session
state, not by a generic length prefix.

| Tag | Record | Following fields/purpose |
| ---: | --- | --- |
| 1 | GREETING | u32 version, flows, payload; u64 file bytes, transfer ID, stripe size; u32 source stripe count, requested destination stripe count. The last field encodes signed `-1` through an int32 representation. |
| 2 | PORTS | u32 count, then count u32 ports. |
| 3 | DONE | u64 completed byte count; single-file checksum mode also sends a u64 checksum. |
| 4 | PUBLISH | Sender's checksum approval. |
| 5 | ABORT | Abort instead of accepting completion. |
| 6–8 | TREE_FILE, TREE_DIR, TREE_DONE | Allocated legacy tags; current recursive manifests use the shared directory-record format. |
| 9 | HELLO | u32 session version, u32 feature mask; sender also appends u64 initial transfer ID. Reply has no ID field. |
| 10 | MTU | u32 probe bound/confirmed size; includes the MSC packet header. |
| 11 | TREE_READY | Receiver has materialized the manifest and opened its file table. |
| 12 | TABLE_BEGIN | u64 transfer ID, file-entry count, total file bytes. |
| 13 | ERROR | u32 stable receiver failure status. |

| Feature bit | Name | Purpose |
| ---: | --- | --- |
| 0 | RWND | Dynamic receiver window. |
| 1 | PMTUD | MTU probe exchange. |
| 2 | ECN | ECT marking and CE feedback. Controller response remains local policy. |
| 3 | FAIR | Sender-local aggregate pacing advertisement. |
| 4 | DSACK | Duplicate range feedback. |
| 5 | DATA_RTT | Pre-data UDP RTT probe/response. |

Recursive table transfer IDs must match the session's expected next ID.
TABLE_BEGIN count and byte total must agree with the materialized file table.
Reused sessions must reject stale DATA even if its flow and sequence resemble
the new transfer.

## UDP control-shim frame

The shim explicitly packs a 24-byte header; it does not send a C struct image.

| Byte offset | Type | Field |
| ---: | --- | --- |
| 0 | u32 | Magic `0x4d634801`. |
| 4 | u32 | Connection ID. |
| 8 | u16 | Flags: SYN=1, ACK=2, FIN=4, RST=8, DATA=16. |
| 10 | u16 | Payload length, at most 1,376. |
| 12 | u32 | Per-direction stream byte sequence. |
| 16 | u32 | Cumulative next expected byte. |
| 20 | u32 | Available receive-window bytes. |

SYN and SYN|ACK carry two payload bytes: shim version and control-mode ID.
Connection IDs distinguish stale connections; they are not authentication.
The control stream uses byte sequence numbers, unlike data flows' unit numbers.

## Compatibility and validation

When changing a record, update its sender and receiver together and decide
whether the session or other relevant layer version must change. A controller
that only changes local window/rate policy needs no protocol change; this is
what makes [controller experiments](adding-a-controller.md) easy to integrate.

Receivers validate packet identity and expected geometry before writing.
Senders validate advertised ports and ACK bounds. These checks prevent many
accidental cross-session/layout errors; they are not cryptographic validation.

[udp_protocol_test.c](../tests/udp_protocol_test.c) checks version rejection.
[udp_recursive_ctl_test.sh](../tests/udp_recursive_ctl_test.sh) exercises recursive
handoff and repeated table behavior; [udp_parity_test.sh](../tests/udp_parity_test.sh)
checks normal transfers, ports, and malformed/incompatible cases represented
in the suite. Add targeted cases when extending a wire record.
