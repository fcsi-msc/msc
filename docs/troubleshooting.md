# Troubleshooting and telemetry

Start with a representative file and collect the effective transport settings:

```sh
msc --stats --stall-timeout 60s --env auto \
  -l localhost -r receiver.example -i sample.bin -o /data/sample.bin
```

Use a new destination for each comparison, or explicitly choose `--force` when
replacement is intended. The receiver directory must exist.

## Reading human statistics

| Output | Interpretation |
| --- | --- |
| `control mode stdio` | Default control carrier over SSH; data still uses UDP. |
| Receiver/sender port list | Actual inbound data ports selected. |
| `profile`, `rtt_ms`, `reason` | Profile decision and its measurement source. `forced` identifies an explicit profile; `data-rtt` a reconsideration. Not every path emits every reason. |
| `cc` | Controller actually selected, or `none` in diagnostic no-CC mode. Check this when adding an algorithm. |
| `retransmitted` | Total repaired units; not necessarily actual network loss. |
| `rtxrto`, `rtxsack`, `rtxdupack` | Repairs triggered by timeout, SACK holes, and duplicate ACKs. |
| `spurious`, `undo` | DSACK-proven unnecessary repairs and recovery undo events. |
| `srtt`, `rto`, `reorder` | Estimated RTT, timeout, and reordering hold; note printed units. |
| `cwnd`, `rwnd`, `in_flight` | Congestion, receiver, and outstanding-unit state. |
| `ce`, `ecncut` | CE observations and controller callback dispatches. In rate mode the callback is a no-op; `ecncut` does not prove a window cut. |
| `rttskip` | RTT samples excluded because recovery/reordering made them ambiguous. |
| `clampdrops`, receive-buffer errors | Socket loss while constrained by kernel buffering. |
| Demultiplexer `drop` | Bounded userspace queue overflow after kernel receipt. |
| `writes`, `avgwrite`, `wstall` | Receiver write granularity and stalls/backpressure. |
| `fairwait` | Fresh sending waited for aggregate pacing tokens. |

Host `UdpRcvbufErrors` is an OS counter, not a per-transfer proof of loss origin.
Other UDP applications can affect it. Zero socket/demultiplexer drops also does
not establish that all retransmissions came from the network: reordering,
delayed ACKs, or an earlier receive layer may be responsible.

## Structured samples

```sh
MSC_UDP_STATS_STREAM=1 MSC_UDP_STATS_STREAM_RATE_HZ=20 \
  msc --stats -l localhost -r receiver.example \
  -i sample.bin -o /data/sample.bin 2>transfer.log
```

Structured records are NDJSON with the prefix `MSC-UDP-STATS `, sharing the log
with human output. Parse only lines whose suffix is a JSON object. Common
envelope fields are `v`, `ts_ns`, `side`, and `event`; events include
`session_start`, `flow_sample`, `acknowledgment`, `write_sample`, `retransmit`,
and `session_done`.

Continuous events are sampled per flow and event kind, normally at 20 Hz.
Retransmit events are rate-limited too, so count totals from cumulative fields,
not the number of event lines. Rate-controller samples include bandwidth,
minimum RTT, and controller state. Human final summaries identify the controller.

`MSC_UDP_STATS_STREAM_OUTPUT` accepts `stderr`, `stdout`, or an append-to-file
path. Under stdio control the stdout choice is redirected to stderr to avoid
corrupting control. A forwarded path is opened independently on each host.
Keep log consumers draining: a blocked sink can slow transfer workers.

## Isolating a problem

| Symptom | Next comparison |
| --- | --- |
| Remote launch fails or reports an argument mismatch | Confirm SSH access and use `-B /absolute/path/to/msc`; install matching builds. |
| TCP works, UDP stalls | Check inbound receiver UDP ports and return ACK traffic. Default stdio removes only the extra TCP control listener. |
| Stalls depend on packet size | Try `-s 1200` and inspect PMTUD output. |
| Offload-dependent behavior | Try `--no-gso --no-gro`. |
| High socket or demux drops | Compare lower `-n`, and separately change `--udp-data-ports`; inspect granted buffers. |
| Small receive window or high write stalls | Compare destination storage and write path; inspect flush time. |
| High spurious/RTO counts | Confirm profile, RTO floor, reordering settings, and controller. |
| Long final tail | Separate data completion, file syncing, checksum work, and metadata work. |
| Retry loop appears unbounded | Checkpointed transfers reconnect by default; use `--reconnect-interval 0` for bounded retries only. |

Change one variable at a time. If an explicit `MSC_UDP_CTL=stdio` is exported,
unset it before a `-T` comparison: the CLI rejects explicit non-TCP control
settings when the data transfer runs on TCP.

## Exit status

| Code | Meaning |
| ---: | --- |
| 0 | Success |
| 2 | Command-line/configuration error |
| 3 | Source error |
| 4 | Destination error |
| 5 | Authentication or remote-launch error |
| 6 | Network/transport error; eligible for retry |
| 7 | Integrity failure |
| 8 | Protocol incompatibility |
| 70 | Internal error |
| 130 | SIGINT |
| 143 | SIGTERM |

When reporting a bug, include the exact command with private paths/hosts
redacted as needed, build revision, both hosts' relevant versions, exit status,
and sender/receiver logs. State whether a checkpoint or final destination remains.

## Implementation and validation

[udp_stats.c](../udp_stats.c) and [udp_stats.h](../udp_stats.h) define sampling;
[udp_session.c](../udp_session.c) emits flow counters.
[exit_code_test.c](../exit_code_test.c), [retry_test.c](../retry_test.c), and
[stall_timeout_test.c](../stall_timeout_test.c) cover the exit/recovery contract.
