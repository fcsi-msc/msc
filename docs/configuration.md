# Configuration reference

These defaults describe the **MSC CLI**, unless explicitly labeled engine or
test-harness defaults. Direct API callers construct their own configuration.
Run `msc -h` or `msc man` for offline help.

## Command-line options

| Option | Meaning and default |
| --- | --- |
| `-l HOSTS`, `-r HOSTS` | Source/destination machine lists with matching counts. Use `-l localhost` for a local source. |
| `-i PATH`, `-o PATH` | Source/destination file, or directory with `-R`. |
| `-c COMMAND` | Feed the transfer to a remote command; requires TCP. |
| `-u USER` | SSH account for the participating hosts. |
| `-B PATH` | Remote MSC executable; default `msc` on the remote PATH. |
| `-T`, `-U` | TCP or reliable UDP; UDP is default. Stdin/command shapes automatically use TCP unless explicit `-U` makes them an error. |
| `-R` | Recursive tree; requires one pair and rejects offsets/slices. |
| `-n N` | Parallel streams/flows. Defaults: UDP file/tree 8, TCP tree 64, TCP file 8, pipe 30. Maximum UDP 256; TCP 2,048. |
| `-s BYTES` | TCP chunk size (default 1 MiB), or explicit UDP file payload. Implicit UDP payload uses the confirmed PMTUD ceiling; 1,400 bytes when probing is disabled. |
| `-a BYTES`, `-b BYTES`, `-t BYTES` | Source offset, destination offset, transfer length; zero length means remaining source. Checkpointed UDP rejects these. |
| `-I HOST` | Address used by the UDP sender; does not select a local interface by device name. |
| `-p PORT` | Socket-based rendezvous starts here; default 16400. Default SSH stdio control needs no such listener. |
| `--port-tries N` | Number of rendezvous ports to try; default 100. |
| `--env NAME` | UDP profile; default `auto`. An existing `MSC_UDP_PROFILE` takes precedence. |
| `--udp-data-ports K` | Data sockets for N flows; default `min(N,8)`. |
| `--udp-port-base B` | First data port scanned; default 17400. |
| `--udp-port-span S` | Scan width, default `4*K`, maximum 1,024; zero requests an exact contiguous block. |
| `--udp-ports LIST` | Exact ordered ports/ranges; fails if unavailable. |
| `--gso`, `--no-gso` | UDP segmentation offload; enabled by default, with fallback. |
| `--gro`, `--no-gro` | UDP receive offload; enabled by default, with fallback. |
| `--stats` | Human transport statistics; off by default. |
| `--checksum=true\|false` | Additional UDP checksum handshake; false by default. `--no-internal-checksum` is the disabling form. |
| `--force` | Permit replacing a single-file published destination; also controls stale partial/checkpoint handling on a fresh checkpointed run. |
| `--dest-stripe-count N` | Destination Lustre width; positive count or `-1` for all. Rejects `-R` and `-c`. |
| `--checkpoint PATH` | Receiver-side checkpoint path; enables resumable transfer. |
| `--resume` | Require a valid checkpoint; derive a default checkpoint path if omitted. |
| `--keep-checkpoint` | Keep checkpoint after completion. |
| `--retries N` | Bounded retries for network failures; default zero. |
| `--retry-delay D` | Initial retry delay; default 1 s, with backoff/jitter. |
| `--stall-timeout D` | No-progress limit; default 30 s, zero disables. |
| `--reconnect-interval D` | Checkpointed-transfer SSH reconnect interval; default 10 s, zero disables. |
| `--progress=auto\|always\|never` | Progress on stderr; default auto. |
| `--progress-interval D` | Progress cadence; default 1 s. |
| `-x` | Print shell-quoted restart commands for a multiple-machine transfer. May be the first option. |
| `-h`, `man` | Short help or embedded manual. |

Use explicit `ms`, `s`, or `m` suffixes for duration arguments. Port flags apply
to `stdio` and `tcp` UDP control modes; see [sockets and control](sockets-and-control.md)
for combination restrictions. Checkpoint/reconnect semantics are in
[correctness and resume](correctness-and-resume.md).

## Precedence and forwarding

MSC shell-quotes and forwards a specific environment allowlist in
`msc_udp_env_forwardable()`. The `MSC_UDP_` prefix alone does not cause a variable
to reach the remote peer.

- Explicit `--env` sets `MSC_UDP_PROFILE` only when that variable is absent.
- Explicit destination-stripe and data-port CLI settings populate their
  corresponding environment values.
- The CLI always writes GSO, GRO, and stall-timeout values from parsed options,
  including defaults. Use `--no-gso`, `--no-gro`, and `--stall-timeout` to change
  them; environment values alone do not override the CLI.
- `--stats` enables human stats; an existing `MSC_UDP_STATS=1` also enables them.
- Profiles fill unpinned knobs. Explicit per-knob environment settings generally
  win, except `rate` always forces pacing and a positive aggregate cap enables it.
- CLI PMTUD cap is 9,000 bytes only when `MSC_UDP_PMTUD_CAP` is absent.

Most engine numeric knobs use simple C numeric conversion, with the conditions
listed below. Invalid values do not all produce CLI errors; some retain a
default. Prefer the documented values and inspect the effective statistics.

## Path profiles

`MSC_UDP_PROFILE` accepts `auto`, `lan`, `wan` (`fiber`, `fiber-shared`),
`wan-long` (`fiber-long`, `longhaul`), and `geo` (`satellite`). Profile aliases do
not select a different congestion controller.

| Setting | LAN base | WAN | WAN-long | GEO |
| --- | ---: | ---: | ---: | ---: |
| Initial window, units | 16 | 4 | 32 | 4 |
| Pacing burst, units | 64 | 64 | 256 | 256 |
| Window-based pacing gain | 1.25 | 1.0 | 1.0 | 1.0 |
| Duplicate-ACK threshold | 3 | 16 | 16 | 16 |
| RTT multiplier used to raise RTO floor | — | 1.0 | 2.0 | 2.5 |
| Startup queue-delay multiplier | Disabled | Disabled | 1.5 | 1.5 |
| Bandwidth restart enabled | No | Yes | Yes | Yes |

The base RTO floor is **200 ms** for all profiles. Applying an RTT multiplier
raises that floor only when larger, unless an explicit floor was supplied.
The final RTO calculation is capped at 2 s. The default `rate` controller paces
under every profile and has its own state-dependent pacing gain. The table's
window-based gain is used by other pacing paths.

`auto` initially classifies the HELLO RTT: below 5 ms is LAN, at least 5 ms WAN,
at least 120 ms WAN-long, and at least 450 ms GEO. The control RTT is provisional.
A negotiated UDP RTT probe can supply a sample before data starts; otherwise
the first accepted data ACK supplies one. The current one-time reconsideration
can promote an initial LAN decision and can lower an inflated control-derived
RTO floor. It is not continuous reclassification of all profile fields. Explicit
profiles skip this automatic reconsideration. GEO remains a provisional preset.

## Controller and timing variables

Defaults below are the base values before profile adjustments.

| Variable | Value/units and effect |
| --- | --- |
| `MSC_UDP_CC` | `rate` (default), `reno`, `cubic`, or an implemented custom name. Unknown names warn and select `rate`. [Add your own](adding-a-controller.md). |
| `MSC_UDP_CC_BETA` | Fraction strictly between 0 and 1; defaults 0.5 for Reno and 0.7 for CUBIC. |
| `MSC_UDP_INIT_CWND` | Initial units, at least 1; base 16. |
| `MSC_UDP_MIN_CWND` | Loss-controller floor, at least 1; default 1. |
| `MSC_UDP_PACE` | Boolean; controller/profile-dependent default. Rate controller requires 1. |
| `MSC_UDP_PACE_GAIN` | Positive window-based pacing multiplier; base 1.25. |
| `MSC_UDP_PACE_BURST` | Positive token-bucket depth in units; base 64. |
| `MSC_UDP_PACE_RATE_MBIT` | Positive aggregate fresh-payload cap in decimal Mbit/s; unset means no explicit cap. |
| `MSC_UDP_FAIR` | 1 enables shared fresh-send pacing; default 0. |
| `MSC_UDP_ACK_EVERY` | Positive in-order unit count between stretch ACKs; 16. |
| `MSC_UDP_DUPACK_THRESH` | Positive duplicate-ACK threshold; base 3. |
| `MSC_UDP_TICK_US` | Nonnegative sender blocked-poll interval in microseconds; 200. |
| `MSC_UDP_MIN_RTO_MS` | Positive RTO floor in ms; base 200. |
| `MSC_UDP_RTO_GENTLE` | 1 uses beta reduction on loss-controller RTO instead of resetting to one; default 0. |
| `MSC_UDP_REORDER_WAIT_MS` | Explicit nonnegative hold in ms, clamped to 2,000; unset uses adaptive SRTT-based hold. |
| `MSC_UDP_REORDER_SRTT_DIV` | Positive adaptive-hold divisor; default 2, with a 100 ms adaptive cap. |
| `MSC_UDP_DSACK` | 0 disables negotiated duplicate feedback/adaptation; default on. |
| `MSC_UDP_ECN` | 0 disables ECT marking/CE response; default on. Actual response depends on controller. |
| `MSC_UDP_RWND` | 0 disables dynamic receive-window accounting; default on. |
| `MSC_UDP_RWND_STALL_MS` | Positive write-latency threshold in ms for reducing advertised capacity; 5. |
| `MSC_UDP_STARTUP_QUEUE_MULT` | Nonnegative SRTT/min-RTT startup-exit ratio; 0 disables; profile-dependent. |
| `MSC_UDP_BW_RESTART` | Boolean rate-controller restart policy; profile-dependent. |
| `MSC_UDP_WAN_RTT_MS` | Positive auto WAN threshold in ms; 5. |
| `MSC_UDP_WAN_LONG_RTT_MS` | Positive auto WAN-long threshold in ms; 120. |
| `MSC_UDP_GEO_RTT_MS` | Positive auto GEO threshold in ms; 450. |
| `MSC_UDP_WAN_GUARD` | Legacy opt-in WAN bootstrap; default off. Prefer `--env`. It also pins a 10 ms reorder hold when applied without an RTT sample. |
| `MSC_UDP_STALL_TIMEOUT_MS` | Engine no-progress limit in ms; CLI overwrites it from `--stall-timeout`. |

## Sockets, payload, and storage variables

| Variable | Value/units and effect |
| --- | --- |
| `MSC_UDP_CTL` | `stdio` default; also `stdio1`, `tcp`, `many`, `one`. Unknown names warn and select stdio. |
| `MSC_UDP_PORTS` | Requested data socket count; default `min(N,8)`. |
| `MSC_UDP_PORT_BASE` | First data port; default 17400. |
| `MSC_UDP_PORT_SPAN` | Scan width; default `4*K`, maximum 1,024; zero means exact block. |
| `MSC_UDP_PORT_LIST` | Exact comma-separated ports/ranges, e.g. `20000,20004,20100-20103`. |
| `MSC_UDP_PMTUD` | 0 disables MTU probing; default on. Data-RTT probing is a separate negotiated feature. |
| `MSC_UDP_PMTUD_CAP` | Positive implicit file-payload ceiling in bytes; CLI 9,000, engine 2,016. |
| `MSC_UDP_GSO`, `MSC_UDP_GRO` | Offload booleans; CLI sets both from flags, default on. |
| `MSC_UDP_RECV_BATCH` | Positive receive slots and write-coalescing limit; default 64, capped at 1,024. |
| `MSC_UDP_RETRANSMIT_MB` | Aggregate sender payload-cache and ring-metadata budget in MiB; default 512. Integer range 1–1,048,576. Power-of-two per-flow rings shrink for smaller transfers, never exceed 16,384 slots, and reserve one unused slot. Does not include I/O buffers, mappings, receiver queues, or kernel socket memory. |
| `MSC_UDP_SOCK_BUFFER` | Positive initial buffer request in bytes; base 16 MiB, kernel-clamped. Explicit setting disables automatic receive seeding. |
| `MSC_UDP_SOCK_AUTOTUNE` | 0 disables buffer autotuning; default on. |
| `MSC_UDP_AUTOTUNE_MAX` | Positive autotune ceiling in bytes; 256 MiB, capped at `INT_MAX`; kernel limits still apply. |
| `MSC_UDP_NO_MMAP` | 1 forces vectored writes; default 0. |
| `MSC_UDP_FORCE_MMAP` | 1 requests mapping for eligible paths including Lustre; default 0. Avoid combining the two overrides. |
| `MSC_UDP_FSYNC_THREADS` | Positive recursive tail-sync worker count; 64. |
| `MSC_UDP_NO_AFFINITY` | 1 disables source stripe-affine partitioning; default 0. |
| `MSC_DEST_STRIPE_COUNT` | Destination stripe count; explicit CLI flag wins. Forwarded for TCP as well as UDP. |

## Telemetry and experimental variables

| Variable | Meaning |
| --- | --- |
| `MSC_UDP_STATS` | 1 enables human summaries; default off. |
| `MSC_UDP_STATS_STREAM` | 1 enables prefixed NDJSON samples; default off. |
| `MSC_UDP_STATS_STREAM_RATE_HZ` | Positive samples/second per flow/event kind; default 20, maximum 1,000. |
| `MSC_UDP_STATS_STREAM_OUTPUT` | `stderr` (default), `stdout`, or append-to-file path. Stdio control redirects the stdout choice to stderr. |
| `MSC_UDP_DROP` | Synthetic sender data loss, integer percent clamped to 0–100; default 0. Disables GSO. |
| `MSC_UDP_CONTROL_DROP` | Synthetic outgoing UDP-control loss, integer percent clamped to 0–100; default 0. |
| `MSC_UDP_CONTROL_TRACE` | 1 prints verbose UDP-control frame tracing. |
| `MSC_UDP_RX_NOWRITE` | Diagnostic data discard; produces invalid file contents. Use only in isolated experiments. |
| `MSC_UDP_NO_CC` | Diagnostic open-window mode; disables normal controller response. |
| `MSC_UDP_STRIPE_SIZE`, `MSC_UDP_STRIPE_COUNT` | Positive synthetic source geometry for partition experiments; zero/unset uses queried layout. |
| `MSC_UDP_PAYLOAD` | `udp_test` harness stand-in for explicit `-s`. Forwarded but not consumed as a payload override by the normal MSC CLI; use `-s` there. |

`MSC_UDP_CHECKPOINT_BYTES` is a local/test chunk-size override (default 64 MiB);
it is not on the SSH forwarding allowlist. `MSC_TEST_*` and the preload shim's
variables are test controls, not supported remote CLI configuration. See
[testing](testing.md).

## SSH settings

`MSC_SSH` selects the SSH executable (default `/usr/bin/ssh`). A name without a
slash is resolved using PATH. `MSC_SSH_CONNECT_TIMEOUT`,
`MSC_SSH_ALIVE_INTERVAL`, and `MSC_SSH_ALIVE_COUNT` accept positive integers and
override the launcher's SSH budgets; the first two use seconds.

| Purpose | Ordinary/auto path: connect, interval, count | Explicit WAN/long-haul/GEO profile or alias |
| --- | --- | --- |
| Transfer launch | 10 s, 15 s, 4 | 30 s, 30 s, 4 |
| Reconnect probe | 5 s, 3 s, 1 | 30 s, 20 s, 3 |

Automatic data-path classification occurs after SSH launch; it cannot
retroactively change that SSH process's options.

## Implementation and validation

[parse.c](../parse.c) and [msc.h](../msc.h) define CLI defaults;
[udp_transport.c](../udp_transport.c) applies options and forwarding;
[udp_session.c](../udp_session.c) reads engine knobs and profiles;
[netutil.c](../netutil.c) chooses SSH budgets.
[transport_default_test.sh](../transport_default_test.sh),
[env_preset_test.sh](../env_preset_test.sh), and
[port_spec_test.c](../port_spec_test.c) cover important configuration paths.
