# Implement and test your own congestion controller

MSC is designed to make congestion-control experiments plug and play at the
source level. Implement callbacks, register a name, rebuild, and select your
algorithm with `MSC_UDP_CC`. The existing transport supplies packet delivery,
ACK/SACK parsing, retransmissions, receive-window enforcement, file I/O, and
the test harness. You can try an algorithm on loopback without SSH, a second
machine, root access, or a wire-protocol change.

The extension interface is `struct msc_udp_cc_ops` in
[udp_session.c](../src/udp_session.c). Controllers currently compile into that
translation unit; there is no runtime shared-library loader or stable binary
plugin ABI.

## Try the testing workflow first

From the repository root:

```sh
make udp_test
sh docs/examples/check-controller.sh rate
sh docs/examples/check-controller.sh reno
sh docs/examples/check-controller.sh cubic
```

The helper runs a clean transfer and a transfer with 5% synthetic sender-side
packet loss, compares the destination bytes, and checks that telemetry names
the requested controller. Logs and fixtures remain in the printed temporary
directory. Each transfer has a 60-second watchdog.

Checking the selected name matters: an unknown `MSC_UDP_CC` value currently
warns and falls back to `rate`. Successful delivery alone would not prove that
your new algorithm actually ran.

## Add a working example in three steps

The [mycc example](examples/mycc.c) implements additive increase and reuses
Reno's loss, ECN, and timeout callbacks. It keeps the experiment focused on ACK
growth. It is a starting point for changing policy, not a performance or
fairness claim.

All three edits below are in `udp_session.c`:

1. Add `MSC_UDP_CC_ALG_MYCC` to `enum msc_udp_cc_algorithm`.
2. In `read_env()`, add the selector branch before the final unknown-name case:

   ```c
   else if (cc != NULL && strcmp(cc, "mycc") == 0)
      g_cc_algorithm = MSC_UDP_CC_ALG_MYCC;
   ```

   Include `mycc` in the diagnostic's accepted-name list too.
3. Paste `docs/examples/mycc.c` immediately before the **definition** of
   `cc_ops_for()` (after the existing ops tables), then add its switch case:

   ```c
   case MSC_UDP_CC_ALG_MYCC: return &cc_mycc_ops;
   ```

Rebuild and exercise the actual registered controller:

```sh
make udp_test msc
sh docs/examples/check-controller.sh mycc
```

For a remote file transfer, install the rebuilt executable on the participating
hosts and select it normally:

```sh
MSC_UDP_CC=mycc msc --stats --env wan \
  -l localhost -r receiver.example -i sample.bin -o /data/sample.bin
```

`MSC_UDP_CC` already belongs to the SSH environment-forwarding allowlist, so a
new controller name requires no new wire field or forwarding rule. The default
SSH stdio control channel remains the same.

## Callback contract

All callback pointers must be populated; use the existing `cc_noop_*` helpers
for unused events. State belongs to a flow, and callbacks run in that flow's
sender worker. Different flows run concurrently. Add algorithm-owned fields to
`sender_flow` and allocate/release resources in `init`/`destroy` as needed.

| Callback | Input and purpose |
| --- | --- |
| `init`, `destroy` | Initialize/release per-flow algorithm state. |
| `on_sent` | Sequence and local timestamp of fresh or retransmitted sends. |
| `on_delivered` | Receiver's cumulative delivered-unit count and local time. |
| `on_rtt` | Accepted RTT sample in nanoseconds and local time. |
| `on_ack` | Newest cumulatively acknowledged sequence, newly acknowledged unit count, and time. Update window growth here. |
| `on_sack` | Highest SACK endpoint and time; useful for delivery sampling and round updates. |
| `on_loss` | Fast-retransmission loss notification. Controller/recovery state prevents repeated cuts for one episode. |
| `on_ecn` | Eligible increase in the receiver's cumulative CE count. |
| `on_rto` | Timeout response; transport owns packet repair and timer backoff. |

`cwnd`, `rwnd`, sequence numbers, and delivered counts use **units**, not bytes.
Timestamps and RTT samples use nanoseconds. Convert using the session payload
size when calculating bit rates. File and stripe tails may be shorter than a
full payload.

In `on_ack`, `snd_una` has not yet advanced to the new cumulative ACK. Use the
supplied `acked` count and `newest_seq` when computing post-ACK state. The core
owns sequence progress, cache retirement, validation, retransmission selection,
and the receive-window limit; controllers should not advance those pointers or
send packets directly.

The example reuses Reno recovery helpers, including DSACK undo bookkeeping.
If you replace loss handling, decide how your extra state should be restored
when an episode proves spurious. Setting `cwnd` alone does not save and restore
arbitrary algorithm state.

## Pacing integration

For a window-based experiment, leave `uses_rate_samples=0`. Existing optional
pacing derives its rate from `MSC_UDP_PACE_GAIN * cwnd/SRTT`. Compare paced and
unpaced runs with the same algorithm using `MSC_UDP_PACE`.

For a delivery-rate experiment, use `cc_rate_ops` and the `rate_*` helpers as
the starting point. `uses_rate_samples=1` activates the existing rate-controller
pacing and telemetry paths, which read `btl_bw_ups`, `rate_pacing_gain`, and
related state. It also forces pacing on. This flag is not a generic subscription
to samples: initialize and maintain the state those paths expect, or extend
the pacing integration deliberately.

Add algorithm-specific fields to `flow_sample` as needed. The human end-of-flow
summary prints the selected ops-table name automatically. See
[congestion control](congestion-control.md) for the built-in policies and
[troubleshooting](troubleshooting.md) for existing metrics.

## Test progressively

The clean/loss helper checks registration and byte delivery. Then use the WAN
shim for delay, jitter, loss, and MTU experiments without privileges:

```sh
make build/wan_shim.so
work=$(mktemp -d)
head -c 8M /dev/urandom > "$work/input.bin"
timeout 90 env LD_PRELOAD="$PWD/build/wan_shim.so" \
  MSC_UDP_CC=mycc MSC_UDP_CTL=stdio MSC_UDP_PROFILE=wan \
  MSC_UDP_STATS=1 MSC_UDP_WANSHIM_DELAY_MS=15 \
  MSC_UDP_WANSHIM_JITTER_MS=3 MSC_UDP_WANSHIM_LOSS_PCT=1 \
  MSC_UDP_WANSHIM_SEED=12345 MSC_UDP_WANSHIM_TRACE=1 \
  ./build/udp_test "$work/input.bin" "$work/output.bin" 2 2>"$work/wan.log"
cmp "$work/input.bin" "$work/output.bin"
```

The local harness forks both endpoints, so both inherit the preload. On real
hosts, preloading a local process does not install/preload the shim remotely.

Exercise one and several flows, small and large files, ACK loss/duplicates,
reordering, RTO recovery, receiver backpressure, and relevant control modes.
The shim supports duplication and initial ACK/FIN drops; [testing](testing.md)
lists its controls. Fix flow count, payload, profile, and offloads when
comparing algorithms. Use a fresh process for each configuration because engine
environment settings are read once.

Run the general regression suites after integration:

```sh
make udp_parity udp_resume_suite resume_suite wanshim
```

Some suite arms explicitly select built-in controllers. Run direct `udp_test`
scenarios with your name to establish your algorithm's coverage. A passing byte
comparison does not establish throughput, stability, or fairness. Algorithm
experiments should state their expected response and include competing traffic
when making shared-path claims.
