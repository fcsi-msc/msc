# Benchmarking MSC

The runners measure complete transfers and verify received bytes outside the
timed interval. Record the MSC revision and executable hashes, compiler and
kernel versions, NIC speed and MTU, source/destination storage, cache state,
payload size, flow count, offloads, socket limits, and any path impairment.
Repeat and interleave comparisons; one fast run does not establish a gain.

## Local transfers and simulated WAN paths

Build MSC and the WAN shim before measuring:

```sh
make -j8 all build/wan_shim.so
python3 benchmarks/benchmark.py --suite local --size-mib 256 --repeats 3 \
  --output /tmp/msc-local-results
python3 benchmarks/benchmark.py --suite tree --tree-files 1024 --repeats 3 \
  --output /tmp/msc-tree-results
python3 benchmarks/benchmark.py --suite wan --size-mib 64 --repeats 3 \
  --output /tmp/msc-wan-results
```

Each output directory must be new. The runner generates fixtures there, uses a
local SSH stand-in, and keeps transfer logs, metadata, CSV rows, and a summary.
It excludes fixture creation and independent byte comparison from timing.
Use `--case NAME` to select cases or `--compare-binary PATH` to interleave an
alternate executable. Failed transfers retain their logs and receive no
throughput value.

The WAN suite models delay and loss in userspace. Its packet cases disable
GSO/GRO, while `udp-n8-offload` exercises the per-datagram GSO-aware shim.
The shim does not model physical NICs, storage, or competing traffic. Compare
only cases with the same impairment and verification settings.

## Two-host transfers

`wired.py` requires both executables to be installed or copied to the peer,
trusted SSH host keys, and working noninteractive SSH. Create a unique
temporary directory on the peer and generate a source fixture before running:

```sh
python3 benchmarks/wired.py --host PEER_IP --user ACCOUNT \
  --remote-dir /tmp/msc-benchmark --source /tmp/generated-source \
  --build baseline,/path/to/baseline,/tmp/msc-benchmark/baseline \
  --build candidate,/path/to/candidate,/tmp/msc-benchmark/candidate \
  --case default --case n8 --case tcp --repeats 3 \
  --output /tmp/msc-wired-results
```

The runner interleaves builds and cases, times the SSH launch through process
exit, checks the destination SHA-256 afterward, and removes only its own
verified destination files. `--case packet` uses 1,400-byte file payloads with
GSO/GRO disabled, useful when kernel netem must act on individual datagrams.
Record the affected direction and queue counters, and restore the original
traffic-control settings after any impairment run.

To see whether a long transfer keeps the sender NIC busy, sample its transmit
counter with `link_utilization.py`:

```sh
python3 benchmarks/link_utilization.py --interface eno0 \
  --source /tmp/generated-source --host PEER_IP --user ACCOUNT \
  --remote-dir /tmp/msc-benchmark --local-bin ./msc \
  --remote-bin /tmp/msc-benchmark/msc --flows 8 \
  --output /tmp/msc-link-utilization
```

The script saves timestamped `tx_bytes` samples, total payload throughput, the
transfer log, and SHA-256 verification. The NIC counter includes protocol
traffic, so compare it with the payload result to distinguish link saturation
from application throughput. It is a software counter, not a physical-layer
analyzer; background traffic on the interface also appears in the samples.

## Directory-manifest scans

`make build/manifest_scan` builds a read-only manifest benchmark. Run
`/usr/bin/time build/manifest_scan SOURCE_DIRECTORY` on a generated tree
to isolate scanning from transfer and destination work. It prints regular-file
count, total bytes, and manifest fingerprint. Compare fingerprints when
benchmarking a code change, especially for trees containing hard links.
