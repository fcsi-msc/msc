#!/usr/bin/env python3
"""Measure sender NIC throughput during one already-deployed wired transfer."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import tempfile
import time
import uuid


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--interface", required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--remote-dir", required=True)
    p.add_argument("--local-bin", type=Path, required=True)
    p.add_argument("--remote-bin", required=True)
    p.add_argument("--flows", type=int, default=8)
    p.add_argument("--transport", choices=("udp", "tcp"), default="udp")
    p.add_argument("--interval", type=float, default=0.25)
    p.add_argument("--timeout", type=float, default=180)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    if a.flows < 1 or a.interval <= 0 or a.timeout <= 0:
        p.error("flows, interval, and timeout must be positive")
    source = a.source.resolve()
    local_bin = a.local_bin.resolve()
    counter = Path("/sys/class/net") / a.interface / "statistics/tx_bytes"
    speed = (Path("/sys/class/net") / a.interface / "speed").read_text().strip()
    mtu = (Path("/sys/class/net") / a.interface / "mtu").read_text().strip()
    expected = digest(source)
    a.output.mkdir(parents=True, exist_ok=False)
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
           "-o", "ConnectTimeout=5", f"{a.user}@{a.host}"]

    def remote(argv):
        return subprocess.check_output(ssh + [shlex.join(argv)], text=True, timeout=30).strip()

    remote_hash = remote(["sha256sum", a.remote_bin]).split()[0]
    if remote_hash != digest(local_bin):
        raise RuntimeError("local and remote executables differ")
    destination = f"{a.remote_dir}/link-util-{uuid.uuid4().hex}"
    metadata = {"source_bytes": source.stat().st_size, "source_sha256": expected,
                "local_binary_sha256": remote_hash, "host": a.host, "user": a.user,
                "interface": a.interface, "interface_speed_mbit": speed,
                "interface_mtu": mtu,
                "flows": a.flows, "transport": a.transport,
                "sample_interval_s": a.interval, "destination": destination,
                "counter": "Linux /sys/class/net/<interface>/statistics/tx_bytes",
                "timing": "process start to exit; SSH included; verification excluded"}
    (a.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("MSC_")}
    env.pop("LD_PRELOAD", None)
    with tempfile.TemporaryDirectory(prefix="msc-link-util-") as temp:
        wrapper = Path(temp) / "ssh"
        wrapper.write_text('#!/bin/sh\nexec ssh -o BatchMode=yes '
                           '-o StrictHostKeyChecking=yes -o ConnectTimeout=5 "$@"\n')
        wrapper.chmod(0o755)
        env["MSC_SSH"] = str(wrapper)
        cmd = [str(local_bin), "-n", str(a.flows)]
        if a.transport == "tcp":
            cmd.append("-T")
        cmd += ["-u", a.user, "-l", "localhost", "-r", a.host,
                "-B", a.remote_bin, "-i", str(source), "-o", destination]
        samples = []
        with (a.output / "transfer.log").open("w") as log:
            start = time.monotonic()
            proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT,
                                    start_new_session=True)
            last_t, last_b = start, int(counter.read_text())
            while proc.poll() is None:
                time.sleep(a.interval)
                now, sent = time.monotonic(), int(counter.read_text())
                samples.append({"elapsed_s": round(now - start, 6),
                                "interval_s": round(now - last_t, 6),
                                "tx_bytes": sent - last_b,
                                "tx_mbit_s": round((sent - last_b) * 8 / (now - last_t) / 1e6, 3)})
                last_t, last_b = now, sent
                if now - start > a.timeout:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
                    raise TimeoutError(f"transfer exceeded {a.timeout} seconds")
            elapsed, rc = time.monotonic() - start, proc.returncode
    with (a.output / "samples.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=samples[0] if samples else
                                ("elapsed_s", "interval_s", "tx_bytes", "tx_mbit_s"))
        writer.writeheader()
        writer.writerows(samples)
    if rc != 0:
        raise RuntimeError(f"MSC exited {rc}; see transfer.log")
    verified = remote(["sha256sum", destination]).split()[0] == expected
    (a.output / "result.json").write_text(json.dumps({"elapsed_s": elapsed,
         "payload_gbit_s": source.stat().st_size * 8 / elapsed / 1e9,
         "verified": verified}, indent=2) + "\n")
    if not verified:
        raise RuntimeError("remote SHA-256 mismatch")
    remote(["rm", "--", destination])
    print(f"{a.transport} n={a.flows}: {elapsed:.3f}s, "
          f"{source.stat().st_size * 8 / elapsed / 1e6:.1f} Mbit/s payload, "
          f"{len(samples)} NIC samples, SHA-256 verified", flush=True)


if __name__ == "__main__":
    main()
