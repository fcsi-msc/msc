#!/usr/bin/env python3
"""Interleave already deployed builds over real SSH; verify outside timing."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shlex
import signal
import statistics
import subprocess
import tempfile
import threading
import time
import uuid

CASES = {"default": [], "n1": ["-n1"], "n4": ["-n4"], "n8": ["-n8"],
         "n30": ["-n30"], "n30-k30": ["-n30", "--udp-data-ports", "30"],
         "tcp": ["-T"], "checksum": ["--checksum=true"],
         "packet": ["--no-gso", "--no-gro", "-s", "1400"],
         "packet-n30": ["--no-gso", "--no-gro", "-s", "1400", "-n30"]}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--remote-dir", required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--build", action="append", required=True,
                   help="label,local-executable,remote-executable (repeatable)")
    p.add_argument("--case", choices=CASES, action="append")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--timeout", type=float, default=90)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    builds = [value.split(",", 2) for value in a.build]
    if any(len(b) != 3 for b in builds) or a.repeats < 1:
        p.error("use label,local-executable,remote-executable and positive repeats")
    if any(not re.fullmatch(r"[A-Za-z0-9_-]+", b[0]) for b in builds):
        p.error("build labels must contain only letters, digits, hyphens and underscores")
    if len({b[0] for b in builds}) != len(builds):
        p.error("build labels must be unique")
    a.output.mkdir(parents=True, exist_ok=False)
    source = a.source.resolve()
    expected, size = digest(source), source.stat().st_size
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
           "-o", "ConnectTimeout=5", f"{a.user}@{a.host}"]

    def remote(argv):
        return subprocess.check_output(ssh + [shlex.join(argv)], text=True, timeout=30)

    metadata = {"host": a.host, "user": a.user, "source_bytes": size,
                "source_sha256": expected, "local_uname": list(os.uname()),
                "remote_uname": remote(["uname", "-a"]).strip(),
                "runner_sha256": digest(Path(__file__)),
                "builds": [{"label": b[0], "local_sha256": digest(Path(b[1])),
                            "remote_sha256": remote(["sha256sum", b[2]]).split()[0]}
                           for b in builds], "argv": vars(a).copy(),
                "timing": "launch to process exit; SSH included; sha256 excluded",
                "cpu": "local process tree only; RSS largest local process"}
    (a.output / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n")
    cases = a.case or ["default", "n8", "tcp"]
    rng, rows, token = random.Random(20260926), [], uuid.uuid4().hex[:12]
    base_env = {k: v for k, v in os.environ.items() if not k.startswith("MSC_")}
    base_env.pop("LD_PRELOAD", None)
    with tempfile.TemporaryDirectory(prefix="msc-wired-runner-") as temp:
        wrapper = Path(temp) / "ssh"
        wrapper.write_text('#!/bin/sh\nexec ssh -o BatchMode=yes -o StrictHostKeyChecking=yes '
                           '-o ConnectTimeout=5 "$@"\n')
        wrapper.chmod(0o755)
        base_env["MSC_SSH"] = str(wrapper)
        for rep in range(1, a.repeats + 1):
            jobs = [(b, case) for b in builds for case in cases]
            rng.shuffle(jobs)
            for (label, local, peer), case in jobs:
                name = f"{rep:02d}-{label}-{case}"
                dest = f"{a.remote_dir}/bench-{token}-{name}"
                timing = a.output / f"{name}.time"
                cmd = ["/usr/bin/time", "-f", "%U %S %M", "-o", str(timing),
                       str(Path(local).resolve()), *CASES[case], "-u", a.user,
                       "-l", "localhost", "-r", a.host, "-B", peer,
                       "-i", str(source), "-o", dest]
                with (a.output / f"{name}.log").open("w") as log:
                    start = time.monotonic()
                    proc = subprocess.Popen(cmd, env=base_env, stdout=log,
                                            stderr=log, start_new_session=True)

                    def kill():
                        try:
                            os.killpg(proc.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

                    timer = threading.Timer(a.timeout, kill)
                    timer.start()
                    try:
                        rc = proc.wait()
                    except BaseException:
                        kill()
                        proc.wait()
                        raise
                    finally:
                        timer.cancel()
                    elapsed = time.monotonic() - start
                verified = False
                verification_error = ""
                if rc == 0:
                    try:
                        verified = remote(["sha256sum", dest]).split()[0] == expected
                    except (subprocess.SubprocessError, IndexError) as error:
                        verification_error = str(error)
                fields = timing.read_text().splitlines() if timing.exists() else []
                fields = fields[-1].split() if fields else []
                if len(fields) != 3:
                    fields = ["", "", ""]
                row = dict(build=label, case=case, repeat=rep, seconds=elapsed,
                           gbit_s=size * 8 / elapsed / 1e9 if verified else "",
                           exit=rc, verified=verified, verification_error=verification_error,
                           user_s=fields[0], system_s=fields[1], max_rss_kb=fields[2])
                rows.append(row)
                with (a.output / "results.csv").open("w") as stream:
                    writer = csv.DictWriter(stream, fieldnames=row.keys())
                    writer.writeheader()
                    writer.writerows(rows)
                goodput = f"{row['gbit_s']:.3f} Gbit/s" if verified else "failed"
                print(f"{name}: {elapsed:.3f}s {goodput} verified={verified}", flush=True)
                if not verified:
                    raise RuntimeError(f"Transfer failed; inspect {name}.log")
                remote(["rm", "--", dest])  # only this run's successfully verified fixture
    summaries = []
    for label, _, _ in builds:
        for case in cases:
            group = [r for r in rows if r["build"] == label and r["case"] == case]
            summaries.append({"build": label, "case": case,
                              "median_seconds": statistics.median(r["seconds"] for r in group),
                              "median_gbit_s": statistics.median(r["gbit_s"] for r in group)})
    (a.output / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")


if __name__ == "__main__":
    main()
