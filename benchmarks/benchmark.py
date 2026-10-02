#!/usr/bin/env python3
"""Measure the real MSC CLI using loopback data and a local SSH stand-in.

Run suites serially on an otherwise idle host. This measures warm-cache local
completion, not physical NIC performance or real SSH setup. Requires GNU time,
cmp, and a built msc; the WAN suite also requires build/wan_shim.so.
"""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import shutil
import signal
import statistics
import subprocess
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "local": [
        ("udp-default", [], {}),
        ("udp-n1", ["-n", "1"], {}),
        ("udp-n4", ["-n", "4"], {}),
        ("udp-n8", ["-n", "8"], {}),
        ("udp-n30-k30", ["-n", "30", "--udp-data-ports", "30"], {}),
        ("udp-default-no-offload", ["--no-gso", "--no-gro"], {}),
        ("udp-default-pwritev", [], {"MSC_UDP_NO_MMAP": "1"}),
        ("udp-default-checksum", ["--checksum=true"], {}),
        ("udp-default-stats", [], {"MSC_UDP_STATS": "1"}),
        ("tcp-default", ["-T"], {}),
    ],
    "wan": [
        ("udp-n30-k8", ["-n", "30"], {}),
        ("udp-n4", ["-n", "4"], {}),
        ("udp-n8", ["-n", "8"], {}),
        ("udp-n30-k30", ["-n", "30", "--udp-data-ports", "30"], {}),
        ("udp-n8-cubic", ["-n", "8"], {"MSC_UDP_CC": "cubic"}),
    ],
    "tree": [
        ("udp-tree-default", ["-R"], {}),
        ("udp-tree-n4", ["-R", "-n", "4"], {}),
        ("udp-tree-n8", ["-R", "-n", "8"], {}),
        ("udp-tree-no-offload", ["-R", "--no-gso", "--no-gro"], {}),
        ("udp-tree-fsync8", ["-R"], {"MSC_UDP_FSYNC_THREADS": "8"}),
        ("tcp-tree-default", ["-R", "-T"], {}),
    ],
}
# Retain the original packet-level matrix for comparison with archived results.
# The corrected shim also impairs GSO segments individually (shim_check gates it).
CASES["wan"] = [(name, ["--no-gso", "--no-gro", "-s", "1400", *flags], settings)
                for name, flags, settings in CASES["wan"]]
CASES["wan"].append(("udp-n8-offload", ["-n", "8", "-s", "1400"], {}))


def command_output(argv):
    result = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify(source, destination):
    if source.is_dir():
        expected = sorted(p.relative_to(source) for p in source.rglob("*"))
        actual = sorted(p.relative_to(destination) for p in destination.rglob("*"))
        if expected != actual:
            return False
        return all(
            destination.joinpath(p).is_dir() if source.joinpath(p).is_dir()
            else subprocess.run(["cmp", "-s", source / p, destination / p]).returncode == 0
            for p in expected
        )
    return subprocess.run(["cmp", "-s", source, destination]).returncode == 0


def run_case(binary, source, destination, fake_ssh, output, name, rep, flags,
             settings, base_env, size, deadline):
    log = output / f"{rep:02d}-{name}.log"
    timing = output / f"{rep:02d}-{name}.time"
    env = dict(base_env, MSC_SSH=str(fake_ssh), **settings)
    # GNU time accounts for the waited-for process tree; max RSS is a maximum
    # for one process, not the sum of simultaneous sender/receiver RSS.
    command = [
        "/usr/bin/time", "-f", "%U\t%S\t%M\t%w\t%c", "-o", str(timing),
        str(binary), *flags, "-l", "localhost", "-r", "127.0.0.1",
        "-B", str(binary), "-i", str(source), "-o", str(destination),
    ]
    start = time.monotonic()
    with log.open("w") as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=stream,
                                   env=env, start_new_session=True)
        expired = threading.Event()

        def stop():
            expired.set()
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        timer = threading.Timer(deadline, stop)
        timer.daemon = True
        timer.start()
        try:
            # A blocking wait avoids wait(timeout)'s polling delay distorting
            # short transfers. The timer only runs if the deadline expires.
            status = process.wait()
        except BaseException:
            stop()
            process.wait()
            raise
        finally:
            timer.cancel()
        if expired.is_set():
            status = 124
    elapsed = time.monotonic() - start
    valid = status == 0 and verify(source, destination)
    timing_lines = timing.read_text().splitlines() if timing.exists() else []
    usage = timing_lines[-1].split("\t") if timing_lines else []
    row = {
        "case": name, "repeat": rep, "bytes": size, "seconds": round(elapsed, 6),
        "gbit_s": round(size * 8 / elapsed / 1e9, 6) if valid else "",
        "user_seconds": usage[0] if len(usage) == 5 else "",
        "system_seconds": usage[1] if len(usage) == 5 else "",
        "largest_process_peak_rss_kib": usage[2] if len(usage) == 5 else "",
        "voluntary_context_switches": usage[3] if len(usage) == 5 else "",
        "involuntary_context_switches": usage[4] if len(usage) == 5 else "",
        "exit_code": status, "verified": valid,
    }
    if destination.is_dir():
        shutil.rmtree(destination)
    elif destination.exists():
        destination.unlink()
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "msc")
    parser.add_argument("--compare-binary", type=Path,
                        help="interleave the same cases using a candidate binary")
    parser.add_argument("--case", action="append", dest="selected_cases",
                        help="case name to include; may be repeated")
    parser.add_argument("--output", type=Path, required=True, help="new results directory")
    parser.add_argument("--suite", choices=CASES, default="local")
    parser.add_argument("--size-mib", type=int, default=256)
    parser.add_argument("--tree-files", type=int, default=1024)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=120, help="seconds per run")
    args = parser.parse_args()
    if min(args.size_mib, args.tree_files, args.repeats, args.timeout) < 1:
        parser.error("sizes, repetitions, and timeout must be positive")
    binary = args.binary.resolve(strict=True)
    candidate = args.compare_binary.resolve(strict=True) if args.compare_binary else None
    chosen = CASES[args.suite]
    if args.selected_cases:
        unknown = set(args.selected_cases) - {name for name, _, _ in chosen}
        if unknown:
            parser.error(f"unknown case(s): {', '.join(sorted(unknown))}")
        chosen = [case for case in chosen if case[0] in args.selected_cases]
    run_cases = [(name, flags, settings, binary) for name, flags, settings in chosen]
    if candidate:
        run_cases += [("candidate-" + name, flags, settings, candidate)
                      for name, flags, settings in chosen]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("MSC_") and k != "LD_PRELOAD"}
    env["LC_ALL"] = "C"
    if args.suite == "wan":
        shim = (ROOT / "build/wan_shim.so").resolve(strict=True)
        env.update(LD_PRELOAD=str(shim), MSC_UDP_WANSHIM_DELAY_MS="25",
                   MSC_UDP_WANSHIM_LOSS_PCT="0.1", MSC_UDP_WANSHIM_SEED="12345",
                   MSC_UDP_WANSHIM_TRACE="1", MSC_UDP_WANSHIM_MTU="1448",
                   MSC_UDP_STATS="1")
    metadata = {
        "revision": command_output(["git", "rev-parse", "HEAD"]),
        "kernel": platform.platform(), "cpus": os.cpu_count(),
        "binary_sha256": sha256(binary), "benchmark_sha256": sha256(Path(__file__)),
        "candidate_sha256": sha256(candidate) if candidate else None,
        "compiler": command_output(["cc", "--version"]).splitlines()[0],
        "filesystem": command_output(["stat", "-f", "-c", "%T", str(output)]),
        "rmem_max": Path("/proc/sys/net/core/rmem_max").read_text().strip(),
        "wmem_max": Path("/proc/sys/net/core/wmem_max").read_text().strip(),
        "suite": args.suite, "size_mib": args.size_mib, "tree_files": args.tree_files,
        "repeats": args.repeats, "cases": [(n, f, s, str(b)) for n, f, s, b in run_cases],
        "impairment": {k: v for k, v in env.items() if k.startswith("MSC_")},
        "scope": "loopback; local SSH stand-in; warm cache; byte checks outside timing",
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    with tempfile.TemporaryDirectory(prefix="fixtures-", dir=output) as temporary:
        work = Path(temporary)
        fake_ssh = work / "ssh-local"
        fake_ssh.write_text('#!/bin/sh\nwhile [ "$1" = "-o" ]; do shift 2; done\n'
                            'shift\nexec /bin/sh -c "$*"\n')
        fake_ssh.chmod(0o700)
        source = work / "source"
        if args.suite == "tree":
            source.mkdir()
            for index in range(args.tree_files):
                parent = source / str(index % 16)
                parent.mkdir(exist_ok=True)
                (parent / str(index)).write_bytes(os.urandom(4096))
            size = args.tree_files * 4096
        else:
            block = os.urandom(1024 * 1024)
            with source.open("wb") as stream:
                for _ in range(args.size_mib):
                    stream.write(block)
            size = args.size_mib * 1024 * 1024
        with (output / "results.csv").open("w", newline="") as stream:
            writer = None
            for rep in range(1, args.repeats + 1):
                cases = list(run_cases)
                random.Random(20260926 + rep).shuffle(cases)
                for name, flags, settings, executable in cases:
                    row = run_case(executable, source, work / "destination", fake_ssh,
                                   output, name, rep, flags, settings, env, size, args.timeout)
                    rows.append(row)
                    if writer is None:
                        writer = csv.DictWriter(stream, fieldnames=row.keys())
                        writer.writeheader()
                    writer.writerow(row)
                    stream.flush()
                    print(json.dumps(row), flush=True)
                    if not row["verified"]:
                        raise SystemExit(f"Failed run; see {output / f'{rep:02d}-{name}.log'}")
    summary = {}
    for name, _, _, _ in run_cases:
        sample = [row["seconds"] for row in rows if row["case"] == name]
        summary[name] = {"median_seconds": statistics.median(sample),
                         "min_seconds": min(sample), "max_seconds": max(sample),
                         "median_gbit_s": size * 8 / statistics.median(sample) / 1e9}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
