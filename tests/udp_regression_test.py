#!/usr/bin/env python3
"""Release regressions: actual file contents and durability, using loopback."""
import os
from pathlib import Path
import signal
import re
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BASE = {k: v for k, v in os.environ.items() if not k.startswith("MSC_")}
BASE.pop("LD_PRELOAD", None)


def run(args, settings=None, expected=0):
    with subprocess.Popen([str(a) for a in args], cwd=ROOT,
                          env=dict(BASE, **(settings or {})),
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, start_new_session=True) as process:
        try:
            output, _ = process.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate()
            raise AssertionError(f"Timed out: {args}\n{output}") from None
    assert process.returncode == expected, (args, process.returncode, expected, output)
    return output


def main():
    version = re.search(r"#define MSC_UDP_PROTO_VERSION\s+(\d+)",
                        (ROOT / "src/udp_session.h").read_text()).group(1)
    documented = re.search(r"protocol version is \*\*(\d+)\*\*",
                           (ROOT / "docs/wire-protocol.md").read_text()).group(1)
    assert version == documented, (version, documented)
    with tempfile.TemporaryDirectory(prefix="msc-regression-") as directory:
        work = Path(directory)
        source = work / "source"
        source.write_bytes(os.urandom(1024 * 1024))
        wan = str(ROOT / "build/wan_shim.so")
        fault = str(ROOT / "build/fsync_fault_shim.so")
        # Test automatic PMTUD with both the GSO and sendmmsg transmit paths.
        for mtu in (1280, 548):
            for offload in (True, False):
                dest = work / f"mtu-{mtu}-{offload}"
                env = {"LD_PRELOAD": wan, "MSC_UDP_WANSHIM_MTU": str(mtu),
                       "MSC_UDP_STATS": "1"}
                if not offload:
                    env.update(MSC_TEST_NO_GSO="1", MSC_TEST_NO_GRO="1")
                run([ROOT / "build/udp_test", source, dest, "2"], env)
                assert dest.read_bytes() == source.read_bytes()
        out = run([ROOT / "build/udp_test", source, work / "no-path", "1"],
                  {"LD_PRELOAD": wan, "MSC_UDP_WANSHIM_MTU": "100"}, expected=6)
        assert "no usable payload" in out
        # With UDP_GRO the kernel can deliver one round's largest probe and the
        # next round's smallest as a single buffer. The ceiling must still come
        # from one probe: on a 2016-byte path, 2016 and not 2016 + 548.
        gro = str(ROOT / "build/gro_coalesce_shim.so")
        dest = work / "gro-probes"
        out = run([ROOT / "build/udp_test", source, dest, "2"],
                  {"LD_PRELOAD": f"{wan} {gro}", "MSC_UDP_WANSHIM_MTU": "2016",
                   "MSC_UDP_STATS": "1"})
        assert dest.read_bytes() == source.read_bytes()
        assert "PMTUD saw payload 2016 " in out, out
        # A receiver that still reports the summed size (an older release)
        # must not push the sender above the largest size it actually probed.
        dest = work / "gro-probes-as-one"
        out = run([ROOT / "build/udp_test", source, dest, "2"],
                  {"LD_PRELOAD": f"{wan} {gro}", "MSC_UDP_WANSHIM_MTU": "2016",
                   "MSC_TEST_COALESCE_AS_ONE": "1", "MSC_UDP_STATS": "1"})
        assert dest.read_bytes() == source.read_bytes()
        assert "PMTUD saw payload 2564 " in out, out
        assert re.search(r"confirmed_payload 2016 -> payload ceiling 1968\b", out), out
        print("ok: reduced MTU with/without offloads; unconfirmed path fails; "
              "coalesced probes", flush=True)

        # MSC_UDP_STATS reports where each side's wall time went, and a slow
        # destination reservation is charged to the receiver's prepare phase.
        storage = str(ROOT / "build/storage_fault_shim.so")
        dest = work / "phases"
        out = run([ROOT / "build/udp_test", source, dest, "2"],
                  {"LD_PRELOAD": storage, "MSC_UDP_RESERVE_UPFRONT": "1",
                   "MSC_TEST_FALLOCATE_DELAY_MS": "1000",
                   "MSC_UDP_STATS": "1"})
        assert dest.read_bytes() == source.read_bytes()
        names = ("start", "open", "probe", "setup", "prepare", "data", "finish", "close")
        pattern = (r"MSC UDP (sender|receiver) phases: "
                   + " ".join(rf"{n} (?P<{n}>\d+\.\d{{3}})s" for n in names)
                   + r" total (?P<total>\d+\.\d{3})s transfers 1\n")
        sides = {m.group(1): m for m in re.finditer(pattern, out)}
        assert set(sides) == {"sender", "receiver"}, out
        for m in sides.values():
            parts = sum(float(m.group(n)) for n in names)
            assert abs(parts - float(m.group("total"))) < 0.01, m.group(0)
        assert float(sides["receiver"].group("prepare")) >= 0.9, out
        assert float(sides["receiver"].group("data")) < 0.9, out
        # A failed transfer still reports both sides' phases; a stall caused
        # by losing every data packet fails the transfer.
        out = run([ROOT / "build/udp_test", source, work / "phases-failed", "2", "100"],
                  {"MSC_TEST_STALL_TIMEOUT_MS": "500", "MSC_UDP_STATS": "1"}, expected=6)
        failed = set(re.findall(r"MSC UDP (sender|receiver) phases: ", out))
        assert failed == {"sender", "receiver"}, out
        print("ok: phase timings charge a slow reservation to the receiver's prepare; "
              "failures report them too", flush=True)

        # The sender waits until the receiver has prepared the destination.
        # Preparation that keeps advancing may outlast the stall timeout (four
        # 1 MiB reservation steps of 0.6 s under a 1 s timeout), and no data is
        # sent, and lost, while the receiver cannot take it.
        dest = work / "slow-prepare"
        source4 = work / "source4"
        source4.write_bytes(os.urandom(4 * 1024 * 1024))
        out = run([ROOT / "build/udp_test", source4, dest, "2"],
                  {"LD_PRELOAD": storage, "MSC_UDP_RESERVE_UPFRONT": "1",
                   "MSC_TEST_FALLOCATE_DELAY_MS": "600",
                   "MSC_TEST_RESERVE_CHUNK_MB": "1", "MSC_TEST_STALL_TIMEOUT_MS": "1000",
                   "MSC_UDP_STATS": "1"})
        assert dest.read_bytes() == source4.read_bytes()
        # The sender's flows start only once the receiver is ready: its own
        # prepare phase covers the receiver's 2.4 s of reservation. (Counting
        # retransmissions instead would also catch ordinary data-phase loss on
        # hosts with small socket buffers.)
        prepare = {m.group(1): float(m.group(2)) for m in re.finditer(
            r"MSC UDP (sender|receiver) phases: .* prepare (\d+\.\d+)s", out)}
        assert set(prepare) == {"sender", "receiver"}, out
        assert prepare["receiver"] >= 2.3 and prepare["sender"] >= 2.3, out
        # Preparation that stops advancing is still a stall: with a reservation
        # step hung for 4 s under a 1 s timeout, the sender gives up. (This
        # harness runs the receiver in-process, so the run itself lasts until
        # the hung step returns; the CLI case below checks how fast msc exits.)
        out = run([ROOT / "build/udp_test", source, work / "stuck-prepare", "2"],
                  {"LD_PRELOAD": storage, "MSC_UDP_RESERVE_UPFRONT": "1",
                   "MSC_TEST_FALLOCATE_DELAY_MS": "4000",
                   "MSC_TEST_STALL_TIMEOUT_MS": "1000"}, expected=6)
        assert "no progress preparing the destination for 1.0 seconds" in out, out
        # Once the sender has given up, the receiver abandons the preparation at
        # its next step instead of finishing it: four 1.5 s steps (6 s in all)
        # under a 1 s timeout end after the first.
        started = time.monotonic()
        out = run([ROOT / "build/udp_test", source4, work / "abandoned-prepare", "2"],
                  {"LD_PRELOAD": storage, "MSC_UDP_RESERVE_UPFRONT": "1",
                   "MSC_TEST_FALLOCATE_DELAY_MS": "1500",
                   "MSC_TEST_RESERVE_CHUNK_MB": "1", "MSC_TEST_STALL_TIMEOUT_MS": "1000",
                   "MSC_UDP_STATS": "1"}, expected=6)
        assert time.monotonic() - started < 4, out
        assert "abandoning the destination" in out, out
        # A transfer that never started its flows spent that time preparing.
        spent = {m.group(1): (float(m.group(2)), float(m.group(3))) for m in re.finditer(
            r"MSC UDP (sender|receiver) phases: .* prepare (\d+\.\d+)s .* finish (\d+\.\d+)s",
            out)}
        assert set(spent) == {"sender", "receiver"}, out
        assert spent["sender"][0] >= 0.9 and spent["sender"][1] < 0.5, out
        assert spent["receiver"][0] >= 1.4 and spent["receiver"][1] < 0.5, out
        print("ok: the sender waits for a slow destination, not a stuck one", flush=True)

        # The receiver's stats summary counts bytes that arrived, not the planned
        # size: a complete transfer reports all of it, a fully lost one none.
        out = run([ROOT / "build/udp_test", source, work / "rx-summary", "2"],
                  {"MSC_UDP_STATS": "1"})
        assert f"receiver rx: {source.stat().st_size} bytes in " in out, out
        out = run([ROOT / "build/udp_test", source, work / "rx-summary-lost", "2", "100"],
                  {"MSC_UDP_STATS": "1", "MSC_TEST_STALL_TIMEOUT_MS": "500"}, expected=6)
        assert re.search(r"receiver rx: 0 bytes in \d+ ms, avg 0\.00 Gbit/s", out), out
        print("ok: receiver rx summary reports bytes received, not the planned size", flush=True)

        # PMTUD stats must tell a rung the local stack refused (EMSGSIZE from a
        # smaller interface: never on the wire) from one lost downstream. Each
        # limit alone, at the same size, must still settle on the same payload.
        stats = {}
        for name, limits in (("local", {"MSC_UDP_WANSHIM_LOCAL_MTU": "2044"}),
                             ("remote", {"MSC_UDP_WANSHIM_MTU": "2044"}),
                             ("both", {"MSC_UDP_WANSHIM_LOCAL_MTU": "2044",
                                       "MSC_UDP_WANSHIM_MTU": "1500"})):
            dest = work / f"pmtud-{name}"
            out = run([ROOT / "build/udp_test", source, dest, "2"],
                      dict(limits, LD_PRELOAD=wan, MSC_UDP_PMTUD_CAP="9000",
                           MSC_UDP_STATS="1", MSC_UDP_WANSHIM_TRACE="1"))
            assert dest.read_bytes() == source.read_bytes()
            line = re.search(r"MSC UDP sender: PMTUD seed_mtu .*", out)
            assert line, out
            stats[name] = {key.replace(" ", "_"): int(value) for key, value in re.findall(
                r"(confirmed_payload|payload ceiling|ladder|probe_losses|probe_unsendable) (\d+)",
                line.group(0))}
            shim = dict(re.findall(r"(mtu_drops|local_mtu_refusals)=(\d+)", out))
            for limit, counter in (("MSC_UDP_WANSHIM_LOCAL_MTU", "local_mtu_refusals"),
                                   ("MSC_UDP_WANSHIM_MTU", "mtu_drops")):
                assert (int(shim[counter]) > 0) == (limit in limits), (name, shim, out)
        local, remote, both = stats["local"], stats["remote"], stats["both"]
        assert local["probe_losses"] == 0, ("refused rungs counted as losses", local)
        assert local["probe_unsendable"] > 0, local
        assert remote["probe_unsendable"] == 0, remote
        assert remote["probe_losses"] == local["probe_unsendable"], (local, remote)
        for key in ("confirmed_payload", "payload_ceiling", "ladder"):
            assert local[key] == remote[key], (key, local, remote)
        assert both["confirmed_payload"] < local["confirmed_payload"], (both, local)
        assert both["probe_losses"] > 0, both
        assert both["probe_unsendable"] == local["probe_unsendable"], (both, local)
        print("ok: PMTUD stats separate locally unsendable probes from path losses", flush=True)

        # A 1 MiB budget forces repeated wraparound, including SACK, DSACK and
        # retransmission-cache reuse. Compare bytes independently of MSC.
        large = work / "large"
        large.write_bytes(source.read_bytes() * 32)
        for controller in ("rate", "reno", "cubic"):
            dest = work / f"ring-{controller}"
            run([ROOT / "build/udp_test", large, dest, "8", "1"],
                {"MSC_UDP_RETRANSMIT_MB": "1", "MSC_UDP_PAYLOAD": "1400",
                 "MSC_UDP_CC": controller, "MSC_UDP_STATS": "1"})
            assert dest.read_bytes() == large.read_bytes()
        print("ok: bounded retransmission rings wrap under loss for all controllers", flush=True)

        tree = work / "tree"
        tree.mkdir()
        for i in range(4):
            (tree / f"file-{i}").write_bytes(os.urandom(4096))
        # One data worker is pthread_create #1; fsync workers are #2 through #5.
        for call in range(2, 6):
            dest = work / f"thread-{call}"
            record = work / f"fsync-{call}.log"
            out = run([ROOT / "build/udp_test", tree, dest, "1"],
                      {"LD_PRELOAD": fault, "MSC_TEST_FAIL_THREAD_AT": str(call),
                       "MSC_TEST_FSYNC_LOG": str(record), "MSC_UDP_FSYNC_THREADS": "4"})
            assert f"injected thread failure at {call}" in out
            synced = set(record.read_text().splitlines())
            for i in range(4):
                path = dest / f"file-{i}"
                assert path.read_bytes() == (tree / path.name).read_bytes()
                assert "F " + str(path) in synced, (call, path, synced)
        print("ok: every file fsynced after failure at each fsync worker index", flush=True)

        fake = work / "ssh"
        fake.write_text('#!/bin/sh\nwhile [ "$1" = "-o" ]; do shift 2; done\n'
                        'shift\nexec /bin/sh -c "$*"\n')
        fake.chmod(0o755)
        for recursive in (False, True):
            for explicit in (False, True):
                flags = (["-R"] if recursive else []) + (["-n3"] if explicit else [])
                out = run([ROOT / "msc", *flags, "-l", "localhost", "-r", "127.0.0.1",
                           "-B", ROOT / "msc", "-i", tree if recursive else source,
                           "-o", work / f"defaults-{recursive}-{explicit}"],
                          {"MSC_SSH": str(fake)})
                assert f"({3 if explicit else 8} flows)" in out
        print("ok: CLI defaults to eight UDP flows for files/trees; explicit -n wins", flush=True)
        for kind in ("files", "directories"):
            dest = work / f"fsync-fail-{kind}"
            out = run([ROOT / "msc", "-n1", "-l", "localhost", "-r", "127.0.0.1",
                       "-B", ROOT / "msc", "-i", source, "-o", dest],
                      {"MSC_SSH": str(fake), "LD_PRELOAD": fault,
                       "MSC_TEST_FAIL_FSYNC": kind}, expected=4)
            assert "fsync" in out
        print("ok: file and publication-directory fsync failures reach CLI exit 4", flush=True)

        # The single-file finish: the receiver says it is alive while it
        # flushes and publishes, and the sender bounds that wait with
        # --flush-timeout (or 1 s per 2 MiB) instead of waiting forever. The
        # shim slows the data file's fsync, or every directory fsync.
        slow = {"MSC_SSH": str(fake), "LD_PRELOAD": fault,
                "MSC_TEST_FSYNC_DELAY_MIN_BYTES": str(1024 * 1024)}

        def finish(name, sender, receiver, extra, settings, expected):
            """Time msc's own exit. The stand-in ssh runs the receiver here,
            where it shares msc's output, so the output goes to a file and a
            receiver still flushing is given time to finish (and to show it
            publishes nothing behind a failed sender) before it is stopped."""
            dest = work / f"finish-{name}"
            log = work / f"finish-{name}.log"
            started = time.monotonic()
            with open(log, "w") as output:
                process = subprocess.Popen(
                    [str(a) for a in (sender, "-n1", "--stall-timeout", "1s", *extra,
                                      "-l", "localhost", "-r", "127.0.0.1",
                                      "-B", receiver, "-i", source, "-o", dest)],
                    cwd=ROOT, env=dict(BASE, **slow, **settings), stdout=output,
                    stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = process.wait(timeout=60)
                    took = time.monotonic() - started
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        os.killpg(process.pid, 0)
                        time.sleep(0.1)
                except ProcessLookupError:
                    pass
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            out = log.read_text()
            assert code == expected, (name, code, expected, out)
            return out, dest, took

        out, dest, _ = finish("slow-flush", ROOT / "msc", ROOT / "msc", [],
                              {"MSC_TEST_FSYNC_DELAY_MS": "3000"}, 0)
        assert dest.read_bytes() == source.read_bytes()
        assert "waiting for the destination to confirm it is durable (limit 30m00s" in out, out
        out, dest, took = finish("stuck-flush", ROOT / "msc", ROOT / "msc",
                                 ["--flush-timeout", "1s"],
                                 {"MSC_TEST_FSYNC_DELAY_MS": "4000"}, 4)
        assert "did not confirm it was durable within 1.0s" in out, out
        assert took < 3.5, (took, out)
        assert not dest.exists(), out
        out, dest, took = finish("slow-publish", ROOT / "msc", ROOT / "msc",
                                 ["--flush-timeout", "1s"],
                                 {"MSC_TEST_DIR_FSYNC_DELAY_MS": "4000"}, 4)
        assert "did not confirm it was durable within 1.0s" in out, out
        assert took < 3.5, (took, out)
        print("ok: a slow UDP finish completes; a stuck flush or publication exits 4 "
              "at --flush-timeout", flush=True)

        # Checkpointed, each chunk also has its check and its checkpoint update,
        # and the file is published after the last: all bounded the same way.
        ckpt = work / "finish-ckpt-slow.ckpt"
        out, dest, _ = finish("ckpt-slow", ROOT / "msc", ROOT / "msc", ["--checkpoint", ckpt],
                              {"MSC_TEST_FSYNC_DELAY_MS": "2000"}, 0)
        assert dest.read_bytes() == source.read_bytes(), out
        assert not ckpt.exists() and "msc reconnect" not in out, out
        ckpt = work / "finish-ckpt-stuck.ckpt"
        out, dest, took = finish("ckpt-stuck", ROOT / "msc", ROOT / "msc",
                                 ["--checkpoint", ckpt, "--flush-timeout", "1s"],
                                 {"MSC_TEST_FSYNC_DELAY_MS": "4000"}, 4)
        assert "did not confirm it was durable within 1.0s" in out, out
        assert took < 3.5 and ckpt.exists() and not dest.exists(), (took, out)
        run([ROOT / "msc", "-n1", "--resume", "--checkpoint", ckpt, "-l", "localhost",
             "-r", "127.0.0.1", "-B", ROOT / "msc", "-i", source, "-o", dest],
            {"MSC_SSH": str(fake)})
        assert dest.read_bytes() == source.read_bytes() and not ckpt.exists()
        print("ok: a checkpointed UDP finish survives slow storage; stuck, it exits 4 "
              "and --resume finishes", flush=True)

        # Against MSC 1.1.0, built from its release tag, neither side bounds the
        # finish: a flush longer than the stall timeout and --flush-timeout
        # still completes, as before.
        old = work / "v1.1.0"
        tagged = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "-q", "--verify",
                                 "v1.1.0^{commit}"], capture_output=True).returncode == 0
        if tagged:
            old.mkdir()
            archive = subprocess.run(["git", "-C", str(ROOT), "archive", "v1.1.0"],
                                     capture_output=True, check=True).stdout
            subprocess.run(["tar", "-x", "-C", str(old)], input=archive, check=True)
            built = subprocess.run(["make", "-C", str(old), "msc"],
                                   capture_output=True).returncode == 0
        if tagged and built:
            finish("old-receiver", ROOT / "msc", old / "msc", ["--flush-timeout", "1s"],
                   {"MSC_TEST_FSYNC_DELAY_MS": "3000"}, 0)
            finish("old-sender", old / "msc", ROOT / "msc", [],
                   {"MSC_TEST_FSYNC_DELAY_MS": "3000"}, 0)
            print("ok: with a 1.1.0 peer the UDP finish waits without a bound, as before",
                  flush=True)
        else:
            print("skip: cannot build v1.1.0 from a release tag here; "
                  "mixed-version UDP finish cases not run", flush=True)

        for failure in ("nospc", "unsupported"):
            dest = work / f"mapped-{failure}"
            out = run([ROOT / "msc", "-n1", "--stats", "--stall-timeout", "2s",
                       "-l", "localhost", "-r", "127.0.0.1",
                       "-B", ROOT / "msc", "-i", source, "-o", dest],
                      {"MSC_SSH": str(fake), "LD_PRELOAD": fault,
                       "MSC_TEST_FALLOCATE_ERROR": failure,
                       "MSC_UDP_RESERVE_UPFRONT": "1"},
                      expected=4 if failure == "nospc" else 0)
            assert f"injected allocation {failure}" in out
            if failure == "nospc":
                assert not dest.exists()
            else:
                assert "write-path: mmap 0 pwritev 1" in out
                assert dest.read_bytes() == source.read_bytes()
        print("ok: mmap reserves space; ENOSPC fails cleanly; unsupported allocation uses pwritev", flush=True)

        # On tmpfs a mapped single file is reserved alongside the data: a helper
        # per flow prefaults that flow's part of the mapping, and flush_run
        # prefaults any run a helper has not reached, instead of one serial
        # fallocate before any data moves.
        shm = Path("/dev/shm")
        shm_type = subprocess.run(["stat", "-f", "-c", "%t", str(shm)],
                                  capture_output=True, text=True).stdout.strip()
        if shm_type == "1021994" and os.access(shm, os.W_OK):
            source8 = work / "source8"
            source8.write_bytes(os.urandom(8 * 1024 * 1024 + 123))
            with tempfile.TemporaryDirectory(prefix="msc-regression-", dir=shm) as d:
                for label, extra, want in (
                        ("helpers", {}, r"reserve: 8 flows prefaulting, \d+ catch-up"),
                        ("catch-up", {"MSC_TEST_NO_PREFAULT_HELPERS": "1"},
                         r"reserve: 8 flows prefaulting, [1-9]\d* catch-up"),
                        ("upfront", {"MSC_UDP_RESERVE_UPFRONT": "1"}, None)):
                    dest = Path(d) / label
                    out = run([ROOT / "build/udp_test", source8, dest, "8"],
                              dict(extra, MSC_UDP_STATS="1"))
                    assert dest.read_bytes() == source8.read_bytes(), (label, out)
                    assert ("reserved alongside data" in out) == (want is not None), (label, out)
                    assert want is None or re.search(want, out), (label, out)
            print("ok: a tmpfs destination is reserved alongside the data, by helpers "
                  "or by catch-up prefaults; MSC_UDP_RESERVE_UPFRONT reserves first", flush=True)
        else:
            print("skip: /dev/shm is not a writable tmpfs; reservation alongside data not run",
                  flush=True)

        # A full tmpfs: the up-front room check fails before any data moves, and
        # with that check skipped, prefaulting fails partway. Either way the
        # transfer exits 4 naming ENOSPC, nothing is published, and the receiver
        # does not die of SIGBUS.
        if subprocess.run(["unshare", "-Urm", "true"], capture_output=True).returncode == 0:
            full = work / "full"
            full.mkdir()
            source2 = work / "source2"
            source2.write_bytes(os.urandom(2 * 1024 * 1024))
            script = ('mount -t tmpfs -o size=1m msc-full "$1" || exit 99; dir=$1; shift; '
                      '"$@"; rc=$?; [ -e "$dir/out" ] && echo PUBLISHED; exit $rc')
            for label, extra, who in (("room check", {}, "receiver"),
                                      ("prefault", {"MSC_TEST_IGNORE_ROOM": "1"},
                                       r"receiver flow \d+")):
                result = subprocess.run(
                    ["unshare", "-Urm", "sh", "-c", script, "sh", str(full),
                     str(ROOT / "msc"), "-n2", "--stats", "--stall-timeout", "2s",
                     "-l", "localhost", "-r", "127.0.0.1", "-B", str(ROOT / "msc"),
                     "-i", str(source2), "-o", str(full / "out")],
                    cwd=ROOT, env=dict(BASE, MSC_SSH=str(fake), **extra),
                    capture_output=True, text=True, timeout=60)
                out = result.stdout + result.stderr
                if result.returncode == 99:
                    print("skip: tmpfs cannot be mounted in a user namespace here", flush=True)
                    break
                assert result.returncode == 4, (label, result.returncode, out)
                assert "PUBLISHED" not in out, (label, out)
                assert re.search("MSC UDP " + who + ": cannot reserve destination storage: "
                                 "No space left on device", out), (label, out)
            else:
                print("ok: a full tmpfs fails with ENOSPC before data (room check) "
                      "and partway (prefault), publishing nothing", flush=True)
        else:
            print("skip: unprivileged user namespaces are unavailable; full tmpfs not run",
                  flush=True)

        # A receiver stuck preparing the destination must not hang msc: it
        # gives up after the stall timeout instead of waiting out the 10 s hang.
        # Time msc itself; the hung receiver keeps the output pipe open, so
        # log to a file and kill what is left of the process group afterwards.
        storage = str(ROOT / "build/storage_fault_shim.so")
        log = work / "stuck-cli.log"
        with open(log, "w") as sink:
            started = time.monotonic()
            process = subprocess.Popen(
                [str(ROOT / "msc"), "-n1", "--stall-timeout", "1s",
                 "-l", "localhost", "-r", "127.0.0.1", "-B", str(ROOT / "msc"),
                 "-i", str(source), "-o", str(work / "stuck-cli")],
                cwd=ROOT, stdout=sink, stderr=subprocess.STDOUT, start_new_session=True,
                env=dict(BASE, MSC_SSH=str(fake), LD_PRELOAD=storage,
                         MSC_UDP_RESERVE_UPFRONT="1",
                         MSC_TEST_FALLOCATE_DELAY_MS="10000"))
            try:
                code = process.wait(timeout=30)
                elapsed = time.monotonic() - started
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        out = log.read_text()
        assert code == 6 and elapsed < 6, (code, elapsed, out)
        assert "no progress preparing the destination" in out, out
        print("ok: a receiver stuck preparing the destination fails the CLI promptly",
              flush=True)

        # Grow and rehash the inode index; links span subdirectories and are
        # interleaved with ordinary files. Receiver inode identity must match.
        links = work / "links"
        (links / "a").mkdir(parents=True)
        (links / "b").mkdir()
        for i in range(512):
            origin = links / "a" / str(i)
            origin.write_bytes(f"inode {i}\n".encode())
            os.link(origin, links / "b" / str(i))
            (links / f"ordinary-{i}").write_bytes(b"ordinary")
        dest = work / "links-copy"
        run([ROOT / "build/udp_test", links, dest, "4"])
        for i in range(512):
            a, b = dest / "a" / str(i), dest / "b" / str(i)
            assert a.read_bytes() == (links / "a" / str(i)).read_bytes()
            assert a.stat().st_ino == b.stat().st_ino
            assert (dest / f"ordinary-{i}").read_bytes() == b"ordinary"
        print("ok: growing hardlink index preserves content and inode relationships", flush=True)


if __name__ == "__main__":
    main()
