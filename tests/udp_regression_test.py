#!/usr/bin/env python3
"""Release regressions: actual file contents and durability, using loopback."""
import os
from pathlib import Path
import signal
import re
import subprocess
import tempfile

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
                  {"LD_PRELOAD": storage, "MSC_TEST_FALLOCATE_DELAY_MS": "1000",
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
        print("ok: phase timings charge a slow reservation to the receiver's prepare",
              flush=True)

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

        for failure in ("nospc", "unsupported"):
            dest = work / f"mapped-{failure}"
            out = run([ROOT / "msc", "-n1", "--stats", "--stall-timeout", "2s",
                       "-l", "localhost", "-r", "127.0.0.1",
                       "-B", ROOT / "msc", "-i", source, "-o", dest],
                      {"MSC_SSH": str(fake), "LD_PRELOAD": fault,
                       "MSC_TEST_FALLOCATE_ERROR": failure},
                      expected=4 if failure == "nospc" else 0)
            assert f"injected allocation {failure}" in out
            if failure == "nospc":
                assert not dest.exists()
            else:
                assert "write-path: mmap 0 pwritev 1" in out
                assert dest.read_bytes() == source.read_bytes()
        print("ok: mmap reserves space; ENOSPC fails cleanly; unsupported allocation uses pwritev", flush=True)

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
