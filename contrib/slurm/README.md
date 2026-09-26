# Running msc on a Slurm cluster

msc starts its receiving side over SSH. Many clusters block SSH between
compute nodes, but msc does not need SSH itself: it needs a program that runs a
command on the other node with stdin and stdout connected back to it, and
`srun` does exactly that inside a job allocation. These scripts use it.

| File | Purpose |
| --- | --- |
| `msc-srun` | Stand-in for `ssh`: point `MSC_SSH` at it and msc starts its remote side with `srun`. |
| `msc-slurm-smoke.sh` | Two-node smoke test: a dozen verified transfers plus a throughput table. |

Both need Slurm 20.11 or later (`srun --overlap`) and a job allocation that
includes the receiving node. msc must be at the same path on both nodes, which a
shared home directory provides.

## Transferring with msc-srun

From inside an allocation (the shell runs on the first node, which sends):

```sh
salloc -N2 -t 1:00:00
MSC_SSH=$PWD/contrib/slurm/msc-srun ./msc -B "$PWD/msc" \
    -l localhost -r node02 -i /dev/shm/src.bin -o /dev/shm/dst.bin
```

The `-r` host doubles as the Slurm node name. If compute nodes cannot resolve
each other's names, or you want the data on a particular network such as
InfiniBand, give `-r` the address to use and name the node separately:

```sh
MSC_SSH=$PWD/contrib/slurm/msc-srun MSC_SRUN_NODE=node02 ./msc -B "$PWD/msc" \
    -l localhost -r 192.168.10.2 -i /dev/shm/src.bin -o /dev/shm/dst.bin
```

## Smoke test

Build msc (`make -j all`), then from inside a two-node allocation:

```sh
salloc -N2 --exclusive -t 1:00:00      # add -p, -w or --mem as your site needs
bash contrib/slurm/msc-slurm-smoke.sh
```

It covers UDP and TCP, single files and directory trees, interrupt and resume,
stdin and `-c` pipes, and exit codes. It checks every result by checksum on
both nodes, prints PASS/FAIL and memory-to-memory throughput, and removes its
test data. The report and per-test logs are kept in `~/msc-results/<time>`. It
takes a few minutes. Settings: `SIZE_MB` (large file size, default 2048),
`B_IP` (receiver's address, default its InfiniBand IPv4 if it has one), `OUT`,
and `MSC_DIR`.

Test data lives in `/dev/shm`, about six times `SIZE_MB` on the receiving node
at its peak. Without `--exclusive`, request enough memory with `--mem` (for
example `--mem=32G` for the default size), because tmpfs pages count against
the job on many sites.

## Notes

- Use node-local storage (`/dev/shm` or local disk) for throughput tests. A
  shared filesystem such as NFS home directories measures that filesystem.
- UDP throughput depends on the kernel's socket buffer ceiling
  (`net.core.rmem_max`, `net.core.wmem_max`). The smoke test reports it for
  both nodes. A ceiling far below 4 MiB can hold UDP back.
- Shared nodes make throughput numbers noisy. Use `--exclusive` for
  measurements you intend to compare.
