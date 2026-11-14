# Remote lab tuning experiments

Scripts that sweep client and server settings on a running remote lab
(`../lab.sh up`). They change nothing outside the lab's own containers.

| Script | What it does |
|---|---|
| `sweep.py` | For each profile and config: sets TCP congestion control in the lab containers' own network namespaces (`silo`, `respproxy`, `iperf` for downloads; `client` for uploads), remounts with extra juicefs flags, and runs `xprobe.py` cold and warm. One JSON line per run. |
| `xprobe.py` | Runs inside the client. Time to first byte at offset 0 (whole-block GET) and at offset 4096 (ranged GET), sequential read for a time budget, random 1–4 MiB reads, a 32 MiB upload, 64 KiB file writes. |
| `pwrite.py` | Parallel 64 KiB writers, like the Mac drainer's small-file lane, in one folder or in separate folders. |
| `matrix.sh` | The full matrix: `base`/`bbr` on every profile, then the readahead configs. |
| `summarize.py` | Turns the JSON lines into one markdown table. |

Configs are defined in `CONFIGS` in `sweep.py`. `bbr` sets
`net.ipv4.tcp_congestion_control=bbr` and an `fq` qdisc through a
privileged helper container that joins only the target container's network
namespace. The host's own sysctls stay unchanged. The first use loads the
host kernel's `tcp_bbr` module if it is not loaded yet.

```sh
cd test/remote-lab
./lab.sh up
./lab.sh exec python3 /lab/lib/probe.py seed --size small
./lab.sh exec python3 /lab/experiments/xprobe.py seed
./experiments/sweep.py --profiles abroad --configs base,bbr,ra32-bbr --out results/x.jsonl
./experiments/summarize.py results/x.jsonl
./lab.sh down
```

Each run is one sample. On lossy profiles, a single 4 MiB block can stall
for tens of seconds, so expect wide run-to-run variance there. Random, even
loss is harsher than most real links (see `../README.md`).
