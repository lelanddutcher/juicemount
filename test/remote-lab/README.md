# Remote lab

The remote lab measures how the JuiceMount server and the JuiceFS client
behave over a slow, distant network link. It runs on one Linux machine with
Docker, or in GitHub Actions. It needs no second site and no phone, and it
changes nothing outside its own containers.

**Its limit:** it tests JuiceFS and the server. It does not test the Mac app,
its NFS server, or its local mirror (the database that lets the Finder list a
folder without asking the server). A number here is what JuiceFS alone costs
on that link. The Mac app may hide some of that cost, but it cannot make it
smaller.

## The parts

The lab is a set of containers on two private Docker networks. Nothing
publishes a port. Nothing changes a host interface or the host firewall.

| Part | Network | What it is |
|---|---|---|
| `redis` | nas | The metadata store: Redis 7.4 with the server's own `redis.conf` and ACL. It holds the folder tree. |
| `respproxy` | nas | The RESP proxy (the server's only door to Redis), built from this checkout. Its counters show how many Redis commands and round trips each operation costs. |
| `silo` | nas | The object store (SILO, a maintained MinIO fork; the same image and digest as `server/docker-compose.yml`). It holds the file bytes. |
| `iperf` | nas | An iperf3 server, used to check the link before measuring. |
| `wan` | nas and remote | The emulated internet. It is the only route between the two networks. It adds delay, jitter and loss with `tc netem` and caps the rate with `tc htb`, separately in each direction. |
| `client` | remote | The "remote Mac": the vendored JuiceFS (`third_party/juicefs`, built as CI builds it) mounted at `/jfs`, with a block cache (local copies of file chunks) in `/var/jfscache`. It mounts with the flags the Mac app uses on a slow link. |

The `wan` and `client` containers get `NET_ADMIN` for their own network
namespace only. The client also gets `/dev/fuse` and `SYS_ADMIN` so it can
mount. The lab generates throwaway passwords into `.state/lab.env`
(git-ignored, mode 600) and deletes them at teardown.

The first use of `tc` loads the kernel's `sch_netem` and `sch_htb` modules,
if they are not loaded already. That is the only effect on the host kernel.

## Profiles

`profiles.tsv` defines the links. RTT (round-trip time) is the time for a
packet to reach the server and come back. The wan adds half of it in each
direction. Loss applies in each direction, so a 1% profile loses about 2% of
pings (a ping crosses the link twice).

| Profile | RTT | Loss | Rate toward client / toward NAS |
|---|---|---|---|
| `lan` | none | none | no cap |
| `same-city` | 20 ms | none | 200 / 200 Mbit/s |
| `abroad` | 200 ms | 1% | 30 / 30 Mbit/s |
| `abroad-noloss` | 200 ms | none | 30 / 30 Mbit/s (diagnostic: separates the cost of distance from the cost of loss) |
| `hotel` | 300 ms | 2% | 10 / 10 Mbit/s |
| `cellular` | 120 ms ± 60 ms jitter | none | 20 / 20 Mbit/s |

Before it measures a profile, `measure.py` checks it. 60 pings must average
within about 10% of the RTT. A UDP iperf3 probe in each direction must arrive
at 80–105% of the rate cap. If either fails, the run stops and says why.

## Run it

You need Docker with Compose v2, Python 3, and a Linux kernel with FUSE.

```sh
cd test/remote-lab
./measure.py --profiles lan,abroad --fixture small   # about 10 minutes
./measure.py                                         # every profile, full fixture (about an hour)
```

`measure.py` starts the lab, writes the fixture on the `lan` profile, runs
each profile, prints the scoreboard and tears the lab down. Add `--keep` to
leave the lab running and `--no-up` to reuse a running lab.

To explore by hand:

```sh
./lab.sh up                 # build, start, format, mount (lan)
./lab.sh shape hotel        # switch the link
./lab.sh mount --cold       # remount with an empty block cache
./lab.sh exec ls -l /jfs/fixture/bigdir | head
./lab.sh proxy-counter      # Redis commands and round trips so far
./lab.sh down               # remove every container, network and volume
```

The fixture is deterministic (seeded). `small`: 300 small files in 6
folders, one folder of 400 entries, and two 64 and 96 MiB "videos" of random
bytes. `full`: 3000 small files in 20 folders, one folder of 1000 entries,
and 1 GiB, 512 MiB and 256 MiB videos.

## What it measures

Each profile runs twice. **Cold:** the client remounts with an empty block
cache. **Warm:** the client remounts and keeps the block cache from the cold
pass. A remount empties the metadata caches both times, as after an app
restart.

| Column | Operation |
|---|---|
| Mount | `juicefs mount` until the mount answers. |
| List 1 folder | Read the 400- or 1000-entry folder, with every file's attributes (what a Finder list view needs). |
| Walk tree | Visit every folder and stat every small file. |
| Stat, Open | First `stat()`, then first `open()`, of sample files across the tree (median). |
| TTFB | Time to first byte: open a large file and read its first 64 KiB. |
| Seq read | Read that file from the start for a time budget (default 60 s) and report the rate. If the budget ends first, the cell shows how much it read. |
| Scrub read | Up to 50 reads of 1–4 MiB at random offsets in another large file, like dragging a playhead. It stops at its time budget. It shows the median and the 90th percentile per read. |
| Write files | Write and close N new 64 KiB files. Without `--writeback`, `close()` returns only after the data and its metadata reached the server. |
| Round trips | From the RESP proxy: reply batches it sent back to the client during the mount and the list probe, per file during the stat/open/read probe, and per file written. Pipelined commands share one batch, so a batch is close to one round trip. |

One time budget can be exceeded by a single read that is already in flight.
JuiceFS fetches whole 4 MiB blocks, and the run waits for that block.

Each run writes `results/<run id>/`:

- `<profile>.json`: everything, including the link check and, for every
  probe, the change in the JuiceFS client's own counters (`/jfs/.stats`:
  metadata operations, FUSE operations, block cache hits and misses).
- `run.json`: the commit, the JuiceFS version, the fixture, and an error if
  the lab broke.
- `scoreboard.md`: the tables.

## How to read the scoreboard

- Divide a time by the profile's RTT. The result is the number of round
  trips the operation waits for one after the other. A number that grows
  with the folder size or the file count means an operation pays one round
  trip per item. That is the cost a remote user feels.
- Compare `abroad` with `abroad-noloss`. Random loss slows one TCP stream far
  more than distance does. A loss-based TCP stream at 200 ms and 1% loss
  carries about 1 Mbit/s, whatever the link rate. Real links often lose
  packets in bursts and less often, so `abroad` is a pessimistic case for
  bulk reads. It is still a fair case for round-trip counts.
- Compare cold with warm. Warm reads come from the local block cache. Warm
  listing and opening still cross the link, because the metadata caches are
  empty after a remount.
- The CI job (`.github/workflows/remote-lab.yml`) runs `lan` and `abroad` on
  the small fixture with short budgets. It fails only when the lab breaks.
  It never fails on a number. The scoreboard is in the job summary and the
  JSON is the `remote-lab-results` artifact.

## Change the client flags

`lab.sh` mounts with the flags the Mac app passes to JuiceFS on a slow link
(`health/fuse.go`, `internal/netprofile`). They are copied, not shared: if
the app's flags change, update `MOUNT_FLAGS` in `lab.sh`. To try other flags
without editing, set `JMLAB_MOUNT_OPTS`, for example
`JMLAB_MOUNT_OPTS="--prefetch 3" ./measure.py --profiles abroad`.

To measure an encrypted volume (`aes256gcm-rsa`), set `JMLAB_ENCRYPT=1`
when the lab is set up (`./lab.sh up` or `./measure.py`). The lab then
formats the volume with a throwaway RSA key in `.state/encrypt.pem`, which
`./lab.sh down` deletes with the rest of `.state`.

## Server BBR

`./measure.py --bbr` (or `JMLAB_BBR=1 ./lab.sh up`) sets the lab up with
`compose.bbr.yml`, the lab copy of the server's opt-in overlay
`server/docker-compose.tcp-bbr.yml`: the object store and the RESP proxy
send with BBR, set by Docker in their own network namespaces. The client
keeps the host's default. `./lab.sh cc` prints what each one uses, and
`run.json` records it. The host kernel needs BBR (`modinfo tcp_bbr`).

## macOS VM

`mac-lab.sh` puts a macOS virtual machine (VM) behind the same emulated WAN,
so the real Mac app can be measured on every profile. The Mac that hosts the
VM keeps its own network as it is.

```
macOS VM --WireGuard over UDP--> gateway --> wan (tc) --> nas network
                                 (remote network, next to the Linux client)
```

- `gateway` (`compose.mac.yml`, `lib/gateway.sh`) is a WireGuard endpoint
  on the remote network. WireGuard runs in userspace (`wireguard-go` on
  `/dev/net/tun`), so the host kernel loads no WireGuard module.
- The VM routes only the nas subnet into the tunnel. Every packet between
  the VM and the server stack crosses the wan container, in both
  directions, on every port: the RESP proxy, the object store and any
  service added to the nas network later.
- The gateway masquerades the tunnel as its own remote address, so the wan
  treats it like the Linux client.
- The one change visible on the host is a published UDP port (default
  51820). `mac-lab.sh profile NAME` switches the profile with no restart.
- It uses its own Docker project (`jmmac`), its own state directory
  (`.state-mac`) and its own subnets (`10.231.110.0/24`, `.120`, `.130`), so it
  can run next to a `measure.py` lab.

The VM host must give SSH access with one key. Its SSH command accepts
`start` (start the VM and print its IP address on the last line), `stop`
and `status`. It also allows a jump (`ssh -W`) to the VM's SSH port. The VM
user needs sudo without a password and Homebrew; `up` installs
`wireguard-tools` and `iperf3` with it. Set these variables. There are no
defaults:

```sh
export JMLAB_MAC_HOST=user@vm-host          # the VM host's SSH destination
export JMLAB_MAC_KEY=~/.ssh/lab_key         # key for the VM host and the VM
export JMLAB_MAC_USER=admin                 # user in the VM
export JMLAB_MAC_ENDPOINT=192.0.2.10        # this Docker host, as the VM reaches it

./mac-lab.sh up               # VM on, lab up (add JMLAB_BBR=1 for server BBR), tunnel in
./mac-lab.sh profile abroad   # switch the link
./mac-lab.sh smoke lan abroad # link smoke test, results in results/mac-<time>/
./mac-lab.sh guest ping -c 3 10.231.110.30
./mac-lab.sh down             # tunnel out, lab removed, VM stopped (JMLAB_MAC_KEEP_VM=1 keeps it)
```

### A Mac on the network instead of a VM

With `JMLAB_MAC_DIRECT=1`, `mac-lab.sh` drives a physical Mac that it can
reach directly over SSH. Nothing is started or stopped, and nothing is
installed: `wireguard-tools` must already be on the Mac. `JMLAB_MAC_HOST` is
then the Mac's address only, and the user is `JMLAB_MAC_USER`. The tunnel
config lives in `~/.jmlab-wireguard` of that user, not in Homebrew's `etc`,
and `down` removes it. As with the VM, only the nas subnet goes into the
tunnel; the Mac's default route and DNS stay as they are.

```sh
export JMLAB_MAC_DIRECT=1
export JMLAB_MAC_HOST=192.0.2.20             # the Mac itself
export JMLAB_MAC_KEY=~/.ssh/lab_key
export JMLAB_MAC_USER=labuser                # needs sudo without a password
export JMLAB_MAC_ENDPOINT=192.0.2.10         # this Docker host, as the Mac reaches it
./mac-lab.sh up
```

The smoke test runs in the VM. For each profile it measures:

- the RTT, with 20 pings;
- a TCP connection to the RESP proxy and one `PING`; the reply is an auth
  error because no password is sent;
- `GET /minio/health/live` on the object store;
- the link rate in each direction with iperf3: UDP at the profile's cap, or
  TCP on `lan`;
- one 100 MB download and one 20 MB upload with curl, each on one TCP
  stream (`JMLAB_MAC_SMOKE_MB`, `JMLAB_MAC_SMOKE_MAX`).

The iperf3 column checks the path. The curl columns show what one stream
gets. On `abroad`, the 1% random loss holds one stream far below the cap
(see "How to read the scoreboard").

**Its limits:**

- The tunnel adds about 0.7 ms RTT on `lan`.
- The macOS TCP stack sends the VM's uploads, as on a real Mac. macOS has no
  BBR.
- There is no Manager in the lab yet. The tunnel already routes the whole
  nas subnet, so a Manager container added there is reachable.
- Before the app can mount in the VM, someone at the VM's screen must allow
  the macFUSE system extension.

## The write path (`drain-lab.sh`)

`measure.py` measures JuiceFS alone. `drain-lab.sh` adds the JuiceMount NFS
layer: it runs `cmd/jm5` with the write spool inside the client container,
over its `/jfs` mount, and drives it over loopback NFSv3 with `drainprobe`.
It times folder creation (MKDIR), and the time until 200 small files and one
large file reach the server.

```sh
./drain-lab.sh build                 # jm5 and drainprobe into $STATE/bin
./drain-lab.sh run abroad            # one run; results/drain-<time>/
DRAIN_SMALL=0 DRAIN_BIG_MB=500 DRAIN_CAP=300 ./drain-lab.sh run abroad big
DRAIN_READ_S=300 ./drain-lab.sh run abroad-noloss after-reads
```

The script header lists every variable. Limits:

- jm5 runs with `--no-fuse --no-mount` and forces the link class (`slow`,
  or `fast` on `lan`). The lab's JuiceFS mount has no `--fast-statfs`, so
  each `statfs` costs two round trips. The app's mount answers it locally.
  The drainer's mount check uses `statfs`, so in the lab it sometimes
  parks the drainer for a few seconds.
- The Redis MONITOR trace slows Redis a little. Compare runs that both have
  it, or both do not.

## Addresses

The lab uses `10.231.10.0/24` (nas) and `10.231.20.0/24` (remote). If those
collide with a network you already have, set `JMLAB_NAS_SUBNET`,
`JMLAB_REMOTE_SUBNET` and the fixed addresses in `compose.yml` (the
`JMLAB_*_IP` variables). To run two labs at once, also give each a different
`JMLAB_PROJECT`.
