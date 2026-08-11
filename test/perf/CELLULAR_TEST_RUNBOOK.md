# Cellular test runbook

How to run a JuiceMount session over a cellular link and come away with numbers
you can trust.

Written for someone who has never run one. Every term is defined before it is
used. Every step says what should happen and how you know it did.

---

## 1. What the system is

**JuiceMount** puts a network drive on a Mac. The drive appears in the Finder at
`/Volumes/zpool` and behaves like a local disk. The files really live on a
server elsewhere.

Four parts do the work:

1. **The NFS server.** NFS is the protocol the Mac uses to talk to a network
   drive. JuiceMount runs its own NFS server inside the app, on the same Mac.
   The Finder talks to it over the loopback network — traffic that never leaves
   the machine.
2. **The mirror.** A local database holding the name, size and date of every
   file. It answers "what is in this folder?" without asking the server. This is
   why a folder can open instantly on a bad link.
3. **JuiceFS.** The component that fetches actual file *contents*. It keeps a
   **block cache** on the Mac's SSD — copies of file chunks already fetched. A
   read served from the block cache never touches the network.
4. **The object store and the metadata store.** The object store (MinIO) holds
   the file bytes. The metadata store (Redis) holds the directory structure.
   Both are on the remote server.

**Why cellular is hard.** A cellular link has high **RTT** — round-trip time,
the delay for one message to reach the server and come back. On a good office
network the RTT is under 1 millisecond. On cellular it is commonly 100–500
milliseconds, sometimes worse. Bandwidth matters less than you expect. What
hurts is the *number of round trips*, because each one costs a full RTT no
matter how few bytes it carries.

The measured example: on 2026-07-29, opening a file for the first time took
1,226–5,660 ms, and opening the same file again took 24–36 ms. The difference is
not the file size — a 5.7 KB file cost the same as a 4.4 MB file. It is the
round trips.

---

## 2. What the test is trying to prove

Four claims, in the order they matter.

| # | Claim | The number that settles it |
|---|---|---|
| 1 | Browsing folders is as fast as a local disk, because the mirror answers without the network | `rpcs.READDIRPLUS.p50_us` and `rpcs.LOOKUP.p50_us` should stay in the microseconds, unchanged from LAN |
| 2 | Indexing does not saturate the link — you can still load a web page while it runs | `keyspace.scan_promotions` and `keyspace.verdict` |
| 3 | Offline, writes and cached reads run at disk speed | Copy timings with the link off, plus `backend.cache_hits` |
| 4 | A cached file is served from the cache, not re-fetched to check it is current | `backend.cache_hits` vs `backend.cache_miss`, and `backend.meta_ops` |

---

## 3. Before you start

**Everything below reads `http://127.0.0.1:11050`.** That is the app's own
status endpoint on the Mac. It is not on the network and needs no password.
Its replies are JSON.

Run these three and read the answers before doing anything else.

```bash
curl -s http://127.0.0.1:11050/whoami
```

Confirms the app is running and lists what it can do.

```bash
curl -s http://127.0.0.1:11050/health
```

`components` must **not** be `{}`. An empty `components` with `healthy: true`
means "nothing has reported yet", not "everything is fine".

```bash
curl -s http://127.0.0.1:11050/metrics | python3 -m json.tool | head -40
```

Check the `network` block:

- `class` — the app's own judgement: `metered`, `slow`, `medium` or `fast`.
- `have_rtt` and `have_bandwidth` — **must both be `true`.** If either is
  `false`, the class is a guess from no data and every conclusion drawn from it
  is worthless.
- `high_latency` — the sticky far-link flag. It turns on at a higher RTT than it
  turns off at, so a link hovering near the boundary gives one stable answer
  instead of flapping. This flag, not `class`, is what selects the longer
  timeouts. If the app behaves like a far link while `rtt_ms` looks fine, this
  field is the explanation.

---

## 4. Arming the open-cache lever

**Read this section before deciding to use it.**

Every time a file is opened, JuiceFS asks the metadata store whether its
contents have changed. On a cellular link that question alone costs one full
round trip, and it is the single largest measured cost — the 66× first-open
versus second-open gap in §1.

`--open-cache` tells JuiceFS to skip that question for a set number of seconds
after the first open. It is **off by default**, deliberately.

**The risk, stated plainly.** JuiceFS offers no way to tell it "that file just
changed, forget what you cached". So inside the window you set, a file rewritten
by someone else — or by another app on your own machine — can be served with
**stale content**: real bytes from the file's previous state, with no error and
nothing to indicate it. This is the same family of bug as the torn reads and the
black frames in Premiere. The window you choose is a hard limit on how wrong the
data can be, and there is no way to shorten it after the fact.

Use it only on a volume nobody else is writing to during the test.

```bash
defaults write com.juicemount.app openCacheTTL -string 5s
```

Then quit and reopen JuiceMount. The setting is read when the drive is mounted,
not while it runs.

To turn it off again:

```bash
defaults delete com.juicemount.app openCacheTTL
```

There is no switch for this in Settings, on purpose.

---

## 5. Running the session

### Step 1 — start the watcher

```bash
juicemount-watch --interval 10s --out cell-run.jsonl
```

Leave this running for the whole session. It records the status endpoint every
ten seconds and flags problems as they happen: the app going offline by itself,
a burst of errors, a latency spike.

### Step 2 — record the starting point

```bash
python3 test/perf/metrics_delta.py snap > before.json
```

### Step 3 — do real work in the Finder

Use the Finder, not shell commands. Copying with `cp` in a terminal exercises a
different and much simpler path than the Finder does, and it has repeatedly
reported success while the Finder was broken. The Finder creates hidden
companion files, reads extended attributes, and asks for information `cp` never
asks for — and those are the operations that cost round trips.

Do all of these:

- Open several folders you have not opened before, including a large one.
- Open a folder you have already opened, to see the warm case.
- Open a file — a video in VLC, an image in Preview.
- Copy a file in, and copy a file out.
- Turn the link off mid-copy, then back on.

### Step 4 — record the end point and compare

```bash
python3 test/perf/metrics_delta.py snap > after.json
python3 test/perf/metrics_delta.py diff before.json after.json
```

**If it prints `INVALID: rpc_total did not move`, there is no result.** It means
the server did no work in that window — the Mac answered everything from its own
short-term memory of the drive. This is not a fast result; it is no result. Use
`test/perf/server_lookup_latency.sh`, which uses filenames that have never been
seen before and so cannot be answered from memory.

---

## 6. Reading the numbers

### Is browsing fast?

```
rpcs.READDIRPLUS.p50_us     opening a folder
rpcs.LOOKUP.p50_us          finding one named item
```

`p50_us` is the median in microseconds — half of all calls were faster. These
should be roughly the same on cellular as on a fast network, because the mirror
answers them locally. If they are not, the mirror is being bypassed, and that is
the finding.

Ignore `rpcs.FSSTAT` latency entirely. That call returns a fixed answer without
doing any work, so its timing measures nothing. Its *count* is still useful: a
large count means the Mac is asking about free space very often, which was worth
1,727 round trips in one past session.

### Did the network actually get used?

```
backend.cache_hits / backend.cache_miss     block reads served locally vs fetched
backend.object_get_bytes                    bytes really pulled from the object store
backend.cache_miss_bytes                    bytes that were actually missing
backend.meta_ops                            round trips to the metadata store
```

`cache_hits / (cache_hits + cache_miss)` is the hit rate. High is good — it means
cached copies were served rather than re-fetched.

Compare `object_get_bytes` against `cache_miss_bytes`. If the first is much
larger, the system pulled far more than it needed — reading ahead past what was
asked for. On a metered link that excess is the number that matters. A measured
LAN session showed 880.1 GB pulled against 726.1 GB actually missing, a factor
of 1.21. On cellular this has been much worse: a 4 KB read once pulled 53 MB.

`meta_ops` is the per-file cost. This is what `--open-cache` reduces.

**If the whole `backend` block is missing, you have no answer to this question.**
It is left out rather than reported as zeros, because zeros would look like a
perfect result.

### Did indexing take over the link?

**Indexing** is how the mirror stays current. There are two ways:

- **Push** — the server tells us the moment a folder changes. Cheap.
- **SCAN** — we re-read the entire directory structure. Roughly 297,000 entries,
  about 20 MB, and about 163 seconds over a tunnel. Expensive.

```
keyspace.verdict            working | broken | unknown | unreachable
keyspace.events_applied     notifications from the real push feed
keyspace.self_write_events   replays of your own writes (a different channel)
keyspace.scan_promotions    times push gave up and did a full SCAN instead
```

`working` means push is delivering and the expensive path is mostly idle.

`unknown` is a refusal, not a pass: nothing happened, so nothing was observed.
Save a file and check again.

`events_applied: 0` with `self_write_events` above zero is the fingerprint of
**push being dead** while your own writes still echo back. Before this was fixed,
that state reported as `working`.

`scan_promotions` counts times more than 200 folders changed at once and the
system abandoned push for one full SCAN. On a cellular link that is the worst
possible moment for it. If this is above zero during your session, note the
value — it is the number that decides whether the 200 threshold is right.

Also watch `/activity`, which shows indexing in progress, and `stall.inflight` /
`stall.oldest_age_ms`, which show requests currently waiting. An
`oldest_age_ms` climbing steadily past ten seconds means something is stuck, and
you can watch it happen rather than finding it in a log afterwards.

### Is offline at disk speed?

Turn the link off and copy files in. Writes go to a local staging area called the
**spool** and should run at SSD speed. Reads of files already in the block cache
should also be fast.

An offline read that has to wait is bounded — it gives up after 1.5 seconds (4
seconds for a pinned file) and reports the file as unavailable rather than
freezing the app. Measured overhead on this path is about twice a normal read,
which is the cost of that safety bound.

---

## 7. Things that will mislead you

- **`cp` and `md5` in a terminal are not a test.** They have passed while the
  Finder was broken, repeatedly. Use real applications.
- **A green `/health` with `components: {}`** means nothing has reported yet.
- **`/warmup`'s `index_pct` reads 100 whenever a scan is not actively running,**
  including when no index exists at all. The tell is `index_scanned: 0`.
- **A second run of anything is faster than the first,** because the Mac
  remembers. Comparing a first run against a second measures memory, not the
  link.
- **One sample is not a result.** The regression benchmark can vary two- to
  four-fold between runs at the same code. Six runs of each before believing a
  difference.
- **`rpc_total` must move.** If it did not, the measurement did not happen.

---

## 8. The harnesses

All under `test/perf/`.

| Script | What it answers |
|---|---|
| `cellular_nav_bench.sh` | Is folder browsing served locally, or leaking to the server? |
| `l2_roundtrips.sh` | How many server round trips does opening one cold folder cost? |
| `server_lookup_latency.sh` | Genuine server timings, using names the Mac has never seen |
| `nav_latency.py` | Per-operation timing of a Finder-shaped folder open, cold and warm |
| `metrics_delta.py` | Before/after comparison of everything above |
| `cell_scoreboard.py` | Unattended runs across several conditions, one JSON row each |

`cellular_nav_bench.sh` and `metrics_delta.py` both refuse to print a table when
`rpc_total` did not move. That refusal is the feature: a number from no data is
worse than silence.
