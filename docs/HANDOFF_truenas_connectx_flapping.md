# Handoff — TrueNAS ConnectX-3 Pro link flapping (network hardware audit & fix)

**Status:** OPEN. Root cause identified from the client side; fix not yet applied.
**Prepared:** 2026-06-01 (from a JuiceMount debugging session).
**Scope of this handoff:** Audit and permanently fix the intermittent 10GbE link
flapping on the TrueNAS server `192.168.0.197`. This is a **network-hardware /
server-side** task. The JuiceMount macOS app + its code are being handled
separately — **do not** change app code; treat the app only as a load generator
and a symptom reporter.

---

## TL;DR

The TrueNAS box's **Mellanox ConnectX-3 Pro** 10GbE NIC chronically **link-flaps**:
both bonded ports drop `Link Down → Link Up` together for 1–3 seconds, **289
carrier changes** since boot, ~130 flap events May 26–29. Each flap = total loss
of connectivity to `192.168.0.197` for a few seconds, which clients see as
"no route to host" / connection timeouts.

The card is **almost certainly not dying** — there are **zero CRC/frame errors,
zero PCIe AER, no thermal/firmware-crash logs**. The signature points to a
**cable/firmware compatibility problem**:
- Both ports use **generic "OEM" DACs** (PN `SFP-H10GB-CU3M`, a *Cisco*-coded
  third-party copper cable). ConnectX NICs are notorious for flapping on
  non-Mellanox-coded DACs.
- **Old firmware `2.36.5150`** (latest CX-3 Pro is `2.42.5000`).

**Important timing nuance:** the flapping **stopped on 2026-05-29 ~22:00 and the
link has been stable for ~2.5 days** (no `Link Down` since). So your first job is
to confirm whether it has recurred, then make a *durable* fix so it can't.

---

## Mission

1. **Re-verify** the current state of the link (is it flapping now? has
   `carrier_changes` climbed since this doc was written?).
2. **Confirm the root cause** with additional evidence (switch side, thermal,
   what changed on 05-29 that stopped it).
3. **Fix it permanently:** genuine DAC cables and/or firmware update, validated
   under sustained load.
4. **Prove it's fixed:** sustained-load test with link-flap monitoring showing
   zero drops over a meaningful window.

Out of scope: the JuiceMount app, the macOS client's networking (already audited
clean — see "Already ruled out"), any data-plane / pool changes.

---

## Environment & access

| Thing | Value |
|---|---|
| TrueNAS host | `truenas`, **192.168.0.197**, TrueNAS SCALE, kernel `6.12.33-production+truenas` |
| Boot time | ~2026-05-26 15:13 (uptime ~5d23h as of 2026-06-01 14:00) |
| SSH from the Mac | `ssh -i ~/.ssh/codex_truenas_tmp root@192.168.0.197` (key `codex-truenas-temp`, user **root**, key-only, `BatchMode=yes` works) |
| NIC under audit | **Mellanox ConnectX-3 Pro** (MT27520), PCI `09:00.0`, driver `mlx4_en` v4.0-0, **firmware `2.36.5150`**, PCIe x8 8.0GT/s (63 Gb/s, healthy) |
| NIC ports | dual-port, both in **`bond1`** (802.3ad / LACP): **`enp9s0`** (port 1) + **`enp9s0d1`** (port 2) |
| Backend IP | `192.168.0.197` lives on `bond1`, MAC `24:8a:07:f7:5c:3e` |
| DAC cables | both ports: Vendor **OEM**, PN **`SFP-H10GB-CU3M`** (Cisco-coded), Connector "Copper pigtail", 3m, SN prefix `CSC…` |
| Services on .197 (clients) | JuiceFS metadata = **Redis** k8s NodePort `:30179`; object store = **MinIO** k8s NodePort `:30151`; bucket `zpool` |
| Client (Mac) | `en21` Thunderbolt 10GbE adapter, 192.168.0.60, MTU 9000, MAC `00:23:a4:07:2f:58` |

> This box is a **production storage server**. You are SSH'ing in **over the very
> link you're auditing** — a mistake in network config can lock you out. Have a
> console/IPMI fallback before touching anything live. Do read-only first.

---

## Evidence collected so far (client-side SSH, read-only)

All gathered via `ssh … root@192.168.0.197`:

- **`carrier_changes`** (link state transitions since boot):
  `cat /sys/class/net/enp9s0/carrier_changes` → **289**;
  `enp9s0d1` → **281**. (Healthy = 1–2.)
- **`bond1` Link Failure Count** (`/proc/net/bonding/bond1`):
  `enp9s0` = **131**, `enp9s0d1` = **134**. Mode = **802.3ad** (LACP).
- **dmesg flap pattern** — ~130 cycles of:
  ```
  mlx4_en: enp9s0:   Link Down
  mlx4_en: enp9s0d1: Link Down       ← BOTH ports drop simultaneously
  (1–3 seconds later)
  mlx4_en: enp9s0:   Link Up
  mlx4_en: enp9s0d1: Link Up
  ```
  Spread across **May 26–29** (dozens/day at peak), **last event 2026-05-29
  21:56:24**, none since.
- **`ethtool -S enp9s0`** — **zero** non-zero error/CRC/drop/pause/fifo counters.
- **`ip -s link show enp9s0`** — RX errors 0, dropped 0; TX errors 0, carrier 0.
- **dmesg** — **no PCIe AER, no thermal, no `mlx4` health/catastrophic/firmware
  reset** events. PCIe trained fine (x8 8GT/s).
- **Transceivers** (`ethtool -m`) — both are **passive copper DAC** ("Copper
  pigtail"), so no optical DDM power/temp to read; vendor "OEM", PN
  `SFP-H10GB-CU3M`.
- **LLDP** — `lldpctl` returned **no neighbor** (lldpd not running or switch not
  advertising). **The switch is currently unidentified** — see open questions.

### Interpretation
Both ports flapping *simultaneously* with *zero* frame/PCIe/thermal errors is a
clean **PHY/link-negotiation** failure, not data corruption or silicon fault.
Simultaneity points to a **common cause**: the card+firmware's handling of the
(generic) DACs, or a switch-side event affecting both ports at once.

---

## Already ruled out (client / Mac side — do not re-audit)

A full client-side audit was done; the Mac is clean:
- `en21` (Thunderbolt 10GbE): **0 interface errors across ~114M packets** under
  heavy load; ICMP to `.197` **0% loss** under load; jumbo (MTU 9000) works
  end-to-end (don't-fragment pings succeed at 9000B).
- Clean topology: single active interface, single route to `.197`, single stable
  ARP entry, **Tailscale stopped**, no duplicate IP, no route flapping.

There is **also a separate, milder software issue** in the JuiceMount app (its
1-second connection probe times out under heavy app CPU load and shows a false
"offline" even when the network is fine). **That is being fixed on the app side
and is NOT your concern.** Don't conflate the two: the NIC flap = seconds-long
*total* outages (this doc); the app probe = sub-second false positives (app team).

---

## Hypotheses (ranked)

1. **Generic/Cisco-coded DAC cables incompatible with ConnectX-3 Pro** *(top
   suspect)*. Classic, heavily documented. Fix: Mellanox-genuine DACs or
   genuine optics+fiber.
2. **Stale firmware `2.36.5150`** (latest `2.42.5000`) with link-stability bugs.
   Fix: update firmware.
3. **Switch-side issue** — both ports flapping together could be a switch event
   (STP recompute, port-flap, the switch also rejecting the generic DACs, a
   failing switch port/SFP cage, or switch-side power/thermal). Unknown until the
   switch is identified. Both ends of a DAC must like the cable.
4. **Marginal/failing card silicon** (the original "dying card" guess) — *least
   supported* (no PCIe/thermal/firmware errors), but not fully excluded until
   cables+firmware are eliminated.
5. **Thermal / airflow** — no thermal logs, but the **NIC temperature was not
   directly readable** (`sensors` only exposed drive temps). Verify card airflow
   and, if possible, temperature under sustained load.

> Note the **"what stopped it on 05-29 ~22:00?"** mystery. Something changed
> (cable reseated? switch port moved? load/thermal change? a reboot of the
> switch?). Identifying that is a strong clue to the true cause. It may simply be
> dormant, not fixed.

---

## Audit plan (do this first — read-only)

Run from the Mac via the SSH key above (or directly on the box). Re-confirm the
state and gather the missing pieces:

```bash
SSH="ssh -i $HOME/.ssh/codex_truenas_tmp -o BatchMode=yes root@192.168.0.197"

# 1) Is it flapping NOW? (compare to baseline 289/281)
$SSH 'for p in enp9s0 enp9s0d1; do echo "$p carrier_changes=$(cat /sys/class/net/$p/carrier_changes)"; done'
$SSH 'dmesg -T | grep -iE "mlx4_en.*Link (Up|Down)" | tail -8'   # any new flaps since 05-29 21:56?

# 2) Link / firmware / errors
$SSH 'ethtool enp9s0 | grep -iE "speed|duplex|link detected"'
$SSH 'ethtool -i enp9s0 | grep -iE "firmware|driver"'
$SSH 'ethtool -S enp9s0 | grep -iE "err|drop|crc|pause|link_down|fifo|miss|health" | grep -v ": 0$"'
$SSH 'ip -s link show enp9s0; ip -s link show enp9s0d1'
$SSH 'grep -iE "Mode|Slave|MII Status|Link Failure|Speed" /proc/net/bonding/bond1'

# 3) Identify the SWITCH (critical missing piece)
$SSH 'apt-get install -y lldpd 2>/dev/null; systemctl start lldpd 2>/dev/null; sleep 35; lldpcli show neighbors'  # if allowed
#   …or read it physically / from the switch's own admin UI.

# 4) Cable + (lack of) thermal data
$SSH 'for p in enp9s0 enp9s0d1; do echo "== $p =="; ethtool -m $p 2>/dev/null | grep -iE "vendor|pn|sn|connector|length.*Copper"; done'
$SSH 'sensors 2>/dev/null | grep -iE "mlx|nic|adapter|temp"; cat /sys/class/net/enp9s0/device/hwmon/hwmon*/temp1_input 2>/dev/null'

# 5) What changed ~05-29 22:00? Correlate logs.
$SSH 'journalctl --since "2026-05-29 21:00" --until "2026-05-29 23:00" | grep -iE "link|bond|mlx|network|switch" | tail -40'
$SSH 'dmesg -T | grep -iE "0000:09:00|aer|pcie.*error|mlx4.*(err|fail|reset|fatal|health)|thermal" | tail'
```

Also identify the switch model/port and pull **switch-side** logs + port error
counters (CRC/flaps on the switch port) — the flap could be logged there too.

---

## Fix plan (apply after the audit confirms cause)

Do these in order of cost/risk. **Maintenance window + console/IPMI fallback
required** for anything that touches the link or firmware.

1. **Replace the generic DACs with Mellanox/NVIDIA-genuine DACs**
   (e.g. `MCP2100-X003A`, 3m passive) **or** genuine optics + fiber. ~$30–50/cable.
   This is the highest-probability, lowest-risk fix. Re-seat/verify both ends.
   - If the switch is also picky, match cables both ends; some switches need
     vendor-coded SFPs too.
2. **Update ConnectX-3 Pro firmware** `2.36.5150` → `2.42.5000`
   (via `mstflint`/`mlxup` from the NVIDIA/Mellanox firmware tools).
   - **HIGH RISK on a production box** — a failed flash can brick the NIC. Back
     up the current firmware (`mstflint … ri backup.bin`), confirm console/IPMI
     access, do it in a window, verify with `ethtool -i` after.
3. **Re-evaluate the bond.** It's `802.3ad` (LACP) with both members flapping
   together, so the bond provides no resilience to this failure (both drop at
   once). Fixing the cables/firmware is the real fix; don't rely on bond mode.
   If they remain on LACP, confirm the switch's LACP/port config and jumbo (MTU
   9000) match.
4. If 1–3 don't stop it after validation → escalate to **card replacement**
   (then the original "dying card" hypothesis is confirmed by elimination).

**Cautions:**
- Production storage server + you're connected over the audited link. Console/IPMI
  fallback before any link/firmware change.
- Don't disrupt the `zpool` / data plane or the k8s services (Redis :30179,
  MinIO :30151) that JuiceMount depends on.
- Read-only first; one change at a time; re-measure between changes.

---

## Verification / success criteria

After each change, prove stability:

1. **Baseline + watch `carrier_changes`** over time (it must stay flat):
   ```bash
   $SSH 'while true; do echo "$(date +%T) enp9s0=$(cat /sys/class/net/enp9s0/carrier_changes) enp9s0d1=$(cat /sys/class/net/enp9s0d1/carrier_changes)"; sleep 30; done'
   ```
2. **No new `Link Down`** in `dmesg -T | grep -i "Link Down"`.
3. **Sustained-load test** Mac↔TrueNAS while monitoring: drive high-throughput
   traffic (e.g. from the Mac: large reads/writes to the JuiceMount mount, or
   `iperf3` to the box) for ≥30 min and confirm **zero** link drops + flat
   `carrier_changes`. (The flapping was load/time-correlated, so test under load.)
4. Definition of done: **≥72 hours of normal + loaded operation with
   `carrier_changes` unchanged and no `Link Down` events.**

---

## Open questions for the next agent

- **What switch is `bond1` plugged into?** (LLDP gave nothing — identify it,
  check its port logs / error counters / DAC compatibility.)
- **What changed at 2026-05-29 ~22:00** that stopped the flapping? (Reboot of the
  switch? cable reseat? load drop? Correlate with any change you can find.)
- Is the NIC running **hot**? (Couldn't read its temp; check airflow/sensors.)
- Are jumbo frames (MTU 9000) configured consistently on the **switch** ports and
  the bond? (Mac side is 9000 and works; verify the path end-to-end.)

---

## Appendix — quick reference

- **Get in:** `ssh -i ~/.ssh/codex_truenas_tmp root@192.168.0.197`
- **The card:** ConnectX-3 Pro, `09:00.0`, `mlx4_en`, fw `2.36.5150`,
  ports `enp9s0` + `enp9s0d1` → `bond1` (LACP).
- **The smoking gun:** `cat /sys/class/net/enp9s0/carrier_changes` (was 289) +
  `dmesg -T | grep "Link Down"`.
- **The likely fix:** genuine Mellanox DACs + firmware → `2.42.5000`.
- **Last known flap:** 2026-05-29 21:56:24. **Stable since** (confirm it's still
  true before assuming "fixed").
