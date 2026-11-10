# Link lab

The Link lab tests JuiceMount Link, the app's built-in remote access, on one
Linux machine with Docker. It needs no second site, no real router and no
internet. It changes nothing outside its own containers.

## Terms

- **Link**: JuiceMount's remote access. It is a private WireGuard network
  between the NAS and each Mac. Each member is a **node**.
- **Control server**: the server that tells nodes about each other. Link uses
  Headscale (an open-source Tailscale control server), which the Manager runs
  as a child process. It carries no file data.
- **NAS node**: the Link member inside the Manager. Macs reach the NAS's
  Redis proxy (port 30179), object store (30151) and Manager (30190) through
  it.
- **NAT**: what a router does so many devices share one public address. It
  rewrites each outgoing connection's address and port (a **mapping**).
  - **Easy NAT** keeps one mapping per device port, whatever the
    destination. Most home routers do this.
  - **Hard NAT** picks a new random port for each destination. Some hotel,
    office and mobile networks do this.
  - **Carrier-grade NAT (CGNAT)**: the internet provider adds a second NAT,
    so the site has no public address of its own.
- **Port forward**: a router rule that sends traffic for one public port to
  one LAN device.
- **Hole punching**: two nodes behind NATs send to each other at the same
  time, so each router lets the other's packets in. It works when at least
  one side has easy NAT and the timing fits.
- **Direct path**: WireGuard packets go straight between the two nodes.
- **Relay (DERP)**: Tailscale's relay. When no direct path forms, nodes send
  their encrypted packets through it over HTTPS.
- **Hairpin NAT**: a router feature. A LAN device dials the home's public
  address and the router loops it back inside. Many routers do not do it.
- **Local DNS entry**: the router's own resolver answers the control
  server's name with the NAS's LAN address, so a Mac at home needs no
  hairpin.
- **Port mapping (NAT-PMP)**: a device asks the router to open a port
  forward for it. UPnP and PCP do the same job.

## The parts

The lab is a set of containers on seven private Docker networks. Every
network is internal: no container reaches the real internet.

| Part | Networks | What it is |
|---|---|---|
| `redis`, `respproxy`, `silo` | nas | The NAS stack, as in `test/remote-lab`: Redis with the server's config and ACL, the RESP proxy built from this checkout, the SILO object store (also named `minio`). |
| `manager` | nas (+ home) | The Manager built from this checkout, with Link on as `server/docker-compose.link.yml` turns it on: Headscale v0.29.3 with its own TLS, the NAS node, the Manager's ACL policy, pairing through `POST /api/net/pair`. |
| `nas-host` | nas, home | The NAS's own Docker NAT: what Docker does for the Manager's published ports (30193 to 8091, 41641/udp). Its home address is "the NAS's LAN address". |
| `home-router` | home, homeup | The home router: NAT, port forwards, hairpin, a LAN resolver with an optional local DNS entry, optional NAT-PMP. |
| `home-mac` | home | A Mac at home: the stock Tailscale client (v1.102.3, the version the app uses) on Linux. |
| `wan` | homeup, awayup, core | The emulated internet. It routes between the sites and shapes both access links with the remote lab's profiles (`../remote-lab/profiles.tsv`, `../remote-lab/lib/shape.sh`). |
| `relay` | core | Tailscale's `derper` (same version): DERP over HTTPS on 443 and STUN on 3478/udp, on a "public" address. It also serves the lab DERP map (`lib/derp/derpmap.json`) on port 8088, the lab's stand-in for Tailscale's public map. |
| `dns` | core | The lab DNS. Only two names resolve: `link.lab.example` (the home's public address) and `derp.lab.example` (the relay). |
| `away-router` | away, awayup, carrier | The router where the Mac is away from home. |
| `cgn` | carrier, awayup | A carrier-grade NAT, used when a case puts the away site behind it. |
| `away-mac` | away | A Mac away from home, the same client as `home-mac`. |

The control URL is `https://link.lab.example:30193`. A lab certificate
authority (CA), made fresh at each `lab.sh up`, signs the certificates for
the control server and the relay. The Mac stand-ins and the Manager trust
it. Nothing else does.

The Mac stand-ins run Tailscale in userspace mode. That is the same network
code the app's embedded node (tsnet) uses, and it needs no TUN device. The
probes reach the NAS through the client's SOCKS5 port, as the app's
loopback proxies reach it through its node.

### What changes on the host

- Docker creates the lab's networks and containers, and removes them at
  `lab.sh down`. Docker's own host rules for its networks are the only host
  firewall change.
- The routers, the wan, the relay and the Mac stand-ins get `NET_ADMIN` for
  their own network namespaces only. The Manager gets none: `lab.sh` sets its
  route from a throwaway helper in its namespace.
- The first `tc` use loads the kernel's `sch_netem` and `sch_htb` modules if
  they are not loaded already, as in the remote lab. The routers use
  nf_tables iptables, which Docker already uses.
- Nothing publishes a port. No host sysctl, interface or route changes.

## Cases

`cases.tsv` lists the cases. Each one sets the routers' switches, joins a new
Mac node with a fresh pairing code, then measures.

| Case | Setup | Expected path |
|---|---|---|
| `direct-forward` | Easy NAT both ends; the home router forwards the control port and the NAS node's UDP port. | direct |
| `hole-punch` | Easy NAT both ends; no UDP forward. | direct |
| `hard-nat-relay` | Hard NAT both ends; no UDP forward. | relay |
| `forward-hard-away` | Hard NAT away; the home forwards the UDP port. | direct |
| `cgnat-away` | The away site behind a hard carrier NAT; no UDP forward. | relay |
| `cgnat-away-forward` | The same, with the home's UDP forward. | direct |
| `home-hairpin` | Mac at home; the router does hairpin NAT. | direct |
| `home-local-dns` | Mac at home; no hairpin; a local DNS entry for the control name. | direct |
| `home-no-hairpin` | Mac at home; no hairpin, no local DNS entry. | no connection |
| `portmap-bridge` | Hard NAT both ends, no forward, the router answers NAT-PMP; the NAS node behind the NAS's Docker NAT (today's setup). | relay |
| `portmap-host` | The same, with the NAS node directly on the home LAN (host networking). | direct |
| `wan-down-boot` | A sequence. The home's internet link goes down and the Manager restarts, as when the NAS boots after a power cut before the router has internet. Link must become ready, and the Mac away must not connect. Then the internet comes back and the Mac away must connect. The Mac at home (local DNS entry) tries to connect while the internet is down; its result is reported (see Limits). | direct |

The control port (TCP 30193) is forwarded in every case: without it a Mac
away from home cannot reach the control server at all.

## What it measures

| Column | Meaning |
|---|---|
| Path | `direct` or `derp`, from `tailscale status --json` (`CurAddr`, `Relay`) and `tailscale ping`. |
| Control | From node start until `tailscale up` returns (registered with the control server). |
| First PING | From node start until the first Redis PING through Link answers. |
| Direct at | From node start until the path is direct, if it becomes direct within 20 s. |
| Redis PING ms | Median round trip of a PING to the Redis proxy through Link, one connection. |
| GET, PUT | Object store throughput through Link: a 64 MiB read toward the Mac, a 16 MiB write. |
| Relay CPU | The relay container's CPU while the bench runs, in % of one core. |
| Router restart → PING | Restart the home router (its NAT mappings are lost), then time until a Redis PING through Link answers again. The path after it is in brackets. |
| Node restart → PING | The same after the Manager restarts (Headscale and the NAS node). |

Each run writes `results/<run id>/`: one JSON file per case, `results.json`,
`run.json` (commit, versions, profile, errors) and `scoreboard.md`.

## Run it

You need Docker with Compose v2 and Python 3 on Linux.

```sh
cd test/link-lab
./measure.py                                   # every case (about 15 minutes)
./measure.py --cases direct-forward,hard-nat-relay --profile abroad
```

`measure.py` starts the lab, runs the cases, prints the scoreboard and tears
the lab down. `--keep` leaves it running; `--no-up` reuses a running lab;
`--no-recovery` skips the restarts. The default profile is `same-city` (20 ms,
200 Mbit/s each way).

To explore by hand:

```sh
./lab.sh up
./lab.sh case hole-punch                   # set the routers for a case
./lab.sh shape hotel                       # change the WAN profile
./lab.sh probe away connect --server https://link.lab.example:30193 --hostname try1
./lab.sh probe away path                   # direct or derp, and tailscale ping
./lab.sh exec away-mac tailscale --socket=/run/tailscale/tailscaled.sock status
./lab.sh uplink home down                  # the home's internet link down (up: back)
./lab.sh status                            # router rules and WAN qdiscs
./lab.sh down
```

The CI job (`.github/workflows/link-lab.yml`) runs `direct-forward` and
`hard-nat-relay`. It fails only when the lab breaks: the lab does not come
up, or a case that should connect does not. A different path or a slow
number never fails it. The scoreboard is in the job summary and the JSON is
the `link-lab-results` artifact.

## Limits

The lab shows how Link's own code behaves against imitated networks. It does
not prove how real networks behave.

- **No real routers.** The NAT types are iptables imitations. Real home
  routers, carrier NATs and hotel networks differ: port preservation,
  mapping timeouts, UPnP bugs, captive portals.
- **Easy NAT is Linux's NAT.** It keeps the source port only while that
  port is free. When the far side's first packet reaches the router before
  the NAS node's first packet goes out, the router has already recorded the
  inbound packet. It then gives the outgoing packet a different port, and
  hole punching fails. Many real routers run Linux and do the same; others
  do not.
- **No macOS client.** The Mac stand-ins are the Linux Tailscale client. The
  lab does not test the app, macOS networking, Local Network permission,
  Wi-Fi to cellular changes, or sleep and wake.
- **No real internet.** No real loss patterns, long routes or home upload
  limits beyond what a profile sets. Everything shares one machine's CPU, so
  relay throughput here is not a server's throughput.
- **No real certificates or dynamic DNS.** The lab CA is not a public CA.
- **IPv4 only.**
- **No JuiceFS mount.** The probes measure the Redis proxy and the object
  store through Link, not a JuiceFS client mounted through it. The remote
  lab (`test/remote-lab`) measures JuiceFS over a link.

- **No relay, Mac at home.** In `wan-down-boot` the Mac at home registers
  with the control server but cannot reach the NAS node while the home has
  no internet. The NAS node sits behind the NAS's Docker NAT and advertises
  only its container address (`10.232.11.10:41641`), which the LAN cannot
  reach. With a relay, the two nodes swap addresses through it; without
  one, they cannot. Advertising the NAS's LAN address and UDP port as an
  extra endpoint fixes it: with `TS_DEBUG_PRETENDPOINT=10.232.10.5:41641`
  on the Manager (lab only, a Tailscale debug setting) the Mac at home
  connected directly 17 s after its start.

### Not in the lab yet

- **UPnP and PCP.** The home router answers NAT-PMP only (`lib/natpmp.py`).
  `miniupnpd` (UPnP, NAT-PMP and PCP) is not used: Alpine's build drives the
  legacy iptables interface, which needs host kernel modules that a stock
  Docker host may not have loaded.
- **Peer relays** and Headscale's embedded DERP server. The relay is a
  standalone `derper` on the WAN.
- Roaming (the away Mac changing address), revoking a Mac, Headscale down
  while a Mac is at home, a relay on the NAS itself.

## Lab-only settings

The lab runs the Manager as `server/docker-compose.link.yml` does, with
these differences:

- `JM_NET_NAS_CONTROL_URL=https://link.lab.example:8091` and a hosts entry
  for that name, so the NAS node reaches its own control server under the
  certificate's name without hairpin NAT. This is one of the documented ways
  to run Headscale's own TLS.
- `JM_NET_DERP_MAP_URL` points at the lab DERP map (`lib/derp/derpmap.json`),
  which the relay serves across the WAN. The Manager fetches it as it fetches
  Tailscale's public map by default, and keeps a copy for Headscale. This
  needs the Manager change that keeps the DERP map as a local file (PR #65
  or later); an older Manager ignores the setting and does not start Link
  here, because the lab has no internet.
- The Manager image (`image/manager.Dockerfile`) is the same `juicemount-manager`
  binary and Headscale release on a smaller base, without the patched JuiceFS
  the lab does not use.

## Addresses

The lab uses `10.232.0.0/16` (`10.232.1-3`, `10.232.10-11` and
`10.232.20-21`). It does not use `100.64.0.0/10`, the range Tailscale itself
uses, even for the carrier network. To run two labs at once, give each a
different `JMLINK_PROJECT` and change the subnets in `compose.yml`.
