# Security policy

JuiceMount sits in front of people's primary storage, so security reports are taken seriously and handled quickly.

## Supported versions

JuiceMount is pre-1.0 (currently the `v0.1.x` beta line). Security fixes land on `main` and ship in the next tagged release. There is no long-term-support branch yet; please test against the latest release before reporting.

| Version | Supported |
|---|---|
| latest `v0.1.x` release / `main` | yes |
| anything older | no — please update first |

## Reporting a vulnerability

**Do not open a public issue for a security problem.** Use either:

- **GitHub private reporting** (preferred): the **Security** tab → **Report a vulnerability**. This opens a private advisory visible only to the maintainers.
- **Email:** security@juicemount.com.

Please include:

- What the issue is and the impact you see (data exposure, remote access, privilege escalation, data loss).
- Steps to reproduce, or a proof of concept.
- The JuiceMount version (menu-bar → About, or the release tag), macOS version, and your server setup (TrueNAS / Synology / plain Docker).
- An **Export Diagnostics** zip if it is relevant (menu-bar → Export Diagnostics) — note that this bundle is built locally and you choose whether to share it; review it before sending.

You will get an acknowledgement within a few days. Once a fix is ready, a release goes out and the advisory is published with credit to you, unless you would rather stay anonymous.

## Scope

In scope: the macOS app, the server stack in [`server/`](server/), the control plane, and how file data and metadata are handled.

Out of scope, and worth knowing before you deploy:

- **The stack is built for a LAN you trust.** By default the server's Redis and MinIO ports are reachable on the local network with no per-client authentication; that is a deliberate design point, not a vulnerability. Firewall those ports, or front the deployment with a VPN such as [Tailscale](https://tailscale.com), if untrusted clients share the network. See [`server/README.md`](server/README.md#security-notes).
- **You own your keys.** MinIO root credentials and any S3 keys live in your own deployment and the JuiceFS volume's format metadata. Choose strong values and keep them off public networks.

## What JuiceMount does not do

- **No telemetry.** The app talks only to the Redis and S3 endpoints you configure plus a loopback control plane on `127.0.0.1`; JuiceFS's own usage reporting is disabled with `--no-usage-report`. No analytics, crash reporting, or update checks. ("No telemetry without opt-in" is a stated project non-negotiable.)
- **No data leaves your hardware** unless you point the object store at a cloud bucket yourself.
