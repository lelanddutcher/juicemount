# Security policy

JuiceMount sits in front of people's primary storage, so security reports are taken seriously and handled quickly.

## Supported versions

JuiceMount is pre-1.0, with 0.5.0 as the current general release. Security fixes land on `main` and ship in the next tagged release. There is no long-term-support branch yet; please test against the latest release before reporting.

| Version | Supported |
|---|---|
| latest release / `main` | yes |
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

- **Keep the server on a trusted network.** The canonical 0.5 compose does not publish Redis directly. An authenticated RESP proxy listens on port 30179 and requires named client principals; it starts in passthrough lease mode with stage births off. MinIO listens on port 30151 and requires an object key. The default client key is shared by approved Macs and can read, write, and list objects without delete rights; separate per-Mac object keys are not enabled by default. RESP authentication and Manager enrollment use plaintext on the wire unless you provide a trusted private route or a separate TLS layer. Do not expose the proxy, MinIO, or Manager to the public Internet. See [`server/README.md`](server/README.md#security-notes).
- **You own your keys.** Keep MinIO root, client, GC, proxy, and Manager credentials in your private deployment state. The current volume format metadata does not hold the client object key; each JuiceFS client supplies its key. Preserve and rotate credentials deliberately, and keep both object data and Redis metadata backed up.

## What JuiceMount does not do

- **No silent telemetry or update checks.** Automatic app traffic is limited to the Redis and S3 endpoints you configure plus a loopback control plane on `127.0.0.1`; JuiceFS usage reporting is disabled with `--no-usage-report`. There is no analytics or crash reporting. The app contacts its signed GitHub release feed only when the user explicitly chooses **Check for Updates…**.
- **No data leaves your hardware** unless you point the object store at a cloud bucket yourself.
