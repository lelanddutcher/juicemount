# JuiceMount 0.5.0 release notes

JuiceMount 0.5.0 (build 37) is a general release for Apple Silicon Macs running macOS 26 or later. Get the app from [GitHub Releases](https://github.com/lelanddutcher/juicemount/releases/latest). The app checks for updates only when you choose **Check for Updates…**; it does not check in the background.

## What 0.5 covers

JuiceMount presents a local NFS volume in Finder, backed by a SQLite metadata mirror, an SSD cache, a local write spool, and the pinned JuiceFS engine bundled with the app. JuiceFS reaches the Redis metadata service through an authenticated RESP proxy and stores file data in S3-compatible object storage. macFUSE is required on the Mac. Resident metadata and file ranges are designed to serve without a synchronous remote request; missing ranges and revalidation can still need the server.

Bounded packaged Finder checks passed for copy and directory workloads; focused checks also covered atomic offline publication, conflicting saves, cancellation, and interrupted upload retry. These results do not establish full Finder, slow-link, power-loss, or two-Mac acceptance. The limits below describe the workflows that still need qualification.

## Requirements and installation

- macOS 26 or later on Apple Silicon. Intel source builds have not been verified.
- [macFUSE](https://macfuse.github.io/). The app bundles its pinned JuiceFS engine; do not substitute an arbitrary Homebrew `juicefs` binary.
- A configured server built from the digest-pinned [0.5 server compose](../server/INSTALL-TrueNAS.md), using image digests from the published release manifest, with the Mac enrolled and approved in Manager. Keep Redis, MinIO, and Manager on a trusted LAN or private route.
- Approval for JuiceMount's bundled eject helper in **System Settings → General → Login Items & Extensions**. The helper must answer before the app starts a mount. **Start at login** is a separate app preference; it is not the eject-helper approval.

Download `JuiceMount.zip` and `SHA256SUMS` from the [0.5.0 release](https://github.com/lelanddutcher/juicemount/releases/tag/v0.5.0), verify the archive against the published checksum, unzip it, and move `JuiceMount.app` to `/Applications`. Start the app, finish Setup Assistant, approve the bundled helper if macOS asks, and confirm the volume appears in Finder. For source builds, see [signing and notarization](signing.md); an ad-hoc signed app cannot start a normal mount because the helper requires JuiceMount's Developer ID identity.

## Upgrading an existing install

Check **Pending uploads** in the menu bar before replacing the app. Let the queue drain to the server, or preserve the existing local spool and metadata so the same pending saves can be recovered. A completed Finder copy does not prove the background upload has finished. Use **Stop** and then **Quit** through the app; if it warns that work is active, resolve that state before replacing the bundle. Keep the previous app and a backup of the server's object data and Redis metadata. Do not delete the local spool, metadata database, or pending-upload records as part of an upgrade.

The repository's `scripts/install.sh` replaces `/Applications/JuiceMount.app` and can quit a running app. It is intended for a prepared source-build install, after the same queue and Stop/Quit checks. A rollback to 0.4 with 0.5 local state has not been qualified; checkpoint journals may use a newer schema, so an older app must not replay them without a separately verified recovery procedure.

## Current limits

- Full Finder acceptance, including final UI responsiveness, slow network behavior, and the complete release QA battery, remains open. Earlier latency and throughput figures describe specific workloads and machines.
- Fresh 0.5 server installation has not been qualified. During the release check, anonymous access to the compose-pinned MinIO image returned 401 at Docker Hub and Quay. That does not establish whether an authenticated account can pull it or whether a cached image can run. Verify access to the exact MinIO image and all matching server and base image digests before a fresh setup or replacing a working stack; preserve the existing deployment and data until then.
- Fresh installation and upgrade on other Macs have not passed final acceptance. Keep the previous app and backups until your own volume and pending-upload state are verified after upgrading.
- Offline reads depend on metadata and ranges already present locally. The complete pin, disconnect, edit, reconnect, and power-loss workflows remain under test. Preserve every pending save until canonical publication is confirmed.
- Two-physical-Mac leases and simultaneous project editing remain under test.
- Link route, reconnect, and revoke behavior is not yet fully accepted. Use a trusted system private route for remote access.
- Automatic recovery of accepted replacement saves is on by default when the spool is enabled, unless you explicitly turn it off. Completing a replacement that needs a new stage also requires provider support for stage births. The default server proxy runs in passthrough mode with stage births off, so those saves remain pending until a capable provider is available. Preserve the local spool and metadata. Enabling stricter proxy modes requires a separately qualified rollout; the complete disconnected and multi-Mac workflows remain unqualified.
- Apple Quick Look previews use original file data. The bundled third-party thumbnail extension does not replace Apple's native preview handling for file types Apple already covers.
- GPU worker and deployed derivative queue recovery, plus the downstream consumer pin workflow, remain open. Consumer provenance and canonical publication across the complete workflow remain unqualified.
