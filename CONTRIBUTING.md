# Contributing to JuiceMount

Thanks for considering it. JuiceMount handles people's footage, so the bar is reliability first. Here is how to help without wasting a round trip.

## The most valuable contribution: testing on real hardware

This project is hardened against real Finder, Resolve, and Premiere workloads, not synthetic checks. Different NAS vendors, network shapes, and NLE versions are exactly what it needs. If you run it on a Synology, a QNAP, an Intel Mac, or over a link the author has never tested, a report — success or failure — is genuinely useful. Real-world testing beats unit tests here; that rule is earned.

## Reporting bugs

Open an issue using the **Bug report** template. The single most helpful thing you can attach is the diagnostic zip: menu-bar → **Export Diagnostics** (also in Preferences → Maintenance). It bundles logs, the mount table, and backend health into a local file, and nothing is sent anywhere until you attach it yourself.

For a security problem, do **not** open a public issue — see [`SECURITY.md`](SECURITY.md).

## Submitting code

- **One theme per PR.** Stability fixes never share a commit with features. A focused diff gets reviewed and merged; a grab-bag stalls.
- **Run the checks on touched packages:**
  ```sh
  go vet ./...
  go test -race ./<touched-package>/...
  ```
- **Request-path changes get an adversarial review pass.** Anything in the NFS handler, the read/write path, the metadata store, or the offline gates is where data safety lives — expect close scrutiny and add a test that exercises the failure you fixed.
- **Match the surrounding code.** Comment density, naming, and idiom should look like the file you are editing.
- Keep commit messages plain and descriptive; explain the *why* when it is not obvious.

## Building from source

Full developer setup — passwordless mount for fast test cycles, the headless CLI, and the config reference — is in [`docs/dev-setup.md`](docs/dev-setup.md). The short version:

```sh
brew install juicefs
brew install --cask macfuse        # approve the system extension if macOS asks
./scripts/build-app.sh             # Go c-archive + Swift app + codesign
./scripts/install.sh               # → /Applications
```

Requires macOS 14+, Go 1.26+, and Xcode command-line tools. Architecture and the data-safety story are in [`ARCHITECTURE_juicemount.md`](ARCHITECTURE_juicemount.md) and [`MENU_BAR_APP.md`](MENU_BAR_APP.md).

## Non-negotiables

So you do not waste a PR, these are firm:

- **No telemetry without opt-in.** No analytics, no phone-home, no silent update checks.
- **No proprietary dependencies** for self-hosters. The whole stack has to run on hardware someone owns.
- **No FileProviderExtension. Ever.** [`docs/no-fileprovider.md`](docs/no-fileprovider.md) is the postmortem; the build script fails if a plugin sneaks into the bundle.
- **Reliability beats novelty.** A clever feature that risks footage is not a trade this project makes.

## Code of conduct

Participation is governed by the [Contributor Covenant](CODE_OF_CONDUCT.md). Be decent.

## License

By contributing, you agree your contributions are licensed under the [Apache License 2.0](LICENSE), the same as the project.
