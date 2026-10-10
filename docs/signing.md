# Signing and notarization

`scripts/build-app.sh` produces a `JuiceMount.app` bundle. Whether that bundle
will run on **other** Macs (or just yours) depends on how it's signed.

The [0.5 release notes](release-0.5.md) explain installation, upgrade
precautions, and open acceptance work. For any artifact, use the checks below
to establish the exact signed and notarized build rather than relying on a
version label alone.

## TL;DR

- **Daily dev:** `./scripts/build-app.sh` — auto-detects a Developer ID
  Application cert if present, otherwise falls back to **ad-hoc** with a
  warning. Ad-hoc builds may launch locally but cannot Start a mount: the
  bundled eject helper admits only JuiceMount Developer ID peers. Gatekeeper
  also rejects ad-hoc builds on other Macs.
- **Fast dev iteration:** `JM_QUICK=1 ./scripts/build-app.sh` — same as above
  but explicitly skips notarization (saves 1-5 minutes per build).
- **Distribution build:** `JM_VERSION=0.5.0 JM_COMMIT=$(git rev-parse HEAD)
  JM_BUILD_NUMBER=37 ./scripts/build-app.sh --distribution` with a Developer ID
  cert in your login keychain **and** a `JuiceMount` notary credential profile
  stored via `xcrun notarytool store-credentials`. The build number must be
  greater than 13 and greater than every build in the published update feed;
  37 is the 0.5 release build number after development build 36. Check the
  live feed and exact release plan before any later distribution build.
  The script reads the published feed from the app's `SUFeedURL`
  (`https://github.com/lelanddutcher/juicemount/releases/latest/download/appcast.xml`)
  and refuses to build if it cannot fetch it. When offline, download that file
  at release time and pass it as `JM_PREVIOUS_APPCAST=<path>`.
  The script signs with timestamp, notarizes, and staples.

The build footer prints a status block so you can tell at a glance what kind
of artifact you got:

```
==> Build complete
    App:        build/JuiceMount.app
    Identity:   Developer ID Application: Jane Doe (TEAMID12)
    Notarized:  yes
    Staple:     yes
```

## Environment variables

| Variable             | Effect                                                                                                         |
| -------------------- | -------------------------------------------------------------------------------------------------------------- |
| `JM_SIGN_IDENTITY`   | Override auto-detection. Pass either the full cert name (`"Developer ID Application: Jane Doe (TEAMID12)"`) or a SHA-1 hash. |
| `JM_NOTARY_PROFILE`  | Name of the `notarytool` keychain profile to use. Default: `JuiceMount`.                                       |
| `JM_QUICK`           | If set (any value), skip notarization entirely. Use this for inner-loop dev iteration.                         |
| `JM_BUILD_NUMBER`    | Positive decimal `CFBundleVersion`. Optional for local builds; required and greater than 13 for `--distribution`. Must be greater than every `sparkle:version` in the published feed, or the build refuses. |
| `JM_PREVIOUS_APPCAST` | Optional path to a downloaded copy of the published feed (`SUFeedURL`: `https://github.com/lelanddutcher/juicemount/releases/latest/download/appcast.xml`). Download it at release time. When unset, `--distribution` fetches the feed itself and refuses if the fetch fails. When set, the live feed is still checked if reachable, and an older local copy refuses. |
| `JM_APPCAST`         | Refused unless `--distribution` is used. Only the distribution path produces a signed update feed. |
| `JM_VERSION`         | Marketing version (`CFBundleShortVersionString`), required for `--distribution`. |
| `JM_COMMIT`          | Exact 40-character source SHA, required for `--distribution`. |

There is no `JM_TEAM_ID` — the team ID is embedded in your Developer ID cert
and in the stored notary profile, so the build script doesn't need to know it.

## Inspecting and preparing the eject helper

The bundled app supports two commands that return one JSON line and exit before
normal app startup. They do not start the serving service, mount a volume, or
read or change saved application preferences:

```bash
/path/to/JuiceMount.app/Contents/MacOS/JuiceMount --eject-helper-status
/path/to/JuiceMount.app/Contents/MacOS/JuiceMount --prepare-eject-helper
```

Status never registers the helper. Prepare registers an absent helper once and
may require approval in **System Settings → General → Login Items & Extensions**.
It requires JuiceMount's exact Developer-ID app identity without a debug
entitlement, and refuses when app isolation is configured. An enabled helper
is only pinged; this command does not replace or re-register it.

Exit `0` means the enabled helper answered an authenticated ping. Exit `2` means
registration or user approval is still needed. Exit `1` reports invalid arguments,
a refused operation, or a failure. These commands do not establish that a mount
has registered its own recovery receipt.

## First-time setup

You need to do this **once per Mac** before you can produce a distributable build.

### 1. Get a Developer ID Application certificate

Requires an active paid Apple Developer Program membership.

1. Open Xcode → **Settings** → **Accounts**.
2. Select your Apple ID, then click **Manage Certificates…**.
3. Click the **+** button at the bottom-left → **Developer ID Application**.
4. Xcode generates the cert and installs it into your **login** keychain.

Verify:

```bash
security find-identity -p codesigning -v | grep "Developer ID Application"
```

You should see at least one line like:

```
1) ABCDEF0123456789… "Developer ID Application: Jane Doe (TEAMID12)"
```

If you see **more than one** such cert (e.g. an old expired one alongside a
new one), the build script will pick the first match and print a NOTE. To
pin it deterministically, set `JM_SIGN_IDENTITY` to the exact quoted name or
to the SHA-1 hash.

### 2. Generate an app-specific password

Notarization is authenticated against Apple ID, and the password used must be
an **app-specific** password — not your real Apple ID password.

1. Sign in at <https://appleid.apple.com>.
2. **Sign-In and Security** → **App-Specific Passwords** → **+** → label it
   `notarytool JuiceMount` (or similar) → copy the generated password
   (looks like `abcd-efgh-ijkl-mnop`).

Store the password somewhere safe (1Password etc.). You won't be shown it
again, but you can always revoke it and make a new one.

### 3. Store the notary credential in your keychain

```bash
xcrun notarytool store-credentials JuiceMount \
    --apple-id you@example.com \
    --team-id TEAMID12 \
    --password abcd-efgh-ijkl-mnop
```

- The first positional arg (`JuiceMount`) is the profile name. The build
  script defaults to this name; override with `JM_NOTARY_PROFILE`.
- `--team-id` is the 10-character Team ID from your developer account. You
  can find it in the cert name (the parenthesized suffix) or at
  <https://developer.apple.com/account> → Membership.

Verify the profile is usable:

```bash
xcrun notarytool history --keychain-profile JuiceMount | head
```

If that prints a (possibly empty) submission history without an auth error,
you're set.

### 4. Produce a distributable build

```bash
# Optional when online: the script fetches SUFeedURL itself.
curl -fsSL -o "$HOME/Downloads/published-appcast.xml" \
  https://github.com/lelanddutcher/juicemount/releases/latest/download/appcast.xml

JM_VERSION=0.5.0 JM_COMMIT=$(git rev-parse HEAD) JM_BUILD_NUMBER=37 \
  JM_PREVIOUS_APPCAST="$HOME/Downloads/published-appcast.xml" \
  ./scripts/build-app.sh --distribution
```

Keep the downloaded feed outside the source tree. A distribution build refuses
untracked files in the tree.

The script will:

1. Sign with the Developer ID cert, `--options runtime`, `--timestamp`, and
   the entitlements at `entitlements.plist`.
2. Zip the app and submit it to Apple's notary service via `xcrun notarytool
   submit … --wait` (blocking until Apple returns a verdict; typically
   1-5 min).
3. Staple the resulting ticket into the bundle.
4. Validate the staple.

The footer line `Notarized: yes` and `Staple: yes` mean the artifact is ready
for the independent release verifier. The distribution path also checks that
the app, QuickLook extension, update archive, and Sparkle appcast agree on
both marketing version and build number, and that each carries
`JMBuildChannel=distribution` (only `--distribution` writes it, so a dev build
cannot pass the release verifier). Increase `JM_BUILD_NUMBER` for every
subsequent published build, including repackaged 0.5 releases.

Before publishing, verify the exact app, archive, checksum, appcast signature,
version/build identity, Gatekeeper acceptance, and stapled notary ticket from
the final artifact. Do not treat a successful development build as that
verification. A user upgrading from an earlier app must first drain or retain
the local upload queue, then Stop and Quit before replacing the bundle.

## Verifying a finished build

```bash
# 1. Inspect the signature (identity, entitlements, runtime flag, timestamp).
codesign -dv --verbose=4 build/JuiceMount.app

# 2. Verify the signature deeply.
codesign --verify --deep --strict --verbose=2 build/JuiceMount.app

# 3. Ask Gatekeeper whether it would accept the bundle.
spctl -a -vv -t install build/JuiceMount.app

# 4. Confirm the notary ticket is stapled (works offline).
xcrun stapler validate build/JuiceMount.app
```

For a fully signed + notarized + stapled build, expected outputs are:

- `codesign -dv`: shows `Authority=Developer ID Application: …`,
  `Authority=Developer ID Certification Authority`, `Authority=Apple Root CA`,
  plus a `Timestamp=` line.
- `spctl -a`: prints `accepted` and `source=Notarized Developer ID`.
- `stapler validate`: prints `The validate action worked!`.

## Common errors

### `errSecInternalComponent` from codesign

The login keychain is locked. Run `security unlock-keychain login.keychain`
(or unlock it via Keychain Access) and retry.

### `errSecCSReqFailed` or `code object is not signed at all`

Usually means the cert chain is broken or the cert was deleted from the
keychain. Re-run **Manage Certificates** in Xcode.

### `--timestamp option requires Apple's timestamp server access`

The build host is offline. The build script only passes `--timestamp` when
signing with a real identity (not ad-hoc). Get network connectivity or
unset the cert (force ad-hoc) for offline builds.

### `Could not find the keychain profile "JuiceMount"`

You haven't run `xcrun notarytool store-credentials …` yet (or used a
different profile name). See [First-time setup](#first-time-setup) §3.

### Notarization fails with `Invalid` status

The submit succeeded, but Apple rejected the artifact. Common causes:

- Some embedded binary isn't signed with `--options runtime` (hardened
  runtime). The build script signs nested Sparkle helpers and the Quick Look
  extension first, then seals the app last. `codesign --verify --deep` checks
  the result; `--deep` is not used to sign nested code. Rebuild if embedded
  code changes.
- The bundle contains a non-allowed entitlement, or claims an entitlement
  that requires a provisioning profile.
- A timestamp couldn't be obtained at sign time.

Retrieve the log for diagnostics:

```bash
xcrun notarytool log <submission-id> --keychain-profile JuiceMount
```

## Build guard: exact PlugIns allowlist

The bundle must contain `Contents/PlugIns/JuiceMountThumbnails.appex`, its
Quick Look thumbnail extension. The build script refuses any other item in
`Contents/PlugIns/`, especially a FileProvider extension. This guard is
load-bearing: see [`docs/no-fileprovider.md`](no-fileprovider.md) for the
postmortem on a stray FileProvider registration.

Any future extension requires a deliberate guard change and a documented
registration and removal lifecycle before merging.
