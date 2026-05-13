# Signing and notarization

`scripts/build-app.sh` produces a `JuiceMount.app` bundle. Whether that bundle
will run on **other** Macs (or just yours) depends on how it's signed.

## TL;DR

- **Daily dev:** `./scripts/build-app.sh` — auto-detects a Developer ID
  Application cert if present, otherwise falls back to **ad-hoc** with a
  warning. Ad-hoc builds run on the build machine but Gatekeeper rejects them
  on every other Mac.
- **Fast dev iteration:** `JM_QUICK=1 ./scripts/build-app.sh` — same as above
  but explicitly skips notarization (saves 1-5 minutes per build).
- **Distribution build:** `./scripts/build-app.sh` with a Developer ID cert in
  your login keychain **and** a `JuiceMount` notary credential profile stored
  via `xcrun notarytool store-credentials`. The script signs with timestamp,
  notarizes, and staples — producing an artifact that runs cleanly on any
  recent macOS.

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

There is no `JM_TEAM_ID` — the team ID is embedded in your Developer ID cert
and in the stored notary profile, so the build script doesn't need to know it.

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
./scripts/build-app.sh
```

The script will:

1. Sign with the Developer ID cert, `--options runtime`, `--timestamp`, and
   the entitlements at `entitlements.plist`.
2. Zip the app and submit it to Apple's notary service via `xcrun notarytool
   submit … --wait` (blocking until Apple returns a verdict; typically
   1-5 min).
3. Staple the resulting ticket into the bundle.
4. Validate the staple.

The footer line `Notarized: yes` and `Staple: yes` mean the artifact is ready
to ship.

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
  runtime). The build script signs the top-level bundle with `--deep`, which
  covers helpers, but if you add a Mach-O file manually, sign it explicitly.
- The bundle contains a non-allowed entitlement, or claims an entitlement
  that requires a provisioning profile.
- A timestamp couldn't be obtained at sign time.

Retrieve the log for diagnostics:

```bash
xcrun notarytool log <submission-id> --keychain-profile JuiceMount
```

## Build guard: no PlugIns / app extensions

The build script refuses to produce a bundle that contains a
`Contents/PlugIns/` directory — meaning **no embedded app extensions, ever**.
This is a load-bearing guard, not a stylistic preference: see
[`docs/no-fileprovider.md`](no-fileprovider.md) for the postmortem on what
happens when a stray FileProviderExtension gets registered with macOS.

If a future architectural change ever genuinely needs an app extension, the
guard must be removed deliberately and the FileProvider domain removal
lifecycle documented before merging.
