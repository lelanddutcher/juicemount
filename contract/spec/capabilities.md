# Capabilities & version skew

OpenLoupe and JuiceMount ship on independent schedules, and JuiceMount has **two deployment modes that expose
different control planes**. Never assume an endpoint exists — feature-detect.

## Two axes of skew

1. **Version skew** — OpenLoupe newer than the installed JuiceMount, or vice-versa.
2. **Deployment-mode skew** — discovered from real JuiceMount6 source:
   - **GUI app (cbridge / `com.juicemount.app`)** serves the **full** control plane: `/health /metrics /pin
     /unpin /cache-status /offline /spool /self-test /verify-pins` (+ `/reclaim /cache-clear /force-eject
     /stop`).
   - **`jm5` CLI** serves a **smaller** set: `/health /metrics /spool` — plus a new `/whoami` under this
     contract (JM-1), and its web-UI routes `/manager` `/migrator` (not contract capabilities). It does
     **not** expose `/pin /cache-status /offline /self-test /verify-pins`. So its capability list is
     `["health","spool","metrics","whoami"]`.

A version floor alone can't express axis 2 — a fully up-to-date `jm5` still lacks `/cache-status`. So the
contract resolves both axes with **one mechanism**.

## The mechanism: `whoami.capabilities`

`GET /whoami` returns `capabilities: string[]` — the list of control-plane routes **this process actually
serves**. OpenLoupe lights up a feature **iff** every endpoint it needs is in that list.

```
detect mount (signature) ──► GET /health (confirm live JuiceMount)
                                   │
                                   ▼
                         GET /whoami  ── absent? ──► PRE-CONTRACT / legacy JuiceMount (no /whoami yet):
                                   │                  fall back to probing /health + /spool only;
                                   │                  show "update JuiceMount for the full experience".
                                   ▼
              read capabilities[] + contract_version
                                   │
        ┌──────────────────────────┼───────────────────────────┐
        ▼                          ▼                           ▼
 has "residency"?           has "cache-status"?          has "pin"?
 → honest resident badge    → warming progress           → warm-before-scrub
 else streaming-only        else aggregate-only badge     else read-only mode
```

## Rules

- **Soft floor, feature-detect.** Pin a *minimum* `contract_version` for the shapes you parse; otherwise
  branch on `capabilities`. A capability you don't recognize is ignored; a capability you need but don't see
  disables only that feature, never the whole integration.
- **`/whoami` absent** ⇒ treat as a **pre-contract / legacy** JuiceMount build (under this contract both the
  GUI and `jm5` serve `/whoami`, so absence means an old build). Degrade to detection + `/health` + `/spool`,
  and surface a one-line "update JuiceMount for the full premium viewer".
- **`contract_version` newer than OpenLoupe pins** ⇒ parse the fields you know (they're still present —
  breaking changes bump the integer and are additive-forward where possible); ignore unknown fields.
- **`contract_version` older than OpenLoupe's minimum for a feature** ⇒ disable that feature, keep the rest.
- **Absent JuiceMount entirely** ⇒ zero-change. OpenLoupe behaves exactly as today on a plain network volume.
  Everything in this contract is gated behind the detection probe + `isJuiceMount`.

## Capability tokens (v1)

The canonical vocabulary — the **only** tokens that may appear in `capabilities`:

`health`, `whoami`, `residency`, `lookup`, `cache-status`, `offline`, `spool`, `activity`, `pin`, `unpin`,
`self-test`, `verify-pins`, `metrics`, `derivatives`, `metadata`, `contribute`, `changes`, `assertions`, `blob`.

(`derivatives` + `metadata` are the JM-14 shared-derivative-platform reads — GUI-only, like residency/lookup.)

(`blob` is the PROXY-CODEC byte-range blob delivery — `GET /blob?inode=N&kind=proxy` (PROXY_CODEC_SPEC §JuiceMount
ratification §3). GUI-only. Streams a derivative blob (proxy.mp4 etc.) with `Accept-Ranges: bytes`, answers `Range`
with `206 Partial Content` (`Content-Range`/`Content-Length`), `Content-Type` from the manifest media_type, and
`200`+`Content-Length` unranged — what a browser `<video>` and a remote AVPlayer need to seek over HTTP. The route
path == the token.)

(`assertions` is the JM-ASSERT portable-human-metadata channel — `POST`/`GET /assertions` (ASSERTIONS_SIDECAR.md).
GUI-only. POST writes the `<media>.loupe.json` sidecar (the source of truth — atomic, LWW, merge-not-clobber) +
upserts JuiceMount's rebuildable, content-hash-`asset_key`-keyed Tier-B index; GET reads the resolved set by
`asset_key`, or by `inode`/`path` resolved to `asset_key`. The route path == the token.)

(`contribute` is the OL-1 on-device-AI contribute-back **write** — `POST /derivatives/register`. GUI-only;
fail-open: a consumer that doesn't see it keeps its AI local, as today. The served route is
`/derivatives/register`, but the capability token is `contribute`, so the derivation maps that one route → the
`contribute` token rather than using the literal route string.)

(`activity` is GUI-only — `jm5` does not serve it, so it appears in the GUI list but not the CLI list.)

### Derivation rule (so "derived, not hardcoded" is deterministic)

> `capabilities` = the **intersection** of the routes *this binary actually serves* with the vocabulary
> above.

- Operational / UI routes are **never** capabilities and are excluded even though the binary serves them:
  `reclaim`, `cache-clear`, `force-eject`, `stop`, `mount-now`, `spool-recover`, `debug/pprof/*` (GUI),
  `manager`, `migrator` (jm5).
- `health` and `metrics` are built-ins (the metrics server), not `ExtraRoutes`, but they **are** capability
  tokens — include them.
- Because a binary that answers `/whoami` is by definition serving the `whoami` route, **`whoami` is always
  present** in its own list.

This makes the list both honest (it can't claim a route it doesn't serve) and deterministic (a conformance
test can predict it exactly).

GUI fixture advertises the full set; the `jm5` CLI fixture advertises
`["health","spool","metrics","whoami"]`. See [`../fixtures/whoami/gui.json`](../fixtures/whoami/gui.json) and
[`../fixtures/whoami/cli.json`](../fixtures/whoami/cli.json).
