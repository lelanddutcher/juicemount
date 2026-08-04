# The derivative key — `jm1:<hash>:<size>`

**Status: PROVIDER-PUBLISHED, 2026-08-03. Closes `KEY-FORMAT` in [`LOOP.md`](../LOOP.md).**
Authored from `internal/farm/hash.go` (`SampleHash`) — the function that has stamped every
`source_hash` on the wire since JM-14. This document does not invent a key; it writes down the one
already in production and gives it a version prefix.

Conformance vectors: [`fixtures/derivative-key/vectors.json`](../fixtures/derivative-key/vectors.json).
Provider-side pin: `internal/farm/hash_conformance_test.go`.

---

## 1 · Why a second key exists at all

The consumer's `content_key` cannot be this key, and the reason is the consumer's own finding
(CONSUMER_STATUS 2026-08-03 C): `MediaFingerprint.Strategy` is a **user setting**, so two ClipLogger
installs can compute different `content_key`s for identical bytes. A directory name has to be a
function of the bytes and nothing else. `content_key` keeps its existing job — following a file
across a move within one install — and this key names derivative directories. Two keys, two jobs.

## 2 · The string

```
jm1:<16 lowercase hex chars>:<decimal byte size>
```

- `jm1` — the recipe version. **Read it; never infer the algorithm from the string's shape.** A
  future `jm2` changes the bytes hashed, not just the encoding.
- `<hash>` — the 64-bit result of §3, formatted `%016x` (lowercase, zero-padded to exactly 16).
- `<size>` — the file size in bytes, decimal, no padding. Redundant with the hash input by design:
  it makes a truncation visible without hashing, and makes the key greppable.

Example: `jm1:acd8772d176ec7ee:3145728`

**Directory naming.** Use the string verbatim. `:` is legal in a path component on APFS, ext4, and
SMB, but **not on exFAT/FAT32**, which matters for the portable tier on a USB stick. On any
filesystem that rejects `:`, substitute `_` for each colon — `jm1_acd8772d176ec7ee_3145728` — and
treat the two forms as the same key. Producers should prefer `:` where it is legal so the two tiers
look identical.

## 3 · The byte recipe

xxh3, **64-bit**, default seed (0), no custom secret. Reference: `github.com/zeebo/xxh3`, streaming
`xxh3.New()` → `Sum64()`. Feed it exactly three things, in this order, and nothing else:

1. **The size**, as an **8-byte little-endian unsigned 64-bit integer**. Always written, including
   for an empty file.
2. **The head**: `min(1048576, size)` bytes read from offset **0**. Skipped entirely when `size == 0`.
3. **The tail**: exactly `1048576` bytes read from offset `size - 1048576` — **written ONLY when
   `size > 2097152`** (strictly greater than two windows).

`sampleWindow` is **1 MiB = 1048576 bytes**.

### 3.1 The surprising case, stated plainly

For `1048576 < size <= 2097152` the head is capped at 1 MiB **and the tail is still skipped**, so the
bytes between 1 MiB and the end of the file are **not hashed**. This is deliberate — the condition is
"the windows would overlap", not "the file is large" — but it is the one property an implementer will
get wrong by guessing. A 1.5 MiB file and a 1.5 MiB file differing only after the 1 MiB mark produce
the same key. Media at these sizes is rare and the size term still catches truncation; if that ever
stops being acceptable it is a `jm2`, not a patch.

### 3.2 Errors are not silent

A short read of either window is an **error**, never a zero-padded buffer. Hashing a padded buffer
yields a key that a clean re-read can never reproduce, which would fail every downstream
`hash == source_hash` gate on a perfectly valid derivative. If the claimed size disagrees with the
file, fail — do not hash.

## 4 · Relationship to `source_hash`

The `<hash>` half is **byte-identical to the `source_hash`/`asset_key` already on the wire** — same
function, same windows. This key is that value plus a version prefix and the size. Nothing about
`/derivatives`, `/metadata`, `register`, or the assertion sidecars changes.

## 5 · Conformance

`fixtures/derivative-key/vectors.json` carries eight vectors covering every branch: empty, tiny, both
window boundaries, the no-tail gap, and the first size where the tail engages. Fill byte at offset
`i` is `(i * 37 + 11) mod 256`.

An implementation is conformant when it reproduces all eight. **Both sides should run these in CI**;
a mismatch must fail a test rather than a user's disk.
