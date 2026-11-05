# Continuity manifest encoding

This package defines pure identity, canonical manifest, and complete logical-byte
hashing functions. It has no catalog, storage bindings, routes, or media effects.

`LibraryID` and `AssetID` are distinct string types containing `lib_` or `asset_`
and exactly 32 lowercase hexadecimal characters. Their all-zero values are
invalid. `NewLibraryID` and `NewAssetID` use `crypto/rand` to create nonzero IDs.
`RevisionID` contains `rev1_` and exactly 64 lowercase hexadecimal characters.
Its digest may be all zeros syntactically. Each type's `Validate` rejects malformed
prefixes, lengths, uppercase characters, and unsupported revision versions.
String conversion or JSON decoding alone does not validate an ID.

`Content` carries a nonnegative `int64` size and a full byte SHA-256 digest as
exactly 64 lowercase hexadecimal characters. `Content.Validate` checks syntax.
A forged digest with the right format cannot be identified by syntax; actual byte
verification is required.

`Manifest.Canonical` requires explicit `Version: 1` and emits these fields in order:

| Field | Encoding |
| --- | --- |
| Domain and version | ASCII `juicemount.revision`, NUL, byte `1` |
| Library identity | 16 decoded bytes |
| Asset identity | 16 decoded bytes |
| Logical content size | Unsigned 64-bit big-endian integer |
| Content SHA-256 | 32 decoded bytes |
| Parent count | Unsigned 16-bit big-endian integer |
| Parents | Each 32-byte revision digest, sorted lexicographically |

There may be zero through 64 parents. Duplicate parents are rejected; sorting
copies the caller's slice. Nil and empty parent lists encode identically. Unknown
manifest versions, invalid IDs/digests, and negative sizes are refused.
`Manifest.RevisionID` computes SHA-256 of this entire canonical encoding exactly
once, including its domain/version prefix, then prepends `rev1_` to the lowercase
hexadecimal result. Library and asset identity participate even when bytes match.
Paths, locations, timestamps, and source observations do not participate.

`HashContent(reader, maxBytes)` consumes all logical bytes through successful EOF
with a fixed 32 KiB buffer and returns their measured size and SHA-256. A reader
for a sparse source must supply holes as zeros. A negative limit, nil reader,
read error, oversized stream, invalid read count, or 100 consecutive empty reads
fails with a zero, unusable descriptor. Read errors remain available through
`errors.Is`; no progress returns `io.ErrNoProgress`. Exact-limit EOF succeeds.
Over-limit detection consumes at most one extra byte and returns
`ErrContentTooLarge`, without adding one to a potentially maximal `int64` limit.

`VerifyContent(reader, expected)` validates the descriptor before reading and
compares complete bytes and exact length. It consumes at most the declared length
plus one byte. Short or changed bytes return `ErrContentMismatch`; longer streams
return `ErrContentTooLarge`; read errors propagate. There is no sampled fallback.

The content SHA-256 identifies complete logical bytes. The manifest SHA-256
identifies this library/asset/content/lineage declaration. A content-graph mapping
SHA-256 identifies observed slice mappings and is neither of those digests.

These functions cannot freeze a mutable source, establish a qualified snapshot,
authenticate authorship, grant access, prove parent membership or acyclic history,
or certify publication/durability. Adapters must supply consistent immutable bytes
and bind qualified source evidence. Catalog admission must separately verify
library/asset parent membership, history, authorization, and publication evidence.
