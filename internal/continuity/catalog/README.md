# Continuity catalog

This internal package stores one library's asset identities, immutable revision
declarations, selected heads, relationships, location observations and ordered metadata operations in a dedicated
SQLite database. It has no routes, policy grants, media writes, filesystem rename
operations, or verified/published/reachable states. A head selects a declaration;
it does not certify the current contents of a filesystem path.

`Create(path, libraryID)` exclusively reserves a new 0600 file in a caller-owned
private local directory. It refuses existing destinations and orphan sidecars.
`Open(path)` requires an existing complete catalog. Both canonicalize the existing
parent and build an escaped file URI internally; caller-supplied SQLite URIs and
memory databases are refused. Final symlinks, nonregular files, multiple hardlinks,
and unsafe WAL/SHM/rollback sidecars are refused. File identity is qualified on
Darwin and Linux; other platforms refuse admission. The directory's owner and
local filesystem/hardware are deployment preconditions. This does not defend
against privileged replacement of path ancestors.

Format 2 uses application ID `0x4a4d4341`, user version 2, an exact schema-object
inventory, and a migration ledger containing the ordered format 1 and 2 SHA-256
fingerprints. New format 2 catalogs record the same supported lineage; these
entries do not claim two runtime upgrades occurred. A ledger
alone cannot validate changed DDL. Unknown, foreign, newer, incomplete, corrupt,
or constraint-invalid databases are refused without repair or replacement.
Open performs version/DDL/ledger/identity/integrity preflight in one deferred
transaction on a separate read-only connection before obtaining a write pool
and enabling WAL. A recognized complete, integrity-valid format 1 catalog returns
`ErrUpgradeRequired`; Open never migrates it. SQLite integrity and foreign-key checks run at admission;
individual revision/event reads also verify their canonical identities. Open does
not rehash the complete operation history or any media.

Initialization records `ready=0`, establishes WAL, syncs the database and parent
directory, and atomically records `ready=1`, followed by another directory sync.
The file is never removed or replaced on failure. Incomplete files refuse Open;
a failure after final commit begins may have an uncertain outcome. Recover with
Open and check the exact requested library identity. Successful unit tests do not
establish physical power-loss safety.

Each Store has one write-pool connection, with `foreign_keys=ON`, a 2000 ms busy
timeout, `synchronous=FULL`, and `_txlock=immediate` applied through its internally
constructed DSN. Effective settings are checked at admission and mutation.
`database/sql` owns transaction commit/rollback and connection release. Contexts
and the busy timeout bound lock waits. Point reads do not start transactions;
ListAssets and the relationship/location lists return one statement's snapshot
of all returned mutable fields and joined membership checks.
`Close` refuses new calls, drains admitted calls, and closes its database and
held file identity. There are no background workers. Exports use a separate
read-only pool with deferred snapshot transactions and the
same lifecycle draining; the immediate write pool is not an export handle.

`Apply` accepts an explicit library ID, version 1, an operation ID, nonempty Actor,
and 1–256 ordered changes. Exactly one pointer variant is required per change:

- `CreateAsset`: new explicit identity and bounded display label.
- `RenameAsset`: compare the exact old label and write a new display label.
- `RecordRevision`: verify C01 canonical identity and already-present same-asset
  parents; existing immutable declarations may be repeated.
- `AdvanceHead`: SQL compare-and-swap from the explicit prior head. A first head
  must have no parents; later heads must list the expected head as a direct parent.
- `PutRelationship` / `RemoveRelationship`: compare full expected state and
  correct, reactivate or tombstone a directed relationship.
- `PutLocation` / `RemoveLocation`: compare full expected state and correct,
  reactivate or tombstone an opaque location observation.

Changes and one event commit in a single immediate transaction. A failed change
rolls back the entire batch without reserving an operation ID or event sequence.
Already-recorded branches survive a separate head conflict. Callers must retain
their media independently. SQL foreign keys/constraints preserve ownership and
chronology; immutable-row triggers refuse revision, parent-edge, and event
updates/deletes. There is no asset/revision deletion API.

Relationship values contain, in wire order, `source_asset_id`, `source_revision`,
`target_asset_id`, `target_revision`, `predicate`, `provenance`, `source`.
Both endpoints must exist. Optional revision anchors are explicit empty strings
or must belong to the corresponding asset/library. Relationships can form cycles
or self edges; they are separate from the acyclic revision-parent graph.
Predicate is 1–128 UTF-8 bytes; source is 1–256 bytes. Provenance is exactly
`observed`, `claimed` or `inferred`, with caller-supplied attribution only.

Location values contain `asset_id`, `revision`, `source`, `locator`. The optional
revision must belong to the owning asset/library. Source is 1–256 UTF-8 bytes and
locator is 1–8192 bytes. The locator is preserved exactly and never resolved,
opened, parsed for credentials or sent over a network. Callers must supply
nonsecret locators; storage cannot reliably detect every secret. An observation
proves no current presence, read permission, cache state or verified bytes.

Relationship/location IDs use the operation-ID spelling/bound in separate
namespaces. Records carry immutable library, ID and CreatedSequence plus mutable
Value, UpdatedSequence and Removed. Location ownership is immutable for the
ID's lifetime, including after removal; relationship endpoints can be corrected.
Get methods return explicit tombstones; lists exclude them. Removal retains the
last value and creation sequence and sets UpdatedSequence to the removal event.
SQL guards refuse physical deletion, identity/creation changes, backwards
chronology and location owner changes. No operation opens or deletes media.

Put wire fields are `id,expected,value`; Remove fields are `id,expected`.
Expected is explicit null for Put only when the ID has never existed. Otherwise
it is `value,updated_sequence,removed`, with positive sequence and exact value,
sequence and removal-state comparison. Remove requires a non-null expected state
and conflicts if already removed. A tombstone can reactivate only through Put
with its exact expected state; expecting absence cannot reuse the ID.
Sequence comparison rejects ABA edits that restored the same value. SQL updates
also condition the current sequence/removal state. Every correction or removal
updates sequence, including a correction to the same value. Ordered changes in
one operation may share sequence because they are not separate observable commits.

Canonical requests use compact Go `encoding/json` Marshal with fixed struct field
order, HTML escaping enabled, valid UTF-8/no NUL, and no Unicode normalization.
Envelope order is `version`, `library_id`, `operation_id`, `actor`, `changes`.
Each change contains exactly its snake_case variant name. Create fields are
`asset_id,label`; rename fields are `asset_id,expected_label,label`; head fields
are `asset_id,expected_revision,revision`. Revision fields are
`version,library_id,asset_id,content,parents`; content fields are `sha256,size`.
Parents are sorted copies and no parents encodes `[]`. Change order is preserved.
Canonical requests are limited to 1,048,576 bytes. Operation IDs are 1–128 ASCII
letters/digits/dash/underscore; Actor is 1–256 UTF-8 bytes and labels 0–4096 bytes.

Payload SHA-256 hashes ASCII `juicemount.catalog.operation`, NUL, byte 1, then the
complete canonical JSON exactly once. Receipt fields/order are `version`,
`library_id`, `operation_id`, `sequence`, `payload_sha256`, `recorded_unix_nano`.
Sequence is positive and orders events. The server wall timestamp is informational.
Stored requests and receipts must decode strictly and reproduce their canonical
bytes; unknown fields/kinds, duplicate/trailing fields, and noncanonical encodings
refuse readback. Attribution and every effective field participate in retry
identity. An identical retry returns the original receipt before checking mutable
preconditions, without another event. Changed reuse returns `ErrChangedRequest`.
A commit error is uncertain: read back or retry the identical ID and payload.

`LibraryID`, `GetAsset`, `GetRevision`, `GetEvent`, `ListAssets`, `ListRevisions`,
`Events`, `GetRelationship`, `GetLocation`, `ListRelationships` and `ListLocations`
return detached records for trusted internal services. Every record
request explicitly binds the library. Pages are 1–1000 records with exclusive
asset-ID, revision-ID, or event-sequence cursors; empty identity/zero sequence
starts at the beginning. Valid identity keys need not name an existing row.
Pages are independently current, not a multi-page snapshot. Typed error categories
distinguish invalid input, missing records, conflicts, changed retries, closed
stores, and invalid/corrupt format; underlying errors remain available for inspection.

Relationship lists select active inbound/outbound edges for an existing asset,
with self edges once. Location lists select active observations for their owner.
Both use exclusive lexicographic ID cursors and the same 1–1000 page bound.
Their one-statement joins verify endpoints and optional revision membership.
Before disclosure, a permission service must constrain both endpoints, traversal,
counts and every returned field.

`Upgrade(ctx,path,expectedLibraryID)` explicitly changes a recognized format 1
catalog to format 2. It applies the same safe-file admission and read-only snapshot
preflight, then repeats schema/library/integrity checks in an immediate transaction.
The migration replaces only the constrained v1 ledger and its guards, adds the
new projections/indexes/guards, and atomically records user version 2. Original
library/asset/revision/parent/event DDL and data stay intact, including exact
canonical requests, receipts, timestamps and event sequences. Format 1's exact
DDL/fingerprint definition is retained for admission and checked-in legacy tests.
Already-valid format 2 of the expected library succeeds without schema/event
mutation. Both migration and idempotent retry close their owners and complete a
parent-directory sync before success, so retry after a failed final barrier
finishes that barrier. Sync/close/commit errors surface; final outcomes may be
uncertain and must never be recovered by replacing the catalog.

The host should drain catalog users before upgrade. SQLite serializes concurrent
writers/upgraders; transaction revalidation accepts a second already-upgraded
store instead of rerunning the migration. This does not claim exclusive process
ownership. Old writers refuse format 2. Old binaries require a separately retained
compatible backup for rollback; there is no schema downgrade that discards history.

Stored Actor strings are caller-supplied attribution, not authenticated identity
or evidence of human activity. Catalog membership/history checks grant no access
and prove no immutable source snapshot or media durability. Permissions/disclosure
belong to the trusted service boundary. Metadata portability below preserves these
declarations and their history; it grants no import authority or media guarantee.

`Store.Export(ctx,library,writer,Limits)` captures a complete schema2 metadata
snapshot. `Restore(ctx,path,expectedLibrary,reader,Limits)` replays it into a new
exclusive 0600 target only. Restore never merges, resumes, downgrades, overwrites
or removes a failed target. Existing Create/Open/Upgrade behavior is preserved.

All three Limits fields are explicit positive budgets: MaxBytes, MaxEvents and
MaxProjectionRows. Hard ceilings are 64 GiB, 1,000,000 events and 10,000,000 logical
projection rows. Zero is invalid, rather than unlimited. Empty catalogs have no
events and one library projection row. Every magic/frame/footer byte counts.
API guardrails do not make maximum-size runs cheap; callers should use smaller
operational budgets and deadlines. ErrLimit distinguishes a reached budget.

The caller owns reader/writer lifecycle, interruptible I/O/deadlines and durable
output storage. These functions do not close caller I/O, rename export files or
fsync exported bytes. Context is checked between I/O and SQL operations; ordinary
io.Reader/io.Writer cannot forcibly interrupt a blocked call. Impossible counts,
100 consecutive empty reads, read errors accompanying bytes and short nil-error
writes refuse. A full footer write returning an error can still deliver a complete
artifact; an export error is not proof of truncation.

Portable format1 begins with ASCII `juicemount.catalog.export`, NUL, byte1.
Frames contain one kind byte, uint32 big-endian payload length, then that payload.
There is exactly one header, its declared number of events, one footer, then EOF.
Any trailing byte, including whitespace, refuses. Header/footer canonical JSON
uses the existing Go encoding/json escaping and strict decode/re-encode equality.
Unknown/missing/duplicate fields, unsupported versions, malformed UTF-8, extra
frames, sequence gaps, duplicate operation IDs and noncanonical encodings refuse.

| Kind | Payload |
| --- | --- |
| 0x01 | JSON fields `version,schema_version,library_id,event_count`, requiring 1 and 2 |
| 0x02 | uint32 request length, exact request bytes, uint32 receipt length, exact receipt bytes |
| 0x03 | JSON fields `event_count,projection_rows,projection_sha256,stream_sha256` |

Header/footer payloads are at most 1 KiB; event payloads at most 1,049,608 bytes,
with the unchanged 1 MiB request and 1 KiB receipt bounds. Inner and outer lengths
must agree exactly. Lengths and remaining budgets are checked before allocation.
Memory is bounded by a frame/event plus decoder buffers or a bounded revision-ID
page, never the entire history. Stored blob lengths are checked before decoding.

P is SHA256 of ASCII `juicemount.catalog.projection`, NUL, byte1, then every
canonical projection frame. H is SHA256 of ASCII
`juicemount.catalog.export.stream`, NUL, byte1, then magic and all exact header/event
frames, then uint64 big-endian event count, uint64 big-endian projection-row count,
then the 32 decoded P bytes. The footer contains lowercase P/H; its own frame is
excluded from H. Hashes detect mismatch, not sender authenticity or authorization.

Projection frames are internal digest input, with the same framing and at most
64 KiB canonical JSON payload. Scan fixed kinds in this order using SQLite BINARY
identifier ordering, including all tombstones:

| Kind | Ordered JSON fields |
| --- | --- |
| 0x11 library | `library_id` |
| 0x12 assets | `library_id,asset_id,label,head,created_sequence,updated_sequence` |
| 0x13 revisions | `library_id,revision_id,asset_id,canonical_hex,manifest,created_sequence` |
| 0x14 parent edges | `library_id,revision_id,parent_id,asset_id` |
| 0x15 relationships | `library_id,id,value,created_sequence,updated_sequence,removed` |
| 0x16 locations | `library_id,id,value,created_sequence,updated_sequence,removed` |

Canonical manifest hex is lowercase; manifest/value objects retain their existing
field order and explicit empty optional revisions. Parent edges sort by child
then parent. Revision canonical identity, parent membership, endpoint/anchor
membership and chronology are validated. Readiness, ledger, indexes and SQLite
layout are excluded. A future logical field/table/schema needs a reviewed format.

Export holds Store lifecycle admission and a separately owned read-only pool
with one deferred transaction. Recognition, integrity, event history/count and
all projections share that snapshot. It never uses the immediate write pool.
Concurrent writers can progress under WAL; the result represents the captured
snapshot. Cursors close before nested queries. Only after source validation,
successful transaction completion and pool close can the footer be emitted.
Path/context checks precede completion. Close drains an admitted export and its
owned pool; no detached worker or caller-I/O close is used.

Restore validates magic/header/expected library and budgets before reserving a
destination. It initializes schema2 with ready=0 and initial file/directory
barriers. Each event reuses the mutation invariants in an immediate transaction,
inserts its original canonical request/digest/timestamp/receipt, and preserves
sequence exactly. It does not call public Apply or sample time. Incremental
projection-row accounting observes actual existing identities inside the same
transaction. New assets/revisions/parent edges/relationships/locations add rows;
repeated revisions, corrections, removals and reactivations do not. An event
exceeding the row budget rolls back before consuming further input or committing;
the running count advances only after commit, without growing-table rescans.

After a valid footer and EOF, a final immediate transaction independently scans
target history/projections, compares counts/P, and validates schema/integrity/path
identity before atomically setting ready=1. Complete source-history/projection
equivalence is proved by this replay comparison. A structurally valid source
projection edited outside history may export successfully but cannot restore
as the same claimed projection. Export success alone does not prove replayability.

Invalid/truncated input, late reader errors, budget failures and cancellation
before completion retain an incomplete target that Open refuses. Prior complete
replay batches may remain inside it. Final commit/close/directory-sync errors can
leave a completed target with uncertain acknowledgment. Restore succeeds only
after closing owned resources and the final directory barrier; recovery inspects
with Open and the exact expected library/history. A second Restore must refuse
that destination and cannot manufacture certainty by overwriting it.
