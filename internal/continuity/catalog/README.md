# Continuity catalog

This internal package stores one library's asset identities, immutable revision
declarations, selected heads, and ordered metadata operations in a dedicated
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

Format 1 uses application ID `0x4a4d4341`, user version 1, an exact schema-object
inventory, and a migration ledger containing its SHA-256 fingerprint. A ledger
alone cannot validate changed DDL. Unknown, foreign, newer, incomplete, corrupt,
or constraint-invalid databases are refused without repair or replacement.
Open performs read-only format/integrity preflight before obtaining a write pool
and enabling WAL. SQLite integrity and foreign-key checks run at admission;
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
ListAssets returns one statement's snapshot of all returned mutable fields.
`Close` refuses new calls, drains admitted calls, and closes its database and
held file identity. There are no background workers. Future consistent exports
must use a separate read-only pool with deferred snapshot transactions and the
same lifecycle draining; the immediate write pool is not an export handle.

`Apply` accepts an explicit library ID, version 1, an operation ID, nonempty Actor,
and 1–256 ordered changes. Exactly one pointer variant is required per change:

- `CreateAsset`: new explicit identity and bounded display label.
- `RenameAsset`: compare the exact old label and write a new display label.
- `RecordRevision`: verify C01 canonical identity and already-present same-asset
  parents; existing immutable declarations may be repeated.
- `AdvanceHead`: SQL compare-and-swap from the explicit prior head. A first head
  must have no parents; later heads must list the expected head as a direct parent.

Changes and one event commit in a single immediate transaction. A failed change
rolls back the entire batch without reserving an operation ID or event sequence.
Already-recorded branches survive a separate head conflict. Callers must retain
their media independently. SQL foreign keys/constraints preserve ownership and
chronology; immutable-row triggers refuse revision, parent-edge, and event
updates/deletes. There is no asset/revision deletion API.

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
and `Events` return detached records for trusted internal services. Every record
request explicitly binds the library. Pages are 1–1000 records with exclusive
asset-ID, revision-ID, or event-sequence cursors; empty identity/zero sequence
starts at the beginning. Valid identity keys need not name an existing row.
Pages are independently current, not a multi-page snapshot. Typed error categories
distinguish invalid input, missing records, conflicts, changed retries, closed
stores, and invalid/corrupt format; underlying errors remain available for inspection.

Stored Actor strings are caller-supplied attribution, not authenticated identity
or evidence of human activity. Catalog membership/history checks grant no access
and prove no immutable source snapshot or media durability. Permissions/disclosure
belong to the trusted service boundary. Relationships, location observations, and
portable export/restore require subsequent format-compatible implementation;
they are not implemented by these four change kinds.
