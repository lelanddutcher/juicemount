# Contract bundled with the 0.5 checkpoint

This directory contains version 3 of the language-neutral control-plane schemas
and golden fixtures used by this JuiceMount source snapshot. The release source
commit identifies these exact bytes. Provider conformance tests read `spec/`,
`fixtures/`, and `VERSION` directly.

The bundle has not been reconciled to a clean canonical consumer-contract commit.
Passing its provider tests does not establish full compatibility with every
consumer. The final consumer workflow, including pin and unpin, remains under
qualification; see [checkpoint limits](../docs/release-0.5.md).

## Updating the bundle

Review schema and fixture changes together with the provider and consumer. Import
only `spec/`, `fixtures/`, and `VERSION` from a clean canonical revision, record
that revision here, and run provider and consumer conformance before claiming
compatibility. A historical version-2 pin does not identify these version-3 bytes.

The schemas are vendored so a public clone can build and run conformance tests
without access to a private repository or submodule.
