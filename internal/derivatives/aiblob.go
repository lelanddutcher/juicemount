package derivatives

import (
	"os"
	"path/filepath"
)

// The `ai` derivative blob under <nas_root>/.juicemount/derivatives/<inode>/.
//
// WIRE-TERM CUTOVER (2026-08-03). The coordinated `loupe` -> `logger` rename
// proposed in CONSUMER_STATUS 07-18 §2 was acked by the founder. We now WRITE
// AIBlobName and READ BOTH names, indefinitely — the legacy name is still
// produced by any pre-cutover consumer and is not scheduled for removal without
// a further coordinated step. contract/spec/schema/register.schema.json accepts
// either value for `blob_rel_path`.
//
// A provider that has completed this cutover advertises `wire_terms:"logger/1"`
// in GET /whoami (cplane.WireTerms).
//
// BOTH FILES CAN EXIST AT ONCE, and that is an EXPECTED transitional state, not
// an edge case: the register schema explicitly tells consumers to "prefer
// ai.logger.json once the provider advertises the flag", so a consumer that
// pushed on-device AI under the legacy name before the cutover and then writes
// under the new name after it leaves two blobs for the same inode, each holding
// sub-kinds (faces / embeddings / transcript) the other lacks. Anything that
// READS the blob must therefore merge across BOTH — see AIBlobReadPaths.
// Picking only one silently and permanently drops the other's sub-kinds, since
// the merged document is written back and the single (inode,"ai") manifest row
// is repointed at it.
const (
	AIBlobName       = "ai.logger.json"
	AIBlobNameLegacy = "ai.loupe.json"
)

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// AIBlobReadPaths returns every AI blob that exists in dir, in MERGE ORDER:
// legacy first, then post-cutover — so a caller merging them in order lets the
// NEW file win any sub-kind both define, while still picking up sub-kinds that
// only the legacy file has.
//
// Returns nil when neither exists. Callers must merge across ALL returned paths;
// reading only the first (or only the last) is the data-loss bug this function
// exists to prevent.
func AIBlobReadPaths(dir string) []string {
	var out []string
	if p := filepath.Join(dir, AIBlobNameLegacy); fileExists(p) {
		out = append(out, p)
	}
	if p := filepath.Join(dir, AIBlobName); fileExists(p) {
		out = append(out, p)
	}
	return out
}

// AIBlobWriteName returns the blob filename to WRITE for dir.
//
// When ONLY the legacy blob exists we keep writing to it, so a pre-cutover
// reader is not left staring at a stale file while the fresh content lands
// under a name it does not know.
//
// When BOTH exist, or neither does, we write the post-cutover name. In the
// both-exist case the union of the two is what gets written (see
// AIBlobReadPaths) and the manifest row is repointed at it, so no sub-kind is
// lost. The legacy blob is deliberately LEFT IN PLACE rather than deleted — we
// never delete a consumer's blob — and is re-merged idempotently on every
// subsequent pass.
func AIBlobWriteName(dir string) string {
	legacy := fileExists(filepath.Join(dir, AIBlobNameLegacy))
	current := fileExists(filepath.Join(dir, AIBlobName))
	if legacy && !current {
		return AIBlobNameLegacy
	}
	return AIBlobName
}

// ResolveAIBlobName returns the name of the AI blob to REFERENCE for dir,
// preferring the post-cutover name when both exist.
//
// This is the "which file are we talking about" helper for callers that only
// need a single name to record — e.g. POST /derivatives/register filling in a
// blob_rel_path the consumer omitted. It is NOT safe for the read-merge-write
// path: use AIBlobReadPaths there, because when both blobs exist this returns
// only one of them.
func ResolveAIBlobName(dir string) string {
	if fileExists(filepath.Join(dir, AIBlobName)) {
		return AIBlobName
	}
	if fileExists(filepath.Join(dir, AIBlobNameLegacy)) {
		return AIBlobNameLegacy
	}
	return AIBlobName
}
