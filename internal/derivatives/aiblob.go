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
const (
	AIBlobName       = "ai.logger.json"
	AIBlobNameLegacy = "ai.loupe.json"
)

// ResolveAIBlobName returns the AI blob filename to USE for the derivative
// directory dir, preferring the post-cutover name.
//
// Order: the new name if that file exists; else the legacy name if THAT file
// exists; else the new name (nothing on disk yet — a fresh write gets the new
// name). Callers that are about to create a blob can use the return value
// directly; callers reading an existing blob get whichever one is actually
// there.
func ResolveAIBlobName(dir string) string {
	if _, err := os.Stat(filepath.Join(dir, AIBlobName)); err == nil {
		return AIBlobName
	}
	if _, err := os.Stat(filepath.Join(dir, AIBlobNameLegacy)); err == nil {
		return AIBlobNameLegacy
	}
	return AIBlobName
}
