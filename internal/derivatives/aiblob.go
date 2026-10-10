package derivatives

import (
	"golang.org/x/sys/unix"
	"os"
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

// NO PATH-FORM HELPERS LIVE HERE ANY MORE.
//
// fileExists / AIBlobReadPaths / AIBlobWriteName / ResolveAIBlobName all took a
// joined directory path and os.Stat'd through it, so an ancestor symlink steered
// which filename got chosen — and one of them decided where the AI blob is
// WRITTEN. They were dead in production once the descriptor form landed, and a
// dead unsafe helper is just a trap for the next caller: this package has twice
// regrown the same bug from exactly that shape (openguard.go, DerivBlobDir).
//
// AIBlobWriteNameAt resolves through a DIRECTORY DESCRIPTOR instead of a path. The path form stat'ed through a consumer-swappable
// <inode> component, so the very choice of filename could be steered from
// outside the volume.
func AIBlobWriteNameAt(dir *os.File) string {
	if existsAt(dir, AIBlobNameLegacy) && !existsAt(dir, AIBlobName) {
		return AIBlobNameLegacy
	}
	return AIBlobName
}

func existsAt(dir *os.File, name string) bool {
	var st unix.Stat_t
	return unix.Fstatat(int(dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW) == nil &&
		st.Mode&unix.S_IFMT == unix.S_IFREG
}
