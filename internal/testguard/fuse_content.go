// Package testguard contains opt-in, fail-closed guards used only by
// acceptance tests. None of its switches are enabled in production by
// default.
package testguard

import (
	"errors"
	"os"
)

// RefuseFUSEContentReadsEnv enables the real-cellular acceptance guard. The
// value is intentionally strict: only the exact value "1" enables it.
const RefuseFUSEContentReadsEnv = "JM_TEST_REFUSE_FUSE_CONTENT_READS"

// ErrFUSEContentReadRefused identifies a content read rejected by the
// acceptance guard. It deliberately carries neither a path nor an endpoint.
var ErrFUSEContentReadRefused = errors.New("test-only FUSE content read refused")

// RefuseFUSEContentReads reports whether acceptance testing has selected a
// zero-byte remote-content budget. The check is made at the operation boundary
// so a test harness can enable the guard before starting a lifecycle without a
// package-global initialization race.
func RefuseFUSEContentReads() bool {
	return os.Getenv(RefuseFUSEContentReadsEnv) == "1"
}
