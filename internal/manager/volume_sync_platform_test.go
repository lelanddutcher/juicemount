package manager

import (
	"runtime"
	"testing"
)

// skipUnlessVolumeSyncPlatform skips a test that runs a sync into the
// volume. The Manager refuses those unless juicefs is the confining -jm3
// build on Linux (juicefsConfinesFileRoots); the Manager ships only in
// Linux images, so on other platforms the refusal is the correct result.
func skipUnlessVolumeSyncPlatform(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("syncs into the volume need the confining juicefs, which runs on Linux only")
	}
}
