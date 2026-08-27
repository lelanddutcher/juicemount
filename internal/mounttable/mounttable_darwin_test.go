//go:build darwin

package mounttable

import (
	"testing"
)

func TestDarwinMountTableRunsOutOfProcess(t *testing.T) {
	cmd := darwinMountCommand()
	if cmd.Path != "/sbin/mount" {
		t.Fatalf("mount-table helper path = %q, want /sbin/mount", cmd.Path)
	}
}
