//go:build latency_acceptance

package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

// This deliberately holds the SQL connection, not Store.mu. It models a
// background batch waiting for local storage and asks whether unrelated cached
// navigation can still finish. Keep this gate separate from network impairment:
// making Redis fast cannot cure a local serving-lock convoy.
func TestNavigationDuringBackgroundSQLWait(t *testing.T) {
	for _, operation := range []string{"bulk", "scan-clear", "scan-delete", "rename", "publish-expected", "confirm-directory"} {
		t.Run(operation, func(t *testing.T) { navigationDuringSQLWait(t, operation) })
	}
}

func navigationDuringSQLWait(t *testing.T, operation string) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	if ServeFromSQLite() {
		t.Fatal("this oracle requires the default RAM serving mode; run with JM_SERVE_FROM_SQLITE=0")
	}
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stamp := time.Unix(1700000000, 0)
	if err := s.BulkInsert([]*Entry{
		MakeEntry("known", true, 0, stamp, 10),
		MakeEntry("known/clip.mov", false, 4096, stamp, 11),
	}, 500); err != nil {
		t.Fatal(err)
	}
	background := MakeEntry("background/old.mov", false, 42, stamp, 101)
	background.LocalOnly = true
	if err := s.Insert(background); err != nil {
		t.Fatal(err)
	}
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	source, destination, err := s.CaptureRename(background.Path, "unrelated/moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	publishInput := MakeEntry("unrelated/published.mov", false, 84, stamp, 9002)
	publishExpected, err := s.CaptureMirror(publishInput.Path)
	if err != nil {
		t.Fatal(err)
	}
	directoryInput := MakeEntry("unrelated/confirmed", true, 0, stamp, 9003)
	directoryExpected, directoryEpoch, err := s.CaptureDirectoryForFUSE(directoryInput.Path)
	if err != nil {
		t.Fatal(err)
	}
	// Join background SQL workers before recording the counter baseline, so
	// only the target operation can satisfy the admission barrier.
	release, waitCount := holdPublicationSQLPool(t, s)
	defer release()
	writerDone := make(chan error, 1)
	go func() {
		var err error
		switch operation {
		case "bulk":
			err = s.BulkInsert([]*Entry{MakeEntry("background/new.mov", false, 42, stamp, 100)}, 500)
		case "scan-clear":
			_, err = s.ScanClearLocalOnlyBatch([]string{background.Path}, epoch)
		case "scan-delete":
			_, err = s.ScanDeletePathsBatch([]string{background.Path}, epoch)
		case "rename":
			_, err = s.RenameExpected(source, destination)
		case "publish-expected":
			_, err = s.PublishExpected(publishExpected, publishInput)
		case "confirm-directory":
			_, err = s.ConfirmAbsentDirectoryFromFUSE(directoryExpected, directoryEpoch, directoryInput)
		default:
			panic("unknown SQL fixture operation")
		}
		writerDone <- err
	}()
	defer func() {
		release()
		select {
		case err := <-writerDone:
			if err != nil {
				t.Errorf("background writer: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("background writer did not settle after SQL release")
		}
	}()
	waitForPublicationSQLWait(t, s, waitCount)
	type result struct {
		path, inode *Entry
		children    []*Entry
		err         error
		elapsed     time.Duration
	}
	navDone := make(chan result, 1)
	go func() {
		start := time.Now()
		path := s.LookupByPath("known/clip.mov")
		inode := s.LookupByInode(11)
		children, err := s.ListChildrenForReadDir("known")
		navDone <- result{path, inode, children, err, time.Since(start)}
	}()
	var r result
	select {
	case r = <-navDone:
		t.Logf("cached path/inode/list completed while SQL held: %s", r.elapsed)
	case <-time.After(250 * time.Millisecond):
		t.Error("cached navigation blocked behind unrelated background SQL wait for >250ms")
		release()
		select {
		case r = <-navDone:
		case <-time.After(5 * time.Second):
			t.Fatal("navigation did not settle after SQL release")
		}
	}
	if r.err != nil || r.path == nil || r.inode == nil || len(r.children) != 1 || r.path.Size != 4096 || r.inode.Path != "known/clip.mov" || r.children[0].Path != "known/clip.mov" {
		t.Fatalf("invalid navigation result: %+v", r)
	}
}
