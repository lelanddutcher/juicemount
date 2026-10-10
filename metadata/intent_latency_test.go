package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestIntentSQLWaitKeepsCachedNavigationResponsive(t *testing.T) {
	for _, operation := range []string{"prepare-create", "abort-create", "complete-create", "begin-online", "complete-online"} {
		t.Run(operation, func(t *testing.T) {
			t.Setenv("JM_FTS_DEFER", "0")
			t.Setenv("JM_SERVE_FROM_SQLITE", "0")
			t.Setenv("JM_READDIR_PAGINATED", "0")
			s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			stamp := time.Unix(1700000000, 0)
			if err := s.BulkInsert([]*Entry{
				MakeEntry("known", true, 0, stamp, 10),
				MakeEntry("known/clip.mov", false, 4096, stamp, 11),
			}, 500); err != nil {
				t.Fatal(err)
			}
			input := MakeEntry("target.mov", false, 42, stamp, 100)
			var expected MirrorToken
			var prepared PreparedCreate
			mutation := onlineTestMutation(input.Path, 42, 84)
			if operation == "begin-online" || operation == "complete-online" {
				if err := s.Insert(input); err != nil {
					t.Fatal(err)
				}
			}
			expected, err = s.CaptureMirror(input.Path)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "abort-create" || operation == "complete-create" {
				prepared, err = s.PrepareCreate(expected, "mount-test")
			} else if operation == "complete-online" {
				mutation, err = s.BeginOnlineMutation(expected, mutation)
			}
			if err != nil {
				t.Fatal(err)
			}
			generation := s.cacheGeneration
			release, baseline := holdPublicationSQLPool(t, s)
			type outcome struct {
				prepared PreparedCreate
				mutation OnlineMutation
				entry    *Entry
				err      error
			}
			done := make(chan outcome, 1)
			settled := false
			go func() {
				var r outcome
				switch operation {
				case "prepare-create":
					r.prepared, r.err = s.PrepareCreate(expected, "mount-test")
				case "abort-create":
					r.err = s.AbortPreparedCreate(prepared)
				case "complete-create":
					r.entry, r.err = s.CompletePreparedCreate(prepared, input)
				case "begin-online":
					r.mutation, r.err = s.BeginOnlineMutation(expected, mutation)
				case "complete-online":
					r.entry, r.err = s.CompleteOnlineMutation(mutation, stamp.Add(time.Second))
				}
				done <- r
			}()
			defer func() {
				release()
				if !settled {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("intent operation did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			// A held RLock can block new readers behind a queued cache writer.
			if s.mu.TryLock() {
				s.mu.Unlock()
			} else {
				t.Error("intent operation retained cache lock during SQL wait")
			}
			type navigation struct {
				path, inode *Entry
				children    []*Entry
				err         error
				elapsed     time.Duration
			}
			navDone := make(chan navigation, 1)
			go func() {
				start := time.Now()
				p := s.LookupByPath("known/clip.mov")
				i := s.LookupByInode(11)
				children, err := s.ListChildrenForReadDir("known")
				navDone <- navigation{p, i, children, err, time.Since(start)}
			}()
			var nav navigation
			select {
			case nav = <-navDone:
				t.Logf("cached navigation before SQL release: %s", nav.elapsed)
			case <-time.After(250 * time.Millisecond):
				t.Error("cached navigation waited >250ms behind durable intent SQL")
				release()
				select {
				case nav = <-navDone:
				case <-time.After(5 * time.Second):
					t.Fatal("cached reader did not settle after SQL release")
				}
			}
			if nav.err != nil || nav.path == nil || nav.path.Size != 4096 || nav.inode == nil || nav.inode.Path != "known/clip.mov" || len(nav.children) != 1 || nav.children[0].Inode != 11 {
				t.Fatalf("incorrect cached navigation: %+v", nav)
			}
			release()
			var result outcome
			select {
			case result = <-done:
				settled = true
			case <-time.After(5 * time.Second):
				t.Fatal("intent operation did not finish after SQL release")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			creates, err := s.InspectPreparedCreates()
			if err != nil {
				t.Fatal(err)
			}
			mutations, err := s.InspectOnlineMutations()
			if err != nil {
				t.Fatal(err)
			}
			wantGeneration := generation
			if operation == "prepare-create" || operation == "abort-create" || operation == "complete-create" {
				wantGeneration++
			}
			if s.cacheGeneration != wantGeneration {
				t.Fatalf("generation=%d want=%d", s.cacheGeneration, wantGeneration)
			}
			if operation == "prepare-create" {
				if len(creates) != 1 || creates[0] != result.prepared || s.preparedCreates[input.Path] != result.prepared || !validPreparedCreate(result.prepared) {
					t.Fatalf("prepared custody mismatch: result=%+v durable=%+v", result.prepared, creates)
				}
			} else if len(creates) != 0 || len(s.preparedCreates) != 0 {
				t.Fatalf("unexpected CREATE custody: %+v", creates)
			}
			if operation == "begin-online" {
				if len(mutations) != 1 || mutations[0] != result.mutation || validOnlineMutation(result.mutation, true) != nil {
					t.Fatalf("online custody mismatch: %+v / %+v", result.mutation, mutations)
				}
			} else if len(mutations) != 0 {
				t.Fatalf("unexpected online custody: %+v", mutations)
			}
			if operation == "prepare-create" || operation == "abort-create" {
				if s.lookupByPathRAM(input.Path) != nil || mirrorDurableEntry(t, s, input.Path) != nil {
					t.Fatal("unpublished CREATE exposed a row")
				}
				return
			}
			wantSize := int64(42)
			wantVersion := expected
			if operation == "complete-online" {
				wantSize = 84
				wantVersion.Revision++
			}
			if operation == "complete-create" {
				wantVersion = result.entry.Version
			}
			if operation != "begin-online" && (result.entry == nil || result.entry.Size != wantSize || result.entry.Version != wantVersion) {
				t.Fatalf("completed row=%+v", result.entry)
			}
			for _, entry := range []*Entry{s.lookupByPathRAM(input.Path), mirrorDurableEntry(t, s, input.Path)} {
				if entry == nil || entry.Inode != 100 || entry.Path != input.Path || entry.IsDir || entry.Size != wantSize || entry.Version.IsZero() || entry.Version != wantVersion {
					t.Fatalf("incorrect committed intent result: %+v (returned %+v)", entry, result.entry)
				}
			}
			if input.Size != 42 || !input.Version.IsZero() {
				t.Fatal("caller entry mutated")
			}

		})
	}
}
