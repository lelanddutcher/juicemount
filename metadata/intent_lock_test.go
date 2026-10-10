package metadata

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func intentLockStore(t *testing.T) *Store {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func intentSQLCount(t *testing.T, s *Store, table, name string) int {
	t.Helper()
	if table != "prepared_creates" && table != "online_mutations" {
		t.Fatalf("unexpected intent table %q", table)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE path=?`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIntentPreparedGateOrdersCacheOnlySuccessor(t *testing.T) {
	for _, operation := range []string{"prepare", "abort", "complete"} {
		t.Run(operation, func(t *testing.T) {
			s := intentLockStore(t)
			stamp := time.Unix(1700000000, 0)
			const name = "target.mov"
			expected, err := s.CaptureMirror(name)
			if err != nil {
				t.Fatal(err)
			}
			var prepared PreparedCreate
			if operation != "prepare" {
				prepared, err = s.PrepareCreate(expected, "mount-17")
				if err != nil {
					t.Fatal(err)
				}
			}
			generation := s.cacheGeneration
			release, baseline := holdPublicationSQLPool(t, s)
			type outcome struct {
				prepared PreparedCreate
				entry    *Entry
				err      error
			}
			done := make(chan outcome, 1)
			settled := false
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() {
				var r outcome
				switch operation {
				case "prepare":
					r.prepared, r.err = s.PrepareCreate(expected, "mount-17")
				case "abort":
					r.err = s.AbortPreparedCreate(prepared)
				case "complete":
					r.entry, r.err = s.CompletePreparedCreate(prepared, MakeEntry(name, false, 42, stamp, 100))
				}
				done <- r
			}()
			defer func() {
				release()
				if !settled {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("prepared operation did not settle")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("cache-only successor did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			if !s.mu.TryLock() {
				t.Error("prepared operation held cache lock during SQL")
			} else {
				s.mu.Unlock()
			}
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				t.Fatal("prepared operation released publication gate during SQL")
			}
			go func() {
				s.InsertToCache(MakeEntry(name, false, 77, stamp.Add(time.Second), 202))
				successorDone <- struct{}{}
			}()
			successorStarted = true
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("cache-only successor overtook held prepared operation")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			var result outcome
			select {
			case result = <-done:
				settled = true
			case <-time.After(5 * time.Second):
				t.Fatal("prepared operation did not finish")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("cache-only successor did not finish")
			}
			wantGeneration := generation + 2
			if operation == "prepare" {
				wantGeneration-- // the overlapping cache-only insert must be refused
			}
			if s.cacheGeneration != wantGeneration {
				t.Fatalf("generation=%d want=%d", s.cacheGeneration, wantGeneration)
			}
			wantCustody := 0
			if operation == "prepare" {
				wantCustody = 1
				if !validPreparedCreate(result.prepared) || s.preparedCreates[name] != result.prepared {
					t.Fatalf("prepared custody not published: %+v", result.prepared)
				}
			} else if _, exists := s.preparedCreates[name]; exists {
				t.Fatal("successful retirement left RAM reservation")
			}
			if n := intentSQLCount(t, s, "prepared_creates", name); n != wantCustody {
				t.Fatalf("durable prepared custody=%d want=%d", n, wantCustody)
			}
			cached := s.lookupByPathRAM(name)
			durable := mirrorDurableEntry(t, s, name)
			if operation == "prepare" {
				if cached != nil || durable != nil {
					t.Fatalf("reserved path was published: cache=%+v SQL=%+v", cached, durable)
				}
				return
			}
			if cached == nil || cached.Inode != 202 || cached.Size != 77 || !cached.Version.IsZero() {
				t.Fatalf("queued cache-only successor lost: %+v", cached)
			}
			if operation == "abort" {
				if durable != nil {
					t.Fatalf("definitive abort published durable entry: %+v", durable)
				}
			} else {
				if result.entry == nil || durable == nil || result.entry.Inode != 100 || durable.Inode != 100 || durable.Size != 42 || durable.Version != result.entry.Version {
					t.Fatalf("committed completion mismatch: result=%+v SQL=%+v", result.entry, durable)
				}
				if alias := s.lookupByInodeRAM(100); alias == nil || alias.Path != name || alias.Inode != 202 || alias.Size != 77 || alias.Version != cached.Version {
					t.Fatalf("old handle alias not redirected: %+v", alias)
				}
			}
		})
	}
}

func TestIntentOnlineGatePreservesCustodyAndSuccessor(t *testing.T) {
	for _, operation := range []string{"begin", "complete"} {
		t.Run(operation, func(t *testing.T) {
			s := intentLockStore(t)
			stamp := time.Unix(1700000000, 0)
			const name = "target.mov"
			if err := s.Insert(MakeEntry(name, false, 42, stamp, 100)); err != nil {
				t.Fatal(err)
			}
			expected, err := s.CaptureMirror(name)
			if err != nil {
				t.Fatal(err)
			}
			proposal := onlineTestMutation(name, 42, 84)
			var mutation OnlineMutation
			if operation == "complete" {
				mutation, err = s.BeginOnlineMutation(expected, proposal)
				if err != nil {
					t.Fatal(err)
				}
			}
			generation := s.cacheGeneration
			release, baseline := holdPublicationSQLPool(t, s)
			type outcome struct {
				mutation OnlineMutation
				entry    *Entry
				err      error
			}
			done := make(chan outcome, 1)
			settled := false
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() {
				var r outcome
				if operation == "begin" {
					r.mutation, r.err = s.BeginOnlineMutation(expected, proposal)
				} else {
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
						t.Error("online operation did not settle")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("cache-only successor did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			if !s.mu.TryLock() {
				t.Error("online operation held cache lock during SQL")
			} else {
				s.mu.Unlock()
			}
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				t.Fatal("online operation released publication gate during SQL")
			}
			go func() {
				if operation == "begin" {
					s.InsertToCache(MakeEntry(name, false, 77, stamp.Add(time.Second), 202))
				} else {
					s.DeleteFromCache(name)
				}
				successorDone <- struct{}{}
			}()
			successorStarted = true
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("cache-only successor overtook held online operation")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			var result outcome
			select {
			case result = <-done:
				settled = true
			case <-time.After(5 * time.Second):
				t.Fatal("online operation did not finish")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("cache-only successor did not finish")
			}
			if s.cacheGeneration != generation+1 {
				t.Fatalf("online intent changed generation beyond cache-only successor: %d -> %d", generation, s.cacheGeneration)
			}
			cached := s.lookupByPathRAM(name)
			durable := mirrorDurableEntry(t, s, name)
			if operation == "begin" {
				if !validOnlineMutationResult(result.mutation) || intentSQLCount(t, s, "online_mutations", name) != 1 {
					t.Fatalf("begin did not retain durable custody: %+v", result.mutation)
				}
				if cached == nil || cached.Inode != 202 || cached.Size != 77 || !cached.Version.IsZero() || durable == nil || durable.Inode != 100 || durable.Size != 42 || durable.Version != expected {
					t.Fatalf("begin/successor mismatch: cache=%+v SQL=%+v", cached, durable)
				}
				if _, err := s.CompleteOnlineMutation(result.mutation, stamp.Add(time.Second)); !errors.Is(err, ErrMirrorConflict) || intentSQLCount(t, s, "online_mutations", name) != 1 {
					t.Fatalf("stale cache allowed completion or lost custody: %v", err)
				}
			} else {
				if result.entry == nil || result.entry.Inode != 100 || result.entry.Size != 84 || cached != nil || s.lookupByInodeRAM(100) != nil || intentSQLCount(t, s, "online_mutations", name) != 0 || durable == nil || durable.Size != 84 || durable.Version != result.entry.Version {
					t.Fatalf("completed online mutation/successor mismatch: result=%+v cache=%+v SQL=%+v", result.entry, cached, durable)
				}
			}
		})
	}
}

func validOnlineMutationResult(m OnlineMutation) bool { return validOnlineMutation(m, true) == nil }

func TestIntentClosedDBPreservesAdmissionPrecedence(t *testing.T) {
	s := intentLockStore(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("present.mov", false, 42, stamp, 100)); err != nil {
		t.Fatal(err)
	}
	present, err := s.CaptureMirror("present.mov")
	if err != nil {
		t.Fatal(err)
	}
	absent, err := s.CaptureMirror("target.mov")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := s.PrepareCreate(absent, "mount-17")
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := s.BeginOnlineMutation(present, onlineTestMutation("present.mov", 42, 84))
	if err != nil {
		t.Fatal(err)
	}
	// The closed DB makes transaction setup fail. Admission conflicts still win
	// where the original method checked them before setup.
	s.InsertToCache(MakeEntry("present.mov", false, 77, stamp, 202))
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	closed := func(err error) bool {
		return err != nil && !errors.Is(err, ErrMirrorConflict) && strings.Contains(strings.ToLower(err.Error()), "closed")
	}
	wrong := prepared
	wrong.OpID = strings.Repeat("0", 32)
	if err := s.AbortPreparedCreate(wrong); !closed(err) {
		t.Fatalf("Abort invalid reservation bypassed setup failure: %v", err)
	}
	cachedPath := prepared
	cachedPath.Path = present.Path
	if _, err := s.CompletePreparedCreate(cachedPath, MakeEntry(cachedPath.Path, false, 1, stamp, 304)); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("Complete cached-path conflict lost precedence: %v", err)
	}
	if _, err := s.CompletePreparedCreate(wrong, MakeEntry(wrong.Path, false, 1, stamp, 304)); !closed(err) {
		t.Fatalf("Complete mismatched reservation bypassed setup failure: %v", err)
	}
	if _, err := s.BeginOnlineMutation(present, onlineTestMutation(present.Path, 42, 84)); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("Begin cached-token conflict lost precedence: %v", err)
	}
	if _, err := s.CompleteOnlineMutation(mutation, stamp.Add(time.Second)); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("Complete online cached-token conflict lost precedence: %v", err)
	}
	if _, err := s.PrepareCreate(MirrorToken{}, ""); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("Prepare input check lost precedence: %v", err)
	}
	if _, err := s.PrepareCreate(absent, "mount-17"); !closed(err) {
		t.Fatalf("Prepare SQL setup failure not observed: %v", err)
	}
}

func TestIntentTransactionalFaultRetainsStateAndAllowsRetry(t *testing.T) {
	for _, operation := range []string{"prepare-insert", "abort-delete", "complete-create-insert", "begin-insert", "complete-online-delete"} {
		t.Run(operation, func(t *testing.T) {
			s := intentLockStore(t)
			stamp := time.Unix(1700000000, 0)
			const name = "target.mov"
			var expected MirrorToken
			var prepared PreparedCreate
			var mutation OnlineMutation
			var err error
			if strings.HasPrefix(operation, "begin-") || operation == "complete-online-delete" {
				if err := s.Insert(MakeEntry(name, false, 42, stamp, 100)); err != nil {
					t.Fatal(err)
				}
			}
			expected, err = s.CaptureMirror(name)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "abort-delete" || operation == "complete-create-insert" {
				prepared, err = s.PrepareCreate(expected, "mount-17")
			} else if operation == "complete-online-delete" {
				mutation, err = s.BeginOnlineMutation(expected, onlineTestMutation(name, 42, 84))
			}
			if err != nil {
				t.Fatal(err)
			}
			generation := s.cacheGeneration
			beforeRAM := s.lookupByPathRAM(name)
			beforeSQL := mirrorDurableEntry(t, s, name)
			beforePrepared := intentSQLCount(t, s, "prepared_creates", name)
			beforeOnline := intentSQLCount(t, s, "online_mutations", name)
			trigger := ""
			switch operation {
			case "prepare-insert":
				trigger = `CREATE TRIGGER fail_l8 BEFORE INSERT ON prepared_creates BEGIN SELECT RAISE(FAIL,'fault'); END`
			case "abort-delete":
				trigger = `CREATE TRIGGER fail_l8 BEFORE DELETE ON prepared_creates BEGIN SELECT RAISE(FAIL,'fault'); END`
			case "complete-create-insert":
				trigger = `CREATE TRIGGER fail_l8 BEFORE INSERT ON entries BEGIN SELECT RAISE(FAIL,'fault'); END`
			case "begin-insert":
				trigger = `CREATE TRIGGER fail_l8 BEFORE INSERT ON online_mutations BEGIN SELECT RAISE(FAIL,'fault'); END`
			case "complete-online-delete":
				trigger = `CREATE TRIGGER fail_l8 BEFORE DELETE ON online_mutations BEGIN SELECT RAISE(FAIL,'fault'); END`
			}
			if _, err := s.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			attempt := func() error {
				switch operation {
				case "prepare-insert":
					_, err := s.PrepareCreate(expected, "mount-17")
					return err
				case "abort-delete":
					return s.AbortPreparedCreate(prepared)
				case "complete-create-insert":
					_, err := s.CompletePreparedCreate(prepared, MakeEntry(name, false, 42, stamp, 100))
					return err
				case "begin-insert":
					_, err := s.BeginOnlineMutation(expected, onlineTestMutation(name, 42, 84))
					return err
				default:
					_, err := s.CompleteOnlineMutation(mutation, stamp.Add(time.Second))
					return err
				}
			}
			if err := attempt(); err == nil {
				t.Fatal("injected transactional fault was acknowledged")
			}
			if s.cacheGeneration != generation || intentSQLCount(t, s, "prepared_creates", name) != beforePrepared || intentSQLCount(t, s, "online_mutations", name) != beforeOnline {
				t.Fatal("failed transaction changed generation or durable custody")
			}
			afterRAM := s.lookupByPathRAM(name)
			afterSQL := mirrorDurableEntry(t, s, name)
			if !sameIntentEntry(beforeRAM, afterRAM) || !sameIntentEntry(beforeSQL, afterSQL) {
				t.Fatalf("failed transaction changed mirror: RAM %+v -> %+v, SQL %+v -> %+v", beforeRAM, afterRAM, beforeSQL, afterSQL)
			}
			if operation == "abort-delete" || operation == "complete-create-insert" {
				if got := s.preparedCreates[name]; got != prepared {
					t.Fatalf("failed operation lost RAM custody: %+v", got)
				}
			}
			for _, lock := range []struct {
				name   string
				try    func() bool
				unlock func()
			}{
				{"writer", s.writeMu.TryLock, s.writeMu.Unlock},
				{"publication", s.cachePublicationMu.TryLock, s.cachePublicationMu.Unlock},
				{"cache", s.mu.TryLock, s.mu.Unlock},
			} {
				if !lock.try() {
					t.Fatalf("%s lock retained after failure", lock.name)
				}
				lock.unlock()
			}
			if _, err := s.db.Exec(`DROP TRIGGER fail_l8`); err != nil {
				t.Fatal(err)
			}
			if err := attempt(); err != nil {
				t.Fatalf("unchanged observation could not retry: %v", err)
			}
			switch operation {
			case "prepare-insert":
				if intentSQLCount(t, s, "prepared_creates", name) != 1 || len(s.preparedCreates) != 1 || s.cacheGeneration != generation+1 || mirrorDurableEntry(t, s, name) != nil {
					t.Fatal("retried Prepare did not commit only custody")
				}
			case "abort-delete":
				if intentSQLCount(t, s, "prepared_creates", name) != 0 || len(s.preparedCreates) != 0 || s.cacheGeneration != generation+1 {
					t.Fatal("retried Abort did not retire custody")
				}
			case "complete-create-insert":
				if durable := mirrorDurableEntry(t, s, name); durable == nil || durable.Inode != 100 || durable.Size != 42 || intentSQLCount(t, s, "prepared_creates", name) != 0 || s.cacheGeneration != generation+1 {
					t.Fatalf("retried CompleteCreate did not commit entry: %+v", durable)
				}
			case "begin-insert":
				if intentSQLCount(t, s, "online_mutations", name) != 1 || s.cacheGeneration != generation {
					t.Fatal("retried Begin did not commit custody")
				}
			case "complete-online-delete":
				if durable := mirrorDurableEntry(t, s, name); durable == nil || durable.Size != 84 || durable.Version.Revision != expected.Revision+1 || intentSQLCount(t, s, "online_mutations", name) != 0 || s.cacheGeneration != generation {
					t.Fatalf("retried CompleteOnline did not commit entry and retire custody: %+v", durable)
				}
			}
		})
	}
}

func sameIntentEntry(a, b *Entry) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Path == b.Path && a.Inode == b.Inode && a.IsDir == b.IsDir && a.Size == b.Size && a.Version == b.Version
}
