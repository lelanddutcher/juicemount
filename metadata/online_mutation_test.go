package metadata

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func onlineTestMutation(path string, old, target int64) OnlineMutation {
	return OnlineMutation{
		Kind: "truncate", Path: path, BackingIdentity: "dev=7,inode=91",
		MountGeneration: "mount-17", OldEOF: old, TargetEOF: target,
	}
}

func onlineTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func onlineTestSeed(t *testing.T, s *Store) MirrorToken {
	t.Helper()
	if err := s.Insert(MakeEntry("reel.mov", false, 8192, time.Now(), 91)); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror("reel.mov")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func requirePendingOnline(t *testing.T, s *Store, want OnlineMutation) {
	t.Helper()
	var pending *PendingOnlineMutation
	if err := s.CheckOnlineMutationRecovery(); !errors.As(err, &pending) {
		t.Fatalf("recovery check=%v, want typed pending", err)
	}
	if len(pending.Mutations) != 1 || pending.Mutations[0] != want {
		t.Fatalf("pending=%+v, want %+v", pending.Mutations, want)
	}
}

func TestOnlineMutationFullTransactionAndCompletion(t *testing.T) {
	for _, flag := range []string{"0", "1"} {
		for _, target := range []int64{0, 2048} {
			t.Run(fmt.Sprintf("normal_all=%s/target=%d", flag, target), func(t *testing.T) {
				t.Setenv("JM_SQLITE_SYNC_NORMAL_ALL", flag)
				file := filepath.Join(t.TempDir(), "mirror.db")
				s, err := Open(file)
				if err != nil {
					t.Fatal(err)
				}
				expected := onlineTestSeed(t, s)
				tx, closeConn, err := s.beginOnlineMutationTx()
				if err != nil {
					t.Fatal(err)
				}
				var mode int
				if err := tx.QueryRow(`PRAGMA synchronous`).Scan(&mode); err != nil {
					t.Fatal(err)
				}
				_ = tx.Rollback()
				closeConn()
				if mode != 2 {
					t.Fatalf("actual transaction connection synchronous=%d, want FULL(2)", mode)
				}
				m, err := s.BeginOnlineMutation(expected, onlineTestMutation("reel.mov", 8192, target))
				if err != nil {
					t.Fatal(err)
				}
				if m.OpID == "" || m.StoreID == "" || m.Incarnation != expected.Incarnation || m.Revision != expected.Revision {
					t.Fatalf("incomplete durable identity: %+v", m)
				}
				requirePendingOnline(t, s, m)
				got, err := s.CompleteOnlineMutation(m, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if got.Size != target || got.Version.Revision != expected.Revision+1 {
					t.Fatalf("committed row=%+v", got)
				}
				if err := s.CheckOnlineMutationRecovery(); err != nil {
					t.Fatalf("intent survived completion: %v", err)
				}
				if durable := s.lookupByPathSQLite("reel.mov"); durable.Size != target || durable.Version != got.Version {
					t.Fatalf("RAM/SQLite mismatch: RAM=%+v SQLite=%+v", got, durable)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(file)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if durable := reopened.LookupByPath("reel.mov"); durable == nil || durable.Size != target || durable.Version != got.Version {
					t.Fatalf("reopened row=%+v, want %+v", durable, got)
				}
			})
		}
	}
}

func TestOnlineMutationRejectsStaleTokenAndOverlappingScope(t *testing.T) {
	s := onlineTestStore(t)
	v := onlineTestSeed(t, s)
	stale := v
	stale.Revision++
	if _, err := s.BeginOnlineMutation(stale, onlineTestMutation("reel.mov", 8192, 0)); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale Begin=%v", err)
	}
	m, err := s.BeginOnlineMutation(v, onlineTestMutation("reel.mov", 8192, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginOnlineMutation(v, onlineTestMutation("reel.mov", 8192, 1)); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("overlapping Begin=%v", err)
	}
	requirePendingOnline(t, s, m)
}

func TestOnlineMutationAncestorAndDescendantExclusion(t *testing.T) {
	for _, first := range []string{"scene", "scene/reel.mov"} {
		t.Run(first, func(t *testing.T) {
			s := onlineTestStore(t)
			paths := []string{"scene", "scene/reel.mov"}
			versions := make(map[string]MirrorToken)
			for i, name := range paths {
				if err := s.Insert(MakeEntry(name, false, 8192, time.Now(), uint64(i+91))); err != nil {
					t.Fatal(err)
				}
				var err error
				versions[name], err = s.CaptureMirror(name)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.BeginOnlineMutation(versions[first], onlineTestMutation(first, 8192, 0)); err != nil {
				t.Fatal(err)
			}
			second := paths[0]
			if second == first {
				second = paths[1]
			}
			if _, err := s.BeginOnlineMutation(versions[second], onlineTestMutation(second, 8192, 0)); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("pending %q allowed overlapping %q: %v", first, second, err)
			}
		})
	}
}

func TestOnlineMutationRefusesVolatileStore(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v := onlineTestSeed(t, s)
	if _, err := s.BeginOnlineMutation(v, onlineTestMutation("reel.mov", 8192, 0)); err == nil || !strings.Contains(err.Error(), "WAL") {
		t.Fatalf("volatile journal accepted: %v", err)
	}
}

func TestOnlineMutationRejectsNoncanonicalPath(t *testing.T) {
	s := onlineTestStore(t)
	v := onlineTestSeed(t, s)
	for _, path := range []string{"reel.mov/", "reel.mov//child", "reel.mov/../reel.mov", "../reel.mov", "/reel.mov"} {
		m := onlineTestMutation(path, 8192, 0)
		expected := v
		expected.Path = path
		if _, err := s.BeginOnlineMutation(expected, m); !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("path %q accepted: %v", path, err)
		}
	}
}

func TestOnlineMutationIgnoredInsertDoesNotAuthorizeBackingMutation(t *testing.T) {
	s := onlineTestStore(t)
	v := onlineTestSeed(t, s)
	if _, err := s.DB().Exec(`CREATE TRIGGER ignore_online_intent BEFORE INSERT ON online_mutations BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginOnlineMutation(v, onlineTestMutation("reel.mov", 8192, 0)); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("ignored INSERT authorized backing mutation: %v", err)
	}
	if err := s.CheckOnlineMutationRecovery(); err != nil {
		t.Fatalf("ignored INSERT unexpectedly committed an intent: %v", err)
	}
}

func TestOnlineMutationCompletionFailuresRetainIntentAndMirror(t *testing.T) {
	for _, fault := range []string{"wrong_op", "wrong_token", "zero_row", "trigger", "delete_trigger", "busy"} {
		t.Run(fault, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "mirror.db")
			s, err := Open(file)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			v := onlineTestSeed(t, s)
			m, err := s.BeginOnlineMutation(v, onlineTestMutation("reel.mov", 8192, 0))
			if err != nil {
				t.Fatal(err)
			}
			attempt := m
			var other *sql.DB
			var lock *sql.Tx
			switch fault {
			case "wrong_op":
				attempt.OpID = strings.Repeat("0", 32)
			case "wrong_token":
				attempt.Revision++
			case "zero_row":
				_, err = s.DB().Exec(`CREATE TRIGGER ignore_online_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(IGNORE); END`)
			case "trigger":
				_, err = s.DB().Exec(`CREATE TRIGGER fail_online_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL,'online publication fault'); END`)
			case "delete_trigger":
				_, err = s.DB().Exec(`CREATE TRIGGER fail_online_delete BEFORE DELETE ON online_mutations BEGIN SELECT RAISE(FAIL,'online delete fault'); END`)
			case "busy":
				s.DB().SetMaxOpenConns(1)
				conn, connErr := s.DB().Conn(t.Context())
				if connErr != nil {
					t.Fatal(connErr)
				}
				_, err = conn.ExecContext(t.Context(), `PRAGMA busy_timeout=1`)
				conn.Close()
				if err == nil {
					other, err = sql.Open("sqlite", file+"?_txlock=immediate")
				}
				if err == nil {
					lock, err = other.Begin()
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if lock != nil {
				defer lock.Rollback()
			}
			if other != nil {
				defer other.Close()
			}
			_, completionErr := s.CompleteOnlineMutation(attempt, time.Now())
			if completionErr == nil {
				t.Fatal("faulty completion acknowledged")
			}
			if fault == "busy" && !strings.Contains(strings.ToLower(completionErr.Error()), "locked") && !strings.Contains(strings.ToLower(completionErr.Error()), "busy") {
				t.Fatalf("busy injection failed for unrelated reason: %v", completionErr)
			}
			if lock != nil {
				_ = lock.Rollback()
			}
			requirePendingOnline(t, s, m)
			if got := s.LookupByPath("reel.mov"); got.Size != 8192 || got.Version != v {
				t.Fatalf("RAM changed: %+v", got)
			}
			if got := s.lookupByPathSQLite("reel.mov"); got.Size != 8192 || got.Version != v {
				t.Fatalf("SQLite changed: %+v", got)
			}
		})
	}
}

func TestOnlineMutationCrashReopen(t *testing.T) {
	const childEnv = "JM_TEST_ONLINE_CRASH_CHILD"
	if file := os.Getenv(childEnv); file != "" {
		s, err := Open(file)
		if err != nil {
			t.Fatal(err)
		}
		v := onlineTestSeed(t, s)
		m, err := s.BeginOnlineMutation(v, onlineTestMutation("reel.mov", 8192, 0))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("ONLINE_INTENT_COMMITTED:%s:%s\n", m.OpID, m.Path)
		select {}
	}
	file := filepath.Join(t.TempDir(), "mirror.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestOnlineMutationCrashReopen$")
	cmd.Env = append(os.Environ(), childEnv+"="+file)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	var committed string
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "ONLINE_INTENT_COMMITTED:") {
			committed = strings.TrimPrefix(scanner.Text(), "ONLINE_INTENT_COMMITTED:")
			break
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if committed == "" {
		t.Fatal("child exited without committing an intent")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	_, err = Open(file)
	var pending *PendingOnlineMutation
	if !errors.As(err, &pending) || len(pending.Mutations) != 1 {
		t.Fatalf("post-SIGKILL Open=%v, pending=%+v", err, pending)
	}
	if got := pending.Mutations[0].OpID + ":" + pending.Mutations[0].Path; got != committed {
		t.Fatalf("reopened intent %q, child committed %q", got, committed)
	}
	inspected, err := InspectOnlineMutationsAt(file)
	if err != nil || len(inspected) != 1 || inspected[0] != pending.Mutations[0] {
		t.Fatalf("offline inspection=%+v, err=%v", inspected, err)
	}
}

func TestOnlineMutationMalformedAndQueryErrorRefuse(t *testing.T) {
	file := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`INSERT INTO online_mutations VALUES ('bad','truncate','reel.mov','store','inc','not-an-int','backing','mount',8192,0,0,0)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckOnlineMutationRecovery(); err == nil {
		t.Fatal("malformed row accepted")
	}
	_ = s.Close()
	if reopened, err := Open(file); err == nil {
		_ = reopened.Close()
		t.Fatal("Open accepted malformed row")
	}
	s, err = Open(filepath.Join(t.TempDir(), "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`DROP TABLE online_mutations`); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckOnlineMutationRecovery(); err == nil {
		t.Fatal("journal query error accepted")
	}
}
