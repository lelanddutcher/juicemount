package mutationauthority

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Exact db.Exec DDL from each historical Open implementation, independent of
// the migration templates. SQLite's persisted DDL normalizes IF NOT EXISTS.
//
//go:embed testdata/*.sql
var historicalDDL embed.FS

func legacyFixture(t *testing.T) (string, *sql.DB) {
	t.Helper()
	return legacyFixtureFor(t, "6e81546")
}

func legacyFixtureFor(t *testing.T, revision string) (string, *sql.DB) {
	t.Helper()
	ddl, err := historicalDDL.ReadFile("testdata/" + revision + ".sql")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "authority.db")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(ddl)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO owners VALUES('volume','session',4,99,0)`); err != nil {
		t.Fatal(err)
	}
	if revision != "a030b94" {
		if _, err = db.Exec(`INSERT INTO backend_processes VALUES(7,'volume','worker7','run7')`); err != nil {
			t.Fatal(err)
		}
	}
	if revision == "6e81546" {
		if _, err = db.Exec(`INSERT INTO backend_configuration VALUES(1,'volume','binding7')`); err != nil {
			t.Fatal(err)
		}
	}
	for _, state := range []State{Prepared, Committed, NoEffect} {
		if _, err = db.Exec(`INSERT INTO operations VALUES('volume',?,'session',4,?,'predecessor',?,'revision','evidence',123)`, string(state), DigestBytes([]byte(state)), state); err != nil {
			t.Fatal(err)
		}
	}
	return path, db
}

func TestTypedMigrationPreservesLegacyState(t *testing.T) {
	path, db := legacyFixture(t)
	db.Close()
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var version, closed, generation int
	var session, worker, run, binding string
	if err = j.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != journalSchemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	if err = j.db.QueryRow(`SELECT session,admission_closed FROM owners`).Scan(&session, &closed); err != nil || session != "session" || closed != 1 {
		t.Fatal(session, closed, err)
	}
	if err = j.db.QueryRow(`SELECT generation,username,redis_run_id FROM backend_processes`).Scan(&generation, &worker, &run); err != nil || generation != 7 || worker != "worker7" || run != "run7" {
		t.Fatal(generation, worker, run, err)
	}
	if err = j.db.QueryRow(`SELECT digest FROM backend_configuration`).Scan(&binding); err != nil || binding != "binding7" {
		t.Fatal(binding, err)
	}
	ctx := context.Background()
	for _, state := range []State{Prepared, Committed, NoEffect} {
		o, err := j.Inspect(ctx, "volume", string(state))
		if err != nil || o.State != state || o.Kind != LegacyOperation || o.FileID != "" || o.ExpectedRevision != "predecessor" || o.Digest != DigestBytes([]byte(state)) {
			t.Fatalf("operation=%+v err=%v", o, err)
		}
		if state == Committed && (o.Receipt.Revision != "revision" || o.Receipt.Evidence != "evidence" || o.Receipt.CommittedAtUnixNS != 123) {
			t.Fatal(o.Receipt)
		}
		if state == NoEffect && o.ResolutionEvidence != "evidence" {
			t.Fatal(o)
		}
	}
	if _, err = j.Claim(ctx, "volume", "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	o, err := reopened.Inspect(ctx, "volume", string(Prepared))
	if err != nil || o.Kind != LegacyOperation || o.State != Prepared {
		t.Fatal(o, err)
	}
}

func TestTypedMigrationRejectsUnknownSchemaBeforeMutation(t *testing.T) {
	for _, change := range []string{`PRAGMA user_version=3`, `CREATE TABLE unrelated(value TEXT)`, `ALTER TABLE owners ADD COLUMN unknown TEXT`, `DROP INDEX operations_unsettled`, `DROP TABLE backend_processes`} {
		t.Run(change, func(t *testing.T) {
			path, db := legacyFixture(t)
			if _, err := db.Exec(change); err != nil {
				t.Fatal(err)
			}
			db.Close()
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			j, err := Open(path)
			if j != nil {
				j.Close()
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("unknown database modified", err)
			}
		})
	}
}

func TestTypedMigrationRollsBackInvalidLegacyRow(t *testing.T) {
	path, db := legacyFixture(t)
	if _, err := db.Exec(`UPDATE operations SET digest='invalid' WHERE id='PREPARED'`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if j, err := Open(path); j != nil || !errors.Is(err, ErrInvalid) {
		if j != nil {
			j.Close()
		}
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, closed int
	var ddl string
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 0 {
		t.Fatal(version, err)
	}
	if err = db.QueryRow(`SELECT sql FROM sqlite_master WHERE name='operations'`).Scan(&ddl); err != nil || strings.Contains(ddl, "file_id") || strings.Contains(ddl, "kind") {
		t.Fatal(ddl, err)
	}
	if err = db.QueryRow(`SELECT admission_closed FROM owners`).Scan(&closed); err != nil || closed != 0 {
		t.Fatal(closed, err)
	}
}

func TestTypedIdentityPersistsAndRejectsChangedRequest(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	r, intent := stageFixture(t, j)
	calls := 0
	backend := backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{}, errors.New("unconfirmed effect")
	})
	if err := j.ExecuteStage(ctx, r, intent, backend); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	o, err := j.Inspect(ctx, r.Volume, r.ID)
	if err != nil || !sameRequest(o.Request, r) {
		t.Fatal(o, err)
	}
	ops, err := j.ListPrepared(ctx, r.Volume, "", 4)
	if err != nil || len(ops) != 1 || !sameRequest(ops[0].Request, r) {
		t.Fatal(ops, err)
	}
	if err = j.ValidateRequests(ctx, func(got Request) bool { return sameRequest(got, r) }); err != nil {
		t.Fatal(err)
	}
	proof := DurabilityProof{Revision: "revision", Evidence: "exact effect evidence"}
	for _, mut := range []func(*Request){func(q *Request) { q.Kind = CustodyOperation; q.FileID = "" }, func(q *Request) { q.FileID = "incarnation_2" }, func(q *Request) { q.ExpectedRevision = "replacement" }} {
		q := r
		mut(&q)
		if _, err = j.Execute(ctx, q, backend); !errors.Is(err, ErrChangedRequest) {
			t.Fatal(err)
		}
		if _, err = j.ResolveCommitted(ctx, q, proof); !errors.Is(err, ErrChangedRequest) {
			t.Fatal(err)
		}
		if err = j.ResolveNoEffect(ctx, q, "no effect"); !errors.Is(err, ErrChangedRequest) {
			t.Fatal(err)
		}
	}
	if _, err := j.ResolveCommitted(ctx, r, proof); !errors.Is(err, ErrInvalid) {
		t.Fatal("generic media receipt issued", err)
	}
	if err := j.ResolveNoEffect(ctx, r, "generic evidence"); !errors.Is(err, ErrInvalid) {
		t.Fatal("generic media retirement admitted", err)
	}
	if _, err := j.Execute(ctx, r, backend); !errors.Is(err, ErrInvalid) || calls != 1 {
		t.Fatal(err, calls)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	o, err = j.Inspect(ctx, r.Volume, r.ID)
	if err != nil || o.Receipt != nil || o.State != Prepared || !sameRequest(o.Request, r) {
		t.Fatal(o, err)
	}
	q := r
	q.FileID = "new"
	if _, err = j.Execute(ctx, q, backend); !errors.Is(err, ErrChangedRequest) || calls != 1 {
		t.Fatal(err, calls)
	}
}

func TestTypedIdentityRefusesInvalidBeforeCallback(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	l, err := j.Claim(ctx, "v", "s", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, mut := range []func(*Request){func(q *Request) { q.Kind = "future" }, func(q *Request) { q.FileID = "" }, func(q *Request) { q.FileID = "public/path" }, func(q *Request) { q.ExpectedRevision = "" }, func(q *Request) { q.Kind = CustodyOperation }, func(q *Request) { q.Kind = LegacyOperation }} {
		r := testRequest(l, "invalid")
		r.Kind = MediaPublishOperation
		r.FileID = "file"
		mut(&r)
		if _, err = j.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
			t.Fatal("invalid identity admitted")
			return DurabilityProof{}, nil
		})); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	ops, err := j.ListPrepared(ctx, "v", "", 100)
	if err != nil || len(ops) != 0 {
		t.Fatal(ops, err)
	}
}

func TestCustodyClassificationAtomicAndStartupOnly(t *testing.T) {
	path, db := legacyFixture(t)
	db.Close()
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()
	if err = j.ClassifyLegacyCustody(ctx, func(r Request) bool { return r.ID != "NO_EFFECT" }); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, id := range []string{"PREPARED", "COMMITTED", "NO_EFFECT"} {
		o, err := j.Inspect(ctx, "volume", id)
		if err != nil || o.Kind != LegacyOperation {
			t.Fatal(o, err)
		}
	}
	if err = j.ClassifyLegacyCustody(ctx, func(Request) bool { return true }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"PREPARED", "COMMITTED", "NO_EFFECT"} {
		o, err := j.Inspect(ctx, "volume", id)
		if err != nil || o.Kind != CustodyOperation {
			t.Fatal(o, err)
		}
	}
	if _, err = j.Claim(ctx, "volume", "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal(err)
	}
	other, _ := testJournal(t)
	if _, err = other.Claim(ctx, "v", "s", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = other.ClassifyLegacyCustody(ctx, func(Request) bool { return true }); !errors.Is(err, ErrInFlight) {
		t.Fatal(err)
	}
}

func TestCustodyClassificationSQLFailureRollsBackAllKinds(t *testing.T) {
	path, db := legacyFixture(t)
	db.Close()
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err = j.db.Exec(`CREATE TRIGGER deny_classification BEFORE UPDATE OF kind ON operations WHEN OLD.id='PREPARED' BEGIN SELECT RAISE(ABORT,'classification failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err = j.ClassifyLegacyCustody(context.Background(), func(Request) bool { return true }); err == nil {
		t.Fatal("SQL failure ignored")
	}
	for _, id := range []string{"PREPARED", "COMMITTED", "NO_EFFECT"} {
		o, err := j.Inspect(context.Background(), "volume", id)
		if err != nil || o.Kind != LegacyOperation {
			t.Fatal(o, err)
		}
	}
}

func TestTypedMigrationCrashHelper(t *testing.T) {
	if os.Getenv("JM_TYPED_MIGRATION_HELPER") != "1" {
		return
	}
	if _, err := Open(os.Getenv("JM_TYPED_MIGRATION_PATH")); err != nil {
		t.Fatal(err)
	}
	// Real process exit before custody classification; the parent must reopen
	// with legacy kinds still intact, not an inferred media/custody receipt.
	os.Exit(77)
}

func TestTypedMigrationProcessExitBeforeClassification(t *testing.T) {
	path, db := legacyFixture(t)
	db.Close()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestTypedMigrationCrashHelper$")
	cmd.Env = append(os.Environ(), "JM_TYPED_MIGRATION_HELPER=1", "JM_TYPED_MIGRATION_PATH="+path)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 77 {
		t.Fatalf("migration child: %s %v", output, err)
	}
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for _, id := range []string{"PREPARED", "COMMITTED", "NO_EFFECT"} {
		o, err := j.Inspect(context.Background(), "volume", id)
		if err != nil || o.Kind != LegacyOperation {
			t.Fatal(o, err)
		}
	}
	if err = j.ClassifyLegacyCustody(context.Background(), func(Request) bool { return true }); err != nil {
		t.Fatal(err)
	}
}

func TestTypedMigrationHistoricalSchemas(t *testing.T) {
	for _, revision := range []string{"a030b94", "9dec00b", "6e81546"} {
		t.Run(revision, func(t *testing.T) {
			path, db := legacyFixtureFor(t, revision)
			db.Close()
			j, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, state := range []State{Prepared, Committed, NoEffect} {
				o, err := j.Inspect(context.Background(), "volume", string(state))
				if err != nil || o.State != state || o.Kind != LegacyOperation || o.FileID != "" {
					t.Fatal(o, err)
				}
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			j, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			var version int
			if err = j.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != journalSchemaVersion {
				t.Fatal(version, err)
			}
		})
	}
}
