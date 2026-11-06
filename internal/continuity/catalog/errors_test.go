package catalog

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"modernc.org/sqlite"
)

// Cancellation and lock contention say nothing about the stored bytes; callers
// must be able to retry them instead of treating the catalog as corrupt.
func TestCatalogOperationalErrorsAreNotFormatErrors(t *testing.T) {
	s, path := newTestStore(t)
	applyTest(t, s, initialOperation())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reads := map[string]func() error{
		"GetAsset":    func() error { _, err := s.GetAsset(ctx, testLibrary, testAsset); return err },
		"GetRevision": func() error { _, err := s.GetRevision(ctx, testLibrary, rootRevision); return err },
		"GetEvent":    func() error { _, err := s.GetEvent(ctx, testLibrary, "initial"); return err },
		"ListAssets":  func() error { _, err := s.ListAssets(ctx, testLibrary, "", 10); return err },
		"ListRevisions": func() error {
			_, err := s.ListRevisions(ctx, testLibrary, testAsset, "", 10)
			return err
		},
		"Events":            func() error { _, err := s.Events(ctx, testLibrary, 0, 10); return err },
		"GetRelationship":   func() error { _, err := s.GetRelationship(ctx, testLibrary, "rel-1"); return err },
		"GetLocation":       func() error { _, err := s.GetLocation(ctx, testLibrary, "loc-1"); return err },
		"ListRelationships": func() error { _, err := s.ListRelationships(ctx, testLibrary, testAsset, "", 10); return err },
		"ListLocations":     func() error { _, err := s.ListLocations(ctx, testLibrary, testAsset, "", 10); return err },
	}
	for name, read := range reads {
		if err := read(); !errors.Is(err, context.Canceled) || errors.Is(err, ErrFormat) {
			t.Errorf("%s with canceled context: %v", name, err)
		}
	}
	// A revision whose owning asset read is canceled must not be called corrupt.
	if _, err := readRevision(ctx, revisionAssetCanceler{s.db}, testLibrary, rootRevision); !errors.Is(err, context.Canceled) || errors.Is(err, ErrFormat) {
		t.Errorf("readRevision with canceled asset read: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Another connection holding an exclusive lock makes admission wait out the
	// busy timeout and fail with SQLITE_BUSY, which is contention, not damage.
	other, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	conn, err := other.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{`PRAGMA locking_mode=EXCLUSIVE`, `BEGIN EXCLUSIVE`, `UPDATE assets SET label=label`} {
		if _, err := conn.ExecContext(context.Background(), q); err != nil {
			t.Fatal(q, err)
		}
	}
	if opened, err := Open(path); err == nil || errors.Is(err, ErrFormat) || !isBusy(err) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("Open of a locked catalog: %v", err)
	}
	if err := Upgrade(context.Background(), path, testLibrary); err == nil || errors.Is(err, ErrFormat) || !isBusy(err) {
		t.Fatalf("Upgrade of a locked catalog: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
}

// revisionAssetCanceler passes the revision query through and cancels the
// nested asset lookup, isolating readRevision's own error wrapping.
type revisionAssetCanceler struct{ q queryer }

func (r revisionAssetCanceler) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return r.q.QueryContext(context.Background(), query, args...)
}

func (r revisionAssetCanceler) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if len(args) == 1 {
		if _, ok := args[0].(continuity.AssetID); ok {
			return r.q.QueryRowContext(ctx, query, args...)
		}
	}
	return r.q.QueryRowContext(context.Background(), query, args...)
}

func isBusy(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code()&0xff == 5
}
