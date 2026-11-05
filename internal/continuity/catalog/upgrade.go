package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// snapshotCatalog uses a separate mode=ro connection with a deferred transaction.
// It must finish and close before a caller acquires an immediate write lock.
func snapshotCatalog(ctx context.Context, path string) (library continuity.LibraryID, version int, resultErr error) {
	probe, err := openDB(path, true)
	if err != nil {
		return "", 0, formatError(err)
	}
	defer func() { resultErr = errors.Join(resultErr, probe.Close()) }()
	tx, err := probe.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, formatError(err)
	}
	defer tx.Rollback()
	library, version, err = recognizeSchema(ctx, tx, false)
	if err != nil {
		return "", 0, err
	}
	if err := checkIntegrity(ctx, tx); err != nil {
		return "", 0, err
	}
	if err := tx.Commit(); err != nil {
		return "", 0, err
	}
	return library, version, nil
}

// Upgrade explicitly recognizes and upgrades an existing catalog of the expected
// library. It never creates a destination and never changes an event's identity.
// Drain catalog users before calling; SQLite serialization is not process ownership.
func Upgrade(ctx context.Context, input string, expectedLibrary continuity.LibraryID) (resultErr error) {
	return upgradeCatalog(ctx, input, expectedLibrary, syncParent)
}

func upgradeCatalog(ctx context.Context, input string, expectedLibrary continuity.LibraryID, directoryBarrier func(string) error) (resultErr error) {
	if expectedLibrary.Validate() != nil {
		return invalid("expected library identity")
	}
	path, err := canonicalPath(input)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("%w: catalog path: %w", ErrNotFound, err)
	}
	if err != nil {
		return err
	}
	if err := checkFileIdentity(info); err != nil {
		return err
	}
	if err := checkSidecars(path); err != nil {
		return err
	}
	file, err := openRegular(path, false)
	if err != nil {
		return err
	}
	defer func() {
		if file != nil {
			resultErr = errors.Join(resultErr, file.Close())
		}
	}()
	library, _, err := snapshotCatalog(ctx, path)
	if err != nil {
		return err
	}
	if library != expectedLibrary {
		return invalid("upgrade library mismatch")
	}
	db, err := openDB(path, false)
	if err != nil {
		return err
	}
	defer func() {
		if db != nil {
			resultErr = errors.Join(resultErr, db.Close())
		}
	}()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	held := &Store{path: path, file: file}
	if err := held.checkPaths(); err != nil {
		return err
	}
	library, version, err := recognizeSchema(ctx, tx, false)
	if err != nil {
		return err
	}
	if library != expectedLibrary {
		return invalid("upgrade library mismatch")
	}
	if err := checkIntegrity(ctx, tx); err != nil {
		return err
	}
	if err := checkSettings(ctx, tx); err != nil {
		return err
	}
	// Each step is checked as the exact next format before the following one.
	for _, step := range []struct {
		from    int
		migrate func(context.Context, *sql.Tx) error
	}{{1, migrateV1}, {2, migrateV2}} {
		if version != step.from {
			continue
		}
		if err := step.migrate(ctx, tx); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != expectedLibrary || current != step.from+1 {
			return corrupt("migration identity mismatch")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
		version = current
	}
	if version != schemaVersion {
		return corrupt("migration did not reach the current format")
	}
	if err := held.checkPaths(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog upgrade commit outcome uncertain: %w", err)
	}
	closeDB := db.Close()
	db = nil
	closeFile := file.Close()
	file = nil
	if err := errors.Join(closeDB, closeFile); err != nil {
		return fmt.Errorf("catalog upgrade close outcome uncertain: %w", err)
	}
	// This barrier is required even for an already-current retry.
	if err := directoryBarrier(path); err != nil {
		return fmt.Errorf("catalog upgrade directory barrier uncertain: %w", err)
	}
	return nil
}

// The caller has validated v1 inside this same immediate transaction.
func migrateV1(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		`DROP TRIGGER schema_migrations_no_update`,
		`DROP TRIGGER schema_migrations_no_delete`,
		`DROP TABLE schema_migrations`, ledgerV2DDL,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(1,?),(2,?)`, schemaFingerprint(legacySchema()), schemaFingerprint(schemaV2())); err != nil {
		return err
	}
	for _, object := range schemaV2() {
		if object.Type == "trigger" && object.Table == "schema_migrations" {
			if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
				return err
			}
		}
	}
	for _, object := range relationalSchema() {
		if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=2`)
	return err
}

// migrateV2 adds the collection tables, indexes and guards to a format 2
// catalog validated inside this same immediate transaction. Nothing existing
// is changed; the ledger gains the format 3 fingerprint.
func migrateV2(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(3,?)`, schemaFingerprint(expectedSchema())); err != nil {
		return err
	}
	for _, object := range collectionSchema() {
		if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=3`)
	return err
}

// Inspect reports the library and format of an existing complete catalog
// without changing it, through the same safe-file admission and read-only
// snapshot as Open. A host uses it to name the expected library to Upgrade.
func Inspect(ctx context.Context, input string) (continuity.LibraryID, int, error) {
	path, err := canonicalPath(input)
	if err != nil {
		return "", 0, err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return "", 0, fmt.Errorf("%w: catalog path: %w", ErrNotFound, err)
	}
	if err != nil {
		return "", 0, err
	}
	if err := checkFileIdentity(info); err != nil {
		return "", 0, err
	}
	if err := checkSidecars(path); err != nil {
		return "", 0, err
	}
	return snapshotCatalog(ctx, path)
}
