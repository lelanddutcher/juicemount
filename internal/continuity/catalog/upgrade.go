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
	return upgradeCatalogVersion(ctx, input, expectedLibrary, schemaVersion, directoryBarrier)
}

func UpgradeAuthority(ctx context.Context, input string, expectedLibrary continuity.LibraryID) error {
	return upgradeCatalogVersion(ctx, input, expectedLibrary, authoritySchemaVersion, syncParent)
}
func UpgradePreferences(ctx context.Context, input string, library continuity.LibraryID) error {
	return upgradeCatalogVersion(ctx, input, library, preferencesSchemaVersion, syncParent)
}

func UpgradeCollections(ctx context.Context, path string, library continuity.LibraryID) error {
	return upgradeCatalogVersion(ctx, path, library, collectionsSchemaVersion, syncParent)
}

func UpgradeSavedQueries(ctx context.Context, path string, library continuity.LibraryID) error {
	return upgradeCatalogVersion(ctx, path, library, savedQueriesSchemaVersion, syncParent)
}

func UpgradeDelivery(ctx context.Context, path string, library continuity.LibraryID) error {
	return upgradeCatalogVersion(ctx, path, library, deliverySchemaVersion, syncParent)
}

func upgradeCatalogVersion(ctx context.Context, input string, expectedLibrary continuity.LibraryID, target int, directoryBarrier func(string) error) (resultErr error) {
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
	if supportsAuthority(target) {
		if err := checkPrivatePaths(path); err != nil {
			return err
		}
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
	library, initialVersion, err := snapshotCatalog(ctx, path)
	if err != nil {
		return err
	}
	if library != expectedLibrary {
		return invalid("upgrade library mismatch")
	}
	if target == savedQueriesSchemaVersion && initialVersion != 5 && initialVersion != 6 {
		return ErrUpgradeRequired
	}
	if target == deliveryLifecycleSchemaVersion && initialVersion != 7 && initialVersion != 8 { return ErrUpgradeRequired }
	if target == deliverySchemaVersion && initialVersion != 6 && initialVersion != 7 { return ErrUpgradeRequired }
	if target == collectionsSchemaVersion && initialVersion != 4 && initialVersion != 5 {
		return ErrUpgradeRequired
	}
	if target == preferencesSchemaVersion && initialVersion != 3 && initialVersion != 4 {
		return ErrUpgradeRequired
	}
	if target == authoritySchemaVersion && initialVersion == 1 {
		return ErrUpgradeRequired
	}
	if target == schemaVersion && initialVersion == authoritySchemaVersion {
		return invalid("authority catalog requires explicit authority upgrade")
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
	held := &Store{path: path, file: file, version: target}
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
	if target == savedQueriesSchemaVersion && version != 5 && version != 6 {
		return ErrUpgradeRequired
	}
	if target == deliveryLifecycleSchemaVersion && version != 7 && version != 8 { return ErrUpgradeRequired }
	if target == deliverySchemaVersion && version != 6 && version != 7 { return ErrUpgradeRequired }
	if target == collectionsSchemaVersion && version != 4 && version != 5 {
		return ErrUpgradeRequired
	}
	if target == preferencesSchemaVersion && version != 3 && version != 4 {
		return ErrUpgradeRequired
	}
	if target == authoritySchemaVersion && version != 2 && version != 3 {
		return ErrUpgradeRequired
	}
	if target == schemaVersion && version != 1 && version != 2 {
		return ErrUpgradeRequired
	}
	if err := checkIntegrity(ctx, tx); err != nil {
		return err
	}
	if err := checkSettings(ctx, tx); err != nil {
		return err
	}
	if version == 1 {
		if err := migrateV1(ctx, tx); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != expectedLibrary || current != schemaVersion {
			return corrupt("migration identity mismatch")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
	}
	if target == authoritySchemaVersion && version == schemaVersion {
		if err := migrateV2(ctx, tx); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != expectedLibrary || current != authoritySchemaVersion {
			return corrupt("authority migration identity")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
	}
	if target == preferencesSchemaVersion && version == 3 {
		for _, kind := range []string{"table", "index", "trigger"} {
			for _, object := range preferencesSchema() {
				if object.Type == kind {
					if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
						return err
					}
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO preference_control(singleton,library_id,clock_floor_unix,subject_count,entry_count,receipt_count) VALUES(1,?,0,0,0,0)`, library); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(4,?)`, schemaFingerprint(expectedSchemaFor(4))); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `PRAGMA user_version=4`); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != library || current != 4 {
			return corrupt("preference migration identity")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
	}
	if target == collectionsSchemaVersion && version == 4 {
		if err := initializeCollections(ctx, tx, string(library), true); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `PRAGMA user_version=5`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(5,?)`, schemaFingerprint(expectedSchemaFor(5))); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != library || current != 5 {
			return corrupt("collection migration identity")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
	}
	if target == savedQueriesSchemaVersion && version == 5 {
		if err := initializeSavedQueries(ctx, tx); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != library || current != 6 {
			return corrupt("saved query migration identity")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
	}
	if target == deliverySchemaVersion && version == savedQueriesSchemaVersion {
		if err := migrateV6Delivery(ctx, tx, library); err != nil { return err }
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil { return err }
		if actual != library || current != deliverySchemaVersion { return corrupt("delivery migration identity") }
		if err := checkIntegrity(ctx, tx); err != nil { return err }
	}
	if target == deliveryLifecycleSchemaVersion && version == deliverySchemaVersion {
		if err := initializeDeliveryLifecycle(ctx, tx, library); err != nil { return err }
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil { return err }
		if actual != library || current != deliveryLifecycleSchemaVersion { return corrupt("lifecycle migration identity") }
		if err := checkIntegrity(ctx, tx); err != nil { return err }
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
	// This barrier is required even for an already-valid-v2 retry.
	if err := directoryBarrier(path); err != nil {
		return fmt.Errorf("catalog upgrade directory barrier uncertain: %w", err)
	}
	return nil
}

func migrateV2(ctx context.Context, tx *sql.Tx) error {
	for _, kind := range []string{"table", "index", "trigger"} {
		for _, object := range authoritySchema() {
			if object.Type == kind {
				if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
					return err
				}
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(3,?)`, schemaFingerprint(expectedSchemaFor(3))); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=3`)
	return err
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(1,?),(2,?)`, schemaFingerprint(legacySchema()), schemaFingerprint(expectedSchema())); err != nil {
		return err
	}
	for _, object := range expectedSchema() {
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
