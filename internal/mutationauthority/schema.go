package mutationauthority

import (
	"database/sql"
	"fmt"
	"strings"
	"unicode"
)

const journalSchemaVersion = 4
const kindColumn = "kind TEXT NOT NULL DEFAULT '' CHECK(kind IN ('','custody-v1','media-publish-v1'))"
const fileColumn = "file_id TEXT NOT NULL DEFAULT ''"

var legacySchema = map[string]string{
	"owners": `CREATE TABLE owners (
		volume TEXT PRIMARY KEY, session TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch > 0),
		expires_ns INTEGER NOT NULL, admission_closed INTEGER NOT NULL DEFAULT 0 CHECK(admission_closed IN (0,1))
	)`,
	"operations": `CREATE TABLE operations (
		volume TEXT NOT NULL, id TEXT NOT NULL, session TEXT NOT NULL, epoch INTEGER NOT NULL,
		digest TEXT NOT NULL, expected_revision TEXT NOT NULL, state TEXT NOT NULL
		  CHECK(state IN ('PREPARED','COMMITTED','NO_EFFECT')),
		revision TEXT NOT NULL DEFAULT '', evidence TEXT NOT NULL DEFAULT '', committed_ns INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(volume,id), FOREIGN KEY(volume) REFERENCES owners(volume)
	)`,
	"operations_unsettled": `CREATE INDEX operations_unsettled ON operations(volume,epoch,state)`,
	"backend_processes": `CREATE TABLE backend_processes (
		generation INTEGER PRIMARY KEY AUTOINCREMENT,
		volume TEXT NOT NULL, username TEXT NOT NULL UNIQUE, redis_run_id TEXT NOT NULL
	)`,
	"backend_configuration": `CREATE TABLE backend_configuration (
		id INTEGER PRIMARY KEY CHECK(id=1), volume TEXT NOT NULL, digest TEXT NOT NULL
	)`,
}

type schemaReader interface {
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

// Compare DDL tokens without erasing whitespace/case inside quoted literals.
// Alternate schemas are deliberately refused, not guessed or partly repaired.
func normalizedDDL(s string) string {
	var b strings.Builder
	quoted := false
	for _, c := range s {
		if c == '\'' {
			quoted = !quoted
			b.WriteRune(c)
			continue
		}
		if quoted {
			b.WriteRune(c)
			continue
		}
		if !unicode.IsSpace(c) {
			b.WriteRune(unicode.ToLower(c))
		}
	}
	return b.String()
}

func typedOperationsDDL() string {
	return strings.Replace(legacySchema["operations"], "PRIMARY KEY(volume,id)", kindColumn+", "+fileColumn+", PRIMARY KEY(volume,id)", 1)
}

func recognizeSchema(q schemaReader) (int, map[string]bool, error) {
	var version int
	if err := q.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return 0, nil, err
	}
	if version < 0 || version > journalSchemaVersion {
		return 0, nil, fmt.Errorf("%w: unsupported journal schema version", ErrInvalid)
	}
	rows, err := q.Query(`SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var typ, name, ddl string
		if err := rows.Scan(&typ, &name, &ddl); err != nil {
			return 0, nil, err
		}
		want, known := legacySchema[name]
		if !known && version >= 2 {
			want, known = stageSchema[name]
		}
		if !known && version >= 3 && name == "media_retention" {
			want, known = retentionDDL, true
		}
		if !known && version >= 4 {
			want, known = publicationSchema[name]
		}
		if !known || ((name == "operations_unsettled" || name == "media_stage_inode") && typ != "index") || (name != "operations_unsettled" && name != "media_stage_inode" && typ != "table") {
			return 0, nil, fmt.Errorf("%w: unrecognized journal schema object", ErrInvalid)
		}
		if version >= 1 && name == "operations" {
			want = typedOperationsDDL()
		}
		if normalizedDDL(ddl) != normalizedDDL(want) {
			return 0, nil, fmt.Errorf("%w: unexpected journal schema for %s", ErrInvalid, name)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	if len(found) == 0 && version == 0 {
		return version, found, nil
	}
	if !found["owners"] || !found["operations"] || !found["operations_unsettled"] {
		return 0, nil, fmt.Errorf("%w: incomplete journal schema", ErrInvalid)
	}
	// Historical v0 schemas added process ownership first, then its configuration.
	// Configuration without process ownership was never a supported journal.
	if found["backend_configuration"] && !found["backend_processes"] {
		return 0, nil, fmt.Errorf("%w: unsupported legacy journal subset", ErrInvalid)
	}
	wantCount := len(legacySchema)
	if version >= 2 {
		wantCount += len(stageSchema)
	}
	if version >= 3 {
		wantCount++
	}
	if version >= 4 {
		wantCount += len(publicationSchema)
	}
	if version >= 1 && len(found) != wantCount {
		return 0, nil, fmt.Errorf("%w: incomplete typed journal schema", ErrInvalid)
	}
	return version, found, nil
}

func migrateSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	version, found, err := recognizeSchema(tx)
	if err != nil {
		return err
	}
	if version == 0 {
		for _, name := range []string{"owners", "operations", "operations_unsettled", "backend_processes", "backend_configuration"} {
			if !found[name] {
				if _, err = tx.Exec(legacySchema[name]); err != nil {
					return err
				}
			}
		}
		for _, column := range []string{kindColumn, fileColumn} {
			if _, err = tx.Exec("ALTER TABLE operations ADD COLUMN " + column); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
			return err
		}
	}
	if version <= 1 {
		for _, name := range []string{"media_stages", "media_stage_inode", "media_slices", "media_object_attempts"} {
			if _, err = tx.Exec(stageSchema[name]); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(`PRAGMA user_version=2`); err != nil {
			return err
		}
	}
	if version <= 2 {
		if _, err = tx.Exec(retentionDDL); err != nil {
			return err
		}
		if _, err = tx.Exec(`PRAGMA user_version=3`); err != nil {
			return err
		}
	}
	if version <= 3 {
		for _, name := range []string{"media_publications", "media_publication_retention"} {
			if _, err = tx.Exec(publicationSchema[name]); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(`PRAGMA user_version=4`); err != nil {
			return err
		}
	}
	// Reject invalid persisted identities before admission or owner mutation.
	rows, err := tx.Query(`SELECT volume,id,session,epoch,digest,expected_revision,kind,file_id FROM operations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r Request
		if err = rows.Scan(&r.Volume, &r.ID, &r.Session, &r.Epoch, &r.Digest, &r.ExpectedRevision, &r.Kind, &r.FileID); err != nil {
			rows.Close()
			return err
		}
		if !validRequest(r) {
			rows.Close()
			return fmt.Errorf("%w: invalid persisted operation identity", ErrInvalid)
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if _, _, err = recognizeSchema(tx); err != nil {
		return err
	}
	if err := validateMediaStages(tx); err != nil {
		return err
	}
	return tx.Commit()
}
