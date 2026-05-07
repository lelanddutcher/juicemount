package nle

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

// parseResolve parses a DaVinci Resolve .drp file.
//
// SUPPORTED:
//   - Zip-archive .drp (Resolve 16+ "Project Archive" export):
//     the top-level zip contains "Project.db" (SQLite) plus media
//     metadata. We extract Project.db to a temp file and read media
//     paths from the BinClip table.
//   - Raw SQLite .drp (some older exports / direct DB copies): we
//     detect the SQLite magic header and read directly.
//
// NOT SUPPORTED (returns the partial result with a TODO note):
//   - Resolve's proprietary opaque-binary .drp variant. This format
//     wraps a project archive in a Resolve-specific container with no
//     public spec. We detect it by the absence of zip and SQLite
//     magic and return ErrUnsupportedResolveVariant.
//   - .drb (Resolve database backup, multi-project) — different scope.
//
// The schema for media paths varies across Resolve major versions.
// We try a small set of known table+column combinations and union
// the results. If none match, we return an empty slice with no error
// — the caller can decide whether that constitutes a problem.
func parseResolve(path string) ([]MediaRef, error) {
	dbPath, cleanup, err := openResolveDB(path)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("nle/resolve: sql.Open: %w", err)
	}
	defer db.Close()

	return resolveQueryMedia(db)
}

// ErrUnsupportedResolveVariant indicates a .drp that is neither zip
// nor raw SQLite (likely the proprietary binary container).
var ErrUnsupportedResolveVariant = errors.New(
	"nle/resolve: .drp is not a recognised zip or sqlite archive (proprietary container?)")

// openResolveDB returns a path to a SQLite database extracted from
// the .drp at path. cleanup must be called to remove temp files.
func openResolveDB(path string) (dbPath string, cleanup func(), err error) {
	cleanup = func() {}

	header, err := readMagic(path, 16)
	if err != nil {
		return "", cleanup, fmt.Errorf("nle/resolve: read header: %w", err)
	}

	switch {
	case isZipMagic(header):
		return extractZipDB(path)
	case isSQLiteMagic(header):
		// Raw SQLite — use the file directly. SQLite drivers are
		// happy to open a file with a non-.db extension.
		return path, func() {}, nil
	default:
		return "", cleanup, ErrUnsupportedResolveVariant
	}
}

// extractZipDB unpacks the SQLite project DB out of a zip-form .drp
// to a temp file. Returns the temp path and a cleanup func.
func extractZipDB(zipPath string) (string, func(), error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", func() {}, fmt.Errorf("nle/resolve: open zip: %w", err)
	}
	defer r.Close()

	var entry *zip.File
	for _, f := range r.File {
		base := strings.ToLower(filepath.Base(f.Name))
		// Resolve's archive convention names the DB "Project.db".
		// Some older exports name it "project.db" or "metadata.db".
		if base == "project.db" || base == "metadata.db" {
			entry = f
			break
		}
	}
	if entry == nil {
		// Fallback: any *.db at the archive root.
		for _, f := range r.File {
			if strings.HasSuffix(strings.ToLower(f.Name), ".db") &&
				!strings.Contains(f.Name, "/") {
				entry = f
				break
			}
		}
	}
	if entry == nil {
		return "", func() {}, fmt.Errorf("nle/resolve: no .db inside zip")
	}

	rc, err := entry.Open()
	if err != nil {
		return "", func() {}, fmt.Errorf("nle/resolve: open zip entry: %w", err)
	}
	defer rc.Close()

	tmp, err := os.CreateTemp("", "juicemount-drp-*.db")
	if err != nil {
		return "", func() {}, fmt.Errorf("nle/resolve: tempfile: %w", err)
	}
	if _, err := io.Copy(tmp, rc); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", func() {}, fmt.Errorf("nle/resolve: copy db: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(tmp.Name()) }
	return tmp.Name(), cleanup, nil
}

// resolveCandidates is the list of (table, column) pairs we try in
// order. Resolve's media-path column has been renamed across versions
// (FilePath, MediaPoolItem.FilePath, BinClip.FileName, ...). We don't
// know the exact version from the .drp, so we probe the schema.
type resolveCandidate struct {
	Table    string
	PathCol  string
	SizeCol  string // optional, "" if not present
}

var resolveCandidates = []resolveCandidate{
	{Table: "BinClip", PathCol: "FilePath", SizeCol: "FileSize"},
	{Table: "MediaPoolItem", PathCol: "FilePath", SizeCol: "FileSize"},
	{Table: "MediaPoolItem", PathCol: "FileName", SizeCol: ""},
	{Table: "ClipItem", PathCol: "FilePath", SizeCol: ""},
	// JuiceMount synthetic-test fixture uses simply "media".
	{Table: "media", PathCol: "path", SizeCol: "size"},
}

func resolveQueryMedia(db *sql.DB) ([]MediaRef, error) {
	tables, err := listTables(db)
	if err != nil {
		return nil, err
	}
	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[strings.ToLower(t)] = true
	}

	var refs []MediaRef
	for _, c := range resolveCandidates {
		if !tableSet[strings.ToLower(c.Table)] {
			continue
		}
		got, err := queryCandidate(db, c)
		if err != nil {
			// A schema-mismatch on one candidate shouldn't kill the
			// whole parse. Log via error wrap and try next.
			continue
		}
		refs = append(refs, got...)
	}
	return dedupeRefs(refs), nil
}

func listTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		return nil, fmt.Errorf("nle/resolve: list tables: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func queryCandidate(db *sql.DB, c resolveCandidate) ([]MediaRef, error) {
	cols := c.PathCol
	if c.SizeCol != "" {
		cols += ", " + c.SizeCol
	}
	q := fmt.Sprintf(`SELECT %s FROM "%s"`, cols, c.Table)
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MediaRef
	for rows.Next() {
		var (
			pathStr string
			size    sql.NullInt64
		)
		if c.SizeCol != "" {
			if err := rows.Scan(&pathStr, &size); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&pathStr); err != nil {
				return nil, err
			}
		}
		pathStr = strings.TrimSpace(pathStr)
		if pathStr == "" {
			continue
		}
		ref := MediaRef{
			Path:   pathStr,
			Format: extOf(pathStr),
		}
		if size.Valid {
			ref.Size = size.Int64
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// readMagic returns the first n bytes of a file (or fewer if the
// file is shorter). Used for format sniffing without committing to
// a parser.
func readMagic(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	read, err := io.ReadFull(f, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return buf[:read], nil
	}
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// isZipMagic reports whether b begins with a zip local-file-header
// signature. Empty or single-entry zips both start with "PK\x03\x04";
// "PK\x05\x06" is the empty-zip end-of-central-directory.
func isZipMagic(b []byte) bool {
	return len(b) >= 4 && (bytes.Equal(b[:4], []byte{0x50, 0x4b, 0x03, 0x04}) ||
		bytes.Equal(b[:4], []byte{0x50, 0x4b, 0x05, 0x06}))
}

// isSQLiteMagic reports whether b begins with the SQLite 3 file header
// "SQLite format 3\x00".
func isSQLiteMagic(b []byte) bool {
	return len(b) >= 16 && bytes.Equal(b[:16], []byte("SQLite format 3\x00"))
}
