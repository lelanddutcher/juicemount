package privatefile

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const nativeInventoryEntries = 4096
const nativeInventoryMetadata = int64(96 << 20)
const nativeInventoryLogical = int64(4 << 30)
const nativeInventorySelectionMax = int64(1 << 30)
const nativeInventoryPhysical = int64(64 << 20)
const nativeInventoryWriteHeadroom = int64(17 << 20)
const nativeProfileSchema = `CREATE TABLE profile (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1), token TEXT NOT NULL, endpoint TEXT NOT NULL, library TEXT NOT NULL)`
const nativeCopiesSchema = `CREATE TABLE copies (copy_id TEXT PRIMARY KEY, intent_sha TEXT NOT NULL, selection_sha TEXT NOT NULL DEFAULT '', metadata_bytes INTEGER NOT NULL CHECK(metadata_bytes=24576), logical_bytes INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL, directory_id TEXT NOT NULL DEFAULT '', lock_id TEXT NOT NULL DEFAULT '', partial_id TEXT NOT NULL DEFAULT '')`

var ErrNativeInventoryLimit = errors.New("native inventory reservation limit")

const (
	NativeIntentPending    = "intent_pending"
	NativeIntentReady      = "intent_ready"
	NativeSelectionPending = "selection_pending"
	NativeSelectionReady   = "selection_ready"
	NativeZeroPending      = "zero_pending"
	NativeZeroBound        = "zero_bound"
	NativeReady            = "ready"
)

type nativeInventory struct {
	db                           *sql.DB
	token                        string
	endpoint                     string
	library                      string
	commitFault, checkpointFault error
}
type nativeMarker struct {
	Version   int      `json:"version"`
	Token     string   `json:"token"`
	Endpoint  string   `json:"endpoint"`
	Library   string   `json:"library"`
	Root      Identity `json:"root"`
	Staging   Identity `json:"staging"`
	Copies    Identity `json:"copies"`
	Control   Identity `json:"control"`
	Inventory Identity `json:"inventory"`
}

// NativeInventoryEntry is a fixed-purpose accounting witness. It never grants
// file access; custody records and live identities must still agree with it.
type NativeInventoryEntry struct {
	CopyID          string
	IntentSHA256    string
	SelectionSHA256 string
	MetadataBytes   int64
	LogicalBytes    int64
	Phase           string
	Directory       Identity
	Lock            Identity
	Partial         Identity
}

func validNativeDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			if s[i] < 'a' || s[i] > 'f' {
				return false
			}
		}
	}
	return true
}
func validNativePhase(s string) bool {
	switch s {
	case NativeIntentPending, NativeIntentReady, NativeSelectionPending, NativeSelectionReady, NativeZeroPending, NativeZeroBound, NativeReady:
		return true
	}
	return false
}
func encodeNativeIdentity(id Identity) string { b, _ := json.Marshal(id); return string(b) }
func decodeNativeIdentity(s string) (Identity, error) {
	if s == "" {
		return Identity{}, nil
	}
	if len(s) > 128 {
		return Identity{}, ErrNativeCopyChanged
	}
	var id Identity
	if err := json.Unmarshal([]byte(s), &id); err != nil || encodeNativeIdentity(id) != s {
		return Identity{}, ErrNativeCopyChanged
	}
	return id, nil
}
func nativeDB(path string) (*sql.DB, error) {
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{}
	q.Set("mode", "rw")
	q.Set("_txlock", "immediate")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "busy_timeout(2000)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}
func (n *nativeInventory) close() error {
	if n == nil || n.db == nil {
		return nil
	}
	return n.db.Close()
}

// CreateNativeRoot never adopts an existing directory. A failed creation
// leaves every artifact in place; only the final synced marker permits Open.
func CreateNativeRoot(path, endpoint, library string) (result *NativeRoot, resultErr error) {
	defer func() {
		if resultErr != nil && result != nil {
			resultErr = errors.Join(resultErr, result.Close())
			result = nil
		}
	}()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || endpoint == "" || library == "" || len(endpoint) > 275 || len(library) > 128 {
		return nil, ErrNativeCopyChanged
	}
	parent, err := walkAbsoluteDirectory(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, parent.Close()) }()
	if _, err := privateDir(parent); err != nil {
		return nil, err
	}
	if err := unix.Mkdirat(int(parent.Fd()), filepath.Base(path), 0700); err != nil {
		return nil, err
	}
	if err := parent.Sync(); err != nil {
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	root, err := openDirectoryAt(parent, filepath.Base(path))
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	rootID, err := privateDir(root)
	if err != nil {
		return nil, err
	}
	staging, err := nativeDirectory(root, "staging")
	if err != nil {
		return nil, err
	}
	stagingID, idErr := privateDir(staging)
	closeErr := staging.Close()
	if idErr != nil || closeErr != nil {
		return nil, errors.Join(idErr, closeErr)
	}
	copies, err := nativeDirectory(root, "copies")
	if err != nil {
		return nil, err
	}
	copiesID, idErr := privateDir(copies)
	closeErr = copies.Close()
	if idErr != nil || closeErr != nil {
		return nil, errors.Join(idErr, closeErr)
	}
	lock, err := OpenAt(root, "control.lock", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	controlID, idErr := privateRegular(lock)
	if idErr == nil {
		idErr = lock.Sync()
	}
	closeErr = lock.Close()
	if idErr != nil || closeErr != nil || controlID.Size != 0 {
		return nil, errors.Join(idErr, closeErr, ErrNativeCopyUncertain)
	}
	initLock, err := OpenAt(root, "control.lock", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	initLockClosed := false
	defer func() {
		if !initLockClosed {
			resultErr = errors.Join(resultErr, initLock.Close())
		}
	}()
	if err := unix.Flock(int(initLock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	dbFile, err := OpenAt(root, "accounting.sqlite", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	if err := dbFile.Sync(); err != nil {
		return nil, errors.Join(err, dbFile.Close(), ErrNativeCopyUncertain)
	}
	inventoryID, idErr := privateRegular(dbFile)
	closeErr = dbFile.Close()
	if idErr != nil || closeErr != nil {
		return nil, errors.Join(idErr, closeErr)
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(tokenBytes)
	db, err := nativeDB(filepath.Join(path, "accounting.sqlite"))
	if err != nil {
		return nil, err
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			resultErr = errors.Join(resultErr, db.Close())
		}
	}()
	if _, err := db.Exec(`PRAGMA page_size=4096`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA max_page_count=4096`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(nativeProfileSchema); err != nil {
		return nil, err
	}
	if _, err := db.Exec(nativeCopiesSchema); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`INSERT INTO profile(singleton,version,token,endpoint,library) VALUES(1,1,?,?,?)`, token, endpoint, library); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA application_id=0x4a4d4e49`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA user_version=1`); err != nil {
		return nil, err
	}
	var busy, log, checkpointed int
	if err := db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &log, &checkpointed); err != nil || busy != 0 {
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := db.Close(); err != nil {
		dbClosed = true
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	dbClosed = true
	opened, err := OpenAt(root, "accounting.sqlite", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	inventoryID, idErr = privateRegular(opened)
	if idErr == nil {
		idErr = opened.Sync()
	}
	closeErr = opened.Close()
	if idErr != nil || closeErr != nil {
		return nil, errors.Join(idErr, closeErr)
	}
	inventoryID.Size = 0
	marker := nativeMarker{1, token, endpoint, library, rootID, stagingID, copiesID, controlID, inventoryID}
	data, err := json.Marshal(marker)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	markerFile, err := OpenAt(root, "initialized.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	if n, err := markerFile.Write(data); err != nil || n != len(data) {
		return nil, errors.Join(err, markerFile.Close(), ErrNativeCopyUncertain)
	}
	if err := markerFile.Sync(); err != nil {
		return nil, errors.Join(err, markerFile.Close(), ErrNativeCopyUncertain)
	}
	if err := markerFile.Close(); err != nil {
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := root.Sync(); err != nil {
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := initLock.Close(); err != nil {
		initLockClosed = true
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	initLockClosed = true
	return OpenNativeRoot(path, endpoint, library)
}

// OpenNativeRoot is strict: it never creates a missing profile component.
func OpenNativeRoot(path, endpoint, library string) (result *NativeRoot, resultErr error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, ErrNativeCopyChanged
	}
	root, err := walkAbsoluteDirectory(path)
	if err != nil {
		return nil, err
	}
	var staging, copies, dbFile, markerFile *os.File
	var inventory *nativeInventory
	var lock *os.File
	defer func() {
		if inventory != nil {
			resultErr = errors.Join(resultErr, inventory.close())
		}
		if lock != nil {
			resultErr = errors.Join(resultErr, lock.Close())
		}
		if resultErr != nil {
			result = nil
			for _, f := range []*os.File{markerFile, dbFile, copies, staging, root} {
				if f != nil {
					resultErr = errors.Join(resultErr, f.Close())
				}
			}
		}
	}()
	rootID, err := privateDir(root)
	if err != nil {
		return nil, err
	}
	staging, err = openDirectoryAt(root, "staging")
	if err != nil {
		return nil, err
	}
	stagingID, err := privateDir(staging)
	if err != nil {
		return nil, err
	}
	copies, err = openDirectoryAt(root, "copies")
	if err != nil {
		return nil, err
	}
	copiesID, err := privateDir(copies)
	if err != nil {
		return nil, err
	}
	lock, err = OpenAt(root, "control.lock", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	controlID, lockErr := privateRegular(lock)
	if lockErr != nil || controlID.Size != 0 {
		return nil, errors.Join(lockErr, ErrNativeCopyChanged)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errors.Join(err, ErrNativeCopyBusy)
	}
	dbFile, err = OpenAt(root, "accounting.sqlite", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	inventoryID, err := privateRegular(dbFile)
	if err != nil {
		return nil, err
	}
	inventoryID.Size = 0
	markerFile, err = OpenAt(root, "initialized.json", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	markerID, err := privateRegular(markerFile)
	if err != nil || markerID.Size < 2 || markerID.Size > 2048 {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	markerBytes, err := io.ReadAll(io.LimitReader(markerFile, 2049))
	if err != nil || int64(len(markerBytes)) != markerID.Size || markerBytes[len(markerBytes)-1] != '\n' {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	var marker nativeMarker
	if err := json.Unmarshal(markerBytes[:len(markerBytes)-1], &marker); err != nil {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	canonical, _ := json.Marshal(marker)
	if string(markerBytes) != string(append(canonical, '\n')) || marker.Version != 1 || marker.Token == "" || marker.Endpoint != endpoint || marker.Library != library || marker.Root != rootID || marker.Staging != stagingID || marker.Copies != copiesID || marker.Control != controlID || marker.Inventory != inventoryID {
		return nil, ErrNativeCopyChanged
	}
	before := &NativeRoot{root: root, path: path, rootID: rootID}
	if err := before.inventoryPhysical(); err != nil {
		return nil, err
	}
	if err := before.rootEntries(); err != nil {
		return nil, err
	}
	db, err := nativeDB(filepath.Join(path, "accounting.sqlite"))
	if err != nil {
		return nil, err
	}
	inventory = &nativeInventory{db: db, token: marker.Token, endpoint: endpoint, library: library}
	r := &NativeRoot{path: path, root: root, staging: staging, copies: copies, inventoryFile: dbFile, markerFile: markerFile, rootID: rootID, stagingID: stagingID, copiesID: copiesID, controlID: controlID, inventoryID: inventoryID, markerID: markerID, token: marker.Token, endpoint: endpoint, library: library}
	if err := r.recognizeInventory(inventory); err != nil {
		return nil, err
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	if err := r.revalidateInventory(); err != nil {
		return nil, err
	}
	if _, err := (&NativeControl{root: r, lock: lock, lockID: controlID, inventory: inventory}).inventoryRows(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *NativeRoot) rootEntries() (resultErr error) {
	// Each observer owns its directory cursor, including copy owners that can
	// revalidate while another owner holds the profile control lock.
	fresh, err := walkAbsoluteDirectory(r.path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, fresh.Close()) }()
	if id, err := privateDir(fresh); err != nil || id != r.rootID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	names, err := fresh.Readdirnames(-1)
	if err != nil {
		return err
	}
	allowed := map[string]bool{"staging": true, "copies": true, "control.lock": true, "accounting.sqlite": true, "initialized.json": true, "accounting.sqlite-wal": true, "accounting.sqlite-shm": true, "accounting.sqlite-journal": true}
	for _, name := range names {
		if !allowed[name] {
			return ErrNativeCopyChanged
		}
	}
	return nil
}
func (r *NativeRoot) copiesEntries() error {
	fresh, err := openDirectoryAt(r.root, "copies")
	if err != nil {
		return err
	}
	defer fresh.Close()
	id, err := privateDir(fresh)
	if err != nil || id != r.copiesID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	names, err := fresh.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		if !nativeCopyID(name) {
			return ErrNativeCopyChanged
		}
		child, err := openDirectoryAt(fresh, name)
		if err != nil {
			return errors.Join(err, ErrNativeCopyChanged)
		}
		_, checkErr := privateDir(child)
		closeErr := child.Close()
		if checkErr != nil || closeErr != nil {
			return errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
		}
	}
	return nil
}

func (r *NativeRoot) revalidateInventory() error {
	for _, leaf := range []struct {
		name    string
		held    *os.File
		want    Identity
		mutable bool
	}{{"accounting.sqlite", r.inventoryFile, r.inventoryID, true}, {"initialized.json", r.markerFile, r.markerID, false}} {
		fresh, err := OpenAt(r.root, leaf.name, os.O_RDONLY, 0)
		if err != nil {
			return errors.Join(err, ErrNativeCopyChanged)
		}
		id, idErr := privateRegular(fresh)
		closeErr := fresh.Close()
		if idErr != nil || closeErr != nil {
			return errors.Join(idErr, closeErr, ErrNativeCopyChanged)
		}
		if leaf.mutable {
			id.Size = 0
		}
		if id != leaf.want {
			return ErrNativeCopyChanged
		}
		id, idErr = privateRegular(leaf.held)
		if idErr != nil {
			return idErr
		}
		if leaf.mutable {
			id.Size = 0
		}
		if id != leaf.want {
			return ErrNativeCopyChanged
		}
	}
	return r.inventoryPhysical()
}
func (r *NativeRoot) inventoryPhysical() error {
	var total int64
	for _, name := range []string{"accounting.sqlite", "accounting.sqlite-wal", "accounting.sqlite-shm", "accounting.sqlite-journal"} {
		f, err := OpenAt(r.root, name, os.O_RDONLY, 0)
		if errors.Is(err, os.ErrNotExist) && name != "accounting.sqlite" {
			continue
		}
		if err != nil {
			return errors.Join(err, ErrNativeCopyChanged)
		}
		id, idErr := privateRegular(f)
		closeErr := f.Close()
		if idErr != nil || closeErr != nil {
			return errors.Join(idErr, closeErr, ErrNativeCopyChanged)
		}
		if name == "accounting.sqlite" && id.Size > 16<<20 {
			return ErrNativeCopyChanged
		}
		total += id.Size
	}
	if total > nativeInventoryPhysical {
		return ErrNativeCopyChanged
	}
	return nil
}
func (r *NativeRoot) inventoryWriteHeadroom() error {
	var total int64
	for _, name := range []string{"accounting.sqlite", "accounting.sqlite-wal", "accounting.sqlite-shm", "accounting.sqlite-journal"} {
		f, err := OpenAt(r.root, name, os.O_RDONLY, 0)
		if errors.Is(err, os.ErrNotExist) && name != "accounting.sqlite" {
			continue
		}
		if err != nil {
			return err
		}
		id, idErr := privateRegular(f)
		closeErr := f.Close()
		if idErr != nil || closeErr != nil {
			return errors.Join(idErr, closeErr)
		}
		total += id.Size
	}
	if total > nativeInventoryPhysical-nativeInventoryWriteHeadroom {
		return ErrNativeInventoryLimit
	}
	return nil
}
func (r *NativeRoot) recognizeInventory(inventory *nativeInventory) error {
	var app, version, page, maxPages int
	for _, p := range []struct {
		query string
		out   *int
	}{{"PRAGMA application_id", &app}, {"PRAGMA user_version", &version}, {"PRAGMA page_size", &page}, {"PRAGMA max_page_count", &maxPages}} {
		if err := inventory.db.QueryRow(p.query).Scan(p.out); err != nil {
			return err
		}
	}
	if app != 0x4a4d4e49 || version != 1 || page != 4096 {
		return ErrNativeCopyChanged
	}
	if maxPages > 4096 {
		if _, err := inventory.db.Exec(`PRAGMA max_page_count=4096`); err != nil {
			return err
		}
	}
	if err := inventory.db.QueryRow(`PRAGMA max_page_count`).Scan(&maxPages); err != nil || maxPages > 4096 {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	var mode string
	var synchronous int
	if err := inventory.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	if err := inventory.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil || synchronous != 2 {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	rows, err := inventory.db.Query(`SELECT type,name,sql FROM sqlite_master ORDER BY name`)
	if err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var typ, name string
		var definition sql.NullString
		if err := rows.Scan(&typ, &name, &definition); err != nil {
			rows.Close()
			return err
		}
		valid := typ == "table" && name == "profile" && definition.Valid && definition.String == nativeProfileSchema || typ == "table" && name == "copies" && definition.Valid && definition.String == nativeCopiesSchema || typ == "index" && name == "sqlite_autoindex_copies_1" && !definition.Valid
		if !valid {
			rows.Close()
			return ErrNativeCopyChanged
		}
		names = append(names, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if len(names) != 3 {
		return ErrNativeCopyChanged
	}
	var token, endpoint, library string
	var schema int
	if err := inventory.db.QueryRow(`SELECT version,token,endpoint,library FROM profile WHERE singleton=1`).Scan(&schema, &token, &endpoint, &library); err != nil {
		return err
	}
	if schema != 1 || token != inventory.token || endpoint != inventory.endpoint || library != inventory.library {
		return ErrNativeCopyChanged
	}
	var profileCount int
	if err := inventory.db.QueryRow(`SELECT COUNT(*) FROM profile`).Scan(&profileCount); err != nil || profileCount != 1 {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	var integrity string
	if err := inventory.db.QueryRow(`PRAGMA quick_check`).Scan(&integrity); err != nil || integrity != "ok" {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	if err := r.rootEntries(); err != nil {
		return err
	}
	return r.inventoryPhysical()
}

func (c *NativeControl) inventoryCommit(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if c.inventory.commitFault != nil {
		return errors.Join(c.inventory.commitFault, ErrNativeCopyUncertain)
	}
	var busy, log, done int
	if err := c.inventory.db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &log, &done); err != nil || busy != 0 {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if c.inventory.checkpointFault != nil {
		return errors.Join(c.inventory.checkpointFault, ErrNativeCopyUncertain)
	}
	if err := c.root.inventoryFile.Sync(); err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := c.root.root.Sync(); err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	return c.root.revalidateInventory()
}
func (c *NativeControl) inventoryRows() (_ []NativeInventoryEntry, err error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	rows, err := c.inventory.db.Query(`SELECT copy_id,intent_sha,selection_sha,metadata_bytes,logical_bytes,phase,directory_id,lock_id,partial_id FROM copies ORDER BY copy_id`)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	var result []NativeInventoryEntry
	var metadata, logical int64
	for rows.Next() {
		var e NativeInventoryEntry
		var dir, lock, partial string
		if err := rows.Scan(&e.CopyID, &e.IntentSHA256, &e.SelectionSHA256, &e.MetadataBytes, &e.LogicalBytes, &e.Phase, &dir, &lock, &partial); err != nil {
			return nil, err
		}
		if !nativeCopyID(e.CopyID) || !validNativeDigest(e.IntentSHA256) || e.MetadataBytes != 24576 || e.LogicalBytes < 0 || e.LogicalBytes > nativeInventorySelectionMax || !validNativePhase(e.Phase) || len(result) >= nativeInventoryEntries {
			return nil, ErrNativeCopyChanged
		}
		if e.SelectionSHA256 != "" && !validNativeDigest(e.SelectionSHA256) {
			return nil, ErrNativeCopyChanged
		}
		var decErr error
		if e.Directory, decErr = decodeNativeIdentity(dir); decErr != nil {
			return nil, decErr
		}
		if e.Lock, decErr = decodeNativeIdentity(lock); decErr != nil {
			return nil, decErr
		}
		if e.Partial, decErr = decodeNativeIdentity(partial); decErr != nil {
			return nil, decErr
		}
		if e.Phase == NativeIntentPending {
			if e.SelectionSHA256 != "" || e.LogicalBytes != 0 || dir != "" || lock != "" || partial != "" {
				return nil, ErrNativeCopyChanged
			}
		} else if dir == "" || lock == "" {
			return nil, ErrNativeCopyChanged
		}
		if e.Phase == NativeIntentReady && (e.SelectionSHA256 != "" || e.LogicalBytes != 0) {
			return nil, ErrNativeCopyChanged
		}
		if e.Phase != NativeIntentPending && e.Phase != NativeIntentReady && e.SelectionSHA256 == "" {
			return nil, ErrNativeCopyChanged
		}
		if (e.Phase == NativeZeroBound || e.Phase == NativeReady) != (partial != "") {
			return nil, ErrNativeCopyChanged
		}
		metadata += e.MetadataBytes
		logical += e.LogicalBytes
		if metadata > nativeInventoryMetadata || logical > nativeInventoryLogical {
			return nil, ErrNativeCopyChanged
		}
		result = append(result, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func (c *NativeControl) InventoryEntries() ([]NativeInventoryEntry, error) { return c.inventoryRows() }

func (c *NativeControl) inventoryTransition(id, from, to string, dir, lock, partial Identity) error {
	if err := c.check(); err != nil {
		return err
	}
	if err := c.root.inventoryWriteHeadroom(); err != nil {
		return err
	}
	if !nativeCopyID(id) || !validNativePhase(from) || !validNativePhase(to) {
		return ErrNativeCopyChanged
	}
	dirS, lockS, partialS := "", "", ""
	if dir != (Identity{}) {
		dirS = encodeNativeIdentity(dir)
	}
	if lock != (Identity{}) {
		lockS = encodeNativeIdentity(lock)
	}
	if partial != (Identity{}) {
		partialS = encodeNativeIdentity(partial)
	}
	tx, err := c.inventory.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE copies SET phase=?, directory_id=CASE WHEN ?!='' THEN ? ELSE directory_id END, lock_id=CASE WHEN ?!='' THEN ? ELSE lock_id END, partial_id=CASE WHEN ?!='' THEN ? ELSE partial_id END WHERE copy_id=? AND phase=?`, to, dirS, dirS, lockS, lockS, partialS, partialS, id, from)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrNativeCopyChanged
	}
	return c.inventoryCommit(tx)
}
func (c *NativeControl) IntentReady(id string, dir, lock Identity) error {
	return c.inventoryTransition(id, NativeIntentPending, NativeIntentReady, dir, lock, Identity{})
}
func (c *NativeControl) SelectionReady(id string) error {
	return c.inventoryTransition(id, NativeSelectionPending, NativeSelectionReady, Identity{}, Identity{}, Identity{})
}
func (c *NativeControl) ZeroPending(id string) error {
	return c.inventoryTransition(id, NativeSelectionReady, NativeZeroPending, Identity{}, Identity{}, Identity{})
}
func (c *NativeControl) ZeroBound(id string, file Identity) error {
	return c.inventoryTransition(id, NativeZeroPending, NativeZeroBound, Identity{}, Identity{}, file)
}
func (c *NativeControl) Ready(id string) error {
	return c.inventoryTransition(id, NativeZeroBound, NativeReady, Identity{}, Identity{}, Identity{})
}

func (c *NativeControl) ReserveIntent(id, digest string) error {
	if err := c.check(); err != nil {
		return err
	}
	if !nativeCopyID(id) || !validNativeDigest(digest) {
		return ErrNativeCopyChanged
	}
	if err := c.root.inventoryWriteHeadroom(); err != nil {
		return err
	}
	tx, err := c.inventory.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var old string
	err = tx.QueryRow(`SELECT intent_sha FROM copies WHERE copy_id=?`, id).Scan(&old)
	if err == nil {
		if old != digest {
			return ErrNativeCopyChanged
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count int
	var metadata int64
	if err := tx.QueryRow(`SELECT COUNT(*),COALESCE(SUM(metadata_bytes),0) FROM copies`).Scan(&count, &metadata); err != nil {
		return err
	}
	if count >= nativeInventoryEntries || metadata > nativeInventoryMetadata-24576 {
		return ErrNativeInventoryLimit
	}
	if _, err := tx.Exec(`INSERT INTO copies(copy_id,intent_sha,metadata_bytes,phase) VALUES(?,?,24576,?)`, id, digest, NativeIntentPending); err != nil {
		return err
	}
	return c.inventoryCommit(tx)
}
func (c *NativeControl) ReserveSelection(id, digest string, size int64) error {
	if err := c.check(); err != nil {
		return err
	}
	if !nativeCopyID(id) || !validNativeDigest(digest) || size < 0 || size > nativeInventorySelectionMax {
		return ErrNativeCopyChanged
	}
	if err := c.root.inventoryWriteHeadroom(); err != nil {
		return err
	}
	tx, err := c.inventory.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var old, phase string
	var oldSize int64
	if err := tx.QueryRow(`SELECT selection_sha,logical_bytes,phase FROM copies WHERE copy_id=?`, id).Scan(&old, &oldSize, &phase); err != nil {
		return err
	}
	if old != "" {
		if old != digest || oldSize != size {
			return ErrNativeCopyChanged
		}
		return nil
	}
	if phase != NativeIntentReady {
		return ErrNativeCopyChanged
	}
	var used int64
	if err := tx.QueryRow(`SELECT COALESCE(SUM(logical_bytes),0) FROM copies`).Scan(&used); err != nil {
		return err
	}
	if size > nativeInventoryLogical-used {
		return ErrNativeInventoryLimit
	}
	if _, err := tx.Exec(`UPDATE copies SET selection_sha=?,logical_bytes=?,phase=? WHERE copy_id=? AND phase=?`, digest, size, NativeSelectionPending, id, NativeIntentReady); err != nil {
		return err
	}
	return c.inventoryCommit(tx)
}
