package credstore

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// saveFile writes an isolated-format entry to path as a private 0600 file in
// a 0700 directory, replacing it atomically (a reader sees the old entry or
// the new one, never part of one).
func saveFile(path string, e isolatedEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".credential-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // after the rename there is nothing to remove
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
