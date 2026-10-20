package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func frozenPreferenceV3(t *testing.T) (string, []byte) {
	t.Helper()
	script, err := os.ReadFile("testdata/continuity-c03c-v3-fixture.sql")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile("testdata/continuity-c03c-v3-fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		data []byte
		sha  string
	}{{script, "d3e7d504c9b6c3f9cb8416386a244eac2587eaaa4802d5a2436bcf70c03c9e43"}, {manifest, "8327c516277a69bf044ddafe848146d634283ef334c747cc1f93391994c9a5d1"}} {
		sum := sha256.Sum256(v.data)
		if hex.EncodeToString(sum[:]) != v.sha {
			t.Fatal("frozenv3 fixture bytes changed")
		}
	}
	path := filepath.Join(t.TempDir(), "v3.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(script))
	if err := errors.Join(err, db.Close()); err != nil {
		t.Fatal(err)
	}
	return path, manifest
}

func TestPreferenceFrozenV3UpgradePreservesPortableBytes(t *testing.T) {
	path, manifest := frozenPreferenceV3(t)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Fingerprints map[string]string `json:"schema_fingerprints"`
		Projection   string            `json:"projection_sha256"`
		Rows         int64             `json:"projection_rows"`
	}
	if err := json.Unmarshal(manifest, &fixture); err != nil {
		t.Fatal(err)
	}
	old := authorityOldRows(t, s.db)
	limits := Limits{MaxBytes: 1 << 20, MaxEvents: 100, MaxProjectionRows: 100}
	var before, after bytes.Buffer
	if err := s.Export(context.Background(), testLibrary, &before, limits); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradePreferences(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.version != 4 || !bytes.Equal(old, authorityOldRows(t, s.db)) {
		t.Fatal("upgrade changed old identities/bytes")
	}
	for version, want := range fixture.Fingerprints {
		var got string
		if err := s.db.QueryRow(`SELECT fingerprint FROM schema_migrations WHERE version=?`, version).Scan(&got); err != nil || got != want {
			t.Fatalf("historical fingerprint%s changed: %s %v", version, got, err)
		}
	}
	digest, rows, err := projectionDigest(context.Background(), s.db, testLibrary, 100)
	if err != nil || digest != fixture.Projection || rows != fixture.Rows {
		t.Fatalf("portable3 projection drift: %s %d %v", digest, rows, err)
	}
	if err := s.Export(context.Background(), testLibrary, &after, limits); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.Bytes(), after.Bytes()) {
		t.Fatal("physical4 changed portable2 framing/events/projection")
	}
	control, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || control != (preferenceControl{}) {
		t.Fatalf("upgrade invented private state: %+v %v", control, err)
	}
}

func TestPreferenceUpgradeRefusalAndFinalBarrierUncertainty(t *testing.T) {
	schema2, path2 := newTestStore(t)
	if err := schema2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradePreferences(context.Background(), path2, testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema2 silently chain-upgraded: %v", err)
	}
	path, _ := frozenPreferenceV3(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := UpgradePreferences(context.Background(), path, otherLibrary); err == nil {
		t.Fatal("wrong library admitted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("wrong-library refusal modified DB")
	}
	barrierFailure := errors.New("directory sync failure")
	calls := 0
	barrier := func(string) error {
		calls++
		if calls == 1 {
			return barrierFailure
		}
		return nil
	}
	if err := upgradeCatalogVersion(context.Background(), path, testLibrary, 4, barrier); !errors.Is(err, barrierFailure) {
		t.Fatalf("barrier uncertainty lost: %v", err)
	}
	opened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if opened.version != 4 {
		t.Fatal("uncertain upgrade rolled back committed4")
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := upgradeCatalogVersion(context.Background(), path, testLibrary, 4, barrier); err != nil || calls != 2 {
		t.Fatalf("idempotent4 skipped barrier: %d %v", calls, err)
	}
	if err := UpgradeAuthority(context.Background(), path, testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("old upgrader downgraded4: %v", err)
	}
}
