package nfs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func privateBirthFixture(t *testing.T) (*SpoolStore, *metadata.Store) {
	t.Helper()
	db, err := metadata.Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		t.Fatal(err)
	}
	index := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(index)
	spool, err := NewSpoolStore(filepath.Join(t.TempDir(), "spool"), 0, index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Stop() })
	return spool, db
}

func TestPrivateBirthCustodyChildExitRecovery(t *testing.T) {
	const body = "child seed body"
	if mode := os.Getenv("JM_TEST_PRIVATE_BIRTH_CHILD"); mode != "" {
		db, err := metadata.Open(os.Getenv("JM_TEST_PRIVATE_BIRTH_DB"))
		if err != nil {
			t.Fatal(err)
		}
		if err := metadata.InitSpoolSchema(db.DB()); err != nil {
			t.Fatal(err)
		}
		index := metadata.NewSpoolStore(db.DB())
		db.SetSpoolStore(index)
		spool, err := NewSpoolStore(os.Getenv("JM_TEST_PRIVATE_BIRTH_ROOT"), 0, index)
		if err != nil {
			t.Fatal(err)
		}
		c, err := spool.PreparePrivateBirth("child.mov", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if mode == "committed" {
			expected, err := db.CaptureMirror("child.mov")
			if err != nil {
				t.Fatal(err)
			}
			entry := metadata.MakeEntry("child.mov", false, int64(len(body)), time.Unix(1730000000, 123456789), 77)
			entry.Mode = 0600
			entry.MtimeNsecKnown = true
			if _, _, err := c.CommitPrivateBirth(db, expected, entry); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(47) // actual process death with FDs/DB still open
	}
	for _, mode := range []string{"prepared", "committed"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			root, dbPath := filepath.Join(base, "spool"), filepath.Join(base, "mirror.db")
			childCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestPrivateBirthCustodyChildExitRecovery$")
			cmd.Env = append(os.Environ(), "JM_TEST_PRIVATE_BIRTH_CHILD="+mode, "JM_TEST_PRIVATE_BIRTH_ROOT="+root, "JM_TEST_PRIVATE_BIRTH_DB="+dbPath)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 47 {
				t.Fatalf("child exit=%v output=%s", err, output)
			}
			entries, err := os.ReadDir(filepath.Join(root, privateBirthIntentDir))
			if err != nil || len(entries) != 1 {
				t.Fatalf("durable intents=(%v,%v)", entries, err)
			}
			record, err := os.ReadFile(filepath.Join(root, privateBirthIntentDir, entries[0].Name()))
			if err != nil {
				t.Fatal(err)
			}
			var intent struct {
				Intent metadata.SpoolBirthIntent `json:"intent"`
				Size   int64                     `json:"size"`
			}
			if err := json.Unmarshal(record, &intent); err != nil || intent.Intent.BirthID == "" || intent.Size != int64(len(body)) {
				t.Fatalf("durable intent=(%+v,%v)", intent, err)
			}
			if got, err := os.ReadFile(intent.Intent.SpoolFile); err != nil || string(got) != body {
				t.Fatalf("fresh image=(%q,%v)", got, err)
			}
			db, err := metadata.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			index := metadata.NewSpoolStore(db.DB())
			db.SetSpoolStore(index)
			var count int
			if err := db.DB().QueryRow(`SELECT count(*) FROM spool_entries WHERE nfs_path='child.mov'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if (mode == "prepared" && count != 0) || (mode == "committed" && count != 1) {
				t.Fatalf("SQL rows=%d", count)
			}
			if mode == "committed" {
				r, born, err := db.ResolveBoundSpoolBirthByNonce(intent.Intent)
				if err != nil || r.Outcome != metadata.SpoolBirthCommitted || born == nil || born.Size != int64(len(body)) || born.Mode != 0600 || born.Mtime.UnixNano() != time.Unix(1730000000, 123456789).UnixNano() {
					t.Fatalf("nonce resolution=(%+v,%+v,%v)", r, born, err)
				}
			}
			fresh, err := NewSpoolStore(root, 0, index)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Stop()
			fresh.used.Store(37)
			report, err := fresh.RecoverOnBoot(context.Background())
			if !errors.Is(err, ErrPendingPrivateBirth) || report != (RecoveryReport{}) {
				t.Fatalf("fresh recovery=(%+v,%v)", report, err)
			}
			if used, _ := fresh.Capacity(); used != 37 {
				t.Fatalf("fresh recovery dropped capacity=%d", used)
			}
		})
	}
}

func TestPrivateBirthCustodySeedAndBootFence(t *testing.T) {
	spool, db := privateBirthFixture(t)
	body := []byte("seed body, exact")
	c, err := spool.PreparePrivateBirth("seed.mov", body)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got, err := os.ReadFile(c.Intent().SpoolFile); err != nil || string(got) != string(body) {
		t.Fatalf("image=(%q,%v)", got, err)
	}
	if used, _ := spool.Capacity(); used != int64(len(body)) {
		t.Fatalf("reservation=%d", used)
	}
	spool.used.Store(91)
	for i := 0; i < 2; i++ {
		report, err := spool.RecoverOnBoot(context.Background())
		if !errors.Is(err, ErrPendingPrivateBirth) || report != (RecoveryReport{}) {
			t.Fatalf("boot=(%+v,%v)", report, err)
		}
		if used, _ := spool.Capacity(); used != 91 {
			t.Fatalf("boot dropped reservation=%d", used)
		}
		if got := db.LookupByPath("seed.mov"); got != nil {
			t.Fatalf("pre-SQL published mirror=%+v", got)
		}
	}
	expected, err := db.CaptureMirror("seed.mov")
	if err != nil {
		t.Fatal(err)
	}
	receipt, born, err := c.CommitPrivateBirth(db, expected, metadata.MakeEntry("seed.mov", false, int64(len(body)), time.Now(), 42))
	if err != nil || born == nil || born.Size != int64(len(body)) || receipt.Outcome != metadata.SpoolBirthCommitted {
		t.Fatalf("commit=(%+v,%+v,%v)", receipt, born, err)
	}
	row, err := spool.meta.Get(receipt.SpoolID)
	if err != nil || row.Size != int64(len(body)) {
		t.Fatalf("row=(%+v,%v)", row, err)
	}
	if got, err := os.ReadFile(c.Intent().SpoolFile); err != nil || string(got) != string(body) {
		t.Fatalf("committed image=(%q,%v)", got, err)
	}
}

func TestPrivateBirthCustodySyncFailuresRemainPending(t *testing.T) {
	for _, stage := range []string{"image", "intent", "files-directory", "intent-directory"} {
		t.Run(stage, func(t *testing.T) {
			spool, db := privateBirthFixture(t)
			injected := errors.New("injected sync refusal")
			spool.birthSyncFailure = func(got string) error {
				if got == stage {
					return injected
				}
				return nil
			}
			c, err := spool.PreparePrivateBirth("pending.mov", []byte("private bytes"))
			if !errors.Is(err, injected) || c == nil {
				t.Fatalf("prepare=(%v,%v)", c, err)
			}
			defer c.Close()
			if used, _ := spool.Capacity(); used != 13 {
				t.Fatalf("reservation=%d", used)
			}
			if got := db.LookupByPath("pending.mov"); got != nil {
				t.Fatalf("published=%+v", got)
			}
			report, err := spool.RecoverOnBoot(context.Background())
			if err == nil || report != (RecoveryReport{}) {
				t.Fatalf("boot=(%+v,%v)", report, err)
			}
			if used, _ := spool.Capacity(); used != 13 {
				t.Fatalf("boot dropped reservation=%d", used)
			}
		})
	}
}

func TestPrivateBirthCustodyRefusesReplacementAndMalformedIntent(t *testing.T) {
	for _, replacement := range []string{"file", "symlink", "same-size-body", "root", "files-dir", "intent-dir"} {
		t.Run(replacement, func(t *testing.T) {
			spool, db := privateBirthFixture(t)
			c, err := spool.PreparePrivateBirth("guard.mov", []byte("original"))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			switch replacement {
			case "file", "symlink":
				old := c.Intent().SpoolFile + ".held"
				if err := os.Rename(c.Intent().SpoolFile, old); err != nil {
					t.Fatal(err)
				}
				if replacement == "file" {
					err = os.WriteFile(c.Intent().SpoolFile, []byte("successor"), 0600)
				} else {
					err = os.Symlink(old, c.Intent().SpoolFile)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "same-size-body":
				if err := os.WriteFile(c.Intent().SpoolFile, []byte("mutated!"), 0600); err != nil {
					t.Fatal(err)
				}
			case "root":
				old := spool.root + ".held"
				if err := os.Rename(spool.root, old); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(spool.root, 0700); err != nil {
					t.Fatal(err)
				}
			case "files-dir", "intent-dir":
				name := SpoolFilesSubdir
				if replacement == "intent-dir" {
					name = privateBirthIntentDir
				}
				old := filepath.Join(spool.root, name)
				if err := os.Rename(old, old+".held"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(old, 0700); err != nil {
					t.Fatal(err)
				}
			}
			expected, err := db.CaptureMirror("guard.mov")
			if err != nil {
				t.Fatal(err)
			}
			_, born, err := c.CommitPrivateBirth(db, expected, metadata.MakeEntry("guard.mov", false, 8, time.Now(), 5))
			if !errors.Is(err, ErrPendingPrivateBirth) || born != nil {
				t.Fatalf("replacement accepted=(%+v,%v)", born, err)
			}
			if got := db.LookupByPath("guard.mov"); got != nil {
				t.Fatalf("published=%+v", got)
			}
			if used, _ := spool.Capacity(); used != 8 {
				t.Fatalf("reservation=%d", used)
			}
		})
	}
	spool, _ := privateBirthFixture(t)
	dir := filepath.Join(spool.root, privateBirthIntentDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "partial.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	spool.used.Store(29)
	report, err := spool.RecoverOnBoot(context.Background())
	if !errors.Is(err, ErrPendingPrivateBirth) || report != (RecoveryReport{}) {
		t.Fatalf("malformed intent boot=(%+v,%v)", report, err)
	}
	if used, _ := spool.Capacity(); used != 29 {
		t.Fatalf("malformed intent dropped reservation=%d", used)
	}
}

func TestPrivateBirthCustodyFreshBootBeforeSQL(t *testing.T) {
	spool, db := privateBirthFixture(t)
	body := []byte("fresh process seed")
	var order []string
	spool.birthSyncFailure = func(stage string) error { order = append(order, stage); return nil }
	c, err := spool.PreparePrivateBirth("fresh.mov", body)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"root", "image", "intent", "files-directory", "intent-directory"}
	if len(order) != len(want) {
		t.Fatalf("sync order=%v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("sync order=%v", order)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := NewSpoolStore(spool.root, 0, spool.meta)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Stop()
	fresh.used.Store(37)
	report, err := fresh.RecoverOnBoot(context.Background())
	if !errors.Is(err, ErrPendingPrivateBirth) || report != (RecoveryReport{}) {
		t.Fatalf("fresh boot=(%+v,%v)", report, err)
	}
	if used, _ := fresh.Capacity(); used != 37 {
		t.Fatalf("fresh boot dropped reservation=%d", used)
	}
	if got, err := os.ReadFile(c.Intent().SpoolFile); err != nil || string(got) != string(body) {
		t.Fatalf("fresh source=(%q,%v)", got, err)
	}
	if got := db.LookupByPath("fresh.mov"); got != nil {
		t.Fatalf("pre-SQL mirror=%+v", got)
	}
}

func TestPrivateBirthCustodyZeroSeedPreservesAttributes(t *testing.T) {
	spool, db := privateBirthFixture(t)
	c, err := spool.PreparePrivateBirth("empty.mov", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if used, _ := spool.Capacity(); used != 0 {
		t.Fatalf("zero seed reservation=%d", used)
	}
	expected, err := db.CaptureMirror("empty.mov")
	if err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1730000000, 987654321)
	entry := metadata.MakeEntry("empty.mov", false, 0, mtime, 11)
	entry.Mode = 0600
	entry.MtimeNsecKnown = true
	r, born, err := c.CommitPrivateBirth(db, expected, entry)
	if err != nil || born == nil || born.Size != 0 || born.Mode != 0600 || born.Mtime.UnixNano() != mtime.UnixNano() {
		t.Fatalf("zero birth=(%+v,%+v,%v)", r, born, err)
	}
	row, err := spool.meta.Get(r.SpoolID)
	if err != nil || row.Size != 0 {
		t.Fatalf("zero row=(%+v,%v)", row, err)
	}
	if fi, err := os.Lstat(c.Intent().SpoolFile); err != nil || !fi.Mode().IsRegular() || fi.Size() != 0 {
		t.Fatalf("zero image=(%+v,%v)", fi, err)
	}
}

func TestPrivateBirthImageOnlyBeforeIntentKeepsCapacity(t *testing.T) {
	spool, db := privateBirthFixture(t)
	path := filepath.Join(spool.root, SpoolFilesSubdir, "birth-unfinished")
	if err := os.WriteFile(path, []byte("partial seed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(spool.root, privateBirthIntentDir), 0700); err != nil {
		t.Fatal(err)
	}
	fresh, err := NewSpoolStore(spool.root, 0, spool.meta)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Stop()
	fresh.used.Store(28)
	report, err := fresh.RecoverOnBoot(context.Background())
	if !errors.Is(err, ErrSpoolOrphanBytes) || report != (RecoveryReport{}) {
		t.Fatalf("image-only recovery=(%+v,%v)", report, err)
	}
	if used, _ := fresh.Capacity(); used != 28 {
		t.Fatalf("image-only dropped capacity=%d", used)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "partial seed" {
		t.Fatalf("image-only bytes=(%q,%v)", got, err)
	}
	if got := db.LookupByPath("unfinished.mov"); got != nil {
		t.Fatalf("image-only published mirror=%+v", got)
	}
}

func TestPrivateBirthCustodyRejectsLossyIntentPaths(t *testing.T) {
	invalid := string([]byte{0xff})
	spool, _ := privateBirthFixture(t)
	for _, path := range []string{invalid, "../escape", "/absolute"} {
		if c, err := spool.PreparePrivateBirth(path, []byte("seed")); err == nil || c != nil {
			t.Fatalf("invalid path %q accepted=(%v,%v)", path, c, err)
		}
	}
	originalRoot := spool.root
	spool.root = originalRoot + invalid
	if c, err := spool.PreparePrivateBirth("valid.mov", []byte("seed")); err == nil || c != nil {
		t.Fatalf("invalid root accepted=(%v,%v)", c, err)
	}
	spool.root = originalRoot
	if used, _ := spool.Capacity(); used != 0 {
		t.Fatalf("invalid intents reserved=%d", used)
	}
	entries, err := os.ReadDir(filepath.Join(originalRoot, SpoolFilesSubdir))
	if err != nil || len(entries) != 0 {
		t.Fatalf("invalid intents wrote files=(%v,%v)", entries, err)
	}
}
