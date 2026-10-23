package privatefile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func managedRecoveryFixture(t *testing.T) (string, *ManagedRoot, *PreparedFile, continuity.Content) {
	t.Helper()
	path, root := managedTestRoot(t)
	payload := []byte("exact recovery payload")
	p := managedTestPrepared(t, root, managedTestID(81), payload)
	sum := sha256.Sum256(payload)
	return path, root, p, continuity.Content{Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
}

func managedOpenReady(t *testing.T, root *ManagedRoot, p *PreparedFile) (*OwnedReady, error) {
	t.Helper()
	owned, err := root.OpenOwnedReady(p.ID(), p.RootIdentity(), p.AttemptIdentity(), p.LockIdentity(), p.FileIdentity())
	if owned != nil {
		t.Cleanup(func() {
			if err := owned.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return owned, err
}

func TestManagedRecoveryIndependentLockAndVerifiedReuse(t *testing.T) {
	_, root, p, content := managedRecoveryFixture(t)
	if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
		t.Fatalf("original exposure: %t %v", exposed, err)
	}
	if _, err := managedOpenReady(t, root, p); err == nil {
		t.Fatal("second independent descriptor acquired live attempt lock")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	owned, err := managedOpenReady(t, root, p)
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.VerifyContent(content); err != nil {
		t.Fatal(err)
	}
	path, err := owned.Path()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, []byte("exact recovery payload")) {
		t.Fatalf("reused bytes: %q %v", got, err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owned.Close(); err != nil {
		t.Fatalf("repeat close changed outcome: %v", err)
	}
	other, err := managedOpenReady(t, root, p)
	if err != nil {
		t.Fatalf("independent acquisition after release: %v", err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedRecoveryRejectsChangedReadyBytesAndEntries(t *testing.T) {
	t.Run("same-size edit", func(t *testing.T) {
		path, root, p, content := managedRecoveryFixture(t)
		if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("expose: %t %v", exposed, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		ready := filepath.Join(path, "ready", p.ID()+".payload")
		if err := os.WriteFile(ready, bytes.Repeat([]byte("x"), int(content.Size)), 0600); err != nil {
			t.Fatal(err)
		}
		owned, err := managedOpenReady(t, root, p)
		if err != nil {
			t.Fatal(err)
		}
		defer owned.Close()
		if err := owned.VerifyContent(content); err == nil {
			t.Fatal("same-size ready edit accepted")
		}
	})
	t.Run("partial and final", func(t *testing.T) {
		path, root, p, _ := managedRecoveryFixture(t)
		if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("expose: %t %v", exposed, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		managedTestFile(t, filepath.Join(path, "attempts", p.ID(), "payload.partial"), []byte("extra partial"))
		if _, err := managedOpenReady(t, root, p); err == nil {
			t.Fatal("simultaneous partial and final accepted")
		}
	})
	t.Run("missing lock", func(t *testing.T) {
		path, root, p, _ := managedRecoveryFixture(t)
		if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("expose: %t %v", exposed, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(path, "attempts", p.ID(), "owner.lock")); err != nil {
			t.Fatal(err)
		}
		if _, err := managedOpenReady(t, root, p); err == nil {
			t.Fatal("missing permanent lock accepted")
		}
	})
	t.Run("replaced lock", func(t *testing.T) {
		path, root, p, _ := managedRecoveryFixture(t)
		if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("expose: %t %v", exposed, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		lock := filepath.Join(path, "attempts", p.ID(), "owner.lock")
		if err := os.Rename(lock, lock+".old"); err != nil {
			t.Fatal(err)
		}
		managedTestFile(t, lock, nil)
		if _, err := managedOpenReady(t, root, p); err == nil {
			t.Fatal("replacement lock accepted")
		}
	})
	for _, kind := range []string{"symlink lock", "hardlinked lock", "wrong-mode lock"} {
		t.Run(kind, func(t *testing.T) {
			path, root, p, _ := managedRecoveryFixture(t)
			if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
				t.Fatalf("expose: %t %v", exposed, err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			lock := filepath.Join(path, "attempts", p.ID(), "owner.lock")
			switch kind {
			case "symlink lock":
				if err := os.Rename(lock, lock+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("owner.lock.old", lock); err != nil {
					t.Fatal(err)
				}
			case "hardlinked lock":
				if err := os.Link(lock, lock+".alias"); err != nil {
					t.Fatal(err)
				}
			case "wrong-mode lock":
				if err := os.Chmod(lock, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := managedOpenReady(t, root, p); err == nil {
				t.Fatalf("%s accepted", kind)
			}
		})
	}
	t.Run("replaced final inode", func(t *testing.T) {
		path, root, p, _ := managedRecoveryFixture(t)
		if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("expose: %t %v", exposed, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		ready := filepath.Join(path, "ready", p.ID()+".payload")
		if err := os.Rename(ready, ready+".old"); err != nil {
			t.Fatal(err)
		}
		managedTestFile(t, ready, []byte("exact recovery payload"))
		if _, err := managedOpenReady(t, root, p); err == nil {
			t.Fatal("replacement ready inode accepted")
		}
	})
	t.Run("constructor failure releases independently acquired lock", func(t *testing.T) {
		path, root, p, _ := managedRecoveryFixture(t)
		if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("expose: %t %v", exposed, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		ready := filepath.Join(path, "ready", p.ID()+".payload")
		backup := ready + ".held"
		if err := os.Rename(ready, backup); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := os.Stat(backup); err == nil {
				if err := os.Rename(backup, ready); err != nil {
					t.Error(err)
				}
			}
		})
		if _, err := managedOpenReady(t, root, p); err == nil {
			t.Fatal("missing final accepted after acquiring recovery lock")
		}
		if err := os.Rename(backup, ready); err != nil {
			t.Fatal(err)
		}
		owned, err := managedOpenReady(t, root, p)
		if err != nil {
			t.Fatalf("failed constructor retained ownership: %v", err)
		}
		if err := owned.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
