package enrollclient

// Review 5 X3: a principal store copied onto a Manager that already started
// keeps the ID its Macs are bound to, so an approved Mac pointed at the new
// address keeps its request.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// X3: migration to a new NAS. Manager B started once (it minted its own ID
// file), then the admin copies Manager A's principal store onto it and
// points the Mac at B's address. The Mac keeps its approved request.
func TestReview5MigratedStoreKeepsApprovedMac(t *testing.T) {
	ma := newRestartableManager(t)
	srvA := httptest.NewServer(ma.start())
	defer srvA.Close()
	id := Identity{InstanceID: instanceA, Host: "mac", DeviceDigest: digestMac}
	r := enrollApproved(t, srvA.URL, id)
	admin(t, srvA, "approve", principalA)
	r = Step(context.Background(), http.DefaultClient, srvA.URL, id, r.State)
	if r.Phase != Approved {
		t.Fatalf("setup %s", r.Phase)
	}
	mb := newRestartableManager(t)
	mb.start() // B's first start mints B's ID file
	raw, err := os.ReadFile(filepath.Join(ma.dir, "principals-store.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mb.dir, "principals-store.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	srvB := httptest.NewServer(mb.start())
	defer srvB.Close()
	st := r.State
	var steps []string
	var last Result
	for i := 0; i < 3; i++ {
		last = Step(context.Background(), http.DefaultClient, srvB.URL, id, st)
		steps = append(steps, string(last.Phase)+":"+last.Message)
		st = last.State
	}
	if st.Secret != r.State.Secret || last.Phase != Approved {
		t.Fatalf("X3: a store copied onto a Manager that had started once lost the approved Mac's enrollment; steps %q", steps)
	}
}
