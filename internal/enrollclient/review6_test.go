package enrollclient

// Review 6: the fingerprint proves only that a server knows this Mac's
// secret, and every Redis endpoint the Mac logs in to receives that secret.
// So a "revoked" answer, the one outcome that deletes data, is taken from a
// bound enrollment only when it carries the bound Manager ID.

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// harvester answers at the Manager's address with the fingerprint computed
// from a secret it read off the Mac's Redis AUTH. It does not know the
// Manager ID.
func harvester(secret, id, status string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req enrollRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if id != "" {
			w.Header().Set(ManagerIDHeader, id)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"manager_id": id, "status": status, "principal": PrincipalFor(req.InstanceID),
			"fingerprint": Fingerprint(secret),
		})
	})
}

// A1c: a harvested fingerprint with no ID, or with another ID, must not
// revoke an approved, bound Mac.
func TestReview6HarvestedFingerprintCannotRevoke(t *testing.T) {
	for _, otherID := range []string{"", strings.Repeat("c", 32)} {
		rec, srv, id, st := boundApprovedMac(t)
		rec.set(harvester(st.Secret, otherID, "revoked"))
		n := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
		if n.Phase != Failed || n.State.Status != "approved" || n.State.ManagerID != st.ManagerID || n.Persist {
			t.Fatalf("A1c (id %q): a harvested fingerprint revoked an approved, bound Mac: phase=%s status=%s id=%s persist=%v",
				short(otherID), n.Phase, n.State.Status, short(n.State.ManagerID), n.Persist)
		}
	}
}

// The real Manager's revoke (it carries the bound ID) still revokes.
func TestReview6BoundManagerRevokeStillRevokes(t *testing.T) {
	_, srv, id, st := boundApprovedMac(t)
	admin(t, srv, "revoke", principalA)
	n := Step(context.Background(), http.DefaultClient, srv.URL, id, st)
	if n.Phase != Revoked || n.State.Status != "revoked" {
		t.Fatalf("the bound Manager's revoke was not taken: phase=%s status=%s msg=%s", n.Phase, n.State.Status, n.Message)
	}
}
