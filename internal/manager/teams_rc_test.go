package manager

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Teams previously looked functional while its sessions and roles were not
// consumed by any Manager or JuiceFS authorization path. The RC must not
// expose those routes or navigation until the complete security model lands.
func TestTeamsSurfaceWithheldFromRC(t *testing.T) {
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
	})
	defer mgr.StopAll()

	for _, target := range []string{"/api/users", "/api/users/alice", "/api/auth/login"} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want teams route withheld (404)", target, rec.Code)
		}
	}

	index, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(index), `href="#/teams"`) {
		t.Fatal("Teams navigation is visible in the RC UI")
	}
	app, err := staticFS.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(app), "'teams',") {
		t.Fatal("Teams is still a routable UI tab")
	}
}
