package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func runFarmEnrollment(t *testing.T, a *API, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/farm/enrollment", strings.NewReader(body))
	rec := httptest.NewRecorder()
	a.handleFarmEnrollment(rec, req)
	return rec
}

func TestFarmEnrollmentFailsClosedWithoutExplicitNodeEndpoint(t *testing.T) {
	a := &API{farmQ: &fakeFarmQ{}}
	rec := runFarmEnrollment(t, a, `{"name":"render-a","role":"render"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestFarmEnrollmentBuildsPersistentExactRolePlan(t *testing.T) {
	a := &API{
		farmQ:           &fakeFarmQ{},
		farmNodeMetaURL: "rediss://worker-user:test-only@nas.example:6379/1",
		farmRenderImage: "registry.example/juicefarm-gpu:rc-test",
	}
	rec := runFarmEnrollment(t, a, `{
		"name":"render-a",
		"role":"render",
		"state_path":"/srv/juice farm/render-a/state",
		"cache_path":"/srv/juice farm/render-a/cache"
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		OK   bool               `json:"ok"`
		Plan farmEnrollmentPlan `json:"plan"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Plan.Role != "render" || response.Plan.Image != a.farmRenderImage {
		t.Fatalf("plan identity = %+v", response.Plan)
	}
	for _, marker := range []string{
		"--restart unless-stopped", "--device /dev/fuse", "--device /dev/dri",
		"JM_WORKER_NAME=render-a", "JM_WORKER_ROLE=render",
		"JM_FARM_KINDS=derivatives,proxy,transcript", "JM_FARM_VCODEC=hevc_vaapi",
		"'/srv/juice farm/render-a/state:/state'", "'/srv/juice farm/render-a/cache:/jfs-cache'",
	} {
		if !strings.Contains(response.Plan.DockerCommand, marker) {
			t.Errorf("docker plan missing %q", marker)
		}
	}
	if response.Plan.LinkCommand != "" {
		t.Fatal("local enrollment unexpectedly minted Link credentials")
	}
}

func TestFarmEnrollmentRejectsUnsafeOrAmbiguousInputs(t *testing.T) {
	a := &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: "redis://nas.example:6379/1"}
	for _, body := range []string{
		`{"name":"../render","role":"render"}`,
		`{"name":"render-a","role":"gpu-ish"}`,
		`{"name":"render-a","role":"render","state_path":"relative","cache_path":"/cache"}`,
		`{"name":"render-a","role":"render","state_path":"/same","cache_path":"/same"}`,
		`{"name":"render-a","role":"render","state_path":"/","cache_path":"/cache"}`,
	} {
		rec := runFarmEnrollment(t, a, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q status = %d: %s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestShellJoinQuotesMetacharacters(t *testing.T) {
	got := shellJoin([]string{"docker", "-e", "TOKEN=a b'c", "image;bad"})
	if got != `docker -e 'TOKEN=a b'"'"'c' 'image;bad'` {
		t.Fatalf("shellJoin = %q", got)
	}
}
