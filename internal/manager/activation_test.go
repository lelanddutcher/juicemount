package manager

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func doJSONHandler(t *testing.T, h http.HandlerFunc, body any) *httptest.ResponseRecorder {
	t.Helper()
	method := http.MethodGet
	var buf bytes.Buffer
	if body != nil {
		method = http.MethodPost
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, "/", &buf)
	rr := httptest.NewRecorder()
	h(rr, req)
	return rr
}

// TestH8BModeSwitch: the manager switches the proxy between passthrough,
// observe and enforce by writing jmlease:v1:cfg (through the admin
// principal). Enforce needs an explicit confirmation; bad modes are refused.
func TestH8BModeSwitch(t *testing.T) {
	fake := &fakeLeaseClient{mode: "passthrough"}
	a := &activation{store: fake}
	rr := doJSONHandler(t, a.handleMode, nil)
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"passthrough"`)) {
		t.Fatalf("get mode: %d %s", rr.Code, rr.Body)
	}
	for _, bad := range []string{"", "anon", "ENFORCE!", "off"} {
		rr = doJSONHandler(t, a.handleMode, map[string]string{"mode": bad})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("mode %q accepted: %d", bad, rr.Code)
		}
	}
	rr = doJSONHandler(t, a.handleMode, map[string]string{"mode": "observe"})
	if rr.Code != http.StatusOK || fake.mode != "observe" {
		t.Fatalf("observe: %d %s mode=%s", rr.Code, rr.Body, fake.mode)
	}
	rr = doJSONHandler(t, a.handleMode, map[string]string{"mode": "enforce"})
	if rr.Code != http.StatusBadRequest || fake.mode != "observe" {
		t.Fatalf("enforce without confirm: %d mode=%s, want 400 and unchanged", rr.Code, fake.mode)
	}
	rr = doJSONHandler(t, a.handleMode, map[string]string{"mode": "enforce", "confirm": "enforce"})
	if rr.Code != http.StatusOK || fake.mode != "enforce" {
		t.Fatalf("enforce with confirm: %d %s mode=%s", rr.Code, rr.Body, fake.mode)
	}
	// Rollback steps are never gated.
	rr = doJSONHandler(t, a.handleMode, map[string]string{"mode": "passthrough"})
	if rr.Code != http.StatusOK || fake.mode != "passthrough" {
		t.Fatalf("passthrough rollback: %d mode=%s", rr.Code, fake.mode)
	}
}
