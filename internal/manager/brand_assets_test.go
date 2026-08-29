package manager

import (
	"net/http/httptest"
	"testing"
)

func TestManagerBrandAssetsAreEmbedded(t *testing.T) {
	for _, name := range []string{
		"static/logo-color.svg",
		"static/fonts/Manrope.woff2",
		"static/fonts/SpaceGrotesk.woff2",
		"static/fonts/JetBrainsMono.woff2",
	} {
		data, err := staticFS.ReadFile(name)
		if err != nil {
			t.Fatalf("read embedded %s: %v", name, err)
		}
		if len(data) == 0 {
			t.Fatalf("embedded %s is empty", name)
		}
	}
}

func TestManagerBrandAssetContentTypes(t *testing.T) {
	for _, tc := range []struct {
		path string
		want string
	}{
		{path: "/logo-color.svg", want: "image/svg+xml"},
		{path: "/fonts/Manrope.woff2", want: "font/woff2"},
	} {
		req := httptest.NewRequest("GET", tc.path, nil)
		res := httptest.NewRecorder()
		(&API{}).handleStatic(res, req)
		if res.Code != 200 {
			t.Fatalf("GET %s: status %d", tc.path, res.Code)
		}
		if got := res.Header().Get("Content-Type"); got != tc.want {
			t.Fatalf("GET %s: content type %q, want %q", tc.path, got, tc.want)
		}
		if got := res.Header().Get("Cache-Control"); got != "no-cache" {
			t.Fatalf("GET %s: cache control %q, want no-cache", tc.path, got)
		}
		if got := res.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("GET %s: X-Content-Type-Options %q, want nosniff", tc.path, got)
		}
	}
}
