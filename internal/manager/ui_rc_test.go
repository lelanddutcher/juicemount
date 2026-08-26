package manager

import (
	"strings"
	"testing"
)

func TestManagerRCUIKeepsAccessibilityAndBoundedErrors(t *testing.T) {
	cssRaw, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(cssRaw)
	for _, marker := range []string{
		"--focus-ring:",
		"outline: 2px solid var(--focus-ring)",
		"@media (prefers-reduced-motion: reduce)",
		"min-height: 2.25rem",
	} {
		if !strings.Contains(css, marker) {
			t.Errorf("Manager CSS lost RC accessibility contract %q", marker)
		}
	}

	appRaw, err := staticFS.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	app := string(appRaw)
	for _, marker := range []string{
		"function compactHTTPError",
		"detail.length > 240",
		"r.headers.get('content-type')",
	} {
		if !strings.Contains(app, marker) {
			t.Errorf("Manager client lost bounded transport-error behavior %q", marker)
		}
	}
}
