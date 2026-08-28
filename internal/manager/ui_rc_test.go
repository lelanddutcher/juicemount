package manager

import (
	"math"
	"regexp"
	"strconv"
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
		"--control-border: rgba(27, 16, 42, 0.5)",
		"--control-border: rgba(250, 253, 232, 0.42)",
		"outline: 2px solid var(--focus-ring)",
		"textarea { border-color: var(--control-border); }",
		"@media (prefers-reduced-motion: reduce)",
		"min-height: 2.25rem",
		".sidebar-list a { min-height: 2.75rem",
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
		"a.setAttribute('aria-current', 'page')",
		"a.removeAttribute('aria-current')",
	} {
		if !strings.Contains(app, marker) {
			t.Errorf("Manager client lost bounded transport-error behavior %q", marker)
		}
	}
}

func TestManagerRCFarmHistoryAndAuthStayTruthful(t *testing.T) {
	indexRaw, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		t.Fatal(err)
	}
	index := string(indexRaw)
	for _, marker := range []string{
		`id="farm-jobs-summary"`,
		`id="farm-jobs-more"`,
		`id="farm-jobs-collapse"`,
		`Authentication not yet verified`,
		`id="auth-dialog"`,
		`aria-labelledby="auth-dialog-title"`,
		`id="auth-key-input"`,
		`id="auth-dialog-status" class="auth-dialog-status" role="alert"`,
		`id="auth-key-button"`,
	} {
		if !strings.Contains(index, marker) {
			t.Errorf("Manager RC markup lost truthful/progressive UI marker %q", marker)
		}
	}
	if strings.Contains(index, "No auth required") {
		t.Fatal("Manager footer claims authentication is disabled before the server verifies it")
	}

	appRaw, err := staticFS.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	app := string(appRaw)
	for _, marker := range []string{
		"const FARM_JOBS_PAGE_SIZE = 12",
		"authPromptDeclined",
		"authRequestPromise",
		"function requestAdminKey",
		"if (authRequestPromise) return authRequestPromise",
		"Admin key verified",
		"Authentication disabled",
		"CPU H.264 fallback",
		"split from ",
		"partial: 'Partial'",
		"Recovery note: ",
		"function renderFarmInterruptedSentence",
		"It is not reported as running",
		"sweepTxt = 'paused'",
		"sweepTxt = 'interrupted — '",
	} {
		if !strings.Contains(app, marker) {
			t.Errorf("Manager client lost truthful/progressive behavior %q", marker)
		}
	}
	if strings.Contains(app, "prompt(") {
		t.Fatal("Manager client must use the accessible in-page auth dialog, not prompt()")
	}

	cssRaw, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(cssRaw)
	for _, marker := range []string{
		"min-height: 2.75rem",
		".auth-dialog::backdrop",
		".auth-dialog-actions button[type=\"submit\"]",
		".farm-job-chip.partial",
		".farm-job-count.fallback",
		".farm-job-notice",
		".farm-jobs-pagination",
		".farm-config-status.ok { color: var(--success); }",
	} {
		if !strings.Contains(css, marker) {
			t.Errorf("Manager CSS lost Farm RC behavior %q", marker)
		}
	}
}

func TestManagerRCFarmNodeLifecycleAndEnrollmentSurface(t *testing.T) {
	indexRaw, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		t.Fatal(err)
	}
	index := string(indexRaw)
	for _, marker := range []string{
		`id="farm-enrollment"`,
		`id="farm-enrollment-form"`,
		`id="farm-enroll-remote"`,
		`id="farm-enroll-docker-command"`,
		`role="status"`,
		`Credentials are shown once and this response is never cached.`,
	} {
		if !strings.Contains(index, marker) {
			t.Errorf("Manager Farm enrollment markup lost %q", marker)
		}
	}

	appRaw, err := staticFS.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	app := string(appRaw)
	for _, marker := range []string{
		"/api/farm/enrollment",
		"/api/farm/workers/control",
		"Restart verified · fresh heartbeat online",
		"no replacement heartbeat appeared within 45 seconds",
		"navigator.clipboard.writeText",
	} {
		if !strings.Contains(app, marker) {
			t.Errorf("Manager Farm lifecycle client lost %q", marker)
		}
	}

	cssRaw, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(cssRaw)
	for _, marker := range []string{
		".farm-enrollment-card",
		".farm-worker-actions",
		".farm-worker-action-status.error",
		".farm-command-block code",
		"white-space: pre-wrap",
	} {
		if !strings.Contains(css, marker) {
			t.Errorf("Manager Farm lifecycle CSS lost %q", marker)
		}
	}
}

func TestManagerRCThemeTokensMeetWCAGContrast(t *testing.T) {
	cssRaw, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(cssRaw)
	base := cssBlock(t, css, ":root")
	darkMedia := cssBlock(t, css, "@media (prefers-color-scheme: dark)")
	dark := cssBlock(t, darkMedia, ":root")

	tests := []struct {
		name       string
		blocks     []string
		foreground string
		background string
		minimum    float64
	}{
		{name: "light text", blocks: []string{base}, foreground: "--text", background: "--bg", minimum: 4.5},
		{name: "light muted", blocks: []string{base}, foreground: "--text-muted", background: "--bg", minimum: 4.5},
		{name: "light accent", blocks: []string{base}, foreground: "--accent-text", background: "--bg", minimum: 4.5},
		{name: "light danger", blocks: []string{base}, foreground: "--danger", background: "--bg", minimum: 4.5},
		{name: "light warning", blocks: []string{base}, foreground: "--running", background: "--bg", minimum: 4.5},
		{name: "light success", blocks: []string{base}, foreground: "--success", background: "--bg", minimum: 4.5},
		{name: "light focus", blocks: []string{base}, foreground: "--focus-ring", background: "--bg", minimum: 3},
		{name: "dark text", blocks: []string{dark, base}, foreground: "--text", background: "--bg", minimum: 4.5},
		{name: "dark accent", blocks: []string{dark, base}, foreground: "--accent-text", background: "--bg", minimum: 4.5},
		{name: "dark danger", blocks: []string{dark, base}, foreground: "--danger", background: "--bg", minimum: 4.5},
		{name: "dark warning", blocks: []string{dark, base}, foreground: "--running", background: "--bg", minimum: 4.5},
		{name: "dark success", blocks: []string{dark, base}, foreground: "--success", background: "--bg", minimum: 4.5},
		{name: "dark focus", blocks: []string{dark, base}, foreground: "--focus-ring", background: "--bg", minimum: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fg := cssResolvedHexToken(t, tc.blocks, tc.foreground)
			bg := cssResolvedHexToken(t, tc.blocks, tc.background)
			if got := contrastRatio(fg, bg); got < tc.minimum {
				t.Fatalf("contrast %s on %s = %.2f:1, want at least %.1f:1", fg, bg, got, tc.minimum)
			}
		})
	}
}

func cssBlock(t *testing.T, source, marker string) string {
	t.Helper()
	start := strings.Index(source, marker)
	if start < 0 {
		t.Fatalf("CSS marker %q not found", marker)
	}
	openRel := strings.Index(source[start:], "{")
	if openRel < 0 {
		t.Fatalf("CSS marker %q has no block", marker)
	}
	open := start + openRel
	depth := 0
	for i := open; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[open+1 : i]
			}
		}
	}
	t.Fatalf("CSS marker %q has an unterminated block", marker)
	return ""
}

func cssResolvedHexToken(t *testing.T, blocks []string, name string) string {
	t.Helper()
	seen := make(map[string]bool)
	for {
		if seen[name] {
			t.Fatalf("CSS token cycle at %s", name)
		}
		seen[name] = true
		var value string
		for _, block := range blocks {
			re := regexp.MustCompile(regexp.QuoteMeta(name) + `\s*:\s*([^;]+);`)
			if match := re.FindStringSubmatch(block); len(match) == 2 {
				value = strings.TrimSpace(match[1])
				break
			}
		}
		if value == "" {
			t.Fatalf("CSS token %s not found", name)
		}
		if strings.HasPrefix(value, "#") {
			if len(value) != 7 {
				t.Fatalf("CSS token %s = %q, want six-digit hex", name, value)
			}
			return value
		}
		ref := regexp.MustCompile(`^var\((--[-a-z0-9]+)\)$`).FindStringSubmatch(value)
		if len(ref) != 2 {
			t.Fatalf("CSS token %s = %q, want hex or direct var reference", name, value)
		}
		name = ref[1]
	}
}

func contrastRatio(a, b string) float64 {
	la, lb := relativeLuminance(a), relativeLuminance(b)
	if la < lb {
		la, lb = lb, la
	}
	return (la + 0.05) / (lb + 0.05)
}

func relativeLuminance(hex string) float64 {
	channel := func(offset int) float64 {
		value, err := strconv.ParseUint(hex[offset:offset+2], 16, 8)
		if err != nil {
			panic(err)
		}
		c := float64(value) / 255
		if c <= 0.04045 {
			return c / 12.92
		}
		return math.Pow((c+0.055)/1.055, 2.4)
	}
	return 0.2126*channel(1) + 0.7152*channel(3) + 0.0722*channel(5)
}
