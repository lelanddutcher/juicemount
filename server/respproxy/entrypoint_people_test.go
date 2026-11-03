package respproxydeploy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// TestEntrypointKeepsPeopleAndConfiguresAudit runs the entrypoint against a
// version-2 principals file the Manager wrote: the re-rendered file keeps
// the format version and the people (dropping them would lift every grant
// until the Manager next writes the file) and the proxy can read it; the
// proxy config names the audit directory beside the principals file.
func TestEntrypointKeepsPeopleAndConfiguresAudit(t *testing.T) {
	for _, tool := range []string{"jq", "flock"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not on PATH (the proxy image has it)", tool)
		}
	}
	root := t.TempDir()
	dir, run := filepath.Join(root, "principals"), filepath.Join(root, "run")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	h, err := respproxy.HashPassword("synthetic", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager := fmt.Sprintf(`{"version":2,"principals":[{"name":"mac-0123456789ab","kind":"mac","host":"bay","argon2id":%q},`+
		`{"name":"farm","kind":"farm","argon2id":%q}],`+
		`"people":[{"id":"p-john","name":"John","access":"granted","grants":["/Clients/A"],"devices":["mac-0123456789ab"]}]}`, h, h)
	if err := os.WriteFile(filepath.Join(dir, "principals.json"), []byte(manager), 0o600); err != nil {
		t.Fatal(err)
	}
	// The fake proxy binary: `hash` prints a valid PHC string, the proxy
	// run copies its config out and exits.
	fake := filepath.Join(root, "proxy")
	script := "#!/bin/sh\nif [ \"$1\" = hash ]; then cat >/dev/null; echo '" + h + "'; exit 0; fi\n" +
		"cp \"$2\" \"" + filepath.Join(root, "config.out") + "\"\n"
	if err := os.WriteFile(fake, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "entrypoint.sh")
	secret := strings.Repeat("s", 40)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "JM_RESPPROXY_DIR=" + dir, "JM_RESPPROXY_RUN_DIR=" + run,
		"JM_RESPPROXY_BIN=" + fake, "JM_RESPPROXY_UPSTREAM_PASSWORD=" + secret}
	for _, k := range []string{"FARM", "MANAGER", "BG", "WEBDAV", "ADMIN"} {
		cmd.Env = append(cmd.Env, "JM_PROXY_"+k+"_PASSWORD="+secret)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("entrypoint: %v\n%s", err, out)
	}
	ps, err := respproxy.LoadPrincipals(filepath.Join(dir, "principals.json"))
	if err != nil {
		t.Fatalf("the proxy refuses the re-rendered file: %v", err)
	}
	if p := ps.PersonOf("mac-0123456789ab"); p == nil || !p.Scoped || len(p.Grants) != 1 || p.Grants[0] != "/Clients/A" {
		t.Fatalf("person after re-render: %+v", p)
	}
	if ps.Len() != 6 {
		t.Fatalf("principals after re-render: %d, want 5 server + 1 Mac", ps.Len())
	}
	var cfg struct {
		AuditDir string `json:"audit_dir"`
	}
	b, err := os.ReadFile(filepath.Join(root, "config.out"))
	if err != nil || json.Unmarshal(b, &cfg) != nil || cfg.AuditDir != filepath.Join(dir, "audit") {
		t.Fatalf("config audit_dir = %q (%v)\n%s", cfg.AuditDir, err, b)
	}
}
