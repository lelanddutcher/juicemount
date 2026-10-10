package juicemountmanager

import (
	"os"
	"strings"
	"testing"
)

func TestManagerDoesNotExposeAdminKeyInArgv(t *testing.T) {
	for _, name := range []string{"Dockerfile", "Dockerfile.binary-only"} {
		t.Run(name, func(t *testing.T) {
			src, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}

			// The binary already defaults --admin-key from JM_ADMIN_KEY.
			// Expanding the secret into the entrypoint command copies it into
			// /proc/<pid>/cmdline and docker top output, where it is no longer
			// an environment-only credential.
			if strings.Contains(string(src), "--admin-key=") {
				t.Fatalf("%s must keep JM_ADMIN_KEY out of process arguments", name)
			}
			if strings.Contains(string(src), `-H "X-JuiceMount-Admin-Key: $JM_ADMIN_KEY"`) {
				t.Fatalf("%s must stream the healthcheck auth header over stdin instead of placing JM_ADMIN_KEY in curl argv", name)
			}
			if !strings.Contains(string(src), "curl --config -") {
				t.Fatalf("%s healthcheck must read its auth header from stdin", name)
			}
		})
	}
}

func TestManagerImageRequiresExactMultiArchReleaseIdentity(t *testing.T) {
	src, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, forbidden := range []string{"ARG JM_COMMIT=unknown", "ARG JM_VERSION=0.5.0", "headscale_0.29.3_linux_amd64"} {
		if strings.Contains(s, forbidden) {
			t.Fatalf("manager image retains non-release or single-architecture value %q", forbidden)
		}
	}
	for _, required := range []string{
		`grep -Eq '^[0-9a-f]{40}$'`,
		`juicemount-manager --build-info`,
		"ARG TARGETARCH",
		"linux_${TARGETARCH}",
	} {
		if !strings.Contains(s, required) {
			t.Fatalf("manager image is missing release contract %q", required)
		}
	}
}
