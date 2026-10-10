package juicefarm

import (
	"os"
	"strings"
	"testing"
)

func TestWorkerImagesAndEntrypointKeepEncodeScratchNodeLocal(t *testing.T) {
	for name, src := range map[string]string{
		"CPU": dockerfile(t, "Dockerfile"),
		"GPU": dockerfile(t, "../juicefarm-gpu/Dockerfile"),
	} {
		if !strings.Contains(src, "ENV JM_FARM_ENCODE_SCRATCH=/tmp") {
			t.Fatalf("%s worker image does not default encode scratch to node-local /tmp", name)
		}
	}

	b, err := os.ReadFile("entrypoint.sh")
	if err != nil {
		t.Fatal(err)
	}
	entrypoint := string(b)
	if !strings.Contains(entrypoint, `ENCODE_SCRATCH="${JM_FARM_ENCODE_SCRATCH:-/tmp}"`) {
		t.Fatal("worker entrypoint does not fail safe to node-local /tmp")
	}
	if strings.Count(entrypoint, `-encode-scratch "$ENCODE_SCRATCH"`) != 2 {
		t.Fatal("worker entrypoint must pass encode scratch to both sweep and queue execution")
	}
}
