package respproxydeploy

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStageBirthEntrypointGuardsBeforeSecretsAndWrites(t *testing.T) {
	entry := readFile(t, "entrypoint.sh")
	if !strings.Contains(entry, `--argjson stage_birth "$STAGE_BIRTH"`) ||
		!strings.Contains(entry, `stage_birth: $stage_birth`) {
		t.Fatal("entrypoint must render stage_birth as a JSON boolean")
	}
	for _, tc := range []struct {
		name, mode, birth, want string
		setBirth                bool
	}{
		{"unset-default", "passthrough", "", "JM_RESPPROXY_UPSTREAM_PASSWORD must be set", false},
		{"empty-default", "passthrough", "", "JM_RESPPROXY_UPSTREAM_PASSWORD must be set", true},
		{"explicit-off", "passthrough", "false", "JM_RESPPROXY_UPSTREAM_PASSWORD must be set", true},
		{"valid-opt-in", "enforce", "true", "JM_RESPPROXY_UPSTREAM_PASSWORD must be set", true},
		{"passthrough-refusal", "passthrough", "true", "JM_RESPPROXY_STAGE_BIRTH=true requires JM_RESPPROXY_MODE=enforce", true},
		{"observe-refusal", "observe", "true", "JM_RESPPROXY_STAGE_BIRTH=true requires JM_RESPPROXY_MODE=enforce", true},
		{"numeric-refusal", "enforce", "1", "JM_RESPPROXY_STAGE_BIRTH must be true or false", true},
		{"uppercase-refusal", "enforce", "TRUE", "JM_RESPPROXY_STAGE_BIRTH must be true or false", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir, run := filepath.Join(root, "principals"), filepath.Join(root, "run")
			marker := filepath.Join(root, "proxy-executed")
			fake := filepath.Join(root, "proxy")
			if err := os.WriteFile(fake, []byte("#!/bin/sh\nprintf invoked > \""+marker+"\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/bin/sh", "entrypoint.sh")
			cmd.Env = []string{
				"JM_RESPPROXY_DIR=" + dir,
				"JM_RESPPROXY_RUN_DIR=" + run,
				"JM_RESPPROXY_BIN=" + fake,
				"JM_RESPPROXY_MODE=" + tc.mode,
			}
			if tc.setBirth {
				cmd.Env = append(cmd.Env, "JM_RESPPROXY_STAGE_BIRTH="+tc.birth)
			}
			out, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("entrypoint guard hung: %v", ctx.Err())
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(out), tc.want) {
				t.Fatalf("guard exit=%v output=%q, want exit 2 with %q", err, out, tc.want)
			}
			for _, path := range []string{dir, run, marker} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("guard created %s before admission: %v", path, err)
				}
			}
		})
	}
}
