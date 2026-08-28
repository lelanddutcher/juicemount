package perf

import (
	"os"
	"strings"
	"testing"
)

func TestDeployVerifyLaunchesBundleByAbsolutePath(t *testing.T) {
	body, err := os.ReadFile("deploy_verify.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	for _, required := range []string{
		`NEW_APP="$(absolute_app_path "$1")"`,
		`GOOD_APP="$(absolute_app_path "$2")"`,
		`open -n "$1"`,
		`verify_stable "$NEW_APP"`,
		`verify_stable "$GOOD_APP"`,
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("deploy harness is missing %q", required)
		}
	}
	if strings.Contains(script, `open -na "$1"`) {
		t.Fatal("deploy harness passes a bundle path to open -a as an application name")
	}
}
