package enginehost

import (
	"encoding/json"
	"os"
	"testing"
)

func TestExpectedVersionMatchesBundledJuiceFS(t *testing.T) {
	raw, err := os.ReadFile("../../third_party/juicefs/SOURCE.json")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Binary struct {
			VersionOutput string `json:"version_output"`
		} `json:"binary"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if want := "juicefs version " + ExpectedJuiceFSVersion; m.Binary.VersionOutput != want {
		t.Fatalf("SOURCE.json pins %q; enginehost.ExpectedJuiceFSVersion gives %q", m.Binary.VersionOutput, want)
	}
}
