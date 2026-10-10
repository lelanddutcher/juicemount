package health

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
)

// health runs only the shared bundled resolver; the no-fallback proof lives
// in internal/juicefsbin with a fake filesystem and PATH.
func TestFindJuiceFSBinIsTheBundledResolver(t *testing.T) {
	restore := juicefsbin.SetForTest("/Applications/JuiceMount.app/Contents/MacOS/juicefs")
	defer restore()
	if got := ResolveJuiceFSBinary(); got != "/Applications/JuiceMount.app/Contents/MacOS/juicefs" {
		t.Fatalf("ResolveJuiceFSBinary() = %q, want the bundled resolver's answer", got)
	}
	if fm := NewFUSEManager(FUSEConfig{}); fm.cfg.JuiceFSBin != "/Applications/JuiceMount.app/Contents/MacOS/juicefs" {
		t.Fatalf("NewFUSEManager chose %q", fm.cfg.JuiceFSBin)
	}
}
