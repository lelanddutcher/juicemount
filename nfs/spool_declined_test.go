package nfs

import (
	"strings"
	"testing"
)

// A clobber refusal is the guard WORKING — it preserved another producer's blob
// rather than letting a client preview replace it. It must not be reported as a
// failure.
//
// Measured 2026-08-17: once the legacy directory-ownership repair removed the
// permission barrier, the guard became the visible one and /spool reported
// failed=333 / failed_files=111, every one of them a deliberate decline. That
// is indistinguishable from breakage, and was read as exactly that.

func TestClobberRefusalCarriesTheDeclinedMarker(t *testing.T) {
	e := &errDerivClobber{
		path: ".juicemount/derivatives/2044545/waveform.json",
		// The real shape: a root-owned farm artifact vs a smaller client one.
		existing: 0, incoming: 501,
		existSize: 171562, newSize: 2048,
	}
	msg := e.Error()
	if !strings.HasPrefix(msg, DeclinedPrefix) {
		t.Fatalf("clobber refusal does not carry %q, so the control plane cannot "+
			"tell it from a real failure and it lands in failed_files: %q",
			DeclinedPrefix, msg)
	}
	// The explanation must survive the prefix — it is what tells the user which
	// blob was kept and why.
	for _, want := range []string{"refusing to overwrite", "waveform.json", "171562"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal lost %q; the reason is the only place the user learns "+
				"which artifact was preserved", want)
		}
	}
}

// The marker must not be so generic that an ordinary failure trips it. Real
// drain errors (ENOSPC, EIO, a SHA mismatch) have to keep counting as failures.
func TestOrdinaryFailuresAreNotClassifiedAsDeclined(t *testing.T) {
	for _, reason := range []string{
		"retry budget exhausted (5 attempts)",
		"write /Volumes/zpool/x: no space left on device",
		"sha mismatch: got abc want def",
		"input/output error",
	} {
		if strings.HasPrefix(reason, DeclinedPrefix) {
			t.Errorf("ordinary failure %q was classified as a policy decline — real "+
				"breakage would stop being counted", reason)
		}
	}
}

// The classification itself, now that it is extractable. Inline in the status
// walk it had no coverage at all.
func TestDeclinedClassification(t *testing.T) {
	if !isDeclinedReason(DeclinedPrefix + "refusing to overwrite x") {
		t.Error("a declined reason was not classified as declined — it would be " +
			"counted in failed_files and read as breakage")
	}
	for _, real := range []string{
		"retry budget exhausted (5 attempts)",
		"no space left on device",
		"sha mismatch",
		"",
	} {
		if isDeclinedReason(real) {
			t.Errorf("real failure %q classified as a policy decline — genuine "+
				"breakage would stop being counted", real)
		}
	}
}
