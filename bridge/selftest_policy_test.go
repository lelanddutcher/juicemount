package main

import (
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func TestSelfTestLinkPolicy(t *testing.T) {
	tests := []struct {
		class netprofile.LinkClass
		size  int64
	}{
		{netprofile.ClassMetered, selfTestMeteredSize},
		{netprofile.ClassSlow, selfTestSlowSize},
		{netprofile.ClassMedium, selfTestLANSize},
		{netprofile.ClassFast, selfTestLANSize},
	}
	for _, tt := range tests {
		t.Run(tt.class.String(), func(t *testing.T) {
			if got := selfTestSizeForClass(tt.class); got != tt.size {
				t.Fatalf("selfTestSizeForClass(%s) = %d, want %d", tt.class, got, tt.size)
			}
		})
	}
}

func TestAutomaticSelfTestDoesNoIOOnAnyLink(t *testing.T) {
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	for _, class := range []netprofile.LinkClass{
		netprofile.ClassMetered,
		netprofile.ClassSlow,
		netprofile.ClassMedium,
		netprofile.ClassFast,
	} {
		netprofile.Default().ForceClass(&class)
		result := runSelfTest(false)
		if result.Status != "deferred" {
			t.Fatalf("automatic %s self-test status = %q, want deferred", class, result.Status)
		}
		if result.BytesRead != 0 || result.Target != "" || result.WriteOK {
			t.Fatalf("automatic %s self-test performed or claimed IO: %+v", class, result)
		}
		if !strings.Contains(result.Hint, "bounded") {
			t.Fatalf("automatic %s hint = %q, want bounded manual-test guidance", class, result.Hint)
		}
	}
}

func TestConstrainedSelfTestClassificationIsNeutral(t *testing.T) {
	for _, class := range []netprofile.LinkClass{netprofile.ClassMetered, netprofile.ClassSlow} {
		status, hint := classifySelfTestForClass(0.125, class)
		if status != "constrained" || !strings.Contains(hint, "safeguards are active") {
			t.Fatalf("classifySelfTestForClass(%s) = (%q, %q)", class, status, hint)
		}
	}
}
