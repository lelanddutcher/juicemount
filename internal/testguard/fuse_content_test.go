package testguard

import "testing"

func TestRefuseFUSEContentReadsRequiresExactOptIn(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", false},
		{"0", false},
		{"true", false},
		{"1", true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(RefuseFUSEContentReadsEnv, tc.value)
			if got := RefuseFUSEContentReads(); got != tc.want {
				t.Fatalf("RefuseFUSEContentReads() = %v, want %v", got, tc.want)
			}
		})
	}
}
