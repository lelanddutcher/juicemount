package metadata

// LB-4 (Phase 3b): SetReconcileInterval backs the app's "Reconcile
// interval" preference (previously a placebo). Pure-struct test — no live
// Redis needed; the method only mutates the pre-Start configuration field.

import (
	"testing"
	"time"
)

func TestSetReconcileInterval(t *testing.T) {
	cases := []struct {
		name string
		set  time.Duration
		want time.Duration
	}{
		{"positive override", 45 * time.Second, 45 * time.Second},
		{"zero keeps default (absent config field)", 0, DefaultReconcileInterval},
		{"negative keeps default", -5 * time.Second, DefaultReconcileInterval},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc := &RedisClient{reconcileInterval: DefaultReconcileInterval}
			rc.SetReconcileInterval(tc.set)
			if rc.reconcileInterval != tc.want {
				t.Fatalf("reconcileInterval = %v, want %v", rc.reconcileInterval, tc.want)
			}
		})
	}
}
