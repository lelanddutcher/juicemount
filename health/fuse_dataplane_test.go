package health

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForFUSEDataPlaneStableRequiresConsecutivePasses(t *testing.T) {
	sequence := []bool{false, true, false, true, true}
	var calls atomic.Int32
	probe := func(string, time.Duration) bool {
		idx := int(calls.Add(1)) - 1
		if idx >= len(sequence) {
			t.Fatalf("probe called %d times, sequence has %d entries", idx+1, len(sequence))
		}
		return sequence[idx]
	}

	if !waitForFUSEDataPlaneStable(
		"/fuse", time.Second, 10*time.Millisecond, 0, time.Millisecond, 2, probe,
	) {
		t.Fatal("stable probe rejected two consecutive successes")
	}
	if got := calls.Load(); got != int32(len(sequence)) {
		t.Fatalf("probe calls = %d, want %d", got, len(sequence))
	}
}

func TestWaitForFUSEDataPlaneStableHonorsTotalBudget(t *testing.T) {
	start := time.Now()
	if waitForFUSEDataPlaneStable(
		"/fuse", 20*time.Millisecond, 5*time.Millisecond, 0, time.Millisecond, 2,
		func(string, time.Duration) bool { return false },
	) {
		t.Fatal("permanently failing data plane reported stable")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("20ms readiness budget took %v", elapsed)
	}
}

func TestFUSEDataPlaneResponsiveLocalDirectory(t *testing.T) {
	if !FUSEDataPlaneResponsive(t.TempDir(), time.Second) {
		t.Fatal("local directory should answer the disposable data-plane probe")
	}
}

func TestFUSEDataPlaneProbeDoesNotEnumerateRoot(t *testing.T) {
	cmd := fuseDataPlaneProbeCommand("/fuse/root")
	if cmd.Path != "/bin/test" {
		t.Fatalf("probe executable = %q, want /bin/test", cmd.Path)
	}
	want := []string{"/bin/test", "-d", "/fuse/root"}
	if len(cmd.Args) != len(want) {
		t.Fatalf("probe args = %q, want %q", cmd.Args, want)
	}
	for i := range want {
		if cmd.Args[i] != want[i] {
			t.Fatalf("probe args = %q, want %q", cmd.Args, want)
		}
	}
}

func TestFUSEDataPlaneProbeBudgetScalesWithMountBudget(t *testing.T) {
	tests := []struct {
		name  string
		total time.Duration
		want  time.Duration
	}{
		{name: "short keeps LAN floor", total: 9 * time.Second, want: 5 * time.Second},
		{name: "fast cold mount", total: 60 * time.Second, want: 20 * time.Second},
		{name: "slow link", total: 90 * time.Second, want: 30 * time.Second},
		{name: "metered link", total: 120 * time.Second, want: 40 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fuseDataPlaneProbeBudget(tt.total, 2); got != tt.want {
				t.Fatalf("probe budget for %v = %v, want %v", tt.total, got, tt.want)
			}
		})
	}
}
