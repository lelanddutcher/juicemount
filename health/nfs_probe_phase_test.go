package health

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// A held dependency must still be physically running at the outer timeout.
// Each case releases and joins its original worker before asserting.
func TestNFSProbeTimeoutNamesLastObservedPhaseAndJoinsWorker(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		holdStat   bool
	}{
		{name: "stat", want: "unresponsive (stat timeout)", holdStat: true},
		{name: "mount_table", want: "unresponsive (mount table timeout)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := New(Config{NFSMountPoint: "/owned/probe"})
			entered := make(chan struct{})
			release := make(chan struct{})
			var tableCalls atomic.Int32
			deps := nfsProbeDeps{
				stat: func(string) (os.FileInfo, error) {
					if tc.holdStat {
						close(entered)
						<-release
					}
					return nil, nil
				},
				mountTable: func(string, time.Duration) (bool, error) {
					tableCalls.Add(1)
					if !tc.holdStat {
						close(entered)
						<-release
					}
					return true, nil
				},
			}
			status, workerDone := m.checkNFSWithProbe(deps, 200*time.Millisecond)
			select {
			case <-entered:
			default:
				close(release)
				<-workerDone
				t.Fatal("original probe did not reach the held dependency")
			}
			select {
			case <-workerDone:
				close(release)
				t.Fatal("worker exited before held dependency was released")
			default:
			}
			close(release)
			<-workerDone
			if status.Healthy || status.Message != tc.want {
				t.Fatalf("timeout status = %#v, want unhealthy %q", status, tc.want)
			}
			if tableCalls.Load() != 1 {
				// The stat-held worker reaches the table only after release; both
				// cases must complete the original probe, not abandon it.
				t.Fatalf("table calls after joined worker = %d, want 1", tableCalls.Load())
			}
		})
	}
}

func TestNFSProbeResolvedOutcomesRemainAuthoritative(t *testing.T) {
	tests := []struct {
		name        string
		statErr     error
		mounted     bool
		tableErr    error
		want        string
		wantHealthy bool
		wantTable   bool
	}{
		{name: "missing stat", statErr: os.ErrNotExist, want: nfsMsgNotMounted},
		{name: "stat error", statErr: errors.New("io"), want: "stale: io"},
		{name: "table error", tableErr: errors.New("query"), want: "stale: mount table unreadable: query", wantTable: true},
		{name: "absent mount", want: nfsMsgNotMounted, wantTable: true},
		{name: "healthy", mounted: true, want: "ok", wantHealthy: true, wantTable: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := New(Config{NFSMountPoint: "/owned/probe"})
			var called atomic.Bool
			status, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
				stat: func(string) (os.FileInfo, error) { return nil, tc.statErr },
				mountTable: func(string, time.Duration) (bool, error) {
					called.Store(true)
					return tc.mounted, tc.tableErr
				},
			}, time.Second)
			<-workerDone
			if status.Healthy != tc.wantHealthy || status.Message != tc.want || called.Load() != tc.wantTable {
				t.Fatalf("resolved status = %#v, table called=%v; want healthy=%v message=%q table=%v",
					status, called.Load(), tc.wantHealthy, tc.want, tc.wantTable)
			}
		})
	}
}

func TestNFSProbeBusySuppressionStillCoversTableTimeout(t *testing.T) {
	m := New(Config{NFSMountPoint: "/owned/probe"})
	m.SetDrainProbe(func() (int64, time.Duration) { return 1, 0 })
	entered, release := make(chan struct{}), make(chan struct{})
	status, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
		stat: func(string) (os.FileInfo, error) { return nil, nil },
		mountTable: func(string, time.Duration) (bool, error) {
			close(entered)
			<-release
			return false, errors.New("resolved table error after timeout")
		},
	}, 200*time.Millisecond)
	select {
	case <-entered:
	default:
		close(release)
		<-workerDone
		t.Fatal("table phase not entered")
	}
	close(release)
	<-workerDone
	if !status.Healthy || status.Message != "busy (heavy ingest)" {
		t.Fatalf("busy timeout status changed: %#v", status)
	}
}
