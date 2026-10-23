package privatefile

import (
	"strings"
	"testing"
)

func TestManagedAttemptOriginalCloseFaultStableAndSingle(t *testing.T) {
	for i, phase := range []AttemptCloseTestPhase{AttemptCloseTestRead, AttemptCloseTestDirectory, AttemptCloseTestLock} {
		t.Run(string(phase), func(t *testing.T) {
			_, root := managedTestRoot(t)
			a, err := root.OpenAttempt(managedTestID(uint64(91 + i)))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })
			if _, err := a.Writer().Write([]byte("close-owned-payload")); err != nil {
				t.Fatal(err)
			}
			p, err := a.Finish()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			fault, err := InstallPreparedCloseFaultForTest(p, phase)
			if err != nil {
				t.Fatal(err)
			}
			first := p.Close()
			if first == nil || !strings.Contains(first.Error(), "injected original attempt close uncertainty") {
				t.Fatalf("reported original close: %v", first)
			}
			if second := p.Close(); second != first {
				t.Fatalf("repeat close changed stable result: first=%v second=%v", first, second)
			}
			if count := fault.Calls(); count != 1 {
				t.Fatalf("original close calls=%d", count)
			}
		})
	}
}

func TestManagedUnfinishedDataCloseFaultStillReleasesLockLast(t *testing.T) {
	_, root := managedTestRoot(t)
	a, err := root.OpenAttempt(managedTestID(96))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.Writer().Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	fault, err := InstallAttemptCloseFaultForTest(a, AttemptCloseTestData)
	if err != nil {
		t.Fatal(err)
	}
	first := a.Close()
	if first == nil || !strings.Contains(first.Error(), "injected original attempt close uncertainty") {
		t.Fatalf("data close fault: %v", first)
	}
	if second := a.Close(); second != first {
		t.Fatalf("repeat close changed result: %v %v", first, second)
	}
	if calls := fault.Calls(); calls != 1 {
		t.Fatalf("data original close calls=%d", calls)
	}
}

func TestManagedSplitCloseKeepsOwnerLockUntilTerminalRelease(t *testing.T) {
	_, root := managedTestRoot(t)
	a, err := root.OpenAttempt(managedTestID(97))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.Writer().Write([]byte("split-close-payload")); err != nil {
		t.Fatal(err)
	}
	p, err := a.Finish()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if exposed, err := p.ExposeNoReplace(); err != nil || !exposed {
		t.Fatalf("original exposure: %t %v", exposed, err)
	}
	fault, err := InstallPreparedCloseFaultForTest(p, AttemptCloseTestRead)
	if err != nil {
		t.Fatal(err)
	}
	pre := p.CloseBeforeRelease()
	if pre == nil || !strings.Contains(pre.Error(), "injected original attempt close uncertainty") {
		t.Fatalf("nonterminal original error: %v", pre)
	}
	if again := p.CloseBeforeRelease(); again != pre || fault.Calls() != 1 {
		t.Fatalf("pre-release close repeated original: %v calls=%d", again, fault.Calls())
	}
	if _, err := managedOpenReady(t, root, p); err == nil {
		t.Fatal("independent owner acquired while pre-release bookkeeping still owns lock")
	}
	if err := p.ReleaseLock(); err != nil {
		t.Fatalf("terminal lock close: %v", err)
	}
	first := p.Close()
	if first == nil || !strings.Contains(first.Error(), "injected original attempt close uncertainty") {
		t.Fatalf("full close lost original nonterminal result: %v", first)
	}
	if second := p.Close(); second != first || fault.Calls() != 1 {
		t.Fatalf("repeat full close changed result: %v calls=%d", second, fault.Calls())
	}
	owned, err := managedOpenReady(t, root, p)
	if err != nil {
		t.Fatalf("independent owner after release: %v", err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
}
