package mountbroker

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type originalDescriptor struct {
	mounted             *bool
	detaches, closes    int
	detachErr, closeErr error
}

func (*originalDescriptor) FSID() [2]int32 { return testEntry().FSID }
func (d *originalDescriptor) Detach() error {
	d.detaches++
	if d.detachErr != nil {
		return d.detachErr
	}
	*d.mounted = false
	return nil
}
func (d *originalDescriptor) Close() error { d.closes++; return d.closeErr }

func armedFixture(t *testing.T) (Creator, Record, *originalDescriptor) {
	t.Helper()
	c := testCreator(t)
	r := observedOperation(t, &c)
	mounted := true
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		if mounted {
			return testSnapshot(testEntry()), nil
		}
		return testSnapshot(), nil
	}
	return c, r, &originalDescriptor{mounted: &mounted}
}

func TestOriginalCustodianArmedCleanupExactAndNoRepeatedDetach(t *testing.T) {
	c, record, d := armedFixture(t)
	a, err := c.ArmDescriptorRetirement(context.Background(), record, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.detaches != 1 || d.closes != 1 || readRecord(t, c).State != Retired {
		t.Fatalf("original action not durably retired: detach=%d close=%d", d.detaches, d.closes)
	}
	if err := a.Cleanup(context.Background()); err == nil || d.detaches != 1 || d.closes != 1 {
		t.Fatal("finished original custodian retried destructive action")
	}
}

func TestOriginalCustodianRefusesBeforeDetach(t *testing.T) {
	for _, variant := range []string{"wrong-operation", "wrong-fsid", "foreign-row", "no-observation", "successor"} {
		t.Run(variant, func(t *testing.T) {
			c, record, d := armedFixture(t)
			if variant == "wrong-operation" {
				record.OperationID = "ffffffffffffffffffffffffffffffff"
			}
			if variant == "wrong-fsid" {
				m := *record.Mount
				m.FSID = [2]int32{99, 88}
				record.Mount = &m
			}
			if variant == "foreign-row" {
				c.Observe = func(context.Context) (mounttable.Snapshot, error) {
					return testSnapshot(testEntry(), mounttable.MountEntry{Path: "/Volumes/other", Source: record.Spec.Source, Type: "nfs", FSID: [2]int32{1, 2}}), nil
				}
			}
			if variant == "no-observation" {
				c.Observe = func(context.Context) (mounttable.Snapshot, error) {
					return mounttable.Snapshot{}, errors.New("unavailable")
				}
			}
			if variant == "successor" {
				j, err := openJournal(c.Directory, record.Spec.MountPath)
				if err != nil {
					t.Fatal(err)
				}
				changed := record
				changed.OperationID = "ffffffffffffffffffffffffffffffff"
				if err := j.write(changed); err != nil {
					t.Fatal(err)
				}
				j.close()
			}
			if _, err := c.ArmDescriptorRetirement(context.Background(), record, d); err == nil {
				t.Fatal("foreign, unknown or successor operation armed")
			}
			if d.detaches != 0 || d.closes != 0 {
				t.Fatal("refusal touched original descriptor")
			}
		})
	}
}

func TestOriginalCustodianPostArmJournalAndObservationFence(t *testing.T) {
	for _, variant := range []string{"successor", "foreign-observation"} {
		t.Run(variant, func(t *testing.T) {
			c, record, d := armedFixture(t)
			a, err := c.ArmDescriptorRetirement(context.Background(), record, d)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "successor" {
				j, err := openJournal(c.Directory, record.Spec.MountPath)
				if err != nil {
					t.Fatal(err)
				}
				changed := record
				changed.OperationID = "ffffffffffffffffffffffffffffffff"
				if err := j.write(changed); err != nil {
					t.Fatal(err)
				}
				j.close()
			} else {
				c.Observe = func(context.Context) (mounttable.Snapshot, error) {
					return testSnapshot(testEntry(), mounttable.MountEntry{Path: "/Volumes/other",
						Source: record.Spec.Source, Type: "nfs", FSID: [2]int32{9, 9}}), nil
				}
				a.creator = c
			}
			if err := a.Cleanup(context.Background()); err == nil || d.detaches != 0 || d.closes != 0 {
				t.Fatal("post-arm successor or foreign mount reached destructive detach")
			}
		})
	}
}

func TestOriginalCustodianUnknownDetachNeverRetried(t *testing.T) {
	c, record, d := armedFixture(t)
	d.detachErr = errors.New("kernel result unknown")
	a, err := c.ArmDescriptorRetirement(context.Background(), record, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Cleanup(context.Background()); err == nil {
		t.Fatal("unknown detach accepted")
	}
	d.detachErr = nil
	if err := a.Cleanup(context.Background()); err == nil || d.detaches != 1 || d.closes != 0 ||
		readRecord(t, c).State != Observed {
		t.Fatal("unknown original detach was retried")
	}
}

func TestOriginalCustodianPostDetachObserverResumesSameAction(t *testing.T) {
	c, record, d := armedFixture(t)
	a, err := c.ArmDescriptorRetirement(context.Background(), record, d)
	if err != nil {
		t.Fatal(err)
	}
	base := c.Observe
	c.Observe = func(ctx context.Context) (mounttable.Snapshot, error) {
		if !*d.mounted {
			return mounttable.Snapshot{}, errors.New("observer unavailable after detach")
		}
		return base(ctx)
	}
	a.creator = c
	if err := a.Cleanup(context.Background()); err == nil || d.detaches != 1 || d.closes != 0 ||
		readRecord(t, c).State != Observed {
		t.Fatal("post-detach observer error falsely retired")
	}
	a.creator.Observe = base
	if err := a.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.detaches != 1 || d.closes != 1 || readRecord(t, c).State != Retired {
		t.Fatal("same original descriptor was detached twice or not retired")
	}
}

func TestOriginalCustodianResyncsSameAttemptWithoutSecondDetach(t *testing.T) {
	c, record, d := armedFixture(t)
	a, err := c.ArmDescriptorRetirement(context.Background(), record, d)
	if err != nil {
		t.Fatal(err)
	}
	syncs := 0
	c.syncDir = func(f *os.File) error {
		syncs++
		if syncs < 3 {
			return errors.New("directory sync unavailable")
		}
		return f.Sync()
	}
	a.creator = c
	for i := 0; i < 2; i++ {
		if err := a.Cleanup(context.Background()); err == nil || d.detaches != 1 || d.closes != 0 {
			t.Fatal("undurable retirement closed or detached original again")
		}
	}
	if err := a.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.detaches != 1 || d.closes != 1 || readRecord(t, c).State != Retired {
		t.Fatal("same-object durability reconciliation failed")
	}
}

func TestOriginalCustodianCloseFailureIsAfterRetirement(t *testing.T) {
	c, record, d := armedFixture(t)
	d.closeErr = errors.New("close uncertain")
	a, err := c.ArmDescriptorRetirement(context.Background(), record, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Cleanup(context.Background()); err == nil || readRecord(t, c).State != Retired {
		t.Fatal("close failure erased durable retirement distinction")
	}
	if err := a.Cleanup(context.Background()); err == nil || d.detaches != 1 || d.closes != 1 {
		t.Fatal("post-retirement close was retried")
	}
}
