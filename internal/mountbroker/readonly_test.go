package mountbroker

import (
	"errors"
	"testing"
)

func TestReadonlyRetirementWaitsForWriterLock(t *testing.T) {
	c := testCreator(t)
	j, err := openJournal(c.Directory, testSpec().MountPath)
	if err != nil {
		t.Fatal(err)
	}
	record := Record{Version: 1, OperationID: "0123456789abcdef0123456789abcdef", BootID: "boot-a", Spec: testSpec(), State: Retired}
	if err := j.write(record); err != nil {
		j.close()
		t.Fatal(err)
	}
	if _, err := ReadRecordReadonly(c.Directory, record.Spec.MountPath); !errors.Is(err, ErrJournalBusy) {
		j.close()
		t.Fatalf("reader crossed writer custody: %v", err)
	}
	j.close()
	got, err := ReadRecordReadonly(c.Directory, record.Spec.MountPath)
	if err != nil || got == nil || *got != record {
		t.Fatalf("retired record: %+v %v", got, err)
	}
}
