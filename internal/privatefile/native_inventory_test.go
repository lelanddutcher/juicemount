package privatefile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeInventoryFull4096RowsAndPendingReservationBoundary(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	digest := strings.Repeat("a", 64)
	maximumDirectory := encodeNativeIdentity(Identity{Device: ^uint64(0), Inode: ^uint64(0), Size: 0, UID: ^uint32(0), Mode: 16832})
	maximumLock := encodeNativeIdentity(Identity{Device: ^uint64(0), Inode: ^uint64(0), Size: 0, UID: ^uint32(0), Mode: 33152})
	maximumPartial := maximumLock
	// Populate 4095 maximum-width rows in one bounded transaction. The final
	// edge and refusal still use the real admission method and disk barriers.
	tx, err := control.inventory.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 4096; i++ {
		id := fmt.Sprintf("copy_%032x", i)
		if _, err := tx.Exec(`INSERT INTO copies(copy_id,intent_sha,selection_sha,metadata_bytes,logical_bytes,phase,directory_id,lock_id,partial_id) VALUES(?,?,?,24576,0,?,?,?,?)`, id, digest, digest, NativeReady, maximumDirectory, maximumLock, maximumPartial); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := control.inventoryCommit(tx); err != nil {
		t.Fatal(err)
	}
	last := fmt.Sprintf("copy_%032x", 4096)
	if err := control.ReserveIntent(last, digest); err != nil {
		t.Fatalf("4096th durable reservation did not fit: %v", err)
	}
	if err := control.ReserveIntent(last, digest); err != nil {
		t.Fatalf("exact pending retry recharged: %v", err)
	}
	if err := control.ReserveIntent(fmt.Sprintf("copy_%032x", 4097), digest); !errors.Is(err, ErrNativeInventoryLimit) {
		t.Fatalf("4097th durable reservation admitted: %v", err)
	}
	full, err := control.inventory.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := full.Exec(`UPDATE copies SET selection_sha=?,logical_bytes=0,phase=?,directory_id=?,lock_id=?,partial_id=? WHERE copy_id=?`, digest, NativeReady, maximumDirectory, maximumLock, maximumPartial, last); err != nil {
		full.Rollback()
		t.Fatal(err)
	}
	if err := control.inventoryCommit(full); err != nil {
		t.Fatal(err)
	}
	rows, err := control.InventoryEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4096 {
		t.Fatalf("durable row count %d", len(rows))
	}
	var charge int64
	for _, row := range rows {
		charge += row.MetadataBytes
	}
	if charge != 96<<20 {
		t.Fatalf("metadata charge %d", charge)
	}
	info, err := os.Stat(filepath.Join(root.path, "accounting.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 16<<20 {
		t.Fatalf("4096 fixed rows exceeded 16 MiB database: %d", info.Size())
	}
	if err := root.inventoryPhysical(); err != nil {
		t.Fatalf("4096 full-size rows exceeded physical bound: %v", err)
	}
}

func TestNativeInventoryFourGiBSelectionPendingCharge(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	digest := strings.Repeat("a", 64)
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("copy_%032x", i)
		if err := control.ReserveIntent(id, digest); err != nil {
			t.Fatal(err)
		}
		copy, err := control.ResumePendingCopy(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := copy.WriteIntent([]byte("{}\n")); err != nil {
			copy.Close()
			t.Fatal(err)
		}
		if err := control.IntentReady(id, copy.DirectoryIdentity(), copy.LockIdentity()); err != nil {
			copy.Close()
			t.Fatal(err)
		}
		if err := copy.Close(); err != nil {
			t.Fatal(err)
		}
		want := int64(1 << 30)
		if i == 5 {
			want = 1
		}
		err = control.ReserveSelection(id, digest, want)
		if i == 5 {
			if !errors.Is(err, ErrNativeInventoryLimit) {
				t.Fatalf("4 GiB+1 pending charge admitted: %v", err)
			}
		} else if err != nil {
			t.Fatalf("1 GiB charge %d: %v", i, err)
		}
	}
	rows, err := control.InventoryEntries()
	if err != nil {
		t.Fatal(err)
	}
	var logical int64
	for _, row := range rows {
		logical += row.LogicalBytes
	}
	if logical != 4<<30 {
		t.Fatalf("pending logical charge %d", logical)
	}
}

func TestNativeInventoryCommitAndCheckpointUncertaintyNeverRecharges(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	digest := strings.Repeat("a", 64)
	first := fmt.Sprintf("copy_%032x", 1)
	fault := errors.New("injected committed result uncertainty")
	control.inventory.commitFault = fault
	if err := control.ReserveIntent(first, digest); !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) {
		t.Fatalf("commit uncertainty lost: %v", err)
	}
	control.inventory.commitFault = nil
	if err := control.ReserveIntent(first, digest); err != nil {
		t.Fatalf("exact committed retry: %v", err)
	}
	second := fmt.Sprintf("copy_%032x", 2)
	control.inventory.checkpointFault = fault
	if err := control.ReserveIntent(second, digest); !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) {
		t.Fatalf("checkpoint uncertainty lost: %v", err)
	}
	control.inventory.checkpointFault = nil
	if err := control.ReserveIntent(second, digest); err != nil {
		t.Fatalf("exact checkpoint retry: %v", err)
	}
	rows, err := control.InventoryEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("uncertain charges lost or doubled: %d", len(rows))
	}
}
