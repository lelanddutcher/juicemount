package nativeclient

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

func nativeJournalTest(t *testing.T) (string, *Journal, nativeMaxVectors) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "native")
	v := maxVectors()
	j, err := CreateJournal(path, v.intent.Endpoint, v.intent.LibraryID, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Errorf("journal root close: %v", err)
		}
	})
	return path, j, v
}

func TestNativeJournalCreateOnceSelectionAndZeroCheckpoint(t *testing.T) {
	_, j, v := nativeJournalTest(t)
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatal(err)
	}
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatalf("exact Intent retry: %v", err)
	}
	changed := v.intent
	changed.Basename = "changed.txt"
	if err := j.CreateIntent(changed); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("changed Intent accepted: %v", err)
	}
	usage, err := j.Usage()
	if err != nil || usage.IntentCount != 1 || usage.MetadataBytes != MetadataReservationBytes || usage.LogicalBytes != 0 {
		t.Fatalf("once-reserved Intent: %+v %v", usage, err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatal(err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatalf("exact Selection retry: %v", err)
	}
	changedSelection := v.selection
	changedSelection.SelectionID = "dsel_" + strings.Repeat("b", 32)
	if err := j.PinSelection(v.intent.CopyID, changedSelection); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("changed selected pin accepted: %v", err)
	}
	usage, err = j.Usage()
	if err != nil || usage.IntentCount != 1 || usage.MetadataBytes != MetadataReservationBytes || usage.LogicalBytes != v.selection.Content.Size {
		t.Fatalf("once-reserved selected bytes: %+v %v", usage, err)
	}
	checkpoint, err := j.InitialPartial(v.intent.CopyID)
	if err != nil || checkpoint.PrefixLength != 0 || checkpoint.File.Size != 0 || checkpoint.PrefixSHA256 != emptyDigest() {
		t.Fatalf("zero checkpoint: %+v %v", checkpoint, err)
	}
	if repeated, err := j.InitialPartial(v.intent.CopyID); err != nil || repeated != checkpoint {
		t.Fatalf("initial checkpoint retry: %+v %v", repeated, err)
	}
	pending, err := j.Recover(v.intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pending.Close(); err != nil {
			t.Errorf("pending owner close: %v", err)
		}
	})
	if pending.Checkpoint != checkpoint || pending.ActualFile != checkpoint.File || pending.NeedsServerPrefixValidation {
		t.Fatalf("pending zero identity: %+v", pending)
	}
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeJournalQuotaArithmeticAndSixSlots(t *testing.T) {
	if MetadataReservationBytes != 6*RecordSlotBytes || NativeMetadataLimit != 96<<20 || NativeLogicalLimit != 4<<30 {
		t.Fatal("settled slot reservation changed")
	}
	last := NativeUsage{IntentCount: NativeIntentLimit - 1, MetadataBytes: NativeMetadataLimit - MetadataReservationBytes}
	if err := reserveIntent(last); err != nil {
		t.Fatalf("4096th Intent refused: %v", err)
	}
	full := NativeUsage{IntentCount: NativeIntentLimit, MetadataBytes: NativeMetadataLimit, LogicalBytes: NativeLogicalLimit}
	if err := reserveIntent(full); !errors.Is(err, ErrJournalLimit) {
		t.Fatalf("4097th Intent admitted: %v", err)
	}
	if err := reserveSelection(NativeUsage{LogicalBytes: NativeLogicalLimit - 10}, 10); err != nil {
		t.Fatalf("exact 4GiB selected reserve refused: %v", err)
	}
	if err := reserveSelection(NativeUsage{LogicalBytes: NativeLogicalLimit - 10}, 11); !errors.Is(err, ErrJournalLimit) {
		t.Fatalf("4GiB+1 selected reserve admitted: %v", err)
	}
	if err := reserveSelection(full, 0); err != nil {
		t.Fatalf("zero selection at full logical cap refused: %v", err)
	}
	if err := reserveIntent(full); !errors.Is(err, ErrJournalLimit) {
		t.Fatalf("reserved release slot recycled: %v", err)
	}
}

func TestNativeJournalRestartReacquiresExactOwnerAndReportsTail(t *testing.T) {
	path, j, v := nativeJournalTest(t)
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatal(err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := j.InitialPartial(v.intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenJournal(path, v.intent.Endpoint, v.intent.LibraryID, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := restarted.Close(); err != nil {
			t.Errorf("restart root close: %v", err)
		}
	})
	pending, err := restarted.Recover(v.intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pending.Close(); err != nil {
			t.Errorf("original pending owner close: %v", err)
		}
	})
	if pending.Checkpoint != checkpoint || pending.NeedsServerPrefixValidation {
		t.Fatalf("restart lost canonical checkpoint: %+v", pending)
	}
	if _, err := restarted.Recover(v.intent.CopyID); !errors.Is(err, privatefile.ErrNativeCopyBusy) {
		t.Fatalf("second owner acquired live lock: %v", err)
	}
	if err := restarted.Close(); !errors.Is(err, privatefile.ErrNativeCopyBusy) {
		t.Fatalf("root closed live partial owner: %v", err)
	}
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(path, "staging", v.intent.CopyID, "payload.partial")
	if err := os.WriteFile(partial, []byte("crash-grown-tail"), 0600); err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.Recover(v.intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("grown-tail owner close: %v", err)
		}
	})
	if !recovered.NeedsServerPrefixValidation || recovered.ActualFile.Size != int64(len("crash-grown-tail")) || recovered.Checkpoint.PrefixLength != 0 {
		t.Fatalf("unverified growth promoted/truncated: %+v", recovered)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.InitialPartial(v.intent.CopyID); err == nil {
		t.Fatal("grown tail accepted as initial zero partial")
	}
}

func TestNativeJournalRecoverControlCloseFaultReleasesOriginalOwner(t *testing.T) {
	_, j, v := nativeJournalTest(t)
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatal(err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := j.InitialPartial(v.intent.CopyID)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected control-close fault")
	j.controlCloseErr = fault
	pending, err := j.Recover(v.intent.CopyID)
	if pending != nil {
		_ = pending.Close()
		t.Fatal("Recover published owner after control-close fault")
	}
	if !errors.Is(err, fault) {
		t.Fatalf("control-close fault lost: %v", err)
	}
	j.controlCloseErr = nil
	retried, err := j.Recover(v.intent.CopyID)
	if err != nil {
		t.Fatalf("original owner/control not joined after fault: %v", err)
	}
	t.Cleanup(func() {
		if err := retried.Close(); err != nil {
			t.Errorf("recovered owner close: %v", err)
		}
	})
	if retried.Checkpoint != checkpoint || retried.ActualFile != checkpoint.File {
		t.Fatalf("retry did not retain original zero partial: %+v", retried)
	}
}

func TestNativeJournalMissingCorruptAndUnknownAccountingFailClosed(t *testing.T) {
	for _, kind := range []string{"missing_intent", "corrupt_intent", "unknown_entry", "symlink_intent"} {
		t.Run(kind, func(t *testing.T) {
			path, j, v := nativeJournalTest(t)
			if err := j.CreateIntent(v.intent); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(path, "staging", v.intent.CopyID)
			intent := filepath.Join(dir, "intent.json")
			switch kind {
			case "missing_intent":
				if err := os.Remove(intent); err != nil {
					t.Fatal(err)
				}
			case "corrupt_intent":
				if err := os.WriteFile(intent, []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown_entry":
				if err := os.WriteFile(filepath.Join(dir, "stray"), []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink_intent":
				if err := os.Rename(intent, filepath.Join(dir, "saved")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("saved", intent); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := j.Usage(); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("corrupt accounting accepted: %v", err)
			}
			other := v.intent
			other.CopyID = "copy_" + strings.Repeat("b", 32)
			if err := j.CreateIntent(other); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("new admission despite corrupt accounting: %v", err)
			}
		})
	}
}

func TestNativeJournalReservationAccountingMultipleImmutableCopies(t *testing.T) {
	_, j, v := nativeJournalTest(t)
	for i := 1; i <= 3; i++ {
		intent := v.intent
		intent.CopyID = fmt.Sprintf("copy_%032x", i)
		intent.OperationID = fmt.Sprintf("native-op-%d", i)
		intent.SelectRequest.ID = intent.OperationID
		if err := j.CreateIntent(intent); err != nil {
			t.Fatal(err)
		}
		selection := v.selection
		selection.OperationID = intent.OperationID
		selection.SelectionID = fmt.Sprintf("dsel_%032x", i)
		if err := j.PinSelection(intent.CopyID, selection); err != nil {
			t.Fatal(err)
		}
	}
	usage, err := j.Usage()
	if err != nil || usage.IntentCount != 3 || usage.MetadataBytes != 3*MetadataReservationBytes || usage.LogicalBytes != 3*v.selection.Content.Size {
		t.Fatalf("immutable reservation sum: %+v %v", usage, err)
	}
}

func TestNativeJournalLostWholeCopyOrAcknowledgedSelectionRetainsCharge(t *testing.T) {
	for _, lost := range []string{"whole_copy", "selection"} {
		t.Run(lost, func(t *testing.T) {
			path, j, v := nativeJournalTest(t)
			if err := j.CreateIntent(v.intent); err != nil {
				t.Fatal(err)
			}
			if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(path, "staging", v.intent.CopyID)
			if lost == "whole_copy" {
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Remove(filepath.Join(dir, "selection.json")); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := OpenJournal(path, v.intent.Endpoint, v.intent.LibraryID, false)
			if err != nil {
				t.Fatalf("open after charged-object loss: %v", err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("reopened journal close: %v", err)
				}
			}()
			if _, err := reopened.Usage(); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("lost charged object accepted: %v", err)
			}
			other := v.intent
			other.CopyID = "copy_" + strings.Repeat("b", 32)
			if err := reopened.CreateIntent(other); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("lost object recycled charge: %v", err)
			}
		})
	}
}

func TestNativeJournalZeroPendingRecoveryAndForeignTailRefusal(t *testing.T) {
	for _, state := range []string{"zero_pending_absent", "zero_pending_present", "zero_bound", "nonzero_tail", "changed_inode"} {
		t.Run(state, func(t *testing.T) {
			path, j, v := nativeJournalTest(t)
			if err := j.CreateIntent(v.intent); err != nil {
				t.Fatal(err)
			}
			if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
				t.Fatal(err)
			}
			control, err := j.root.LockControl()
			if err != nil {
				t.Fatal(err)
			}
			copy, err := control.OpenCopy(v.intent.CopyID)
			if err != nil {
				control.Close()
				t.Fatal(err)
			}
			if err := control.ZeroPending(v.intent.CopyID); err != nil {
				copy.Close()
				control.Close()
				t.Fatal(err)
			}
			var original privatefile.Identity
			if state != "zero_pending_absent" {
				original, err = copy.CreateZeroPartial()
				if err != nil {
					copy.Close()
					control.Close()
					t.Fatal(err)
				}
			}
			if state == "zero_bound" || state == "changed_inode" {
				if err := control.ZeroBound(v.intent.CopyID, original); err != nil {
					copy.Close()
					control.Close()
					t.Fatal(err)
				}
			}
			if err := copy.Close(); err != nil {
				control.Close()
				t.Fatal(err)
			}
			if err := control.Close(); err != nil {
				t.Fatal(err)
			}
			partial := filepath.Join(path, "staging", v.intent.CopyID, "payload.partial")
			if state == "nonzero_tail" {
				if err := os.WriteFile(partial, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if state == "changed_inode" {
				replacement := partial + ".replacement"
				if err := os.WriteFile(replacement, nil, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(replacement, partial); err != nil {
					t.Fatal(err)
				}
			}
			checkpoint, err := j.InitialPartial(v.intent.CopyID)
			if state == "nonzero_tail" || state == "changed_inode" {
				if err == nil {
					t.Fatal("foreign partial adopted")
				}
				return
			}
			if err != nil || checkpoint.File.Size != 0 || checkpoint.PrefixLength != 0 {
				t.Fatalf("zero recovery: %+v %v", checkpoint, err)
			}
			if state != "zero_pending_absent" && checkpoint.File != original {
				t.Fatalf("original zero inode replaced: got %+v want %+v", checkpoint.File, original)
			}
		})
	}
}

func TestNativeJournalPendingIntentAndSelectionExactRetry(t *testing.T) {
	_, j, v := nativeJournalTest(t)
	intentLine, intentSHA, err := EncodeIntent(v.intent, false)
	if err != nil {
		t.Fatal(err)
	}
	control, err := j.root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReserveIntent(v.intent.CopyID, intentSHA); err != nil {
		control.Close()
		t.Fatal(err)
	}
	copy, err := control.ResumePendingCopy(v.intent.CopyID)
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := copy.WriteIntent(intentLine); err != nil {
		copy.Close()
		control.Close()
		t.Fatal(err)
	}
	if err := copy.Close(); err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	usage, err := j.Usage()
	if err != nil || usage.IntentCount != 1 || usage.MetadataBytes != MetadataReservationBytes {
		t.Fatalf("pending Intent uncharged: %+v %v", usage, err)
	}
	changed := v.intent
	changed.Basename = "other.txt"
	if err := j.CreateIntent(changed); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("changed pending Intent admitted: %v", err)
	}
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatalf("exact pending Intent did not finish: %v", err)
	}
	record := SelectionRecord{Version: 1, IntentSHA256: intentSHA, Selection: v.selection, LogicalReservationBytes: v.selection.Content.Size}
	selectionLine, selectionSHA, err := EncodeSelection(record)
	if err != nil {
		t.Fatal(err)
	}
	control, err = j.root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReserveSelection(v.intent.CopyID, selectionSHA, v.selection.Content.Size); err != nil {
		control.Close()
		t.Fatal(err)
	}
	copy, err = control.OpenCopy(v.intent.CopyID)
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := copy.WriteSelection(selectionLine); err != nil {
		copy.Close()
		control.Close()
		t.Fatal(err)
	}
	if err := copy.Close(); err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	usage, err = j.Usage()
	if err != nil || usage.LogicalBytes != v.selection.Content.Size {
		t.Fatalf("pending Selection uncharged: %+v %v", usage, err)
	}
	changedSelection := v.selection
	changedSelection.SelectionID = "dsel_" + strings.Repeat("b", 32)
	if err := j.PinSelection(v.intent.CopyID, changedSelection); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("changed pending Selection admitted: %v", err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatalf("exact pending Selection did not finish: %v", err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatalf("acknowledged Selection retry: %v", err)
	}
}

func TestNativeJournalPartialPendingSelectionWriteStaysUncertain(t *testing.T) {
	path, j, v := nativeJournalTest(t)
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatal(err)
	}
	_, intentSHA, err := EncodeIntent(v.intent, false)
	if err != nil {
		t.Fatal(err)
	}
	record := SelectionRecord{Version: 1, IntentSHA256: intentSHA, Selection: v.selection, LogicalReservationBytes: v.selection.Content.Size}
	_, selectionSHA, err := EncodeSelection(record)
	if err != nil {
		t.Fatal(err)
	}
	control, err := j.root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReserveSelection(v.intent.CopyID, selectionSHA, v.selection.Content.Size); err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(path, "staging", v.intent.CopyID, "selection.json")
	if err := os.WriteFile(leaf, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("partial pending Selection repaired: %v", err)
	}
	if contents, err := os.ReadFile(leaf); err != nil || string(contents) != "{" {
		t.Fatalf("uncertain leaf replaced: %q %v", contents, err)
	}
}

func TestNativeJournalMalformedInitialCheckpointPreserved(t *testing.T) {
	path, j, v := nativeJournalTest(t)
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatal(err)
	}
	if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
		t.Fatal(err)
	}
	control, err := j.root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	copy, err := control.OpenCopy(v.intent.CopyID)
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.ZeroPending(v.intent.CopyID); err != nil {
		copy.Close()
		control.Close()
		t.Fatal(err)
	}
	file, err := copy.CreateZeroPartial()
	if err != nil {
		copy.Close()
		control.Close()
		t.Fatal(err)
	}
	if err := control.ZeroBound(v.intent.CopyID, file); err != nil {
		copy.Close()
		control.Close()
		t.Fatal(err)
	}
	if err := copy.Close(); err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(path, "staging", v.intent.CopyID, "checkpoint.json")
	if err := os.WriteFile(leaf, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.InitialPartial(v.intent.CopyID); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("malformed zero-bound checkpoint repaired: %v", err)
	}
	if data, err := os.ReadFile(leaf); err != nil || string(data) != "{" {
		t.Fatalf("uncertain checkpoint overwritten: %q %v", data, err)
	}
}

func TestNativeJournalCheckpointIdentityAndAcknowledgedLossBlockAdmission(t *testing.T) {
	for _, damage := range []string{"wrong_root", "lost_checkpoint", "lost_partial"} {
		t.Run(damage, func(t *testing.T) {
			path, j, v := nativeJournalTest(t)
			if err := j.CreateIntent(v.intent); err != nil {
				t.Fatal(err)
			}
			if err := j.PinSelection(v.intent.CopyID, v.selection); err != nil {
				t.Fatal(err)
			}
			checkpoint, err := j.InitialPartial(v.intent.CopyID)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(path, "staging", v.intent.CopyID)
			switch damage {
			case "wrong_root":
				checkpoint.Root.Inode++
				_, intentSHA, err := EncodeIntent(v.intent, false)
				if err != nil {
					t.Fatal(err)
				}
				record := SelectionRecord{Version: 1, IntentSHA256: intentSHA, Selection: v.selection, LogicalReservationBytes: v.selection.Content.Size}
				if err := ValidateChain(Chain{Intent: v.intent, Selection: &record, Checkpoint: &checkpoint}, false); err != nil {
					t.Fatalf("fixture must remain record-chain valid: %v", err)
				}
				line, _, err := EncodeCheckpoint(checkpoint)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), line, 0600); err != nil {
					t.Fatal(err)
				}
			case "lost_checkpoint":
				if err := os.Remove(filepath.Join(dir, "checkpoint.json")); err != nil {
					t.Fatal(err)
				}
			case "lost_partial":
				if err := os.Remove(filepath.Join(dir, "payload.partial")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := j.Usage(); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("%s passed admission scan: %v", damage, err)
			}
			other := v.intent
			other.CopyID = "copy_" + strings.Repeat("b", 32)
			if err := j.CreateIntent(other); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatalf("%s recycled admission: %v", damage, err)
			}
		})
	}
}
