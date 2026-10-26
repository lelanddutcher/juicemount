package nativeclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

var ErrNativeExposureUncertain = errors.New("native named exposure outcome uncertain")

// LocalCopyStatus names a verified Mac-local copy. Path is a derived output for
// Finder/consumers, never a server path, URL, or input to custody operations.
// PreviouslyDownloaded does not assert current server rights.
type LocalCopyStatus struct {
	CopyID               string
	Basename             string
	Size                 int64
	SHA256               string
	Path                 string
	PreviouslyDownloaded bool
}

// VerifiedLocalCopy is an opaque local exposure capability. Only Receive after
// End+EOF+Body.Close+whole hash+durable Prepared, or explicit exact Prepared
// recovery, can mint it. Close joins the original independent copy owner.
type VerifiedLocalCopy struct {
	mu           sync.Mutex
	journal      *Journal
	owner        *privatefile.NativeCopy
	intent       Intent
	selection    SelectionRecord
	checkpoint   Checkpoint
	prepared     Prepared
	preparedSHA  string
	alreadyFinal bool
	closed       bool
	closeErr     error
}

func (v *VerifiedLocalCopy) closeLocked() error {
	if v.closed {
		return v.closeErr
	}
	v.closed = true
	if v.owner != nil {
		v.closeErr = v.owner.Close()
		v.owner = nil
	}
	return v.closeErr
}
func (v *VerifiedLocalCopy) Close() error {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.closeLocked()
}

func (j *Journal) localStatus(intent Intent, selection SelectionRecord) LocalCopyStatus {
	return LocalCopyStatus{CopyID: intent.CopyID, Basename: intent.Basename, Size: selection.Selection.Content.Size, SHA256: selection.Selection.Content.SHA256, Path: filepath.Join(j.rootPath, "copies", intent.CopyID, intent.Basename), PreviouslyDownloaded: true}
}

// Exact durability barriers are repeated after an uncertain write, including
// when a complete canonical leaf was already visible. Fault fields are narrow
// test probes; production always calls the original owned file primitives.
func (j *Journal) syncPrepared(owner *privatefile.NativeCopy) error {
	if j.preparedBarrierErr != nil {
		return errors.Join(j.preparedBarrierErr, privatefile.ErrNativeCopyUncertain)
	}
	return owner.SyncPreparedOwned()
}
func (j *Journal) syncTarget(owner *privatefile.NativeCopy, target privatefile.Identity) error {
	if j.targetBarrierErr != nil {
		return errors.Join(j.targetBarrierErr, privatefile.ErrNativeCopyUncertain)
	}
	return owner.SyncTargetBarriers(target)
}
func (j *Journal) syncVerified(owner *privatefile.NativeCopy) error {
	if j.verifiedBarrierErr != nil {
		return errors.Join(j.verifiedBarrierErr, privatefile.ErrNativeCopyUncertain)
	}
	return owner.SyncVerifiedOwned()
}

func (j *Journal) ensureTargetForPrepared(p *PendingCopy, full privatefile.Identity) (_ privatefile.Identity, resultErr error) {
	control, err := j.root.LockControl()
	if err != nil {
		return privatefile.Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	entries, _, err := j.scan(control)
	if err != nil {
		return privatefile.Identity{}, err
	}
	entry, ok := entries[p.Intent.CopyID]
	if !ok || entry.inventory.Phase != privatefile.NativeReady || entry.prepared != nil || entry.verified != nil || entry.released != nil || !entry.partial || entry.intent != p.Intent || entry.selection == nil || *entry.selection != p.Selection || entry.checkpoint == nil || *entry.checkpoint != p.Checkpoint {
		return privatefile.Identity{}, ErrJournalConflict
	}
	actual, err := control.CopyPartialIdentity(p.Intent.CopyID)
	if err != nil || actual != full {
		return privatefile.Identity{}, errors.Join(err, ErrJournalCorrupt)
	}
	return control.EnsureTargetDirectory(p.Intent.CopyID)
}

// Receive continues only the durable original Intent/Selection. It never
// reselects, truncates, overwrites or buffers a whole image. The original copy
// owner survives the HTTP request; global control/SQL ownership does not.
// InitialPartial must have completed before calling Receive.
func (j *Journal) Receive(ctx context.Context, client *Client, copyID string) (result *VerifiedLocalCopy, resultErr error) {
	if ctx == nil || j == nil || j.root == nil || client == nil || client.endpoint.URL != j.endpoint || client.library != j.library || !validCopyID(copyID) {
		return nil, ErrInvalidClient
	}
	p, err := j.Recover(copyID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if p != nil {
			if e := p.Close(); e != nil {
				resultErr = errors.Join(resultErr, e)
				result = nil
			}
		}
	}()
	selected := p.Selection.Selection
	actual, checkpointHash, prefixHash, err := p.owner.HashPartial(ctx, p.Checkpoint.PrefixLength)
	if err != nil {
		return nil, err
	}
	if actual != p.ActualFile || checkpointHash != p.Checkpoint.PrefixSHA256 || actual.Size > selected.Content.Size {
		return nil, ErrJournalCorrupt
	}
	request := delivery.TransferRequest{Version: 1, LibraryID: j.library, SelectionID: selected.SelectionID, OperationID: p.Intent.OperationID, Offset: actual.Size, PrefixSHA256: prefixHash}
	appendOwner, err := p.owner.OpenAppend(actual, selected.Content.Size)
	if err != nil {
		return nil, err
	}
	progress, transferErr := client.Transfer(ctx, request, selected, appendOwner)
	closeErr := appendOwner.Close()
	if transferErr != nil || closeErr != nil {
		return nil, errors.Join(transferErr, closeErr)
	}
	full, checkpointHash, wholeHash, err := p.owner.HashPartial(ctx, p.Checkpoint.PrefixLength)
	if err != nil {
		return nil, err
	}
	if full.Size != selected.Content.Size || wholeHash != selected.Content.SHA256 || checkpointHash != p.Checkpoint.PrefixSHA256 || progress.BytesWritten != selected.Content.Size-actual.Size {
		return nil, ErrJournalCorrupt
	}
	target, err := j.ensureTargetForPrepared(p, full)
	if err != nil {
		return nil, err
	}
	prepared := Prepared{Version: 1, IntentSHA256: p.Checkpoint.IntentSHA256, Header: progress.Header, End: progress.End, Root: p.owner.RootIdentity(), Directory: p.owner.DirectoryIdentity(), Lock: p.owner.LockIdentity(), File: full, TargetDirectory: target}
	if err := ValidateChain(Chain{Intent: p.Intent, Selection: &p.Selection, Checkpoint: &p.Checkpoint, Prepared: &prepared}, j.allowLoopbackHTTP); err != nil {
		return nil, ErrJournalCorrupt
	}
	line, digest, err := EncodePrepared(prepared)
	if err != nil {
		return nil, err
	}
	if err := p.owner.WritePreparedOwned(line, target); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		prior, readErr := p.owner.ReadPreparedOwned()
		if readErr != nil || !slices.Equal(prior, line) {
			return nil, errors.Join(readErr, ErrJournalCorrupt)
		}
	}
	if err := j.syncPrepared(p.owner); err != nil {
		return nil, err
	}
	result = &VerifiedLocalCopy{journal: j, owner: p.owner, intent: p.Intent, selection: p.Selection, checkpoint: p.Checkpoint, prepared: prepared, preparedSHA: digest}
	p.owner = nil
	return result, nil
}

// RecoverPrepared is an explicit local recovery operation. It checks a
// retained completed receipt and whole bytes without asking for new server
// rights. It does not rename or publish Verified by itself.
func (j *Journal) RecoverPrepared(ctx context.Context, copyID string) (result *VerifiedLocalCopy, resultErr error) {
	if ctx == nil || j == nil || j.root == nil || !validCopyID(copyID) {
		return nil, ErrInvalidRecord
	}
	control, err := j.root.LockControl()
	if err != nil {
		return nil, err
	}
	controlClosed := false
	defer func() {
		if !controlClosed {
			resultErr = errors.Join(resultErr, control.Close())
		}
	}()
	entries, _, err := j.scan(control)
	if err != nil {
		return nil, err
	}
	entry, ok := entries[copyID]
	if !ok || entry.prepared == nil || entry.released != nil || entry.checkpoint == nil || entry.selection == nil || entry.inventory.Phase != privatefile.NativeReady {
		return nil, ErrJournalConflict
	}
	owner, err := control.OpenCopy(copyID)
	if err != nil {
		return nil, err
	}
	if err := control.Close(); err != nil {
		controlClosed = true
		return nil, errors.Join(err, owner.Close())
	}
	controlClosed = true
	owned := true
	defer func() {
		if owned {
			if e := owner.Close(); e != nil {
				resultErr = errors.Join(resultErr, e)
				result = nil
			}
		}
	}()
	content := entry.selection.Selection.Content
	if entry.final {
		actual, digest, err := owner.HashTarget(ctx, entry.intent.Basename, entry.prepared.TargetDirectory, content.Size)
		if err != nil || actual != entry.prepared.File || digest != content.SHA256 {
			return nil, errors.Join(err, ErrJournalCorrupt)
		}
	} else {
		actual, checkpointHash, digest, err := owner.HashPartial(ctx, entry.checkpoint.PrefixLength)
		if err != nil || actual != entry.prepared.File || checkpointHash != entry.checkpoint.PrefixSHA256 || digest != content.SHA256 {
			return nil, errors.Join(err, ErrJournalCorrupt)
		}
	}
	if err := j.syncPrepared(owner); err != nil {
		return nil, err
	}
	if entry.final {
		if err := j.syncTarget(owner, entry.prepared.TargetDirectory); err != nil {
			return nil, err
		}
		if entry.verified != nil {
			if err := j.syncVerified(owner); err != nil {
				return nil, err
			}
		}
	}
	_, preparedSHA, err := EncodePrepared(*entry.prepared)
	if err != nil {
		return nil, err
	}
	result = &VerifiedLocalCopy{journal: j, owner: owner, intent: entry.intent, selection: *entry.selection, checkpoint: *entry.checkpoint, prepared: *entry.prepared, preparedSHA: preparedSHA, alreadyFinal: entry.final}
	owned = false
	return result, nil
}

// ExposeNoReplace is an explicit caller action. It joins its original owner on
// every return. A completed rename with any later failure remains uncertain;
// RecoverPrepared rechecks the same inode and repeats barriers, never rename.
func (v *VerifiedLocalCopy) ExposeNoReplace(ctx context.Context) (status LocalCopyStatus, resultErr error) {
	if ctx == nil || v == nil {
		return LocalCopyStatus{}, ErrInvalidRecord
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.owner == nil {
		return LocalCopyStatus{}, ErrJournalConflict
	}
	defer func() {
		if e := v.closeLocked(); e != nil {
			resultErr = errors.Join(resultErr, e)
			status = LocalCopyStatus{}
		}
	}()
	if err := ctx.Err(); err != nil {
		return LocalCopyStatus{}, err
	}
	if err := v.journal.syncPrepared(v.owner); err != nil {
		return LocalCopyStatus{}, err
	}
	content := v.selection.Selection.Content
	if !v.alreadyFinal {
		actual, checkpointHash, digest, err := v.owner.HashPartial(ctx, v.checkpoint.PrefixLength)
		if err != nil || actual != v.prepared.File || checkpointHash != v.checkpoint.PrefixSHA256 || digest != content.SHA256 {
			return LocalCopyStatus{}, errors.Join(err, ErrJournalCorrupt)
		}
		if err := ctx.Err(); err != nil {
			return LocalCopyStatus{}, err
		}
		exposed, err := v.owner.ExposeNoReplace(ctx, v.prepared.TargetDirectory, v.intent.Basename, v.prepared.File)
		if err != nil {
			if exposed {
				return LocalCopyStatus{}, errors.Join(err, ErrNativeExposureUncertain)
			}
			return LocalCopyStatus{}, err
		}
		if !exposed {
			return LocalCopyStatus{}, ErrNativeExposureUncertain
		}
	}
	actual, digest, err := v.owner.HashTarget(ctx, v.intent.Basename, v.prepared.TargetDirectory, content.Size)
	if err != nil || actual != v.prepared.File || digest != content.SHA256 {
		return LocalCopyStatus{}, errors.Join(err, ErrNativeExposureUncertain)
	}
	if err := v.journal.syncTarget(v.owner, v.prepared.TargetDirectory); err != nil {
		return LocalCopyStatus{}, errors.Join(err, ErrNativeExposureUncertain)
	}
	verified := Verified{Version: 1, IntentSHA256: v.prepared.IntentSHA256, PreparedSHA256: v.preparedSHA, CopyID: v.intent.CopyID, FinalFile: actual}
	if err := ValidateChain(Chain{Intent: v.intent, Selection: &v.selection, Checkpoint: &v.checkpoint, Prepared: &v.prepared, Verified: &verified}, v.journal.allowLoopbackHTTP); err != nil {
		return LocalCopyStatus{}, ErrJournalCorrupt
	}
	line, _, err := EncodeVerified(verified)
	if err != nil {
		return LocalCopyStatus{}, err
	}
	if err := v.owner.WriteVerifiedOwned(line); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return LocalCopyStatus{}, errors.Join(err, ErrNativeExposureUncertain)
		}
		prior, readErr := v.owner.ReadVerifiedOwned()
		if readErr != nil || !slices.Equal(prior, line) {
			return LocalCopyStatus{}, errors.Join(readErr, ErrJournalCorrupt)
		}
	}
	if err := v.journal.syncVerified(v.owner); err != nil {
		return LocalCopyStatus{}, errors.Join(err, ErrNativeExposureUncertain)
	}
	return v.journal.localStatus(v.intent, v.selection), nil
}

// CopyStatus is offline and rehashes the exact previously verified final
// file. A cached record or path alone never claims the copy is available.
func (j *Journal) CopyStatus(ctx context.Context, copyID string) (status LocalCopyStatus, resultErr error) {
	if ctx == nil || j == nil || j.root == nil || !validCopyID(copyID) {
		return LocalCopyStatus{}, ErrInvalidRecord
	}
	control, err := j.root.LockControl()
	if err != nil {
		return LocalCopyStatus{}, err
	}
	controlClosed := false
	defer func() {
		if !controlClosed {
			resultErr = errors.Join(resultErr, control.Close())
		}
	}()
	entries, _, err := j.scan(control)
	if err != nil {
		return LocalCopyStatus{}, err
	}
	entry, ok := entries[copyID]
	if !ok || entry.verified == nil || entry.prepared == nil || !entry.final || entry.selection == nil || entry.released != nil {
		return LocalCopyStatus{}, ErrJournalConflict
	}
	owner, err := control.OpenCopy(copyID)
	if err != nil {
		return LocalCopyStatus{}, err
	}
	if err := control.Close(); err != nil {
		controlClosed = true
		return LocalCopyStatus{}, errors.Join(err, owner.Close())
	}
	controlClosed = true
	defer func() {
		if e := owner.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			status = LocalCopyStatus{}
		}
	}()
	content := entry.selection.Selection.Content
	actual, digest, err := owner.HashTarget(ctx, entry.intent.Basename, entry.prepared.TargetDirectory, content.Size)
	if err != nil || actual != entry.verified.FinalFile || digest != content.SHA256 {
		return LocalCopyStatus{}, errors.Join(err, ErrJournalCorrupt)
	}
	if err := j.syncPrepared(owner); err != nil {
		return LocalCopyStatus{}, err
	}
	if err := j.syncTarget(owner, entry.prepared.TargetDirectory); err != nil {
		return LocalCopyStatus{}, err
	}
	if err := j.syncVerified(owner); err != nil {
		return LocalCopyStatus{}, err
	}
	return j.localStatus(entry.intent, *entry.selection), nil
}
