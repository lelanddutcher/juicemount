package nativeclient

import (
	"errors"
	"os"
	"slices"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

const NativeIntentLimit = 4096
const NativeMetadataLimit int64 = NativeIntentLimit * MetadataReservationBytes
const NativeLogicalLimit int64 = 4 << 30

var ErrJournalCorrupt = errors.New("native journal accounting or record corrupt")
var ErrJournalConflict = errors.New("native journal immutable request conflict")
var ErrJournalLimit = errors.New("native journal reservation limit")
var ErrJournalUnverifiedTail = errors.New("native partial tail requires server prefix validation")

// Journal derives irreversible reservations from the root inventory under the
// control lock, then compares every acknowledged custody object against it.
// Pending phases remain charged while exact original requests finish them.
type Journal struct {
	root               *privatefile.NativeRoot
	rootPath           string
	endpoint           string
	library            continuity.LibraryID
	allowLoopbackHTTP  bool
	controlCloseErr    error // test-only post-close fault; original control is always closed
	preparedBarrierErr error // test-only exact receipt-barrier fault; no callback authority
	targetBarrierErr   error // test-only exact target-barrier fault
	verifiedBarrierErr error // test-only exact receipt-barrier fault
}

type NativeUsage struct {
	IntentCount   int
	MetadataBytes int64
	LogicalBytes  int64
}

func reserveIntent(usage NativeUsage) error {
	if usage.IntentCount >= NativeIntentLimit || usage.MetadataBytes > NativeMetadataLimit-MetadataReservationBytes {
		return ErrJournalLimit
	}
	return nil
}
func reserveSelection(usage NativeUsage, size int64) error {
	if size < 0 || size > NativeLogicalLimit-usage.LogicalBytes {
		return ErrJournalLimit
	}
	return nil
}

type journalEntry struct {
	inventory       privatefile.NativeInventoryEntry
	intent          Intent
	intentSHA       string
	selection       *SelectionRecord
	checkpoint      *Checkpoint
	prepared        *Prepared
	verified        *Verified
	released        *Released
	partial         bool
	targetPending   bool
	targetDirectory privatefile.Identity
	final           bool
}

func OpenJournal(rootPath, endpoint string, library continuity.LibraryID, allowLoopbackHTTP bool) (*Journal, error) {
	if _, err := ParseEndpoint(endpoint, allowLoopbackHTTP); err != nil || library.Validate() != nil {
		return nil, ErrInvalidRecord
	}
	root, err := privatefile.OpenNativeRoot(rootPath, endpoint, string(library))
	if err != nil {
		return nil, err
	}
	return &Journal{root: root, rootPath: rootPath, endpoint: endpoint, library: library, allowLoopbackHTTP: allowLoopbackHTTP}, nil
}
func CreateJournal(rootPath, endpoint string, library continuity.LibraryID, allowLoopbackHTTP bool) (*Journal, error) {
	if _, err := ParseEndpoint(endpoint, allowLoopbackHTTP); err != nil || library.Validate() != nil {
		return nil, ErrInvalidRecord
	}
	root, err := privatefile.CreateNativeRoot(rootPath, endpoint, string(library))
	if err != nil {
		return nil, err
	}
	return &Journal{root: root, rootPath: rootPath, endpoint: endpoint, library: library, allowLoopbackHTTP: allowLoopbackHTTP}, nil
}
func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	return j.root.Close()
}

func optionalRecord(control *privatefile.NativeControl, id, kind string) ([]byte, error) {
	var b []byte
	var err error
	switch kind {
	case "selection":
		b, err = control.ReadSelection(id)
	case "checkpoint":
		b, err = control.ReadCheckpoint(id)
	case "prepared":
		b, err = control.ReadPrepared(id)
	case "verified":
		b, err = control.ReadVerified(id)
	case "released":
		b, err = control.ReadReleased(id)
	default:
		return nil, ErrJournalCorrupt
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

func (j *Journal) scan(control *privatefile.NativeControl) (map[string]journalEntry, NativeUsage, error) {
	var usage NativeUsage
	witnesses, err := control.InventoryEntries()
	if err != nil {
		return nil, usage, errors.Join(ErrJournalCorrupt, err)
	}
	ids, err := control.CopyIDs()
	if err != nil {
		return nil, usage, errors.Join(ErrJournalCorrupt, err)
	}
	if len(witnesses) > NativeIntentLimit {
		return nil, usage, ErrJournalCorrupt
	}
	targetIDs, err := control.TargetIDs()
	if err != nil {
		return nil, usage, errors.Join(ErrJournalCorrupt, err)
	}
	ownedTargets := make(map[string]bool, len(targetIDs))
	for _, id := range targetIDs {
		ownedTargets[id] = true
	}
	entries := make(map[string]journalEntry, len(witnesses))
	ownedDirs := make(map[string]bool, len(ids))
	for _, id := range ids {
		ownedDirs[id] = true
	}
	for _, w := range witnesses {
		id := w.CopyID
		entry := journalEntry{inventory: w, intentSHA: w.IntentSHA256}
		usage.IntentCount++
		usage.MetadataBytes += w.MetadataBytes
		usage.LogicalBytes += w.LogicalBytes
		if !ownedDirs[id] {
			if w.Phase != privatefile.NativeIntentPending || ownedTargets[id] {
				return nil, usage, ErrJournalCorrupt
			}
			entries[id] = entry
			continue
		}
		delete(ownedDirs, id)
		var names []string
		if w.Phase == privatefile.NativeIntentPending {
			names, err = control.PendingCopyEntries(id)
		} else {
			names, err = control.CopyEntries(id)
		}
		if err != nil {
			return nil, usage, errors.Join(ErrJournalCorrupt, err)
		}
		if w.Phase != privatefile.NativeIntentPending {
			dirID, lockID, err := control.CopyPinnedIdentity(id)
			if err != nil || dirID != w.Directory || lockID != w.Lock {
				return nil, usage, errors.Join(err, ErrJournalCorrupt)
			}
		}
		entry.partial = slices.Contains(names, "payload.partial")
		if slices.Contains(names, "intent.json") {
			line, err := control.ReadIntent(id)
			if err != nil {
				return nil, usage, errors.Join(ErrJournalCorrupt, err)
			}
			intent, digest, err := DecodeIntent(line, j.allowLoopbackHTTP)
			if err != nil || digest != w.IntentSHA256 || intent.CopyID != id || intent.Endpoint != j.endpoint || intent.LibraryID != j.library {
				return nil, usage, ErrJournalCorrupt
			}
			entry.intent = intent
		} else if w.Phase != privatefile.NativeIntentPending {
			return nil, usage, ErrJournalCorrupt
		}
		selectionLine, err := optionalRecord(control, id, "selection")
		if err != nil {
			return nil, usage, errors.Join(ErrJournalCorrupt, err)
		}
		if selectionLine != nil {
			record, digest, err := DecodeSelection(selectionLine)
			if err != nil || digest != w.SelectionSHA256 || record.LogicalReservationBytes != w.LogicalBytes {
				return nil, usage, ErrJournalCorrupt
			}
			entry.selection = &record
		} else if w.Phase != privatefile.NativeIntentPending && w.Phase != privatefile.NativeIntentReady && w.Phase != privatefile.NativeSelectionPending {
			return nil, usage, ErrJournalCorrupt
		}
		checkpointLine, err := optionalRecord(control, id, "checkpoint")
		if err != nil {
			return nil, usage, errors.Join(ErrJournalCorrupt, err)
		}
		if checkpointLine != nil {
			record, _, err := DecodeCheckpoint(checkpointLine)
			if err != nil {
				return nil, usage, ErrJournalCorrupt
			}
			entry.checkpoint = &record
		} else if w.Phase == privatefile.NativeReady {
			return nil, usage, ErrJournalCorrupt
		}
		preparedLine, err := optionalRecord(control, id, "prepared")
		if err != nil {
			return nil, usage, errors.Join(ErrJournalCorrupt, err)
		}
		if preparedLine != nil {
			record, _, err := DecodePrepared(preparedLine)
			if err != nil {
				return nil, usage, ErrJournalCorrupt
			}
			entry.prepared = &record
		}
		verifiedLine, err := optionalRecord(control, id, "verified")
		if err != nil {
			return nil, usage, errors.Join(ErrJournalCorrupt, err)
		}
		if verifiedLine != nil {
			record, _, err := DecodeVerified(verifiedLine)
			if err != nil {
				return nil, usage, ErrJournalCorrupt
			}
			entry.verified = &record
		}
		releasedLine, err := optionalRecord(control, id, "released")
		if err != nil {
			return nil, usage, errors.Join(ErrJournalCorrupt, err)
		}
		if releasedLine != nil {
			record, _, err := DecodeReleased(releasedLine)
			if err != nil {
				return nil, usage, ErrJournalCorrupt
			}
			entry.released = &record
		}
		if entry.intentSHA != "" && entry.intent.CopyID != "" {
			if err := ValidateChain(Chain{Intent: entry.intent, Selection: entry.selection, Checkpoint: entry.checkpoint, Prepared: entry.prepared, Verified: entry.verified, Released: entry.released}, j.allowLoopbackHTTP); err != nil {
				return nil, usage, ErrJournalCorrupt
			}
		}
		switch w.Phase {
		case privatefile.NativeIntentPending:
			if entry.selection != nil || entry.checkpoint != nil || entry.partial {
				return nil, usage, ErrJournalCorrupt
			}
		case privatefile.NativeIntentReady, privatefile.NativeSelectionPending, privatefile.NativeSelectionReady:
			if entry.checkpoint != nil || entry.partial {
				return nil, usage, ErrJournalCorrupt
			}
		case privatefile.NativeZeroPending, privatefile.NativeZeroBound, privatefile.NativeReady:
			if w.Phase == privatefile.NativeZeroPending && entry.checkpoint != nil {
				return nil, usage, ErrJournalCorrupt
			}
			if w.Phase != privatefile.NativeZeroPending && !entry.partial && entry.prepared == nil {
				return nil, usage, ErrJournalCorrupt
			}
			if entry.checkpoint != nil {
				initial := Checkpoint{Version: 1, IntentSHA256: entry.intentSHA, Root: control.RootIdentity(), Directory: w.Directory, Lock: w.Lock, File: w.Partial, PrefixLength: 0, PrefixSHA256: emptyDigest()}
				if *entry.checkpoint != initial {
					return nil, usage, ErrJournalCorrupt
				}
			}
			if entry.partial {
				actual, err := control.CopyPartialIdentity(id)
				if err != nil {
					return nil, usage, errors.Join(err, ErrJournalCorrupt)
				}
				if w.Phase != privatefile.NativeZeroPending && (actual.Device != w.Partial.Device || actual.Inode != w.Partial.Inode || actual.UID != w.Partial.UID || actual.Mode != w.Partial.Mode) {
					return nil, usage, ErrJournalCorrupt
				}
				if w.Phase != privatefile.NativeReady && actual.Size != 0 {
					return nil, usage, ErrJournalCorrupt
				}
			}
		}
		if entry.released != nil {
			return nil, usage, ErrJournalCorrupt
		}
		if ownedTargets[id] {
			delete(ownedTargets, id)
			if w.Phase != privatefile.NativeReady {
				return nil, usage, ErrJournalCorrupt
			}
			directory, names, err := control.TargetDirectory(id)
			if err != nil {
				return nil, usage, errors.Join(err, ErrJournalCorrupt)
			}
			entry.targetDirectory = directory
			if entry.prepared == nil {
				if len(names) != 0 || entry.verified != nil || !entry.partial {
					return nil, usage, ErrJournalCorrupt
				}
				entry.targetPending = true
			} else {
				p := entry.prepared
				if p.Root != control.RootIdentity() || p.Directory != w.Directory || p.Lock != w.Lock || p.TargetDirectory != directory {
					return nil, usage, ErrJournalCorrupt
				}
				switch {
				case len(names) == 0 && entry.partial && entry.verified == nil:
					actual, err := control.CopyPartialIdentity(id)
					if err != nil || actual != p.File {
						return nil, usage, errors.Join(err, ErrJournalCorrupt)
					}
				case len(names) == 1 && names[0] == entry.intent.Basename && !entry.partial:
					actual, err := control.TargetFileIdentity(id, entry.intent.Basename)
					if err != nil || actual != p.File {
						return nil, usage, errors.Join(err, ErrJournalCorrupt)
					}
					if entry.verified != nil && entry.verified.FinalFile != actual {
						return nil, usage, ErrJournalCorrupt
					}
					entry.final = true
				default:
					return nil, usage, ErrJournalCorrupt
				}
			}
		} else if entry.prepared != nil || entry.verified != nil {
			return nil, usage, ErrJournalCorrupt
		}
		if entry.verified != nil && !entry.final {
			return nil, usage, ErrJournalCorrupt
		}
		entries[id] = entry
	}
	if len(ownedDirs) != 0 || len(ownedTargets) != 0 || usage.MetadataBytes > NativeMetadataLimit || usage.LogicalBytes > NativeLogicalLimit {
		return nil, usage, ErrJournalCorrupt
	}
	return entries, usage, nil
}

func (j *Journal) Usage() (_ NativeUsage, resultErr error) {
	if j == nil || j.root == nil {
		return NativeUsage{}, ErrJournalCorrupt
	}
	control, err := j.root.LockControl()
	if err != nil {
		return NativeUsage{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	_, usage, err := j.scan(control)
	return usage, err
}

// CreateIntent is idempotent only for the exact canonical bytes. A failed
// create leaves its directory/leaf for inspection; it is never replaced.
func (j *Journal) CreateIntent(intent Intent) (resultErr error) {
	if j == nil || j.root == nil || intent.Endpoint != j.endpoint || intent.LibraryID != j.library {
		return ErrInvalidRecord
	}
	line, digest, err := EncodeIntent(intent, j.allowLoopbackHTTP)
	if err != nil {
		return err
	}
	control, err := j.root.LockControl()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	entries, usage, err := j.scan(control)
	if err != nil {
		return err
	}
	if prior, ok := entries[intent.CopyID]; ok {
		if prior.intentSHA != digest {
			return ErrJournalConflict
		}
		if prior.inventory.Phase != privatefile.NativeIntentPending {
			priorLine, err := control.ReadIntent(intent.CopyID)
			if err != nil {
				return errors.Join(ErrJournalCorrupt, err)
			}
			if !slices.Equal(priorLine, line) || prior.intent != intent {
				return ErrJournalConflict
			}
			copy, err := control.OpenCopy(intent.CopyID)
			if err != nil {
				return err
			}
			defer func() { resultErr = errors.Join(resultErr, copy.Close()) }()
			return copy.SyncIntent()
		}
		copy, err := control.ResumePendingCopy(intent.CopyID)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, copy.Close()) }()
		return finishPendingIntent(control, copy, intent.CopyID, line)
	}
	if err := reserveIntent(usage); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.targetPending || entry.inventory.Phase != privatefile.NativeReady && entry.inventory.Phase != privatefile.NativeSelectionReady && entry.inventory.Phase != privatefile.NativeIntentReady {
			return ErrJournalCorrupt
		}
	}
	if err := control.ReserveIntent(intent.CopyID, digest); err != nil {
		if errors.Is(err, privatefile.ErrNativeInventoryLimit) {
			return ErrJournalLimit
		}
		return err
	}
	copy, err := control.ResumePendingCopy(intent.CopyID)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, copy.Close()) }()
	return finishPendingIntent(control, copy, intent.CopyID, line)
}
func finishPendingIntent(control *privatefile.NativeControl, copy *privatefile.NativeCopy, id string, line []byte) error {
	prior, err := copy.ReadIntent()
	if errors.Is(err, os.ErrNotExist) {
		if err := copy.WriteIntent(line); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !slices.Equal(prior, line) {
		return ErrJournalConflict
	}
	if err := copy.SyncIntent(); err != nil {
		return err
	}
	if err := copy.SyncNamespace(); err != nil {
		return err
	}
	return control.IntentReady(id, copy.DirectoryIdentity(), copy.LockIdentity())
}

// PinSelection charges the selected logical size before any payload leaf can
// be created. A repeated exact selection neither replaces it nor recharges.
func (j *Journal) PinSelection(copyID string, selection catalog.DeliverySelection) (resultErr error) {
	if j == nil || j.root == nil {
		return ErrJournalCorrupt
	}
	control, err := j.root.LockControl()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	entries, usage, err := j.scan(control)
	if err != nil {
		return err
	}
	entry, ok := entries[copyID]
	if !ok {
		return ErrJournalConflict
	}
	record := SelectionRecord{Version: 1, IntentSHA256: entry.intentSHA, Selection: selection, LogicalReservationBytes: selection.Content.Size}
	if err := ValidateChain(Chain{Intent: entry.intent, Selection: &record}, j.allowLoopbackHTTP); err != nil {
		return ErrInvalidRecord
	}
	line, digest, err := EncodeSelection(record)
	if err != nil {
		return err
	}
	if entry.inventory.SelectionSHA256 != "" {
		if entry.inventory.SelectionSHA256 != digest {
			return ErrJournalConflict
		}
		if entry.inventory.Phase == privatefile.NativeSelectionPending {
			copy, err := control.OpenCopy(copyID)
			if err != nil {
				return err
			}
			defer func() { resultErr = errors.Join(resultErr, copy.Close()) }()
			return finishPendingSelection(control, copy, copyID, line)
		}
		prior, err := control.ReadSelection(copyID)
		if err != nil {
			return errors.Join(ErrJournalCorrupt, err)
		}
		if !slices.Equal(prior, line) {
			return ErrJournalConflict
		}
		copy, err := control.OpenCopy(copyID)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, copy.Close()) }()
		return copy.SyncSelection()
	}
	if err := reserveSelection(usage, selection.Content.Size); err != nil {
		return err
	}
	if entry.inventory.Phase != privatefile.NativeIntentReady {
		return ErrJournalConflict
	}
	for id, other := range entries {
		if id != copyID && (other.targetPending || other.inventory.Phase != privatefile.NativeReady && other.inventory.Phase != privatefile.NativeSelectionReady && other.inventory.Phase != privatefile.NativeIntentReady) {
			return ErrJournalCorrupt
		}
	}
	if err := control.ReserveSelection(copyID, digest, selection.Content.Size); err != nil {
		if errors.Is(err, privatefile.ErrNativeInventoryLimit) {
			return ErrJournalLimit
		}
		return err
	}
	copy, err := control.OpenCopy(copyID)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, copy.Close()) }()
	return finishPendingSelection(control, copy, copyID, line)
}
func finishPendingSelection(control *privatefile.NativeControl, copy *privatefile.NativeCopy, id string, line []byte) error {
	prior, err := copy.ReadSelection()
	if errors.Is(err, os.ErrNotExist) {
		if err := copy.WriteSelection(line); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !slices.Equal(prior, line) {
		return ErrJournalConflict
	}
	if err := copy.SyncSelection(); err != nil {
		return err
	}
	return control.SelectionReady(id)
}

// PendingCopy retains the independent owner lock and the exact checkpoint.
// Tail growth is reported as unverified; this layer never truncates or hashes
// it into a trusted cursor. A future receiver must validate it with the server.
type PendingCopy struct {
	owner                       *privatefile.NativeCopy
	Intent                      Intent
	Selection                   SelectionRecord
	Checkpoint                  Checkpoint
	ActualFile                  privatefile.Identity
	NeedsServerPrefixValidation bool
}

func (p *PendingCopy) Close() error {
	if p == nil || p.owner == nil {
		return nil
	}
	err := p.owner.Close()
	p.owner = nil
	return err
}

func (j *Journal) InitialPartial(copyID string) (result Checkpoint, resultErr error) {
	if j == nil || j.root == nil {
		return Checkpoint{}, ErrJournalCorrupt
	}
	control, err := j.root.LockControl()
	if err != nil {
		return Checkpoint{}, err
	}
	defer func() {
		if e := control.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			result = Checkpoint{}
		}
	}()
	entries, _, err := j.scan(control)
	if err != nil {
		return Checkpoint{}, err
	}
	entry, ok := entries[copyID]
	if !ok || entry.selection == nil || entry.prepared != nil || entry.verified != nil || entry.released != nil {
		return Checkpoint{}, ErrJournalConflict
	}
	phase := entry.inventory.Phase
	if phase != privatefile.NativeSelectionReady && phase != privatefile.NativeZeroPending && phase != privatefile.NativeZeroBound && phase != privatefile.NativeReady {
		return Checkpoint{}, ErrJournalConflict
	}
	copy, err := control.OpenCopy(copyID)
	if err != nil {
		return Checkpoint{}, err
	}
	defer func() {
		if e := copy.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			result = Checkpoint{}
		}
	}()
	if phase == privatefile.NativeReady {
		actual, err := copy.PartialIdentity()
		if err != nil || actual.Device != entry.checkpoint.File.Device || actual.Inode != entry.checkpoint.File.Inode || actual.UID != entry.checkpoint.File.UID || actual.Mode != entry.checkpoint.File.Mode || actual.Size != 0 || *entry.checkpoint != (Checkpoint{Version: 1, IntentSHA256: entry.intentSHA, Root: copy.RootIdentity(), Directory: copy.DirectoryIdentity(), Lock: copy.LockIdentity(), File: actual, PrefixLength: 0, PrefixSHA256: emptyDigest()}) {
			return Checkpoint{}, errors.Join(err, ErrJournalCorrupt)
		}
		if err := errors.Join(copy.SyncPartial(), copy.SyncInitialCheckpoint()); err != nil {
			return Checkpoint{}, err
		}
		return *entry.checkpoint, nil
	}
	if phase == privatefile.NativeSelectionReady {
		if err := control.ZeroPending(copyID); err != nil {
			return Checkpoint{}, err
		}
		phase = privatefile.NativeZeroPending
	}
	var fileID privatefile.Identity
	if phase == privatefile.NativeZeroPending {
		fileID, err = copy.PartialIdentity()
		if errors.Is(err, os.ErrNotExist) {
			fileID, err = copy.CreateZeroPartial()
		}
		if err != nil || fileID.Size != 0 {
			return Checkpoint{}, errors.Join(err, ErrJournalCorrupt)
		}
		if err := copy.SyncPartial(); err != nil {
			return Checkpoint{}, err
		}
		if err := control.ZeroBound(copyID, fileID); err != nil {
			return Checkpoint{}, err
		}
		phase = privatefile.NativeZeroBound
	} else {
		fileID, err = copy.PartialIdentity()
		if err != nil || fileID != entry.inventory.Partial || fileID.Size != 0 {
			return Checkpoint{}, errors.Join(err, ErrJournalCorrupt)
		}
	}
	checkpoint := Checkpoint{Version: 1, IntentSHA256: entry.intentSHA, Root: copy.RootIdentity(), Directory: copy.DirectoryIdentity(), Lock: copy.LockIdentity(), File: fileID, PrefixLength: 0, PrefixSHA256: emptyDigest()}
	if err := ValidateChain(Chain{Intent: entry.intent, Selection: entry.selection, Checkpoint: &checkpoint}, j.allowLoopbackHTTP); err != nil {
		return Checkpoint{}, err
	}
	line, _, err := EncodeCheckpoint(checkpoint)
	if err != nil {
		return Checkpoint{}, err
	}
	prior, err := copy.ReadCheckpoint()
	if errors.Is(err, os.ErrNotExist) {
		if err := copy.WriteInitialCheckpoint(line); err != nil {
			return Checkpoint{}, err
		}
	} else if err != nil {
		return Checkpoint{}, err
	} else if !slices.Equal(prior, line) {
		return Checkpoint{}, ErrJournalCorrupt
	}
	if err := copy.SyncInitialCheckpoint(); err != nil {
		return Checkpoint{}, err
	}
	if err := control.Ready(copyID); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

func (j *Journal) Recover(copyID string) (_ *PendingCopy, resultErr error) {
	if j == nil || j.root == nil {
		return nil, ErrJournalCorrupt
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
	if !ok || entry.inventory.Phase != privatefile.NativeReady || entry.selection == nil || entry.checkpoint == nil || !entry.partial || entry.prepared != nil || entry.verified != nil || entry.released != nil {
		return nil, ErrJournalConflict
	}
	copy, err := control.OpenCopy(copyID)
	if err != nil {
		return nil, err
	}
	actual, err := copy.PartialIdentity()
	if err != nil || actual.Device != entry.checkpoint.File.Device || actual.Inode != entry.checkpoint.File.Inode || actual.UID != entry.checkpoint.File.UID || actual.Mode != entry.checkpoint.File.Mode || actual.Size < entry.checkpoint.PrefixLength || actual.Size > entry.selection.Selection.Content.Size {
		return nil, errors.Join(err, copy.Close(), ErrJournalCorrupt)
	}
	if entry.checkpoint.Root != copy.RootIdentity() || entry.checkpoint.Directory != copy.DirectoryIdentity() || entry.checkpoint.Lock != copy.LockIdentity() {
		return nil, errors.Join(copy.Close(), ErrJournalCorrupt)
	}
	closeErr := errors.Join(control.Close(), j.controlCloseErr)
	controlClosed = true
	if closeErr != nil {
		return nil, errors.Join(closeErr, copy.Close())
	}
	return &PendingCopy{owner: copy, Intent: entry.intent, Selection: *entry.selection, Checkpoint: *entry.checkpoint, ActualFile: actual, NeedsServerPrefixValidation: actual.Size > entry.checkpoint.PrefixLength}, nil
}
