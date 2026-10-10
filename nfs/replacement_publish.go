package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path"
	"sync"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// Replacement publication is separate from ordinary drain completion: a
// committed server receipt does not release the accepted local image.
// These interfaces bind one already-qualified parent directory; names never
// select a different parent. The production implementation retains its FD.
type replacementPublishFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (replaceguard.File, error)
	SetMtime(int64) error
	SetMode(os.FileMode) error
	Sync() error
	Close() error
}
type replacementPublishIO interface {
	Check() error
	Open(name string, create bool) (replacementPublishFile, error)
	Create(name string, mode os.FileMode) (replacementPublishFile, error)
	OpenOwned(name string, expected replaceguard.File) (replacementPublishFile, error)
	Stat(name string) (replaceguard.File, error)
	Rename(source, destination string) error
}

// A row stays READY, so ordinary MarkDraining is deliberately not used.
// This reservation supplements durable journal CAS and namespace ownership.
type replacementWorkers struct {
	mu   sync.Mutex
	rows map[int64]bool
}

func (r *replacementWorkers) claim(id int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rows[id] {
		return false
	}
	if r.rows == nil {
		r.rows = map[int64]bool{}
	}
	r.rows[id] = true
	return true
}
func (r *replacementWorkers) release(id int64) { r.mu.Lock(); delete(r.rows, id); r.mu.Unlock() }

func (r *replacementWorkers) isEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rows) == 0
}

// Admission and retirement share Stop's lock. A replacement may begin before
// Start, but never after Stop seals admission. All owned I/O closes first.
func (d *Drainer) beginReplacement(id int64) bool {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.stopping || !d.replacementWorkers.claim(id) {
		return false
	}
	d.inFlight.Add(1)
	return true
}

func (d *Drainer) endReplacement(id int64) {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.replacementWorkers.release(id)
	d.inFlight.Done()
}

func replacementRecordBytes(r replaceguard.Record) string {
	b, _ := replaceguard.Encode(r)
	return string(b)
}
func sameReplacementEvidence(a, b replaceguard.Record, normalizeOwner bool) bool {
	if b.Epoch < a.Epoch {
		return false
	}
	a.Proxy, a.Epoch, a.Phase = b.Proxy, b.Epoch, b.Phase
	if normalizeOwner {
		a.Owner = b.Owner
	}
	return replacementRecordBytes(a) != "" && replacementRecordBytes(a) == replacementRecordBytes(b)
}

func (c *ProjectLeaseClient) replacementRead(ctx context.Context, stage uint64) (*replaceguard.Record, error) {
	if c == nil || stage <= 1 {
		return nil, metadata.ErrMirrorConflict
	}
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	raw, err := c.rdb.Do(ctx, "GET", replaceguard.Key(stage)).Text()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r, err := replaceguard.Decode([]byte(raw))
	if err != nil || r.Stage.Inode != stage {
		return nil, metadata.ErrMirrorConflict
	}
	return &r, nil
}
func (c *ProjectLeaseClient) replacementArm(ctx context.Context, wanted replaceguard.Record, prior *replaceguard.Record) (replaceguard.Record, error) {
	var empty replaceguard.Record
	if c == nil || wanted.Phase != "prepared" {
		return empty, metadata.ErrMirrorConflict
	}
	raw, err := replaceguard.Encode(wanted)
	if err != nil {
		return empty, err
	}
	args := []any{"JM.LEASE", "ARM_REPLACE", string(raw)}
	if prior != nil {
		p, err := replaceguard.Encode(*prior)
		if err != nil || prior.Phase != "prepared" {
			return empty, metadata.ErrMirrorConflict
		}
		args = append(args, string(p))
	}
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	response, err := c.rdb.Do(ctx, args...).Text()
	if err != nil {
		return empty, err
	}
	got, err := replaceguard.Decode([]byte(response))
	if err != nil || got.Phase != "prepared" || got.Epoch != wanted.Epoch || !sameReplacementEvidence(wanted, got, prior == nil) {
		return empty, metadata.ErrMirrorConflict
	}
	return got, nil
}

func verifyReplacementStage(backend replacementPublishIO, p string, image metadata.SpoolReplacementImage, expected *replaceguard.File) (replaceguard.File, error) {
	var empty replaceguard.File
	f, err := backend.Open(p, false)
	if err != nil {
		return empty, err
	}
	before, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return empty, err
	}
	if before.Length != uint64(image.Size) || before.MtimeNS != image.MtimeNS || (expected != nil && before != *expected) {
		_ = f.Close()
		return empty, metadata.ErrMirrorConflict
	}
	digest := sha256.New()
	n, readErr := io.Copy(digest, io.NewSectionReader(f, 0, image.Size))
	after, statErr := f.Stat()
	closeErr := f.Close()
	named, nameErr := backend.Stat(p)
	if readErr != nil || statErr != nil || closeErr != nil || nameErr != nil || n != image.Size || before != after || named != before || !bytes.Equal(digest.Sum(nil), image.SHA256) {
		return empty, errors.Join(metadata.ErrMirrorConflict, readErr, statErr, closeErr, nameErr)
	}
	return before, backend.Check()
}

func uploadReplacementStage(ctx context.Context, backend replacementPublishIO, name string, source *replacementImage, image metadata.SpoolReplacementImage, mode os.FileMode, openStage func() (replacementPublishFile, error)) (replaceguard.File, error) {
	var empty replaceguard.File
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := backend.Check(); err != nil {
		return empty, err
	}
	if openStage == nil {
		return empty, metadata.ErrMirrorConflict
	}
	f, err := openStage()
	if err != nil {
		return empty, err
	}
	// No error path removes or truncates the stage. The opener establishes
	// fresh-create or receipt-qualified custody before any bytes are written.
	buf := make([]byte, 1<<20)
	for off := int64(0); off < image.Size; {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return empty, err
		}
		amount := min(int64(len(buf)), image.Size-off)
		n, err := source.file.ReadAt(buf[:amount], off)
		if err != nil || int64(n) != amount {
			_ = f.Close()
			return empty, errors.Join(err, io.ErrUnexpectedEOF)
		}
		wrote, err := f.WriteAt(buf[:n], off)
		if err != nil || wrote != n {
			_ = f.Close()
			return empty, errors.Join(err, io.ErrShortWrite)
		}
		off += int64(n)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return empty, err
	}
	if err := f.SetMode(mode.Perm()); err != nil {
		_ = f.Close()
		return empty, err
	}
	if err := f.SetMtime(image.MtimeNS); err != nil {
		_ = f.Close()
		return empty, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return empty, err
	}
	created, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return empty, err
	}
	if err := f.Close(); err != nil {
		return empty, err
	}
	if err := source.VerifyImage(image); err != nil {
		return empty, err
	}
	return verifyReplacementStage(backend, name, image, &created)
}

func makeReplacementRecord(w metadata.SpoolReplacementWitness, stage, original replaceguard.File, epoch uint64) (replaceguard.Record, error) {
	r := replaceguard.Record{Version: 1, OperationID: w.OperationID, Owner: "pending-authentication", Proxy: "pending-incarnation", BackendUUID: w.Binding.FormatUUID, BackendDigest: w.Binding.Digest, AcceptedEpoch: w.AcceptedEpoch, Epoch: epoch, Stage: stage, Destination: original, GraphDigest: w.GraphDigest, ImageSHA256: hex.EncodeToString(w.Image.SHA256), Phase: "prepared"}
	r.Parents = []replaceguard.Parent{{Inode: 1}}
	for _, node := range w.Destination.Chain[:len(w.Destination.Chain)-1] {
		r.Parents = append(r.Parents, replaceguard.Parent{Name: path.Base(node.Path), Inode: node.Inode})
		if node.Path == projectOf(w.Destination.Path) {
			r.Project = node.Inode
		}
	}
	last := w.Destination.Chain[len(w.Destination.Chain)-1]
	if last.MtimeSec < 0 || last.MtimeNsec >= 1e9 || last.MtimeSec > (math.MaxInt64-int64(last.MtimeNsec))/1e9 {
		return r, metadata.ErrMirrorConflict
	}
	if original.Inode != last.Inode || original.Length != uint64(last.Size) || original.MtimeNS != last.MtimeSec*1e9+int64(last.MtimeNsec) || original.Name != path.Base(last.Path) || stage.Length != uint64(w.Image.Size) || stage.MtimeNS != w.Image.MtimeNS {
		return r, metadata.ErrMirrorConflict
	}
	return r, r.Validate()
}

// publishReplacement records the server receipt and retains READY/local bytes.
// It is not dispatched by the ordinary queue until settlement/cleanup is
// qualified. Factory and binding verification must refer to the same selected
// backend as the handler; a factory never adopts a stage merely by its name.
func (d *Drainer) publishReplacement(ctx context.Context, rowID int64, factory func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error), verifyBinding func(metadata.CachePathBinding) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.spool == nil || d.onDrainAdmission == nil || factory == nil || verifyBinding == nil || !d.beginReplacement(rowID) {
		return metadata.ErrMirrorConflict
	}
	defer d.endReplacement(rowID)
	return d.publishReplacementOwned(ctx, rowID, factory, verifyBinding)
}

// publishReplacementOwned requires the caller's lifecycle/row claim.
func (d *Drainer) publishReplacementOwned(ctx context.Context, rowID int64, factory func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error), verifyBinding func(metadata.CachePathBinding) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.spool == nil || d.onDrainAdmission == nil || factory == nil || verifyBinding == nil {
		return metadata.ErrMirrorConflict
	}
	if d.spool.meta.Owner() == nil {
		return metadata.ErrMirrorConflict
	}
	row, err := d.spool.meta.Get(rowID)
	if err != nil {
		return err
	}
	op := d.onDrainAdmission(row.NFSPath)
	if op == nil {
		return metadata.ErrMirrorConflict
	}
	defer op.finish(nil)
	image, w, err := d.spool.openAcceptedReplacement(row)
	if err != nil {
		return err
	}
	if w == nil {
		return metadata.ErrMirrorConflict
	}
	defer image.Close()
	verify := func() error {
		if err := verifyBinding(w.Binding); err != nil {
			return err
		}
		return image.VerifyImage(w.Image)
	}
	if err := verify(); err != nil {
		return err
	}
	hashed, err := image.HashImage()
	if err != nil || !bytes.Equal(hashed.SHA256, w.Image.SHA256) {
		return errors.Join(metadata.ErrMirrorConflict, err)
	}
	p, err := d.spool.meta.ReplacementPublication(row)
	if err != nil || p == nil {
		return errors.Join(metadata.ErrMirrorConflict, err)
	}
	if p.Phase == metadata.ReplacementPublicationCommitted {
		return nil
	}
	if pin.IsOffline() {
		return metadata.ErrMirrorConflict
	}
	if p.Phase == metadata.ReplacementPublicationQueued {
		p, err = d.spool.meta.Owner().BeginSpoolReplacementPublication(rowID, p.Expected, verify)
		if err != nil {
			return err
		}
	}
	backend, closeBackend, err := factory(*w)
	if err != nil {
		return err
	}
	if backend == nil || closeBackend == nil {
		return metadata.ErrMirrorConflict
	}
	defer closeBackend()
	if err := backend.Check(); err != nil {
		return err
	}
	client := d.leasesPtr.Load()
	if client == nil {
		return errLeasesUnsupported
	}
	advance := func(phase string, record replaceguard.Record) error {
		next := *p
		next.Phase = phase
		next.Record = replacementRecordBytes(record)
		next.Revision++
		updated, err := d.spool.meta.Owner().AdvanceSpoolReplacementPublication(*p, next, verify)
		if err != nil {
			return err
		}
		p = updated
		return nil
	}
	// A completed operation is historical evidence, not a new write. Record
	// its exact receipt without acquiring or disturbing a later writer's lease.
	// Current destination settlement and private cleanup remain separate.
	if p.Phase == metadata.ReplacementPublicationArmed {
		saved, err := replaceguard.Decode([]byte(p.Record))
		if err != nil {
			return err
		}
		remote, err := client.replacementRead(ctx, saved.Stage.Inode)
		if err != nil {
			return err
		}
		if remote == nil {
			return metadata.ErrMirrorConflict
		}
		if remote.Phase == "committed" {
			saved.Phase = "committed"
			if replacementRecordBytes(saved) != replacementRecordBytes(*remote) {
				return metadata.ErrMirrorConflict
			}
			return advance(metadata.ReplacementPublicationCommitted, *remote)
		}
	}
	prior, err := d.spool.meta.ReplacementDisposition(rowID)
	if err != nil {
		return errors.Join(metadata.ErrMirrorConflict, err)
	}
	if prior != nil && prior.State == metadata.ReplacementWriterChanged {
		return &metadata.ReplacementRefusalError{State: prior.State, ObservedPrincipal: prior.ObservedPrincipal, ObservedEpoch: prior.ObservedEpoch}
	}
	// An armed schedule hold admits only the historical receipt read above.
	// Until its ordinary wakeup, it must not acquire a lease or resume writing.
	if p.Phase == metadata.ReplacementPublicationArmed && prior != nil && prior.State == metadata.ReplacementWaitingWriter {
		wait, err := d.spool.meta.WaitProject(rowID)
		if err != nil {
			return err
		}
		if wait != "" {
			return &metadata.ReplacementRefusalError{State: prior.State, ObservedPrincipal: prior.ObservedPrincipal, ObservedEpoch: prior.ObservedEpoch}
		}
	}
	lease, err := client.EnsureForDrain(projectOf(row.NFSPath))
	if err != nil {
		return errors.Join(metadata.ErrMirrorConflict, err)
	}
	if !lease.held {
		if lease.heldBy != "" && lease.epoch > 0 {
			if err := d.spool.meta.RecordReplacementDisposition(*p, metadata.ReplacementWaitingWriter, lease.heldBy, lease.epoch, projectOf(row.NFSPath)); err != nil {
				return errors.Join(metadata.ErrMirrorConflict, err)
			}
			return &metadata.ReplacementRefusalError{State: metadata.ReplacementWaitingWriter, ObservedPrincipal: lease.heldBy, ObservedEpoch: lease.epoch}
		}
		return metadata.ErrMirrorConflict
	}
	foreignEpoch, foreignPrincipal := replacementForeignObservation(prior, lease, w.AcceptedEpoch)
	if foreignEpoch > w.AcceptedEpoch {
		if err := d.spool.meta.RecordReplacementDisposition(*p, metadata.ReplacementWriterChanged, foreignPrincipal, foreignEpoch, ""); err != nil {
			return errors.Join(metadata.ErrMirrorConflict, err)
		}
		return &metadata.ReplacementRefusalError{State: metadata.ReplacementWriterChanged, ObservedPrincipal: foreignPrincipal, ObservedEpoch: foreignEpoch}
	}
	// A grant reply may have been lost in a previous process. Consult durable
	// authenticated history before opening or rewriting the saved stage.
	foreignObserved, err := client.replacementHistory(ctx, *w, lease.epoch)
	if err != nil {
		return errors.Join(metadata.ErrMirrorConflict, err)
	}
	if foreignObserved > w.AcceptedEpoch {
		if err := d.spool.meta.RecordReplacementDisposition(*p, metadata.ReplacementWriterChanged, "", foreignObserved, ""); err != nil {
			return errors.Join(metadata.ErrMirrorConflict, err)
		}
		return &metadata.ReplacementRefusalError{State: metadata.ReplacementWriterChanged, ObservedEpoch: foreignObserved}
	}
	if p.Phase == metadata.ReplacementPublicationIntent {
		mirror := d.spool.meta.Owner().LookupByPath(row.NFSPath)
		if mirror == nil || mirror.Version != p.Expected {
			return metadata.ErrMirrorConflict
		}
		stageName := path.Base(p.StagePath)
		stage, err := uploadReplacementStage(ctx, backend, stageName, image, w.Image, mirror.Mode, func() (replacementPublishFile, error) {
			return client.openReplacementStage(ctx, backend, *w, stageName, mirror.Mode)
		})
		if err != nil {
			return err
		}
		original, err := backend.Stat(path.Base(row.NFSPath))
		if err != nil {
			return err
		}
		record, err := makeReplacementRecord(*w, stage, original, lease.epoch)
		if err != nil {
			return err
		}
		if err := advance(metadata.ReplacementPublicationArming, record); err != nil {
			return err
		}
	}
	recorded, err := replaceguard.Decode([]byte(p.Record))
	if err != nil {
		return err
	}
	observed, err := client.replacementRead(ctx, recorded.Stage.Inode)
	if err != nil {
		return err
	}
	if p.Phase == metadata.ReplacementPublicationArming {
		if observed == nil {
			if _, err := verifyReplacementStage(backend, recorded.Stage.Name, w.Image, &recorded.Stage); err != nil {
				return err
			}
			if lease.epoch != recorded.Epoch {
				recorded.Epoch = lease.epoch
				if err := advance(metadata.ReplacementPublicationArming, recorded); err != nil {
					return err
				}
			}
			got, err := client.replacementArm(ctx, recorded, nil)
			if err != nil {
				return err
			}
			observed = &got
		}
		if observed.Phase != "prepared" || !sameReplacementEvidence(recorded, *observed, true) {
			return metadata.ErrMirrorConflict
		}
		if err := advance(metadata.ReplacementPublicationArmed, *observed); err != nil {
			return err
		}
		recorded = *observed
	}
	if p.Phase != metadata.ReplacementPublicationArmed || observed == nil || !sameReplacementEvidence(recorded, *observed, false) {
		return metadata.ErrMirrorConflict
	}
	if observed.Phase == "prepared" {
		// First reconcile a successful but locally unrecorded authority refresh.
		if p.Record != replacementRecordBytes(*observed) {
			if err := advance(metadata.ReplacementPublicationArmed, *observed); err != nil {
				return err
			}
			recorded = *observed
		}
		desired := recorded
		desired.Epoch = lease.epoch
		refreshed, err := client.replacementArm(ctx, desired, &recorded)
		if err != nil {
			return err
		}
		if p.Record != replacementRecordBytes(refreshed) {
			if err := advance(metadata.ReplacementPublicationArmed, refreshed); err != nil {
				return err
			}
		}
		recorded = refreshed
		if err := verify(); err != nil {
			return err
		}
		if err := backend.Check(); err != nil {
			return err
		}
		// Rename errors have uncertain outcomes. Inspect the exact receipt; never
		// recreate a vanished stage or infer success from destination existence.
		if err := ctx.Err(); err != nil {
			return err
		}
		renameErr := backend.Rename(recorded.Stage.Name, recorded.Destination.Name)
		observed, err = client.replacementRead(ctx, recorded.Stage.Inode)
		if err != nil || observed == nil || observed.Phase != "committed" || !sameReplacementEvidence(recorded, *observed, false) {
			return errors.Join(metadata.ErrMirrorConflict, renameErr, err)
		}
	}
	if observed.Phase != "committed" || !sameReplacementEvidence(recorded, *observed, false) {
		return metadata.ErrMirrorConflict
	}
	return advance(metadata.ReplacementPublicationCommitted, *observed)
}

func replacementForeignObservation(prior *metadata.ReplacementDisposition, lease leaseDecision, accepted uint64) (uint64, string) {
	epoch, principal := lease.foreignBefore, ""
	// A new lease client may receive foreign_since=0 even after a prior
	// authenticated peer-held reply. The durable observation cannot vanish
	// when ReleaseProjectHold clears only the scheduling wait.
	if prior != nil && prior.State == metadata.ReplacementWaitingWriter && prior.ObservedEpoch > accepted {
		if prior.ObservedEpoch > epoch {
			epoch = prior.ObservedEpoch
		}
		principal = prior.ObservedPrincipal
	}
	return epoch, principal
}
