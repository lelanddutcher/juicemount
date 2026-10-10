package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// settleReplacement is deliberately not dispatched by the production queue.
// It transfers serving authority to the durable resident image, retaining the
// private source and its capacity until descriptor-bound cleanup is qualified.
func (h *JuiceMountHandler) settleReplacement(ctx context.Context, rowID int64) error {
	d := h.drainer.Load()
	if d == nil || !d.beginReplacement(rowID) {
		return metadata.ErrMirrorConflict
	}
	defer d.endReplacement(rowID)
	return h.settleReplacementOwned(ctx, rowID, d)
}

// settleReplacementOwned retains the coordinator's captured drainer/spool.
func (h *JuiceMountHandler) settleReplacementOwned(ctx context.Context, rowID int64, d *Drainer) (retErr error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	s, b := d.spool, h.cacheBinding.Load()
	if h.drainer.Load() != d || h.spool.Load() != s || s == nil || s.meta.Owner() != h.store || b == nil || b.session == nil || b.paths == nil ||
		b.rc == nil || b.rc.RawDB() == nil || b.readProof == nil {
		return metadata.ErrMirrorConflict
	}
	row, err := s.meta.Get(rowID)
	if err != nil {
		return err
	}
	op := h.namespace.begin([]string{row.NFSPath})
	if op == nil {
		return metadata.ErrMirrorConflict
	}
	op.blockReads()
	defer func() { op.finish(retErr) }()
	shard := s.pathShard(row.NFSPath)
	shard.Lock()
	defer shard.Unlock()
	row, err = s.meta.Get(rowID)
	if err != nil {
		return err
	}
	journal, err := s.meta.ReplacementPublication(row)
	if err != nil || journal == nil || journal.Phase != metadata.ReplacementPublicationCommitted {
		return errors.Join(err, metadata.ErrMirrorConflict)
	}
	image, w, err := s.openAcceptedReplacement(row)
	if err != nil || w == nil {
		return errors.Join(err, metadata.ErrMirrorConflict)
	}
	defer image.Close()
	actual, err := image.HashImage()
	if err != nil || !bytes.Equal(actual.SHA256, w.Image.SHA256) {
		return errors.Join(err, metadata.ErrMirrorConflict)
	}
	captured, err := b.paths.CaptureReplacementSettlement(*journal)
	if err != nil {
		return fmt.Errorf("replacement capture: %w", err)
	}
	receipt, err := replaceguard.Decode([]byte(journal.Record))
	if err != nil {
		return err
	}
	r := h.cacheReader.Load()
	binding, valid := b.session.SnapshotBinding()
	epoch, ready := b.session.RemoteObservation()
	if r == nil || r != b.session.Reader() || !valid || !ready ||
		(metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}) != w.Binding {
		return metadata.ErrMirrorConflict
	}
	verify := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if h.cacheBinding.Load() != b || h.cacheReader.Load() != r ||
			h.cacheBackendError() != nil || !b.session.TryMatchRemoteObservation(epoch, binding, r) {
			return metadata.ErrMirrorConflict
		}
		return image.VerifyImage(w.Image)
	}
	if err := verify(); err != nil {
		return err
	}
	ids := captured.ExpectedInodes()
	ids[len(ids)-1] = receipt.Stage.Inode
	before, err := b.readProof(ctx, binding.UUID, captured.Path, ids)
	if err != nil {
		return fmt.Errorf("replacement before proof: %w", err)
	}
	attr := cache.ResidentAttr{Size: w.Image.Size, MtimeNS: w.Image.MtimeNS, Valid: true}
	prepare := func(reader *cache.Reader) error {
		last := int64(0)
		if attr.Size > 0 {
			last = (attr.Size - 1) / cache.ChunkSize
		}
		for chunk := int64(0); chunk <= last; chunk++ {
			if err := verify(); err != nil {
				return err
			}
			if err := reader.PrepareChunkMapping(ctx, receipt.Stage.Inode, chunk); err != nil {
				return err
			}
		}
		return nil
	}
	r.InvalidateSliceCache(receipt.Stage.Inode)
	if err := prepare(r); err != nil {
		return fmt.Errorf("replacement mapping: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], w.Image.SHA256)
	graph, err := r.FlushResidentImage(ctx, receipt.Stage.Inode, attr, digest)
	if err != nil {
		return fmt.Errorf("replacement resident durability: %w", err)
	}
	// A new owned client/Reader has no inherited mapping or snapshot. Observe
	// the current graph independently after byte verification; never give Stop
	// the metadata client's shared connection pool.
	opts := *b.rc.RawDB().Options()
	opts.ContextTimeoutEnabled, opts.MaxRetries = true, -1
	client := redis.NewClient(&opts)
	fresh, err := cache.NewVolumeReader(binding, client)
	if err != nil {
		_ = client.Close()
		return err
	}
	defer fresh.Stop()
	if err := prepare(fresh); err != nil {
		return fmt.Errorf("replacement graph recapture: %w", err)
	}
	recaptured, err := fresh.ExportContentGraph(receipt.Stage.Inode, attr)
	if err != nil || !recaptured.Token.Valid() || !bytes.Equal(recaptured.Canonical(), graph.Canonical()) {
		return errors.Join(err, metadata.ErrMirrorConflict)
	}
	after, err := b.readProof(ctx, binding.UUID, captured.Path, ids)
	if err != nil {
		return fmt.Errorf("replacement after proof: %w", err)
	}
	verifySettlement := func() error {
		if !graph.Token.Valid() || !recaptured.Token.Valid() {
			return metadata.ErrMirrorConflict
		}
		return verify()
	}
	// The callback enters no metadata APIs; SQL owns the exact row/ancestry
	// CAS while these independent session, Reader and private-FD checks run.
	if err := s.rememberReplacementCleanupCharge(row); err != nil {
		return err
	}
	_, err = b.paths.SettleSpoolReplacement(*journal, captured, before, after, graph.Canonical(), verifySettlement)
	if err != nil {
		var uncertain *metadata.PendingSpoolReplacement
		if errors.As(err, &uncertain) {
			op.poison = true // ordinary reopen reconciles the durable phase
		}
		return fmt.Errorf("replacement settlement: %w", err)
	}
	// Only the predecessor's mappings are invalidated. The newly fsynced
	// stage inode must remain usable after this handoff and after restart.
	h.onScanContentInvalidated(row.NFSPath, receipt.Destination.Inode)
	h.invalidatePooledReadFDs(row.NFSPath)
	if h.sidecar != nil {
		h.sidecar.invalidate(row.NFSPath)
	}
	if e, ok := s.index.Lookup(row.NFSPath); ok && e.id == row.ID {
		s.index.DeleteIfMatches(row.NFSPath, e)
	}
	return nil
}
