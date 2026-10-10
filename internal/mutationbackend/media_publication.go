package mutationbackend

import (
	"context"
	"errors"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

type newNamePublisher interface {
	PublishNewName(meta.Context, meta.NewNameLink) syscall.Errno
	RecoverNewName(meta.Context, meta.NewNameLink) (bool, syscall.Errno)
}

func publicationExpectation(s mutationauthority.MediaStage) (meta.NewNameLink, error) {
	var body mediaStageEnvelope
	var e meta.NewNameLink
	if !s.Closed || strictJSON(s.Intent.Body, &body) != nil || body.Version != "media-stage-v2-new-name" || body.Target == nil || !validNewNameTarget(*body.Target) {
		return e, ErrUnavailable
	}
	e = meta.NewNameLink{Name: path.Base(body.Target.Path), StageName: strings.TrimPrefix(s.Intent.Path, "/"), Marker: s.Marker, Inode: meta.Ino(s.Inode), Length: uint64(s.Intent.Size)}
	for _, p := range body.Target.Parents {
		e.Parents = append(e.Parents, meta.NewNameParent{Name: p.Name, Inode: meta.Ino(p.Inode)})
	}
	return e, nil
}

// PublishMediaForNewName exposes the exact prepared v2 stage without replacing
// any destination or changing its original parents. The checkpoint is observed
// namespace state only: the operation remains PREPARED, custody is retained,
// and this API does not return a durability receipt or permit spool retirement.
func (r *SDK) PublishMediaForNewName(ctx context.Context, lease mutationauthority.Lease, id string) (mutationauthority.StagePublication, error) {
	var empty mutationauthority.StagePublication
	if lease.Volume != r.media.volume || id == "" {
		return empty, mutationauthority.ErrInvalid
	}
	if err := r.Check(ctx); err != nil {
		return empty, err
	}
	r.mu.Lock()
	if r.closing || r.closed || r.publishing || r.stageUncertain || r.activeStages != 0 {
		r.mu.Unlock()
		return empty, mutationauthority.ErrInFlight
	}
	r.publishing = true
	r.activeStages++
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.publishing = false; r.activeStages--; r.mu.Unlock() }()
	publisher, ok := r.metadata.(newNamePublisher)
	if !ok {
		return empty, ErrUnavailable
	}
	s, err := r.media.journal.InspectStage(ctx, lease.Volume, id)
	if err != nil {
		return empty, err
	}
	if err := r.media.validateRetainedLayout(s); err != nil {
		return empty, err
	}
	e, err := publicationExpectation(s)
	if err != nil {
		return empty, err
	}
	prior, err := r.media.journal.InspectPublication(ctx, lease.Volume, id)
	if err != nil {
		return empty, err
	}
	if prior != nil {
		return empty, mutationauthority.ErrUncertain
	}
	var body mediaStageEnvelope
	_ = strictJSON(s.Intent.Body, &body)
	if err := r.checkNewNameTarget(ctx, *body.Target); err != nil {
		return empty, err
	}
	// Root's first private stage may still have a pending SDK stat flush.
	// Wait for initialized fields before recording intent; do not manufacture
	// a baseline with GetDirStat while a delayed delta might still arrive.
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		ready := true
		for _, key := range []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes"} {
			_, err := r.reader.HGet(waitCtx, key, strconv.FormatUint(uint64(e.Parents[len(e.Parents)-1].Inode), 10)).Result()
			if errors.Is(err, redis.Nil) {
				ready = false
			} else if err != nil {
				return empty, ErrUnavailable
			}
		}
		if ready {
			break
		}
		select {
		case <-waitCtx.Done():
			return empty, waitCtx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	s, err = r.media.journal.BeginPublication(ctx, lease, s.Request)
	if err != nil {
		return empty, err
	}
	r.media.event("publication-intent")
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	if eno := publisher.PublishNewName(fctx, e); eno != 0 {
		r.changed.Store(true) // uncertain namespace effects require a fresh fence
		return empty, mutationauthority.ErrUncertain
	}
	r.media.event("publication-linked")
	if err := r.media.journal.ObservePublication(ctx, s); err != nil {
		r.changed.Store(true)
		return empty, err
	}
	r.media.event("publication-observed")
	return mutationauthority.StagePublication{Snapshot: mutationauthority.StageSnapshotDigest(s), Observed: true}, nil
}
