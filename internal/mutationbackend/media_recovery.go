package mutationbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"strings"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func sdkBinding(volume string, cfg SDKConfig) string {
	b, _ := json.Marshal(struct {
		Kind, Volume string
		Config       SDKConfig
	}{"juicefs-1.3.1-sdk-config-v2-create-only", volume, cfg})
	return digest(b)
}

// PreflightStageRecovery performs no backend I/O. It checks every persisted
// media row before mixed custody startup may classify its legacy requests.
// The returned predicate identifies exact preflighted operations, not receipts.
func PreflightStageRecovery(ctx context.Context, j *mutationauthority.Journal, volume string, cfg SDKConfig) (func(mutationauthority.Request) bool, error) {
	stages, err := j.StageRecoveryCandidates(ctx, volume)
	if err != nil {
		return nil, err
	}
	accepted := make(map[string]mutationauthority.Request)
	for _, s := range stages {
		if err := validateStageIntent(s, volume, sdkBinding(volume, cfg)); err != nil {
			return nil, err
		}
		accepted[s.Request.ID] = s.Request
	}
	return func(r mutationauthority.Request) bool {
		prior, ok := accepted[r.ID]
		return ok && prior == r
	}, nil
}

func validateStageIntent(s mutationauthority.MediaStage, volume, binding string) error {
	var body mediaStageEnvelope
	if len(s.Intent.Body) == 0 || len(s.Intent.Body) > 64<<10 || strictJSON(s.Intent.Body, &body) != nil {
		return ErrUnavailable
	}
	if body.Version == "media-stage-v1" {
		if body.Target != nil {
			return ErrUnavailable
		}
	} else if body.Version != "media-stage-v2-new-name" || body.Target == nil || !validNewNameTarget(*body.Target) {
		return ErrUnavailable
	}
	canonical, _ := json.Marshal(body)
	p := body.Request
	r := mutationauthority.Request{Lease: mutationauthority.Lease{Volume: p.Volume, Session: p.Session, Epoch: p.Epoch}, ID: p.ID, Digest: digest(canonical), Kind: mutationauthority.MediaPublishOperation, FileID: p.FileID, ExpectedRevision: p.ExpectedRevision}
	if !bytes.Equal(canonical, s.Intent.Body) || r != s.Request || p.Volume != volume || p.Size < 0 || p.Size > 1<<40 || !stageHash(p.SHA256) || !stageHash(p.CustodyArtifactSHA256) ||
		s.Intent.Size != p.Size || s.Intent.SHA256 != p.SHA256 || s.Intent.Backend != binding || !stageHash(binding) ||
		s.Intent.Path != "/.juicemount-stage-"+digest([]byte(p.Volume+"\x00"+p.ID)) {
		return ErrUnavailable
	}
	if s.Inode == 0 {
		// Create/xattr crash gaps may leave a namespace entry. Keep the path
		// reference, but do not adopt, write, delete or infer absence from it.
		if s.Marker != "" || s.Closed || s.Manifest != "" || len(s.Slices) != 0 || len(s.Objects) != 0 {
			return ErrUnavailable
		}
	} else if s.Inode <= 1 || s.Inode > math.MaxInt64 || s.Marker != r.Digest {
		return ErrUnavailable
	}
	if !s.Closed && s.Manifest != "" {
		return ErrUnavailable
	}
	return nil
}

func (m *sdkMedia) validateRetainedLayout(s mutationauthority.MediaStage) error {
	if err := validateStageIntent(s, m.volume, m.binding); err != nil {
		return err
	}
	slices := make(map[uint64]mutationauthority.StageSlice)
	for _, sl := range s.Slices {
		if sl.ID == 0 || sl.ID > math.MaxInt64 || sl.Inode != s.Inode || sl.Inode == 0 {
			return ErrUnavailable
		}
		if _, duplicate := slices[sl.ID]; duplicate {
			return ErrUnavailable
		}
		if sl.Written {
			end := uint64(sl.ChunkIndex)*meta.ChunkSize + uint64(sl.Offset) + uint64(sl.Length)
			if sl.Size == 0 || sl.Length == 0 || sl.Size > meta.ChunkSize || uint64(sl.Offset)+uint64(sl.Length) > meta.ChunkSize || uint64(sl.SliceOffset)+uint64(sl.Length) > uint64(sl.Size) || end > uint64(s.Intent.Size) {
				return ErrUnavailable
			}
		} else if sl.ChunkIndex != 0 || sl.Offset != 0 || sl.Size != 0 || sl.SliceOffset != 0 || sl.Length != 0 {
			return ErrUnavailable
		}
		slices[sl.ID] = sl
	}
	var previousAttempt int64
	for _, a := range s.Objects {
		slice, _, _, err := m.parseKey(a.Key)
		h := fnv.New32a()
		_, _ = h.Write([]byte(a.Key))
		shard := int(h.Sum32() % uint32(max(1, m.format.Shards)))
		bucket := m.format.Bucket
		if m.format.Shards > 1 {
			bucket = fmt.Sprintf(bucket, shard)
		}
		_, registered := slices[slice]
		if err != nil || !registered || a.Slice != slice || a.Attempt <= previousAttempt || a.Provider != strings.ToLower(m.format.Storage) || a.Bucket != bucket || a.Shard != shard || a.Size <= 0 || a.Size > 32<<20 || !stageHash(a.SHA256) {
			return ErrUnavailable
		}
		previousAttempt = a.Attempt
	}
	if s.Closed {
		if !stageHash(s.Manifest) || s.Inode == 0 {
			return ErrUnavailable
		}
		for _, sl := range s.Slices {
			if !sl.Written {
				return ErrUnavailable
			}
		}
		original := s
		original.Closed, original.Manifest = false, ""
		if mutationauthority.StageSnapshotDigest(original) != s.Manifest || m.verifyManifest(s) != nil {
			return ErrUnavailable
		}
	}
	return nil
}

func (r *SDK) retainPriorStages(ctx context.Context) error {
	stages, err := r.media.journal.StageRecoveryCandidates(ctx, r.media.volume)
	if err != nil {
		return err
	}
	if err := r.Check(ctx); err != nil {
		return err
	}
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	published := make(map[string]bool)
	for _, s := range stages {
		if err := r.media.validateRetainedLayout(s); err != nil {
			return err
		}
		intent, err := r.media.journal.InspectPublication(ctx, s.Request.Volume, s.Request.ID)
		if err != nil {
			return err
		}
		if intent != nil {
			if intent.Snapshot != mutationauthority.StageSnapshotDigest(s) {
				return ErrUnavailable
			}
			e, err := publicationExpectation(s)
			if err != nil {
				return err
			}
			publisher, ok := r.metadata.(newNamePublisher)
			if !ok {
				return ErrUnavailable
			}
			isPublic, eno := publisher.RecoverNewName(fctx, e)
			if eno != 0 {
				return ErrUnavailable
			}
			if isPublic {
				published[s.Request.ID] = true
			} else if intent.Observed {
				return ErrUnavailable
			}
			continue
		}
		if s.Inode == 0 {
			continue
		}
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Lookup(fctx, meta.RootInode, strings.TrimPrefix(s.Intent.Path, "/"), &inode, &attr, false); eno != 0 || uint64(inode) != s.Inode {
			return ErrUnavailable
		}
		if eno := r.metadata.GetAttr(fctx, inode, &attr); eno != 0 || attr.Typ != meta.TypeFile || attr.Nlink != 1 || attr.Parent != meta.RootInode || attr.Length > uint64(s.Intent.Size) || (s.Closed && attr.Length != uint64(s.Intent.Size)) {
			return ErrUnavailable
		}
		var marker []byte
		if eno := r.metadata.GetXattr(fctx, inode, "user.juicemount.operation", &marker); eno != 0 || !bytes.Equal(marker, []byte(s.Marker)) {
			return ErrUnavailable
		}
	}
	if len(stages) != 0 {
		r.media.enabled.Store(true)
	}
	return r.media.journal.CertifyRecoveredStages(ctx, r.fence.process, r.media.binding, stages, published, func() error { return r.Check(ctx) })
}
