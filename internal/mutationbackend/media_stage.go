package mutationbackend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

// MediaStageRequest names immutable custody bytes and their intended file
// incarnation/predecessor. StageMedia does not authorize namespace publication.
type MediaStageRequest struct {
	Volume, Session               string
	Epoch                         int64
	ID, FileID, ExpectedRevision  string
	Size                          int64
	SHA256, CustodyArtifactSHA256 string
}

type sdkMedia struct {
	journal         *mutationauthority.Journal
	volume, binding string
	format          meta.Format
	enabled         atomic.Bool
	hook            func(string) // owned crash fixtures only; never installed in production
}

func (m *sdkMedia) event(phase string) {
	if m.hook != nil {
		m.hook(phase)
	}
}
func (m *sdkMedia) Begin(key string, size int64, sha string) (func() error, error) {
	// There is no exported SDK write API other than StageMedia. Private legacy
	// SDK test paths remain usable before stage admission; admission is one-way.
	if !m.enabled.Load() {
		return nil, nil
	}
	slice, _, _, err := m.parseKey(key)
	if err != nil {
		return nil, err
	}
	s, err := m.journal.StageForSlice(context.Background(), m.volume, slice)
	if err != nil {
		return nil, ErrUnavailable
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	shard := int(h.Sum32() % uint32(max(1, m.format.Shards)))
	bucket := m.format.Bucket
	if m.format.Shards > 1 {
		bucket = fmt.Sprintf(bucket, shard)
	}
	attempt, err := m.journal.BeginObjectAttempt(context.Background(), s.Request, mutationauthority.ObjectAttempt{Slice: slice, Provider: strings.ToLower(m.format.Storage), Bucket: bucket, Shard: shard, Key: key, Size: size, SHA256: sha})
	if err != nil {
		return nil, ErrUnavailable
	}
	m.event("object-prepared")
	return func() error {
		m.event("object-readback")
		return m.journal.VerifyObjectAttempt(context.Background(), s.Request, attempt)
	}, nil
}

func (m *sdkMedia) key(slice uint64, block int, size int) string {
	prefix := m.format.Name + "/chunks/"
	if m.format.HashPrefix {
		return fmt.Sprintf("%s%02X/%d/%d_%d_%d", prefix, slice%256, slice/1000000, slice, block, size)
	}
	return fmt.Sprintf("%s%d/%d/%d_%d_%d", prefix, slice/1000000, slice/1000, slice, block, size)
}

func (m *sdkMedia) parseKey(key string) (uint64, int, int, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		return 0, 0, 0, ErrUnavailable
	}
	tail := strings.Split(parts[4], "_")
	if len(tail) != 3 {
		return 0, 0, 0, ErrUnavailable
	}
	slice, e1 := strconv.ParseUint(tail[0], 10, 64)
	block, e2 := strconv.Atoi(tail[1])
	size, e3 := strconv.Atoi(tail[2])
	bs := m.format.BlockSize * 1024
	if e1 != nil || e2 != nil || e3 != nil || slice == 0 || block < 0 || block >= meta.ChunkSize/bs || size <= 0 || size > bs || m.key(slice, block, size) != key {
		return 0, 0, 0, ErrUnavailable
	}
	return slice, block, size, nil
}

type mediaMetadata struct {
	meta.Meta
	media *sdkMedia
}

func (m *mediaMetadata) NewSliceForInode(ctx meta.Context, inode meta.Ino, id *uint64) syscall.Errno {
	if !m.media.enabled.Load() {
		return m.Meta.NewSlice(ctx, id)
	}
	s, err := m.media.journal.StageForInode(context.Background(), m.media.volume, uint64(inode))
	if err != nil || s.Closed {
		return syscall.EPERM
	}
	var allocated uint64
	if eno := m.Meta.NewSlice(ctx, &allocated); eno != 0 {
		return eno
	}
	if err := m.media.journal.RecordStageSlice(context.Background(), s.Request, uint64(inode), allocated); err != nil {
		return syscall.EPERM
	}
	*id = allocated
	return 0
}

func (m *mediaMetadata) Write(ctx meta.Context, inode meta.Ino, index, off uint32, slice meta.Slice, mtime time.Time) syscall.Errno {
	if !m.media.enabled.Load() {
		return m.Meta.Write(ctx, inode, index, off, slice, mtime)
	}
	s, err := m.media.journal.StageForSlice(context.Background(), m.media.volume, slice.Id)
	if err != nil || s.Closed || s.Inode != uint64(inode) {
		return syscall.EPERM
	}
	if eno := m.Meta.Write(ctx, inode, index, off, slice, mtime); eno != 0 {
		return eno
	}
	if err := m.media.journal.RecordSliceWritten(context.Background(), s.Request, mutationauthority.StageSlice{ID: slice.Id, Inode: uint64(inode), ChunkIndex: index, Offset: off, Size: slice.Size, SliceOffset: slice.Off, Length: slice.Len}); err != nil {
		return syscall.EPERM
	}
	return 0
}

// StageMedia creates a reserved, operation-bound stage and closes its raw PUT
// admission after exact object/slice observation. The returned manifest remains
// PREPARED: it is not a receipt, durability claim, or permission to retire spool
// bytes. Failed/crashed stages are never automatically replayed or removed.
func (r *SDK) StageMedia(ctx context.Context, spec MediaStageRequest, input io.Reader) (mutationauthority.MediaStage, error) {
	body, _ := json.Marshal(mediaStageEnvelope{Version: "media-stage-v1", Request: spec})
	return r.stageMedia(ctx, spec, body, input)
}

func (r *SDK) stageMedia(ctx context.Context, spec MediaStageRequest, body []byte, input io.Reader) (mutationauthority.MediaStage, error) {
	var empty mutationauthority.MediaStage
	if input == nil || spec.Volume != r.media.volume || spec.Size < 0 || spec.Size > 1<<40 || !stageHash(spec.SHA256) || !stageHash(spec.CustodyArtifactSHA256) {
		return empty, mutationauthority.ErrInvalid
	}
	if err := r.Check(ctx); err != nil {
		return empty, err
	}
	r.mu.Lock()
	if r.closing || r.closed || r.publishing {
		r.mu.Unlock()
		return empty, ErrUnavailable
	}
	if r.stageUncertain {
		r.mu.Unlock()
		return empty, mutationauthority.ErrUncertain
	}
	r.activeStages++
	r.media.enabled.Store(true)
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.activeStages--; r.mu.Unlock() }()
	request := mutationauthority.Request{Lease: mutationauthority.Lease{Volume: spec.Volume, Session: spec.Session, Epoch: spec.Epoch}, ID: spec.ID, Digest: digest(body), Kind: mutationauthority.MediaPublishOperation, FileID: spec.FileID, ExpectedRevision: spec.ExpectedRevision}
	intent := mutationauthority.StageIntent{Body: body, Path: "/.juicemount-stage-" + digest([]byte(spec.Volume+"\x00"+spec.ID)), Backend: r.media.binding, Size: spec.Size, SHA256: spec.SHA256}
	backend := &mediaStageBackend{runtime: r, intent: intent, input: input}
	err := r.media.journal.ExecuteStage(ctx, request, intent, backend)
	if backend.started && backend.result == nil {
		r.mu.Lock()
		r.stageUncertain = true
		r.mu.Unlock()
	}
	if backend.result != nil && errors.Is(err, mutationauthority.ErrUncertain) {
		return *backend.result, nil
	}
	return empty, err
}

func stageHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

type mediaStageBackend struct {
	runtime *SDK
	intent  mutationauthority.StageIntent
	input   io.Reader
	result  *mutationauthority.MediaStage
	started bool
}

func (b *mediaStageBackend) Apply(ctx context.Context, request mutationauthority.Request) (mutationauthority.DurabilityProof, error) {
	b.started = true
	fail := func(e error) (mutationauthority.DurabilityProof, error) {
		return mutationauthority.DurabilityProof{}, e
	}
	r, m := b.runtime, b.runtime.media
	m.event("intent")
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	file, eno := r.filesystem.Create(fctx, b.intent.Path, 0600, 0077)
	if eno != 0 {
		return fail(ErrUnavailable)
	}
	// No deferred Close: on failure it could flush more uncertain work. Process
	// ownership and this stage are retained until dedicated-process recovery.
	m.event("created")
	marker := digest(b.intent.Body)
	if eno := r.metadata.SetXattr(fctx, file.Inode(), "user.juicemount.operation", []byte(marker), meta.XattrCreate); eno != 0 {
		return fail(ErrUnavailable)
	}
	m.event("marker")
	if err := m.journal.BindStageInode(ctx, request, uint64(file.Inode()), marker); err != nil {
		return fail(err)
	}
	m.event("bound")
	verifyInode := func() error {
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Lookup(fctx, meta.RootInode, strings.TrimPrefix(b.intent.Path, "/"), &inode, &attr, false); eno != 0 || inode != file.Inode() {
			return ErrUnavailable
		}
		var got []byte
		if eno := r.metadata.GetXattr(fctx, inode, "user.juicemount.operation", &got); eno != 0 || !bytes.Equal(got, []byte(marker)) {
			return ErrUnavailable
		}
		return nil
	}
	if err := verifyInode(); err != nil {
		return fail(err)
	}
	h := sha256.New()
	buffer := make([]byte, 256<<10)
	var offset int64
	for offset < b.intent.Size {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		want := min(int64(len(buffer)), b.intent.Size-offset)
		n, err := io.ReadFull(b.input, buffer[:int(want)])
		if err != nil || n != int(want) {
			return fail(ErrUnavailable)
		}
		_, _ = h.Write(buffer[:n])
		written, eno := file.Pwrite(fctx, buffer[:n], offset)
		if eno != 0 || written != n {
			return fail(ErrUnavailable)
		}
		offset += int64(n)
		if offset%(4<<20) == 0 {
			if eno := file.Fsync(fctx); eno != 0 {
				return fail(ErrUnavailable)
			}
		}
	}
	var extra [1]byte
	if n, err := io.ReadFull(b.input, extra[:]); n != 0 || err != io.EOF || hex.EncodeToString(h.Sum(nil)) != b.intent.SHA256 {
		return fail(ErrUnavailable)
	}
	if eno := file.Fsync(fctx); eno != 0 {
		return fail(ErrUnavailable)
	}
	if eno := file.Close(fctx); eno != 0 {
		return fail(ErrUnavailable)
	}
	if err := verifyInode(); err != nil {
		return fail(err)
	}
	var attr meta.Attr
	if eno := r.metadata.GetAttr(fctx, file.Inode(), &attr); eno != 0 || attr.Length != uint64(b.intent.Size) {
		return fail(ErrUnavailable)
	}
	m.event("fsynced")
	checkpoint, err := m.journal.CloseStage(ctx, request, m.verifyManifest)
	if err != nil {
		return fail(err)
	}
	b.result = &checkpoint
	m.event("closed")
	return fail(nil)
}

func (m *sdkMedia) verifyManifest(s mutationauthority.MediaStage) error {
	var offset uint64
	expected := make(map[string]uint64)
	for _, slice := range s.Slices {
		if slice.Inode != s.Inode || slice.SliceOffset != 0 || slice.Length != slice.Size || uint64(slice.ChunkIndex)*meta.ChunkSize+uint64(slice.Offset) != offset {
			return ErrUnavailable
		}
		offset += uint64(slice.Length)
		bs := m.format.BlockSize * 1024
		for block, left := 0, int(slice.Size); left > 0; block++ {
			size := min(left, bs)
			expected[m.key(slice.ID, block, size)] = slice.ID
			left -= size
		}
	}
	if offset != uint64(s.Intent.Size) || len(expected) != len(s.Objects) {
		return ErrUnavailable
	}
	for _, a := range s.Objects {
		if expected[a.Key] != a.Slice || !a.Verified {
			return ErrUnavailable
		}
		delete(expected, a.Key)
	}
	if len(expected) != 0 {
		return ErrUnavailable
	}
	return nil
}
