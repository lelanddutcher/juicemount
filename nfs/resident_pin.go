package nfs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// OwnedPinProvider admits only app-owned FUSE materialization. Arbitrary
// external cache fills have no provenance and cannot use this handoff.
type OwnedPinProvider struct {
	h    *JuiceMountHandler
	pins *pin.Store
	mu   sync.RWMutex
	live map[string]pinLiveAuthority
}

type pinWitness struct {
	Version  int
	Binding  cache.VolumeBinding
	Observed metadata.CachePathObservation
	Inode    uint64
	Size     int64
	MtimeNS  int64
	Digest   [sha256.Size]byte
}

type pinLiveAuthority struct {
	generation       string
	witness          pinWitness
	authorityDigest  [sha256.Size]byte
	identity         metadata.CachedFileIdentity
	writerGeneration uint64
	reader           *cache.Reader
	token            cache.ResidentToken
}

type ownedPinJob struct {
	provider         *OwnedPinProvider
	b                *cacheBinding
	name             string
	entry            pin.Entry
	identity         metadata.CachedFileIdentity
	observed         metadata.CachePathObservation
	binding          cache.VolumeBinding
	writerGeneration uint64
	reader           *cache.Reader
	token            cache.ResidentToken
	tokenSet         bool
	releaseOnce      sync.Once
	admission        *readAdmission
}

func NewOwnedPinProvider(h *JuiceMountHandler, pins *pin.Store) *OwnedPinProvider {
	return &OwnedPinProvider{h: h, pins: pins, live: make(map[string]pinLiveAuthority)}
}

func (p *OwnedPinProvider) DiscardReady(e pin.Entry) { p.Revoke(e.Path, e.Generation) }

// Revoke drops only the retired incarnation. It never touches the pin store
// and is safe after Store releases readyMu even when a pin mutation holds s.mu.
func (p *OwnedPinProvider) Revoke(path, generation string) {
	p.mu.Lock()
	delete(p.live, path+"\x00"+generation)
	p.mu.Unlock()
}

func (p *OwnedPinProvider) inMount(canonical string) (string, bool) {
	if p == nil || p.h == nil {
		return "", false
	}
	mp := p.h.mountPoint.Load()
	if mp == nil || *mp == "" {
		return "", false
	}
	prefix := strings.TrimRight(*mp, "/") + "/"
	if !strings.HasPrefix(canonical, prefix) {
		return "", false
	}
	name := strings.TrimPrefix(canonical, prefix)
	return name, name != "" && !strings.HasPrefix(name, "/")
}

func (p *OwnedPinProvider) writerSnapshot(name string) (uint64, bool) {
	p.h.activeWritersMu.Lock()
	defer p.h.activeWritersMu.Unlock()
	return p.h.activeWriterGen[name], p.h.activeWriters[name] == 0
}

func (p *OwnedPinProvider) noPendingWrites(name string, size int64) bool {
	if contiguous, written, writer, tracked := p.h.inPlaceHoles.routing(name); tracked && (writer || written > contiguous) {
		return false
	}
	p.h.writeSizeMu.Lock()
	high := p.h.writeSizes[name]
	p.h.writeSizeMu.Unlock()
	if high > size {
		return false
	}
	if spool := p.h.spool.Load(); spool != nil && spool.HasPending(name) {
		return false
	}
	return true
}

func (p *OwnedPinProvider) Capture(ctx context.Context, e pin.Entry) (pin.OwnedAuthorityJob, error) {
	name, ok := p.inMount(e.Path)
	if !ok || ctx.Err() != nil {
		return nil, pin.ErrAuthorityUnavailable
	}
	b := p.h.cacheBinding.Load()
	if b == nil || b.paths == nil || b.session == nil || b.workCtx.Err() != nil || p.h.cacheBackendError() != nil {
		return nil, pin.ErrAuthorityUnavailable
	}
	b.workMu.Lock()
	if b.workClosed {
		b.workMu.Unlock()
		return nil, pin.ErrAuthorityUnavailable
	}
	b.workActive++
	b.workMu.Unlock()
	j := &ownedPinJob{provider: p, b: b, name: name, entry: e}
	fail := func(err error) (pin.OwnedAuthorityJob, error) { j.Release(); return nil, err }
	admission, err := p.h.namespace.admitRead(name)
	if err != nil {
		return fail(err)
	}
	j.admission = admission
	identity, found := p.h.store.LookupCachedFileIdentity(name)
	if !found || identity.Size != e.Size || p.h.namespace.servingError(name) != nil {
		return fail(pin.ErrAuthorityUnavailable)
	}
	if !p.noPendingWrites(name, identity.Size) {
		return fail(pin.ErrAuthorityUnavailable)
	}
	writerGen, noWriter := p.writerSnapshot(name)
	if !noWriter {
		return fail(pin.ErrAuthorityUnavailable)
	}
	binding, valid := b.session.SnapshotBinding()
	if !valid {
		return fail(pin.ErrAuthorityUnavailable)
	}
	observed, err := b.paths.CaptureCachePathContext(ctx, name)
	if err != nil {
		return fail(err)
	}
	j.identity, j.observed, j.binding, j.writerGeneration = identity, observed, binding, writerGen
	return j, nil
}

func (j *ownedPinJob) Release() {
	j.releaseOnce.Do(func() {
		if j.admission != nil {
			j.admission.release()
		}
		j.b.workMu.Lock()
		j.b.workActive--
		if j.b.workClosed && j.b.workActive == 0 {
			close(j.b.workDone)
		}
		j.b.workMu.Unlock()
	})
}

func (j *ownedPinJob) Expected() pin.ExpectedFile {
	return pin.ExpectedFile{Inode: j.identity.Inode, Size: j.identity.Size, MtimeNS: j.identity.MtimeNS}
}

func (j *ownedPinJob) SourceCurrent() bool {
	p := j.provider
	if j.admission != nil && j.admission.check() != nil {
		return false
	}
	if j.b.workCtx.Err() != nil || p.h.namespace.servingError(j.name) != nil {
		return false
	}
	if !p.noPendingWrites(j.name, j.identity.Size) {
		return false
	}
	gen, clear := p.writerSnapshot(j.name)
	if !clear || gen != j.writerGeneration {
		return false
	}
	live, ok := p.h.store.LookupCachedFileIdentity(j.name)
	return ok && live == j.identity && j.b.paths.CachePathObservationCurrent(j.observed)
}

func (j *ownedPinJob) current() bool {
	p := j.provider
	if !j.SourceCurrent() || p.h.cacheBackendError() != nil {
		return false
	}
	live := j.identity
	selected := metadata.CachePathBinding{Digest: j.binding.Backend, FormatUUID: j.binding.UUID}
	if !j.b.paths.CachePathEligible(selected, live) {
		return false
	}
	binding, valid := j.b.session.SnapshotBinding()
	return valid && binding == j.binding && (j.reader == nil || j.reader == p.h.cacheReader.Load()) && (!j.tokenSet || j.token.Valid())
}

// The prefetcher persists Complete's error text as last_error. A stage names
// the first observed rejection without repeating authority probes for logging.
func pinCompleteFailure(stage string, cause error) error {
	return fmt.Errorf("pin authority %s: %w", stage, cause)
}

func (j *ownedPinJob) Complete(ctx context.Context, digest [sha256.Size]byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, pinCompleteFailure("context_entry", err)
	}
	if !j.provider.h.cacheReaderServeReady(j.binding) {
		return nil, pinCompleteFailure("reader_binding", pin.ErrAuthorityUnavailable)
	}
	selected := metadata.CachePathBinding{Digest: j.binding.Backend, FormatUUID: j.binding.UUID}
	if !j.b.paths.CachePathEligible(selected, j.identity) {
		epoch, ready := j.b.session.RemoteObservation()
		if !ready {
			return nil, pinCompleteFailure("remote_observation", pin.ErrAuthorityUnavailable)
		}
		proofCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		observed, err := j.b.qualifyPathOwned(proofCtx, selected, j.identity, epoch)
		cancel()
		if err != nil || !reflect.DeepEqual(observed.Chain, j.observed.Chain) {
			return nil, pinCompleteFailure("path_qualification", pin.ErrAuthorityUnavailable)
		}
	}
	if !j.current() {
		return nil, pinCompleteFailure("current_before_mapping", pin.ErrAuthorityUnavailable)
	}
	r := j.b.session.Reader()
	if r == nil || r != j.provider.h.cacheReader.Load() {
		return nil, pinCompleteFailure("reader_selected", pin.ErrAuthorityUnavailable)
	}
	j.reader = r
	last := int64(0)
	if j.identity.Size > 0 {
		last = (j.identity.Size - 1) / cache.ChunkSize
	}
	for chunk := int64(0); chunk <= last; chunk++ {
		if ctx.Err() != nil || !j.current() {
			return nil, pinCompleteFailure("current_during_mapping", pin.ErrAuthorityUnavailable)
		}
		mappingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := r.PrepareChunkMapping(mappingCtx, j.identity.Inode, chunk)
		cancel()
		if err != nil {
			return nil, pinCompleteFailure("mapping_prepare", err)
		}
	}
	expected := cache.ResidentAttr{Size: j.identity.Size, MtimeNS: j.identity.MtimeNS, Valid: true}
	token, err := r.ResidentEOF(j.identity.Inode, expected)
	if err != nil || !token.Valid() {
		return nil, pinCompleteFailure("resident_eof", pin.ErrAuthorityUnavailable)
	}
	j.token = token
	j.tokenSet = true
	if _, err := r.FlushResidentImage(ctx, j.identity.Inode, expected, digest); err != nil {
		if errors.Is(err, cache.ErrResidentDigest) {
			return nil, pinCompleteFailure("resident_bytes", pinCompleteFailure("bytes_digest", pin.ErrAuthorityUnavailable))
		}
		return nil, pinCompleteFailure("resident_flush", errors.Join(pin.ErrAuthorityUnavailable, err))
	}
	if !j.current() {
		return nil, pinCompleteFailure("current_after_flush", pin.ErrAuthorityUnavailable)
	}
	w := pinWitness{Version: 1, Binding: j.binding, Observed: j.observed, Inode: j.identity.Inode, Size: j.identity.Size, MtimeNS: j.identity.MtimeNS, Digest: digest}
	authority, err := json.Marshal(w)
	if err != nil {
		return nil, pinCompleteFailure("witness_encode", err)
	}
	return authority, nil
}

func verifyOwnedPinBytes(ctx context.Context, r *cache.Reader, id metadata.CachedFileIdentity, want [sha256.Size]byte, baseline cache.ResidentToken) error {
	expected := cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for off := int64(0); off < id.Size; {
		if err := ctx.Err(); err != nil {
			return pinCompleteFailure("bytes_context", err)
		}
		if !baseline.Valid() {
			return pinCompleteFailure("bytes_baseline_token", pin.ErrAuthorityUnavailable)
		}
		n := int(min(int64(len(buf)), id.Size-off))
		token, err := r.ReadResidentRange(id.Inode, off, buf[:n], expected)
		if err != nil || !token.Valid() {
			return pinCompleteFailure("bytes_range", pin.ErrAuthorityUnavailable)
		}
		_, _ = h.Write(buf[:n])
		off += int64(n)
	}
	eof, err := r.ResidentEOF(id.Inode, expected)
	if err != nil || !eof.Valid() || !baseline.Valid() {
		return pinCompleteFailure("bytes_eof", pin.ErrAuthorityUnavailable)
	}
	var got [sha256.Size]byte
	copy(got[:], h.Sum(nil))
	if got != want {
		return pinCompleteFailure("bytes_digest", pin.ErrAuthorityUnavailable)
	}
	return nil
}

func (h *JuiceMountHandler) cacheReaderServeReady(binding cache.VolumeBinding) bool {
	r := h.cacheReader.Load()
	if r == nil {
		return false
	}
	selected, ok := r.VolumeBinding()
	return ok && selected == binding
}

func (j *ownedPinJob) Finalize(ctx context.Context, e pin.Entry) (bool, error) {
	if ctx.Err() != nil || !j.current() {
		return false, nil
	}
	var witness pinWitness
	if json.Unmarshal(e.Authority, &witness) != nil || witness.Version != 1 || witness.Binding != j.binding || witness.Inode != j.identity.Inode {
		return false, nil
	}
	p := j.provider
	p.mu.Lock()
	p.live[e.Path+"\x00"+e.Generation] = pinLiveAuthority{generation: e.Generation, witness: witness, authorityDigest: sha256.Sum256(e.Authority), identity: j.identity, writerGeneration: j.writerGeneration, reader: j.reader, token: j.token}
	p.mu.Unlock()
	admitted, err := p.pins.AdmitReady(e)
	if err != nil || !admitted || !j.current() {
		p.pins.RevokeReadyIfGeneration(e.Path, e.Generation)
		p.mu.Lock()
		delete(p.live, e.Path+"\x00"+e.Generation)
		p.mu.Unlock()
		return false, err
	}
	return true, nil
}

func (p *OwnedPinProvider) Current(e pin.Entry) bool {
	p.mu.RLock()
	live, ok := p.live[e.Path+"\x00"+e.Generation]
	p.mu.RUnlock()
	if !ok || live.generation != e.Generation || sha256.Sum256(e.Authority) != live.authorityDigest || live.reader != p.h.cacheReader.Load() || !live.token.Valid() {
		return false
	}
	name, good := p.inMount(e.Path)
	if !good || p.h.cacheBackendError() != nil || p.h.namespace.servingError(name) != nil {
		return false
	}
	if !p.noPendingWrites(name, live.identity.Size) {
		return false
	}
	gen, clear := p.writerSnapshot(name)
	if !clear || gen != live.writerGeneration {
		return false
	}
	id, found := p.h.store.LookupCachedFileIdentity(name)
	if !found || id != live.identity {
		return false
	}
	b := p.h.cacheBinding.Load()
	if b == nil || b.workCtx.Err() != nil || !b.paths.CachePathObservationCurrent(live.witness.Observed) {
		return false
	}
	selected := metadata.CachePathBinding{Digest: live.witness.Binding.Backend, FormatUUID: live.witness.Binding.UUID}
	if !b.paths.CachePathEligible(selected, id) {
		return false
	}
	binding, valid := b.session.SnapshotBinding()
	return valid && binding == live.witness.Binding
}

func (p *OwnedPinProvider) ValidateReady(ctx context.Context, e pin.Entry) bool {
	if len(e.Authority) == 0 || ctx.Err() != nil {
		return false
	}
	var w pinWitness
	if json.Unmarshal(e.Authority, &w) != nil || w.Version != 1 || w.Size != e.Size || w.Inode == 0 {
		return false
	}
	name, ok := p.inMount(e.Path)
	if !ok {
		return false
	}
	b := p.h.cacheBinding.Load()
	if b == nil || b.paths == nil || b.session == nil || b.workCtx.Err() != nil {
		return false
	}
	id, found := p.h.store.LookupCachedFileIdentity(name)
	if !found || id.Inode != w.Inode || id.Size != w.Size || id.MtimeNS != w.MtimeNS {
		return false
	}
	observed, err := b.paths.CaptureCachePathContext(ctx, name)
	if err != nil || observed.Epoch.StoreID != w.Observed.Epoch.StoreID || !reflect.DeepEqual(observed.Chain, w.Observed.Chain) {
		return false
	}
	selected := metadata.CachePathBinding{Digest: w.Binding.Backend, FormatUUID: w.Binding.UUID}
	if !b.paths.CachePathEligible(selected, id) {
		return false
	}
	binding, valid := b.session.SnapshotBinding()
	if !valid || binding != w.Binding {
		return false
	}
	gen, clear := p.writerSnapshot(name)
	if !clear || !p.noPendingWrites(name, id.Size) {
		return false
	}
	r := b.session.Reader()
	if r == nil || r != p.h.cacheReader.Load() {
		return false
	}
	token, err := r.ResidentEOF(id.Inode, cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true})
	if err != nil || !token.Valid() || verifyOwnedPinBytes(ctx, r, id, w.Digest, token) != nil {
		return false
	}
	p.mu.Lock()
	p.live[e.Path+"\x00"+e.Generation] = pinLiveAuthority{generation: e.Generation, witness: w, authorityDigest: sha256.Sum256(e.Authority), identity: id, writerGeneration: gen, reader: r, token: token}
	p.mu.Unlock()
	if !p.Current(e) {
		p.DiscardReady(e)
		return false
	}
	return true
}

var _ pin.OwnedAuthorityProvider = (*OwnedPinProvider)(nil)
var _ pin.OwnedAuthorityJob = (*ownedPinJob)(nil)
