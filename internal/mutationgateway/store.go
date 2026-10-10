package mutationgateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationbackend"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"golang.org/x/sys/unix"
)

type Config struct {
	Directory string
	Volume    string
	LeaseTTL  time.Duration
	SDK       *mutationbackend.SDKConfig
}

// Gateway owns both its journal and immutable stage descriptors. No writable
// descriptor or backend credential is handed to clients or subprocesses.
type Gateway struct {
	mu      sync.Mutex
	active  int
	closing bool
	closed  bool
	dir     *os.File
	journal *mutationauthority.Journal
	sdk     *mutationbackend.SDK
	volume  string
	ttl     time.Duration
	slots   chan struct{}
	// Test crash boundaries are private; production never installs this hook.
	stageHook func(string)
}

// Open requires a pre-provisioned, private durable local directory. It syncs
// its ancestor chain before issuing receipts. Existing unknown databases are
// never adopted. Public requests admit custody operations only. Optional SDK startup can
// retain supported media stages in this SAME whole-volume authority; retention
// does not expose a media publication endpoint.
func Open(cfg Config) (_ *Gateway, err error) {
	if cfg.SDK == nil && mutationbackend.SDKAttempted() {
		return nil, mutationauthority.ErrProcessLifetime
	}
	if !filepath.IsAbs(cfg.Directory) || !identifier.MatchString(cfg.Volume) || cfg.LeaseTTL <= 0 || cfg.LeaseTTL > 5*time.Minute {
		return nil, mutationauthority.ErrInvalid
	}
	fi, err := os.Lstat(cfg.Directory)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() || fi.Mode().Perm() != 0700 {
		return nil, mutationauthority.ErrInvalid
	}
	dir, err := os.Open(cfg.Directory)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = dir.Close()
		}
	}()
	var st unix.Stat_t
	if err = unix.Fstat(int(dir.Fd()), &st); err != nil {
		return nil, err
	}
	opened, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	if st.Uid != uint32(os.Getuid()) || !os.SameFile(fi, opened) {
		return nil, mutationauthority.ErrInvalid
	}
	// Trusted ancestors may not be renamed/replaced while the gateway runs.
	for p := filepath.Clean(cfg.Directory); ; p = filepath.Dir(p) {
		f, e := os.Open(p)
		if e != nil {
			return nil, e
		}
		e = errors.Join(f.Sync(), f.Close())
		if e != nil {
			return nil, e
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	marker, _ := json.Marshal(struct{ Kind, Volume string }{format, cfg.Volume})
	if err = ensureFormat(dir, marker); err != nil {
		return nil, err
	}
	j, err := mutationauthority.Open(filepath.Join(cfg.Directory, "authority.db"))
	if err != nil {
		return nil, err
	}
	g := &Gateway{dir: dir, journal: j, volume: cfg.Volume, ttl: cfg.LeaseTTL, slots: make(chan struct{}, 4)}
	if cfg.SDK == nil {
		if err = j.RequireNoBackend(context.Background()); err != nil {
			_ = j.Close()
			return nil, err
		}
	}
	var acceptMedia func(mutationauthority.Request) bool
	if cfg.SDK != nil {
		acceptMedia, err = mutationbackend.PreflightStageRecovery(context.Background(), j, cfg.Volume, *cfg.SDK)
		if err != nil {
			_ = j.Close()
			return nil, err
		}
	}
	if err = g.preflight(context.Background(), acceptMedia); err != nil {
		_ = j.Close()
		return nil, err
	}
	if cfg.SDK != nil {
		g.sdk, err = mutationbackend.OpenSDK(context.Background(), j, cfg.Volume, *cfg.SDK)
		if err != nil {
			// The backend pin survives every constructor failure. No listener or
			// custody-only fallback is returned; the dedicated process must exit.
			return nil, errors.Join(err, j.Close())
		}
	}
	if err = g.reconcile(context.Background()); err != nil {
		return nil, errors.Join(err, j.Close())
	}
	return g, nil
}

func ensureFormat(dir *os.File, want []byte) error {
	f, err := privatefile.OpenAt(dir, "format.json", os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		entries, e := dir.ReadDir(-1)
		if e != nil {
			return e
		}
		if len(entries) != 0 {
			return fmt.Errorf("%w: unknown gateway state", mutationauthority.ErrInvalid)
		}
		f, err = privatefile.OpenAt(dir, "format.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, e = f.Write(want)
		return errors.Join(e, f.Sync(), f.Close(), dir.Sync())
	}
	if err != nil {
		return err
	}
	defer f.Close()
	if err := regularPrivate(f); err != nil {
		return err
	}
	got, err := io.ReadAll(io.LimitReader(f, 1025))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%w: wrong gateway format or volume", mutationauthority.ErrInvalid)
	}
	return nil
}

func regularPrivate(f *os.File) error {
	var s unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &s); err != nil {
		return err
	}
	if s.Mode&unix.S_IFMT != unix.S_IFREG || s.Nlink != 1 || s.Uid != uint32(os.Getuid()) || s.Mode&0777 != 0600 {
		return ErrIntegrity
	}
	return nil
}

func (g *Gateway) enter() (func(), error) {
	g.mu.Lock()
	if g.closing || g.closed {
		g.mu.Unlock()
		return nil, mutationauthority.ErrClosed
	}
	g.active++
	g.mu.Unlock()
	done := func() { g.mu.Lock(); g.active--; g.mu.Unlock() }
	// Count this admission while checking, so Close cannot clean up the SDK
	// beneath it. A client cancellation must not poison the shared authority
	// check; the backend imposes its own short local deadlines.
	if g.sdk != nil {
		if err := g.sdk.Check(context.Background()); err != nil {
			g.mu.Lock()
			g.closing = true
			g.active--
			g.mu.Unlock()
			return nil, err
		}
	}
	return done, nil
}

// Close permanently stops new admission before checking for active requests.
// ErrInFlight retains every resource while admitted requests finish; callers
// must drain HTTP and retry Close. A process-pinned journal remains locked until
// actual process exit, even after requests drain and repeated Close calls.
func (g *Gateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closing = true
	if g.closed {
		return nil
	}
	if g.active != 0 {
		return mutationauthority.ErrInFlight
	}
	if g.sdk != nil {
		if err := g.sdk.Close(); err != nil {
			return errors.Join(err, g.journal.Close())
		}
	}
	if err := g.journal.Close(); err != nil {
		return err
	}
	g.closed = true
	return g.dir.Close()
}

func (g *Gateway) leaf(id string) string {
	b, _ := json.Marshal([]string{g.volume, id})
	return hash(b) + ".custody"
}

func artifactBytes(h artifactHeader, data []byte) []byte {
	b, _ := json.Marshal(h)
	out := make([]byte, 4, 4+len(b)+len(data))
	binary.BigEndian.PutUint32(out, uint32(len(b)))
	out = append(out, b...)
	return append(out, data...)
}

func artifactProof(b []byte) mutationauthority.DurabilityProof {
	return mutationauthority.DurabilityProof{Revision: format + ":" + hash(b), Evidence: "private-stage-fsync-v1:" + hash(b)}
}

type stageBackend struct {
	g     *Gateway
	chunk Chunk
	data  []byte
}

func (s stageBackend) Apply(_ context.Context, r mutationauthority.Request) (mutationauthority.DurabilityProof, error) {
	if !sameRequest(s.g.request(s.chunk), r) {
		return mutationauthority.DurabilityProof{}, mutationauthority.ErrChangedRequest
	}
	if s.g.stageHook != nil {
		s.g.stageHook("prepared")
	}
	b := artifactBytes(artifactHeader{Kind: format, Volume: s.g.volume, Chunk: s.chunk}, s.data)
	f, err := privatefile.OpenAt(s.g.dir, s.g.leaf(r.ID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return mutationauthority.DurabilityProof{}, err
	}
	// Never delete a partial or uncertain artifact. It remains gateway custody.
	headerEnd := 4 + int(binary.BigEndian.Uint32(b[:4]))
	_, writeErr := f.Write(b[:headerEnd])
	if writeErr != nil {
		_ = f.Close()
		return mutationauthority.DurabilityProof{}, writeErr
	}
	if s.g.stageHook != nil {
		s.g.stageHook("partial")
	}
	_, writeErr = f.Write(b[headerEnd:])
	if s.g.stageHook != nil {
		s.g.stageHook("written")
	}
	err = errors.Join(writeErr, f.Sync())
	if err != nil {
		_ = f.Close()
		return mutationauthority.DurabilityProof{}, err
	}
	if s.g.stageHook != nil {
		s.g.stageHook("file-synced")
	}
	err = errors.Join(f.Close(), s.g.dir.Sync())
	if err != nil {
		return mutationauthority.DurabilityProof{}, err
	}
	if s.g.stageHook != nil {
		s.g.stageHook("durable")
	}
	return artifactProof(b), nil
}

// readArtifact pins the exact inode and verifies the complete bounded artifact,
// including canonical metadata and payload hash. Filename/size alone never
// establish custody. Recovery additionally re-syncs this verified descriptor.
func (g *Gateway) readArtifact(r mutationauthority.Request, syncFile bool) (artifactHeader, []byte, mutationauthority.DurabilityProof, error) {
	var h artifactHeader
	f, err := privatefile.OpenAt(g.dir, g.leaf(r.ID), os.O_RDONLY, 0)
	if err != nil {
		return h, nil, mutationauthority.DurabilityProof{}, err
	}
	defer f.Close()
	if err = regularPrivate(f); err != nil {
		return h, nil, mutationauthority.DurabilityProof{}, err
	}
	b, err := io.ReadAll(io.LimitReader(f, maxArtifactBytes+1))
	if err != nil {
		return h, nil, mutationauthority.DurabilityProof{}, err
	}
	if len(b) < 4 || len(b) > maxArtifactBytes {
		return h, nil, mutationauthority.DurabilityProof{}, ErrIntegrity
	}
	n := int(binary.BigEndian.Uint32(b[:4]))
	if n <= 0 || n > 4092 || n > len(b)-4 {
		return h, nil, mutationauthority.DurabilityProof{}, ErrIntegrity
	}
	if json.Unmarshal(b[4:4+n], &h) != nil {
		return h, nil, mutationauthority.DurabilityProof{}, ErrIntegrity
	}
	canonical, _ := json.Marshal(h)
	data := b[4+n:]
	if !bytes.Equal(canonical, b[4:4+n]) || h.Kind != format || h.Volume != g.volume || !h.Chunk.valid() ||
		!sameRequest(g.request(h.Chunk), r) || int64(len(data)) != h.Chunk.Size || hash(data) != h.Chunk.SHA256 {
		return h, nil, mutationauthority.DurabilityProof{}, ErrIntegrity
	}
	if syncFile {
		if err := errors.Join(f.Sync(), g.dir.Sync()); err != nil {
			return h, nil, mutationauthority.DurabilityProof{}, err
		}
	}
	return h, data, artifactProof(b), nil
}

func (g *Gateway) preflight(ctx context.Context, acceptMedia func(mutationauthority.Request) bool) error {
	if err := g.journal.RequireOnlyVolume(ctx, g.volume); err != nil {
		return err
	}
	return g.journal.ClassifyStartup(ctx, func(r mutationauthority.Request) bool {
		return r.Volume == g.volume && strings.HasPrefix(r.ID, format+":") && identifier.MatchString(strings.TrimPrefix(r.ID, format+":")) &&
			r.ExpectedRevision == format && validSession(r.Session) && r.Epoch > 0 && validHash(r.Digest)
	}, acceptMedia)
}

func (g *Gateway) reconcile(ctx context.Context) error {
	after := ""
	for {
		ops, err := g.journal.ListPrepared(ctx, g.volume, after, 128)
		if err != nil {
			return err
		}
		if len(ops) == 0 {
			return nil
		}
		for _, op := range ops {
			after = op.ID
			if op.Kind == mutationauthority.MediaPublishOperation {
				if err := g.journal.RequireRecoveredStage(ctx, op.Request); err != nil {
					return err
				}
				continue
			}
			if !strings.HasPrefix(op.ID, format+":") || !identifier.MatchString(strings.TrimPrefix(op.ID, format+":")) || op.ExpectedRevision != format || op.Kind != mutationauthority.CustodyOperation || op.FileID != "" {
				return fmt.Errorf("%w: non-custody operation in custody journal", mutationauthority.ErrInvalid)
			}
			_, _, proof, err := g.readArtifact(op.Request, true)
			// Missing/partial/corrupt stages remain PREPARED; no guessed NO_EFFECT.
			// Claim will refuse succession, but existing committed reads still work.
			if err != nil {
				continue
			}
			if _, err := g.journal.ResolveCommitted(ctx, op.Request, proof); err != nil {
				return err
			}
		}
	}
}

func (g *Gateway) custody(ctx context.Context, c Chunk) (CustodyReceipt, []byte, error) {
	r := g.request(c)
	op, err := g.journal.Inspect(ctx, g.volume, r.ID)
	if err != nil {
		return CustodyReceipt{}, nil, err
	}
	if !sameRequest(op.Request, r) {
		return CustodyReceipt{}, nil, mutationauthority.ErrChangedRequest
	}
	if op.State != mutationauthority.Committed || op.Receipt == nil {
		return CustodyReceipt{}, nil, mutationauthority.ErrUncertain
	}
	h, data, proof, err := g.readArtifact(r, false)
	if err != nil {
		return CustodyReceipt{}, nil, errors.Join(ErrIntegrity, err)
	}
	if op.Receipt.Revision != proof.Revision || op.Receipt.Evidence != proof.Evidence {
		return CustodyReceipt{}, nil, ErrIntegrity
	}
	commitBytes := custodyCommitBytes(op.Receipt)
	headerBytes, _ := json.Marshal(h)
	return CustodyReceipt{Kind: format, Volume: g.volume, Chunk: h.Chunk, ArtifactSHA256: strings.TrimPrefix(proof.Revision, format+":"), ArtifactSize: int64(4 + len(headerBytes) + len(data)), RequestSHA256: r.Digest, ReceiptID: hash(commitBytes), CommittedAtUnixNS: op.Receipt.CommittedAtUnixNS}, data, nil
}
