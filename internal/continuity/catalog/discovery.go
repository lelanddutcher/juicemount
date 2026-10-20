package catalog

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	discoveryQuota          int64 = 64 << 20
	discoveryAdmissionBytes int64 = 4 << 20
	discoveryWorkBytes      int64 = 12 << 20
	discoveryBuildBytes     int64 = 8 << 20
	discoveryCopyBytes      int64 = 1 << 20
	discoveryNewBytes             = discoveryWorkBytes + discoveryBuildBytes + discoveryCopyBytes
)

type discoveryBinding struct {
	handle          *AuthHandle
	generation      int64
	issued, expires int64
}
type DiscoveryService struct {
	store      *Store
	library    continuity.LibraryID
	mu         sync.Mutex
	closing    bool
	drained    chan struct{}
	active     map[uint64]context.CancelFunc
	next       uint64
	sql        chan struct{}
	bytes      int64
	ordinal    uint64
	windows    map[uint64]*discoveryWindow
	tokens     map[string]*discoveryPage
	building   map[SubjectID]int
	cleanupErr error
	// Test-only scheduling seam: assigned before calls, never through a public
	// setter. Production leaves it nil. It precedes the fresh authority read.
	beforeFence func(context.Context) error
}

func NewDiscoveryService(ctx context.Context, s *Store, library continuity.LibraryID) (result *DiscoveryService, resultErr error) {
	if s == nil {
		return nil, invalid("nil discovery catalog")
	}
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if s.version != authoritySchemaVersion {
		return nil, ErrUpgradeRequired
	}
	tx, done, err := s.readSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, done())
		if resultErr != nil {
			result = nil
		}
	}()
	actual, version, err := recognizeSchema(ctx, tx, false)
	if err != nil {
		return nil, err
	}
	if actual != library || version != 3 {
		return nil, corrupt("discovery catalog identity")
	}
	a, err := readAuthority(ctx, tx, library)
	if err != nil {
		return nil, err
	}
	if a.Incarnation == "" {
		return nil, ErrUnauthenticated
	}
	return &DiscoveryService{store: s, library: library, drained: make(chan struct{}), active: make(map[uint64]context.CancelFunc), sql: make(chan struct{}, 4), bytes: discoveryAdmissionBytes, windows: make(map[uint64]*discoveryWindow), tokens: make(map[string]*discoveryPage), building: make(map[SubjectID]int)}, nil
}

func (d *DiscoveryService) admit(parent context.Context) (context.Context, func(), error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return nil, nil, ErrClosed
	}
	if len(d.active) >= 16 {
		return nil, nil, errDiscoveryBusy
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	d.next++
	id := d.next
	d.active[id] = cancel
	return ctx, func() {
		cancel()
		d.mu.Lock()
		delete(d.active, id)
		if d.closing && len(d.active) == 0 {
			select {
			case <-d.drained:
			default:
				close(d.drained)
			}
		}
		d.mu.Unlock()
	}, nil
}

func (d *DiscoveryService) Close(ctx context.Context) error {
	d.mu.Lock()
	if !d.closing {
		d.closing = true
		for _, cancel := range d.active {
			cancel()
		}
		for _, w := range d.windows {
			d.retireLocked(w)
		}
		if len(d.active) == 0 {
			close(d.drained)
		}
	}
	done := d.drained
	d.mu.Unlock()
	select {
	case <-done:
		d.mu.Lock()
		err := d.cleanupErr
		d.mu.Unlock()
		return err
	case <-ctx.Done():
		return errors.Join(ErrDiscoveryDraining, ctx.Err())
	}
}
func (d *DiscoveryService) finishSnapshot(done func() error) error {
	err := done()
	if err != nil {
		d.mu.Lock()
		d.cleanupErr = errors.Join(d.cleanupErr, err)
		d.mu.Unlock()
	}
	return err
}

// releaseBuildingLocked drops the subject key when its final reservation ends.
// Retaining zero entries would grow uncharged state across distinct subjects.
func (d *DiscoveryService) releaseBuildingLocked(subject SubjectID) {
	if d.building[subject] <= 1 {
		delete(d.building, subject)
	} else {
		d.building[subject]--
	}
}

// Query releases every read owner before the final authority fence and writes
// exactly once. Started covers attempted writes even when no bytes are reported.
func (d *DiscoveryService) Query(parent context.Context, bearer []byte, input DiscoveryRequest, output io.Writer) (result DisclosureResult, resultErr error) {
	if d == nil || output == nil {
		return result, invalid("discovery service or writer")
	}
	ctx, leave, err := d.admit(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	request, err := freezeDiscoveryRequest(input, d.library)
	if err != nil {
		return result, err
	}
	if err := d.store.enter(); err != nil {
		return result, err
	}
	defer d.store.leave()
	select {
	case d.sql <- struct{}{}:
	default:
		return result, errDiscoveryBusy
	}
	sqlHeld := true
	defer func() {
		if sqlHeld {
			<-d.sql
		}
	}()
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	handle, c, subject, err := d.store.authenticateSnapshot(ctx, tx, d.library, bearer)
	if err != nil {
		return result, errors.Join(err, d.finishSnapshot(done))
	}
	result.SubjectID, result.Selector = c.subject, c.selector
	binding := discoveryBinding{handle, subject.AuthorizationGeneration, c.issued, c.expires}
	var body []byte
	var window *discoveryWindow
	if request.Kind == "page" {
		if err := d.finishSnapshot(done); err != nil {
			return result, err
		}
		body, window, err = d.leasePage(request.Token, binding)
		if err != nil {
			return result, err
		}
		defer d.releasePage(window)
	} else {
		if err := d.reserveNew(binding); err != nil {
			return result, errors.Join(err, d.finishSnapshot(done))
		}
		reserved := discoveryNewBytes
		buildingHeld := true
		defer func() {
			d.mu.Lock()
			d.bytes -= reserved
			if buildingHeld {
				d.releaseBuildingLocked(c.subject)
			}
			d.mu.Unlock()
		}()
		rights, err := readDiscoveryRights(ctx, tx, d.library, c)
		if err != nil {
			return result, errors.Join(err, d.finishSnapshot(done))
		}
		reader := discoveryReader{ctx: ctx, q: tx, library: d.library, rights: rights, assets: make(map[continuity.AssetID]*Asset)}
		items, single, complete, err := reader.query(request)
		err = errors.Join(err, d.finishSnapshot(done))
		if err != nil {
			return result, err
		}
		if single != nil {
			body, err = encodeDiscoveryValue(single, 1<<20)
		} else {
			window, err = d.makeWindow(request, binding, items, complete)
			if err == nil {
				d.mu.Lock()
				if d.closing || ctx.Err() != nil {
					err = ErrClosed
				}
				for _, token := range window.tokens {
					if d.tokens[token] != nil {
						err = errDiscoveryBusy
					}
				}
				if err == nil {
					d.bytes -= discoveryBuildBytes
					reserved -= discoveryBuildBytes
					d.bytes += window.charge
					d.releaseBuildingLocked(c.subject)
					buildingHeld = false
					d.windows[window.ordinal] = window
					for i, token := range window.tokens {
						d.tokens[token] = &discoveryPage{window, i}
					}
					window.leases = 1
				}
				d.mu.Unlock()
				if err == nil {
					body = append([]byte(nil), window.bodies[0]...)
					defer d.releaseLease(window)
				}
			}
		}
		if err != nil {
			return result, err
		}
	}
	if d.beforeFence != nil {
		if err := d.beforeFence(ctx); err != nil {
			if window != nil {
				d.mu.Lock()
				d.retireLocked(window)
				d.mu.Unlock()
			}
			return result, err
		}
	}
	if err := d.fence(ctx, binding); err != nil {
		if window != nil {
			d.mu.Lock()
			d.retireLocked(window)
			d.mu.Unlock()
		}
		return result, err
	}
	<-d.sql
	sqlHeld = false
	// Nothing that can queue, serialize or query is permitted between this last
	// clock/context check and the writer's first call.
	now := d.store.now().Unix()
	if now < binding.issued || now >= binding.expires {
		if window != nil {
			d.mu.Lock()
			d.retireLocked(window)
			d.mu.Unlock()
		}
		return result, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		if window != nil {
			d.mu.Lock()
			d.retireLocked(window)
			d.mu.Unlock()
		}
		return result, err
	}
	result.Started = true
	n, err := output.Write(body)
	if err != nil {
		return result, err
	}
	if n != len(body) {
		return result, io.ErrShortWrite
	}
	return result, nil
}

func (d *DiscoveryService) fence(ctx context.Context, b discoveryBinding) error {
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return err
	}
	// The FIRST authority read establishes this fresh snapshot's fence.
	a, err := readAuthority(ctx, tx, d.library)
	if err == nil && a.Incarnation != b.handle.incarnation {
		err = ErrUnauthenticated
	}
	if err == nil {
		_, subject, e := d.store.currentHandle(ctx, tx, b.handle)
		err = e
		if err == nil && subject.AuthorizationGeneration != b.generation {
			err = errDiscoveryRestart
		}
	}
	return errors.Join(err, d.finishSnapshot(done))
}
