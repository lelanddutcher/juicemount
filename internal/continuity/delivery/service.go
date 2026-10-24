package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationclient"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

type routeConfig struct {
	ID              string `json:"id"`
	Endpoint        string `json:"endpoint"`
	CertificateFile string `json:"certificate_file"`
	KeyFile         string `json:"key_file"`
	RootsFile       string `json:"roots_file"`
	StoreID         string `json:"store_id"`
}
type config struct {
	Version             int                  `json:"version"`
	LibraryID           continuity.LibraryID `json:"library_id"`
	MaterializationRoot string               `json:"materialization_root"`
	Routes              []routeConfig        `json:"routes"`
}
type route struct {
	client              *mutationclient.Client
	digest, leaf, store string
}

// managedOutputRoot keeps resource closure part of the service ownership boundary.
// Production always uses the descriptor-backed privatefile.ManagedRoot.
type managedOutputRoot interface {
	OpenAttempt(string) (*privatefile.Attempt, error)
	OpenOwnedReady(string, privatefile.Identity, privatefile.Identity, privatefile.Identity, privatefile.Identity) (*privatefile.OwnedReady, error)
	ReadyPath(string) (string, error)
	Close() error
}

var errServiceClosed = errors.New("delivery service closed")

type Service struct {
	store     *catalog.Store
	library   continuity.LibraryID
	root      managedOutputRoot
	routes    map[string]route
	mu        sync.Mutex
	closed    bool
	closeDone chan struct{}
	closeErr  error
	workers   sync.WaitGroup
	active    map[uint64]context.CancelFunc
	next      uint64
}

func readConfig(path string) (config, error) {
	raw, _, err := privatefile.ReadPrivate(path, 32768)
	if err != nil {
		return config{}, err
	}
	var c config
	if err := json.Unmarshal(raw, &c); err != nil {
		return config{}, err
	}
	encoded, err := json.Marshal(c)
	if err != nil || !bytes.Equal(raw, encoded) || c.Version != 1 || c.LibraryID.Validate() != nil || c.Routes == nil || len(c.Routes) == 0 || len(c.Routes) > 16 {
		return config{}, errors.New("invalid delivery route registry")
	}
	return c, nil
}
func loadRoute(c routeConfig) (route, error) {
	if c.ID == "" || len(c.ID) > 128 || !mutationauthority.ImageIDValid(c.StoreID, "ims_") {
		return route{}, errors.New("invalid delivery route")
	}
	certPEM, _, err := privatefile.ReadPrivate(c.CertificateFile, 65536)
	if err != nil {
		return route{}, err
	}
	keyPEM, _, err := privatefile.ReadPrivate(c.KeyFile, 65536)
	if err != nil {
		return route{}, err
	}
	rootsPEM, _, err := privatefile.ReadPrivate(c.RootsFile, 65536)
	if err != nil {
		return route{}, err
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return route{}, err
	}
	if len(cert.Certificate) == 0 {
		return route{}, errors.New("empty reader certificate")
	}
	if _, err := x509.ParseCertificate(cert.Certificate[0]); err != nil {
		return route{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootsPEM) {
		return route{}, errors.New("invalid reader trust roots")
	}
	client, err := mutationclient.NewClient(c.Endpoint, cert, roots)
	if err != nil {
		return route{}, err
	}
	leaf := sha256.Sum256(cert.Certificate[0])
	encoded, _ := json.Marshal(c)
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.delivery.route\x00\x01"))
	h.Write(encoded)
	for _, b := range [][]byte{certPEM, keyPEM, rootsPEM} {
		sum := sha256.Sum256(b)
		h.Write(sum[:])
	}
	return route{client, hex.EncodeToString(h.Sum(nil)), hex.EncodeToString(leaf[:]), c.StoreID}, nil
}
func NewService(ctx context.Context, store *catalog.Store, library continuity.LibraryID, configPath string) (_ *Service, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || library.Validate() != nil {
		return nil, errors.New("invalid delivery catalog")
	}
	c, err := readConfig(configPath)
	if err != nil {
		return nil, err
	}
	if c.LibraryID != library {
		return nil, errors.New("delivery registry library mismatch")
	}
	root, err := privatefile.OpenManagedRoot(c.MaterializationRoot)
	if err != nil {
		return nil, err
	}
	routes := make(map[string]route)
	defer func() {
		if err != nil {
			for _, r := range routes {
				r.client.Close()
			}
			err = errors.Join(err, root.Close())
		}
	}()
	for _, cfg := range c.Routes {
		if _, found := routes[cfg.ID]; found {
			return nil, errors.New("duplicate delivery route")
		}
		loaded, e := loadRoute(cfg)
		if e != nil {
			return nil, e
		}
		routes[cfg.ID] = loaded
	}
	if _, err := store.LocalOwner(library); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Service{store: store, library: library, root: root, routes: routes, active: map[uint64]context.CancelFunc{}}, nil
}
func (s *Service) own(parent context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, errServiceClosed
	}
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	s.next++
	id := s.next
	s.active[id] = cancel
	s.workers.Add(1)
	return ctx, func() { cancel(); s.mu.Lock(); delete(s.active, id); s.mu.Unlock(); s.workers.Done() }, nil
}
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	s.closeDone = make(chan struct{})
	for _, cancel := range s.active {
		cancel()
	}
	s.mu.Unlock()
	// Operation completion needs mu. Keep it available while joining the
	// original calls; no resource is closed until all of them have returned.
	s.workers.Wait()
	for _, r := range s.routes {
		r.client.Close()
	}
	s.closeErr = s.root.Close()
	close(s.closeDone)
	return s.closeErr
}
func (s *Service) routeFor(binding catalog.DeliveryChunkBinding) (route, error) {
	r, ok := s.routes[binding.RouteID]
	if !ok || r.digest != binding.RouteDigest || r.leaf != binding.ReaderLeafSHA256 || r.store != binding.View.StoreID {
		return route{}, errors.New("configured delivery reader unavailable")
	}
	return r, nil
}
func (s *Service) BindSource(parent context.Context, input catalog.SourceBindRequest, routeID string) (catalog.SourceBindResult, error) {
	ctx, done, err := s.own(parent)
	if err != nil {
		return catalog.SourceBindResult{}, err
	}
	defer done()
	r, ok := s.routes[routeID]
	if !ok {
		return catalog.SourceBindResult{}, errors.New("delivery route unavailable")
	}
	input.RouteID = routeID
	input.RouteDigest = r.digest
	input.ReaderLeafSHA256 = r.leaf
	if input.Image.StoreID != r.store {
		return catalog.SourceBindResult{}, errors.New("delivery store mismatch")
	}
	owner, err := s.store.LocalOwner(s.library)
	if err != nil {
		return catalog.SourceBindResult{}, err
	}
	return owner.BindDeliverySource(ctx, input, r.client)
}
func (s *Service) Select(parent context.Context, bearer []byte, input catalog.DeliverySelectRequest) (catalog.DeliverySelection, error) {
	ctx, done, err := s.own(parent)
	if err != nil {
		return catalog.DeliverySelection{}, err
	}
	defer done()
	return s.store.SelectDelivery(ctx, bearer, input)
}
func (s *Service) Status(parent context.Context, bearer []byte, id string) (catalog.DeliverySelection, error) {
	ctx, done, err := s.own(parent)
	if err != nil {
		return catalog.DeliverySelection{}, err
	}
	defer done()
	return s.store.DeliveryStatus(ctx, bearer, id)
}

// Materialize owns the original fetch and descriptor lifetime. On interruption
// the partial and SQL attempt remain for explicit conservative reconciliation.
func (s *Service) Materialize(parent context.Context, bearer []byte, selectionID, operationID string) (string, catalog.MaterializationAttempt, error) {
	return s.materialize(parent, bearer, selectionID, operationID, nil)
}

// MaterializeForHandoff returns the one opaque cleanup authority needed if
// the caller's later shared-service Close fails before emitting its receipt.
func (s *Service) MaterializeForHandoff(parent context.Context, bearer []byte, selectionID, operationID string) (string, catalog.MaterializationAttempt, *catalog.MaterializationCleanup, error) {
	var cleanup *catalog.MaterializationCleanup
	path, attempt, err := s.materialize(parent, bearer, selectionID, operationID, &cleanup)
	return path, attempt, cleanup, err
}

func (s *Service) materialize(parent context.Context, bearer []byte, selectionID, operationID string, cleanupOut **catalog.MaterializationCleanup) (path string, attempt catalog.MaterializationAttempt, resultErr error) {
	ctx, done, err := s.own(parent)
	if err != nil {
		return "", catalog.MaterializationAttempt{}, err
	}
	defer done()
	binding, err := s.store.AdmitDelivery(ctx, bearer, selectionID)
	if err != nil {
		return "", catalog.MaterializationAttempt{}, err
	}
	r, err := s.routeFor(binding)
	if err != nil {
		return "", catalog.MaterializationAttempt{}, err
	}
	attempt, cleanup, err := s.store.ReserveOwnedMaterialization(ctx, bearer, selectionID, operationID)
	if err != nil {
		return "", attempt, err
	}
	if cleanupOut != nil {
		*cleanupOut = cleanup
	}
	if attempt.State == "COMPLETED" {
		return s.recoverExisting(ctx, bearer, binding, r, attempt, cleanup)
	}
	if attempt.State != "RESERVED" {
		return "", attempt, catalog.ErrReconciliationRequired
	}
	output, err := s.root.OpenAttempt(attempt.ID)
	if err != nil {
		return "", attempt, err
	}
	ownerBeforeRelease := output.CloseBeforeRelease
	ownerReleaseLock := output.ReleaseLock
	defer func() {
		beforeErr := ownerBeforeRelease()
		if beforeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("materialization original owner pre-release close uncertain: %w", beforeErr))
		}
		if resultErr != nil {
			path = ""
			if attempt.State != "RESERVED" {
				if markErr := cleanup.MarkUncertain(); markErr != nil {
					resultErr = errors.Join(resultErr, fmt.Errorf("materialization durable state uncertain: %w", markErr))
				}
			}
		}
		if terminalErr := ownerReleaseLock(); terminalErr != nil {
			path = ""
			resultErr = errors.Join(resultErr, fmt.Errorf("materialization terminal lock close uncertain: %w", terminalErr))
			if attempt.State != "RESERVED" {
				if markErr := cleanup.MarkUncertain(); markErr != nil {
					resultErr = errors.Join(resultErr, fmt.Errorf("materialization post-release state uncertain: %w", markErr))
				}
			}
		}
	}()
	view := binding.View
	current, err := r.client.ImageStatus(ctx, mutationauthority.ImageRef{Version: 1, StoreID: view.StoreID, LibraryID: view.LibraryID, ImageID: view.ImageID})
	if err != nil {
		return "", attempt, err
	}
	expected, _ := json.Marshal(view)
	actual, _ := json.Marshal(current)
	if !bytes.Equal(expected, actual) {
		return "", attempt, mutationclient.ErrIntegrity
	}
	file := output.Writer()
	if file == nil {
		return "", attempt, errors.New("managed partial unavailable")
	}
	for index := range view.Chunks {
		pin, err := s.store.AdmitDeliveryChunk(ctx, bearer, selectionID, int64(index))
		if err != nil {
			return "", attempt, err
		}
		currentPin, _ := json.Marshal(pin.View)
		if !bytes.Equal(expected, currentPin) {
			return "", attempt, mutationclient.ErrIntegrity
		}
		chunk, err := r.client.ImageReadback(ctx, view, int64(index))
		if err != nil {
			return "", attempt, err
		}
		n, err := file.Write(chunk)
		if err != nil {
			return "", attempt, err
		}
		if n != len(chunk) {
			return "", attempt, io.ErrShortWrite
		}
	}
	prepared, err := output.Finish()
	if err != nil {
		return "", attempt, err
	}
	ownerBeforeRelease = prepared.CloseBeforeRelease
	ownerReleaseLock = prepared.ReleaseLock
	updated, err := s.store.RecordPreparedMaterialization(ctx, bearer, prepared)
	if updated.ID == attempt.ID {
		attempt = updated
	}
	if err != nil {
		return "", attempt, err
	}
	// The remote status is fresh immediately before the local writer fence.
	// It cannot make a remote revoke atomic with the final local rename.
	current, err = r.client.ImageStatus(ctx, mutationauthority.ImageRef{Version: 1, StoreID: view.StoreID, LibraryID: view.LibraryID, ImageID: view.ImageID})
	if err != nil {
		return "", attempt, err
	}
	actual, _ = json.Marshal(current)
	if !bytes.Equal(expected, actual) {
		return "", attempt, mutationclient.ErrIntegrity
	}
	updated, err = s.store.PublishMaterialization(ctx, bearer, prepared)
	if updated.ID == attempt.ID {
		attempt = updated
	}
	if err != nil {
		return "", attempt, err
	}
	path, err = prepared.ReadyPath()
	if err != nil {
		return "", attempt, fmt.Errorf("published file path unavailable: %w", err)
	}
	return path, attempt, nil
}

// Reconcile never creates a partial, moves a file or repeats publication. It
// accepts only an exact existing final under its original permanent lock.
func (s *Service) Reconcile(parent context.Context, bearer []byte, selectionID, operationID string) (string, catalog.MaterializationAttempt, error) {
	return s.reconcile(parent, bearer, selectionID, operationID, nil)
}

func (s *Service) ReconcileForHandoff(parent context.Context, bearer []byte, selectionID, operationID string) (string, catalog.MaterializationAttempt, *catalog.MaterializationCleanup, error) {
	var cleanup *catalog.MaterializationCleanup
	path, attempt, err := s.reconcile(parent, bearer, selectionID, operationID, &cleanup)
	return path, attempt, cleanup, err
}

func (s *Service) reconcile(parent context.Context, bearer []byte, selectionID, operationID string, cleanupOut **catalog.MaterializationCleanup) (string, catalog.MaterializationAttempt, error) {
	ctx, done, err := s.own(parent)
	if err != nil {
		return "", catalog.MaterializationAttempt{}, err
	}
	defer done()
	binding, err := s.store.AdmitDelivery(ctx, bearer, selectionID)
	if err != nil {
		return "", catalog.MaterializationAttempt{}, err
	}
	r, err := s.routeFor(binding)
	if err != nil {
		return "", catalog.MaterializationAttempt{}, err
	}
	attempt, cleanup, err := s.store.ExistingOwnedMaterialization(ctx, bearer, selectionID, operationID)
	if err != nil {
		return "", attempt, err
	}
	if cleanupOut != nil {
		*cleanupOut = cleanup
	}
	if attempt.State == "RESERVED" {
		return "", attempt, catalog.ErrReconciliationRequired
	}
	return s.recoverExisting(ctx, bearer, binding, r, attempt, cleanup)
}

func (s *Service) recoverExisting(ctx context.Context, bearer []byte, pinned catalog.DeliveryChunkBinding, r route, attempt catalog.MaterializationAttempt, cleanup *catalog.MaterializationCleanup) (path string, result catalog.MaterializationAttempt, resultErr error) {
	result = attempt
	bound, err := s.store.MaterializationRecoveryBinding(ctx, bearer, attempt.ID)
	if err != nil {
		return "", attempt, err
	}
	if bound.Attempt != attempt {
		return "", attempt, catalog.ErrConflict
	}
	owned, err := s.root.OpenOwnedReady(attempt.ID, bound.Root, bound.Directory, bound.Lock, bound.File)
	if err != nil {
		return "", attempt, err
	}
	defer func() {
		beforeErr := owned.CloseBeforeRelease()
		if beforeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("recovery owner pre-release close uncertain: %w", beforeErr))
		}
		if resultErr != nil {
			path = ""
			if markErr := cleanup.MarkUncertain(); markErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("materialization durable state uncertain: %w", markErr))
			}
		}
		if terminalErr := owned.ReleaseLock(); terminalErr != nil {
			path = ""
			resultErr = errors.Join(resultErr, fmt.Errorf("recovery terminal lock close uncertain: %w", terminalErr))
			if markErr := cleanup.MarkUncertain(); markErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("materialization post-release state uncertain: %w", markErr))
			}
		}
	}()
	result, err = s.store.RecoverMaterialization(ctx, bearer, owned, r.client, pinned.View)
	if err != nil {
		return "", attempt, err
	}
	path, err = owned.Path()
	if err != nil {
		return "", result, err
	}
	return path, result, nil
}
