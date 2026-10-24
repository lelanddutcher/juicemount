package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationclient"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

// Transfer owns the original service operation, exact final descriptor and
// permanent lock until Complete or Close joins them. It is not a path receipt.
// EOF is only a cursor result. Only Complete can issue server_stream_complete.
type Transfer struct {
	mu sync.Mutex
	service *Service
	ctx context.Context
	cancel context.CancelFunc
	done chan struct{}
	bearer []byte
	binding catalog.DeliveryChunkBinding
	header TransferHeader
	reader *privatefile.ReadyTransfer
	remaining int64
	closed bool
	completed bool
	failure error
	closeErr error
}

func (t *Transfer) String() string { return "delivery.Transfer[owned]" }
func (t *Transfer) GoString() string { return t.String() }

// OpenTransfer never silently reconciles an ambiguous attempt or selects a new
// revision. A cursor retry keeps the exact original materialization operation.
func (s *Service) OpenTransfer(parent context.Context, bearer []byte, request TransferRequest) (_ *Transfer, resultErr error) {
	_,digest,err := CanonicalTransferRequest(request)
	if err != nil || request.LibraryID != s.library { return nil,ErrInvalidTransfer }
	ctx,release,err := s.own(parent)
	if err != nil { return nil,err }
	transferred := false
	defer func() { if !transferred { release() } }()
	binding,err := s.store.AdmitDelivery(ctx,bearer,request.SelectionID)
	if err != nil { return nil,err }
	if request.Offset > binding.Selection.Content.Size { return nil,ErrInvalidTransfer }
	deadline := min(time.Now().Add(900*time.Second).UnixNano(),time.Unix(binding.Selection.ExpiresUnix,0).UnixNano())
	ctx,cancel := context.WithDeadline(ctx,time.Unix(0,deadline))
	defer func() { if !transferred { cancel() } }()
	r,err := s.routeFor(binding)
	if err != nil { return nil,err }
	attempt,_,err := s.store.ReserveOwnedMaterialization(ctx,bearer,request.SelectionID,request.OperationID)
	if err != nil { return nil,err }
	if attempt.State == "RESERVED" {
		// Existing materialize performs the original fixed publication under its
		// writer fence. Its identical reserve retry cannot create a second row.
		_,attempt,err = s.materialize(ctx,bearer,request.SelectionID,request.OperationID,nil)
		if err != nil { return nil,err }
	}
	if attempt.State != "COMPLETED" { return nil,catalog.ErrReconciliationRequired }
	bound,err := s.store.MaterializationRecoveryBinding(ctx,bearer,attempt.ID)
	if err != nil { return nil,err }
	if bound.Attempt != attempt { return nil,catalog.ErrConflict }
	owned,err := s.root.OpenOwnedReady(attempt.ID,bound.Root,bound.Directory,bound.Lock,bound.File)
	if err != nil { return nil,err }
	// The new stream owns this exact descriptor; it never uses recoverExisting's
	// returned path. Recovery repeats full verification and durability barriers.
	recovered,err := s.store.RecoverMaterialization(ctx,bearer,owned,r.client,binding.View)
	if err != nil { return nil,errors.Join(err,owned.Close()) }
	if recovered != attempt { return nil,errors.Join(catalog.ErrConflict,owned.Close()) }
	reader,err := privatefile.OpenReadyTransfer(ctx,owned,attempt.Content,request.Offset,request.PrefixSHA256)
	if err != nil { return nil,err } // constructor consumed the OwnedReady
	t := &Transfer{service:s,ctx:ctx,cancel:cancel,done:make(chan struct{}),bearer:append([]byte(nil),bearer...),binding:binding,reader:reader,remaining:attempt.Content.Size-request.Offset}
	t.header = TransferHeader{Version:1,RequestSHA256:digest,AuthorityIncarnation:binding.AuthorityIncarnation,Selection:binding.Selection,Attempt:attempt,Offset:request.Offset,Length:t.remaining,PrefixSHA256:request.PrefixSHA256}
	if err := ValidateTransferResponse(request,t.header,nil); err != nil { return nil,errors.Join(err,reader.Close()) }
	// Even cached and empty responses sample current Gateway authority, then
	// catalog/selector/source authority, after the full/prefix hash.
	if err := t.admit(ctx); err != nil { return nil,errors.Join(err,reader.Close()) }
	transferred = true
	// One owner performs terminal closure. Cancellation interrupts admitted
	// network I/O first; locking then joins the original read/complete call.
	go func() {
		<-ctx.Done()
		t.mu.Lock()
		if !t.closed {
			t.closeErr = t.reader.Close()
			t.closed = true
		}
		clear(t.bearer)
		t.bearer = nil
		t.mu.Unlock()
		release()
		close(t.done)
	}()
	return t,nil
}

// Header returns only detached value fields; no bearer, reader or route escapes.
func (t *Transfer) Header() TransferHeader { return t.header }

// callContext intersects caller cancellation with the retained original absolute
// deadline/service lifetime. Background cannot extend or revive this transfer.
func (t *Transfer) callContext(caller context.Context) (context.Context,func()) {
	ctx,cancel := context.WithCancel(t.ctx)
	stop := context.AfterFunc(caller,cancel)
	if caller.Err()!=nil { cancel() }
	return ctx,func(){ stop(); cancel() }
}
func (t *Transfer) admit(ctx context.Context) error {
	if err := ctx.Err(); err != nil { return err }
	r,err := t.service.routeFor(t.binding)
	if err != nil { return err }
	view := t.binding.View
	current,err := r.client.ImageStatus(ctx,mutationauthority.ImageRef{Version:1,StoreID:view.StoreID,LibraryID:view.LibraryID,ImageID:view.ImageID})
	if err != nil { return err }
	pinned,_ := json.Marshal(view)
	actual,_ := json.Marshal(current)
	if !bytes.Equal(pinned,actual) { return mutationclient.ErrIntegrity }
	binding,err := t.service.store.AdmitDelivery(ctx,t.bearer,t.header.Selection.SelectionID)
	if err != nil { return err }
	if _,err := t.service.routeFor(binding); err != nil { return err }
	actual,_ = json.Marshal(binding.View)
	if binding.Selection != t.header.Selection || binding.AuthorityIncarnation != t.header.AuthorityIncarnation || binding.RouteID != t.binding.RouteID || binding.RouteDigest != t.binding.RouteDigest || binding.ReaderLeafSHA256 != t.binding.ReaderLeafSHA256 || !bytes.Equal(pinned,actual) { return mutationclient.ErrIntegrity }
	return ctx.Err()
}
func (t *Transfer) fail(err error) error {
	if t.failure == nil { t.failure = err }
	t.cancel()
	return err
}
func (t *Transfer) usable(ctx context.Context) error {
	if t.failure != nil { return t.failure }
	if t.closed { return ErrTransferClosed }
	if err := ctx.Err(); err != nil { return err }
	return nil
}
func (t *Transfer) ReadChunk(caller context.Context, maximum int) ([]byte,error) {
	ctx,done := t.callContext(caller)
	defer done()
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.usable(ctx); err != nil { return nil,t.fail(err) }
	if maximum<1 || maximum>MaxTransferChunkBytes { return nil,t.fail(ErrInvalidTransfer) }
	if err := t.admit(ctx); err != nil { return nil,t.fail(err) }
	b,err := t.reader.ReadChunk(ctx,maximum)
	if err != nil {
		if err==io.EOF && t.remaining==0 { return nil,io.EOF }
		return nil,t.fail(err)
	}
	t.remaining-=int64(len(b))
	return b,nil
}
func (t *Transfer) Complete(caller context.Context) (TransferEnd,error) {
	ctx,done := t.callContext(caller)
	defer done()
	t.mu.Lock()
	if err := t.usable(ctx); err != nil { t.fail(err); t.mu.Unlock(); return TransferEnd{},err }
	if t.remaining!=0 { t.fail(ErrTransferIncomplete); t.mu.Unlock(); return TransferEnd{},ErrTransferIncomplete }
	if err := t.reader.VerifyComplete(ctx); err != nil { t.fail(err); t.mu.Unlock(); return TransferEnd{},err }
	if err := t.admit(ctx); err != nil { t.fail(err); t.mu.Unlock(); return TransferEnd{},err }
	t.closeErr=t.reader.Close()
	t.closed=true
	if err := errors.Join(t.closeErr,ctx.Err()); err != nil { t.fail(err); t.mu.Unlock(); <-t.done; return TransferEnd{},err }
	t.completed=true
	h := t.header
	end := TransferEnd{Version:1,RequestSHA256:h.RequestSHA256,AttemptID:h.Attempt.ID,Offset:h.Offset,Length:h.Length,Result:"server_stream_complete"}
	t.cancel()
	t.mu.Unlock()
	<-t.done
	return end,nil
}

// Close aborts and joins the exact original operation. It cannot issue End,
// modify COMPLETED history, remove output or claim any HTTP trailer was flushed.
func (t *Transfer) Close() error {
	if t==nil { return nil }
	t.cancel()
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeErr
}
