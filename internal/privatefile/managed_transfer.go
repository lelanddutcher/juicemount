package privatefile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const MaxReadyTransferChunk = 1 << 20
const maxReadyTransferBytes = 1 << 30

// ReadyTransfer consumes one OwnedReady lifetime. It never exposes a descriptor,
// path, seek, or caller-supplied I/O callback. The caller must Close it, including
// after EOF or verification failure; EOF alone says nothing about delivery.
type ReadyTransfer struct {
	mu       sync.Mutex
	ctx      context.Context
	owned    *OwnedReady
	content  continuity.Content
	offset   int64
	sent     hash.Hash
	closed   bool
	failed   error
	closeErr error
}

// OpenReadyTransfer verifies the whole held file and the immutable resume prefix
// in one bounded scan before any bytes can escape. Ownership is consumed even on
// failure, so there is exactly one descriptor/lock close responsibility.
func OpenReadyTransfer(ctx context.Context, owned *OwnedReady, content continuity.Content, offset int64, prefixSHA256 string) (_ *ReadyTransfer, resultErr error) {
	if owned == nil {
		return nil, errors.New("owned transfer unavailable")
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, owned.Close())
		}
	}()
	if ctx == nil || content.Validate() != nil || content.Size > maxReadyTransferBytes || offset < 0 || offset > content.Size {
		return nil, errors.New("invalid owned transfer binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owned.Revalidate(); err != nil {
		return nil, err
	}
	if owned.fileID.Size != content.Size {
		return nil, errors.New("owned transfer length changed")
	}
	whole, prefix := sha256.New(), sha256.New()
	buf := make([]byte, 32768)
	for at := int64(0); at < content.Size; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		length := min(int64(len(buf)), content.Size-at)
		n, err := owned.read.ReadAt(buf[:int(length)], at)
		if err != nil || int64(n) != length {
			return nil, errors.Join(err, io.ErrUnexpectedEOF)
		}
		_, _ = whole.Write(buf[:n])
		if at < offset {
			_, _ = prefix.Write(buf[:int(min(length, offset-at))])
		}
		at += length
	}
	if hex.EncodeToString(whole.Sum(nil)) != content.SHA256 || hex.EncodeToString(prefix.Sum(nil)) != prefixSHA256 {
		return nil, errors.New("owned transfer content or prefix mismatch")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owned.Revalidate(); err != nil {
		return nil, err
	}
	return &ReadyTransfer{ctx: ctx, owned: owned, content: content, offset: offset, sent: prefix}, nil
}

func (r *ReadyTransfer) check(ctx context.Context) error {
	if r.closed {
		return errors.New("owned transfer closed")
	}
	if r.failed != nil {
		return r.failed
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.owned.Revalidate()
}

// Revalidate checks the exact held identity after an external authority wait.
// It does not expose a path or confer catalog publication authority.
func (r *ReadyTransfer) Revalidate(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		r.failed = err
		return err
	}
	return nil
}

// ReadChunk returns at most one bounded buffer. A failed read never releases
// partial bytes. Callers perform their authority admission before each call.
func (r *ReadyTransfer) ReadChunk(ctx context.Context, maximum int) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		r.failed = err
		return nil, err
	}
	if maximum < 1 || maximum > MaxReadyTransferChunk {
		r.failed = errors.New("invalid owned transfer chunk bound")
		return nil, r.failed
	}
	if r.offset == r.content.Size {
		return nil, io.EOF
	}
	next := make([]byte, int(min(int64(maximum), r.content.Size-r.offset)))
	n, err := r.owned.read.ReadAt(next, r.offset)
	if err != nil || n != len(next) {
		r.failed = errors.Join(err, io.ErrUnexpectedEOF)
		return nil, r.failed
	}
	if err := r.check(ctx); err != nil {
		r.failed = err
		return nil, err
	}
	_, _ = r.sent.Write(next)
	r.offset += int64(n)
	return next, nil
}

// VerifyComplete binds the bytes actually handed off, including the verified
// prefix, to the full immutable content. It does not mint a receipt or close.
func (r *ReadyTransfer) VerifyComplete(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		r.failed = err
		return err
	}
	if r.offset != r.content.Size || hex.EncodeToString(r.sent.Sum(nil)) != r.content.SHA256 {
		r.failed = errors.New("owned transfer incomplete or changed content")
		return r.failed
	}
	return nil
}

func (r *ReadyTransfer) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.closeErr = r.owned.Close()
	}
	return r.closeErr
}
