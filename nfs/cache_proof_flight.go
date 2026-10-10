package nfs

import (
	"context"
	"reflect"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

const cacheProofFlightTimeout = 2 * time.Second

// A flight owns only one remote proof. Every qualifier independently captures
// and confirms its full local observation, so sharing cannot publish another
// caller's ancestry or borrow its canceled deadline.
type cacheProofFlight struct {
	selected metadata.CachePathBinding
	observed metadata.CachePathObservation
	epoch    uint64
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	proof    metadata.RedisPathProof
	err      error
}

func sameProofFlight(f *cacheProofFlight, selected metadata.CachePathBinding, observed metadata.CachePathObservation, epoch uint64) bool {
	return f.selected == selected && f.epoch == epoch && reflect.DeepEqual(f.observed, observed)
}

func (b *cacheBinding) sharedPathProof(ctx context.Context, selected metadata.CachePathBinding, observed metadata.CachePathObservation, epoch uint64) (metadata.RedisPathProof, error) {
	if err := ctx.Err(); err != nil {
		return metadata.RedisPathProof{}, err
	}
	b.proofMu.Lock()
	for _, flight := range b.proofFlights {
		if !sameProofFlight(flight, selected, observed, epoch) {
			continue
		}
		if flight.ctx.Err() != nil {
			b.proofMu.Unlock()
			return metadata.RedisPathProof{}, syscall.EAGAIN
		}
		b.proofMu.Unlock()
		if hook := b.testProofFlightJoin; hook != nil {
			hook()
		}
		select {
		case <-flight.done:
			if err := ctx.Err(); err != nil {
				return metadata.RedisPathProof{}, err
			}
			return flight.proof, flight.err
		case <-ctx.Done():
			return metadata.RedisPathProof{}, ctx.Err()
		case <-b.workCtx.Done():
			return metadata.RedisPathProof{}, syscall.EAGAIN
		}
	}
	// qualifyPath admits at most four physical caller jobs. A finished owner
	// removes its flight by pointer; an expired but unsettled one occupies its
	// slot until the original remote call really returns.
	if len(b.proofFlights) >= 4 {
		b.proofMu.Unlock()
		return metadata.RedisPathProof{}, syscall.EAGAIN
	}
	proofCtx, cancel := context.WithTimeout(b.workCtx, cacheProofFlightTimeout)
	flight := &cacheProofFlight{selected: selected, observed: observed, epoch: epoch,
		ctx: proofCtx, cancel: cancel, done: make(chan struct{})}
	b.proofFlights = append(b.proofFlights, flight)
	b.proofMu.Unlock()

	// This runs on the original owned qualifier goroutine, not a detached
	// worker. Caller timeout may return early, but workActive remains held.
	proof, err := b.readProof(proofCtx, selected.FormatUUID, observed.Path, observed.ExpectedInodes())
	if err == nil && proofCtx.Err() != nil {
		err = proofCtx.Err()
	}
	flight.proof, flight.err = proof, err
	close(flight.done)
	b.proofMu.Lock()
	for i, current := range b.proofFlights {
		if current == flight {
			b.proofFlights = append(b.proofFlights[:i], b.proofFlights[i+1:]...)
			break
		}
	}
	b.proofMu.Unlock()
	cancel()
	return proof, err
}
