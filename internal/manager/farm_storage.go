package manager

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

const (
	defaultFarmMinFree = uint64(64 << 30)
	farmStoragePoll    = 5 * time.Second
)

type farmStorageSnapshot struct {
	TotalBytes     uint64 `json:"total_bytes"`
	AvailableBytes uint64 `json:"available_bytes"`
}

type farmStorageGate struct {
	Configured     bool   `json:"configured"`
	Safe           bool   `json:"safe"`
	Path           string `json:"-"`
	TotalBytes     uint64 `json:"total_bytes,omitempty"`
	AvailableBytes uint64 `json:"available_bytes,omitempty"`
	RequiredBytes  uint64 `json:"required_bytes,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

type farmSafetyPauser interface {
	PauseForSafety(context.Context, string, string) (farmqueueControl, error)
}

type farmStoragePermitter interface {
	PublishStoragePermit(context.Context, farmqueue.StoragePermit) error
	RevokeStoragePermit(context.Context) error
}

// Alias keeps the optional interface decoupled from the concrete queue client
// while preserving its exact method signature.
type farmqueueControl = farmqueue.FarmControl

func deriveFarmStoragePath(explicit, statusPath string) string {
	if path := strings.TrimSpace(explicit); path != "" {
		return filepath.Clean(path)
	}
	if path := strings.TrimSpace(statusPath); path != "" {
		return filepath.Dir(filepath.Clean(path))
	}
	return ""
}

func readFarmStorageSnapshot(path string) (farmStorageSnapshot, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return farmStorageSnapshot{}, err
	}
	if st.Bsize <= 0 {
		return farmStorageSnapshot{}, fmt.Errorf("invalid filesystem block size %d", st.Bsize)
	}
	bsize := uint64(st.Bsize)
	return farmStorageSnapshot{
		TotalBytes:     uint64(st.Blocks) * bsize,
		AvailableBytes: uint64(st.Bavail) * bsize,
	}, nil
}

func farmRequiredHeadroom(total, configured uint64) uint64 {
	if configured > 0 {
		return configured
	}
	required := total / 100
	if required < defaultFarmMinFree {
		required = defaultFarmMinFree
	}
	return required
}

func (a *API) inspectFarmStorage() farmStorageGate {
	if a == nil || a.farmStoragePath == "" {
		return farmStorageGate{Safe: true}
	}
	probe := a.farmStorageStat
	if probe == nil {
		probe = readFarmStorageSnapshot
	}
	snapshot, err := probe(a.farmStoragePath)
	if err != nil {
		return farmStorageGate{
			Configured: true, Safe: false, Path: a.farmStoragePath,
			Reason: "backend storage headroom could not be verified",
		}
	}
	required := farmRequiredHeadroom(snapshot.TotalBytes, a.farmMinFree)
	gate := farmStorageGate{
		Configured: true, Safe: snapshot.AvailableBytes >= required,
		Path: a.farmStoragePath, TotalBytes: snapshot.TotalBytes,
		AvailableBytes: snapshot.AvailableBytes, RequiredBytes: required,
	}
	if !gate.Safe {
		gate.Reason = "backend free space is below the farm safety reserve; reclaim storage before resuming"
	}
	return gate
}

// enforceFarmStorageGuard couples physical pool admission to the shared Redis
// control plane. It never auto-resumes: recovered headroom only makes the Play
// action eligible again, leaving the operator in control of restart timing.
func (a *API) enforceFarmStorageGuard(ctx context.Context) farmStorageGate {
	gate := a.inspectFarmStorage()
	if !gate.Configured || a.farmQ == nil {
		return gate
	}
	if permitter, ok := a.farmQ.(farmStoragePermitter); ok {
		if gate.Safe {
			if err := permitter.PublishStoragePermit(ctx, farmqueue.StoragePermit{
				Source: "manager", TotalBytes: gate.TotalBytes,
				AvailableBytes: gate.AvailableBytes, RequiredBytes: gate.RequiredBytes,
			}); err != nil {
				// The permit TTL makes this fail closed for render nodes. Do not
				// mislabel measured physical headroom as unsafe solely because the
				// control plane is temporarily unreachable.
				log.Printf("manager: farm storage permit refresh failed: %v", err)
			}
			return gate
		}
		if err := permitter.RevokeStoragePermit(ctx); err != nil {
			// Expiry still closes admission if an overloaded Redis cannot accept
			// the immediate revocation.
			log.Printf("manager: farm storage permit revoke failed: %v", err)
		}
	} else if gate.Safe {
		return gate
	}
	code := "storage-pressure"
	if gate.TotalBytes == 0 {
		code = "storage-probe-failed"
	}
	if pauser, ok := a.farmQ.(farmSafetyPauser); ok {
		if _, err := pauser.PauseForSafety(ctx, code, gate.Reason); err != nil {
			log.Printf("manager: farm storage safety pause failed: %v", err)
		}
	}
	return gate
}

func (a *API) runFarmStorageGuard() {
	check := func() {
		ctx, cancel := context.WithTimeout(context.Background(), farmQueueProbeTimeout)
		defer cancel()
		a.enforceFarmStorageGuard(ctx)
	}
	check()
	ticker := time.NewTicker(farmStoragePoll)
	defer ticker.Stop()
	for range ticker.C {
		check()
	}
}
