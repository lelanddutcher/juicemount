package manager

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// Type aliases + helpers that keep farm_config.go readable and fakeable.
// The concrete types ARE the farmqueue types; the aliases exist so handler
// tests can stub the farmQueue interface without a live Redis.

type stdCtx = context.Context
type farmQueueFarmConfig = farmqueue.FarmConfig
type farmQueueWorker = farmqueue.Worker

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}

func farmqueueValidKeys() []string {
	out := make([]string, 0, len(farmqueue.ValidFarmConfigKeys))
	for k := range farmqueue.ValidFarmConfigKeys {
		out = append(out, k)
	}
	return out
}

func farmqueueValidate(patch map[string]any) error { return farmqueue.ValidateFarmConfig(patch) }

// validWorkerName mirrors destinations.go's nameRegex discipline: strict
// lowercase identifier — it becomes a JSON object key and a UI badge.
func validWorkerName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// mergePatch returns dst updated with src's keys (shallow merge — the config
// sections are flat maps by design).
func mergePatch(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = map[string]any{}
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

var _ = fmt.Sprintf // keep fmt import if helpers shrink

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
