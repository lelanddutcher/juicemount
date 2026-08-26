package farmqueue

import (
	"context"
	"sort"
	"strings"
)

const (
	QueueClassServer = "server"
	QueueClassRender = "render"
	QueueClassCPU    = "cpu"
)

func classQueue(kind, class string) string {
	return QueueKey + ":" + kind + ":" + class
}

// RouteJob selects a concrete execution lane from verified live worker
// profiles. Hardware HEVC wins, then hardware H.264; software H.264 is the
// fallback only when no render worker reports a working encoder.
func (c *Client) RouteJob(ctx context.Context, j *Job) {
	if j == nil || len(j.Kinds) != 1 {
		return
	}
	kind := j.Kinds[0]
	workers, err := c.ActiveWorkers(ctx)
	if err != nil {
		workers = nil
	}
	sort.SliceStable(workers, func(i, k int) bool {
		return workers[i].Benchmarks.EncodeFPS > workers[k].Benchmarks.EncodeFPS
	})

	switch kind {
	case KindDerivatives:
		j.QueueClass = QueueClassServer
		j.SelectedBackend = "server-cpu"
		j.RequiredCapabilities = []string{"metadata"}
	case KindProxy:
		if selected, enc, ok := preferredHardwareWorker(workers); ok {
			j.QueueClass = QueueClassRender
			j.VCodec = enc
			j.SelectedBackend = enc
			j.SelectedWorker = workerDisplayName(selected)
			j.RequiredCapabilities = []string{"encoder:" + enc, "worker:" + selected.ID}
			return
		}
		j.QueueClass = QueueClassCPU
		j.VCodec = "libx264"
		j.SelectedBackend = "libx264"
		j.SelectedWorker = ""
		j.RequiredCapabilities = []string{"cpu"}
	case KindTranscript:
		if selected, backend, ok := preferredTranscriptWorker(workers); ok {
			j.QueueClass = QueueClassRender
			j.SelectedBackend = backend
			j.SelectedWorker = workerDisplayName(selected)
			j.RequiredCapabilities = []string{"transcript:" + backend, "worker:" + selected.ID}
			return
		}
		j.QueueClass = QueueClassCPU
		j.SelectedBackend = "cpu"
		j.SelectedWorker = ""
		j.RequiredCapabilities = []string{"cpu"}
	}
}

func preferredHardwareWorker(workers []Worker) (Worker, string, bool) {
	ordered := append([]Worker(nil), workers...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Benchmarks.EncodeFPS > ordered[j].Benchmarks.EncodeFPS
	})
	for _, family := range []string{"hevc_", "h264_"} {
		for _, w := range ordered {
			if w.Role != QueueClassRender {
				continue
			}
			for _, enc := range w.Encoders {
				if strings.HasPrefix(enc, family) && enc != "libx265" && enc != "libx264" {
					return w, enc, true
				}
			}
		}
	}
	return Worker{}, "", false
}

func preferredHardwareEncoder(workers []Worker) (string, bool) {
	_, encoder, ok := preferredHardwareWorker(workers)
	return encoder, ok
}

func preferredTranscriptWorker(workers []Worker) (Worker, string, bool) {
	ordered := append([]Worker(nil), workers...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Benchmarks.TranscriptXReal > ordered[j].Benchmarks.TranscriptXReal
	})
	for _, w := range ordered {
		if w.Role != QueueClassRender {
			continue
		}
		for _, backend := range w.TranscriptBackends {
			if backend != "" && backend != "cpu" {
				return w, backend, true
			}
		}
	}
	return Worker{}, "", false
}

func preferredTranscriptBackend(workers []Worker) (string, bool) {
	_, backend, ok := preferredTranscriptWorker(workers)
	return backend, ok
}

func workerDisplayName(w Worker) string {
	if w.Name != "" {
		return w.Name
	}
	return w.ID
}

// WorkerQueueKeys returns only lanes this worker is allowed to execute. A
// render node never drains the CPU fallback lane, and a server node never
// races a render node for accelerator work.
func WorkerQueueKeys(w Worker) []string {
	var keys []string
	switch w.Role {
	case QueueClassServer:
		keys = append(keys,
			classQueue(KindDerivatives, QueueClassServer),
			classQueue(KindProxy, QueueClassCPU),
			classQueue(KindTranscript, QueueClassCPU))
	case QueueClassRender:
		if len(w.Encoders) > 0 {
			keys = append(keys, classQueue(KindProxy, QueueClassRender))
		}
		if backend, ok := preferredTranscriptBackend([]Worker{w}); ok && backend != "" {
			keys = append(keys, classQueue(KindTranscript, QueueClassRender))
		}
	default:
		// Legacy workers retain the v1 subscription contract.
		keys = append(keys, QueueKeysForKinds(w.Kinds)...)
		return uniqueStrings(keys)
	}
	// Legacy catch-all remains a compatibility lane. Server owns it so an old
	// multi-kind producer cannot send a render-only job to an arbitrary node.
	if w.Role == QueueClassServer {
		keys = append(keys, QueueKey)
	}
	return uniqueStrings(keys)
}

func workerCapabilitySet(w Worker) map[string]bool {
	set := map[string]bool{"cpu": true}
	if w.ID != "" {
		set["worker:"+w.ID] = true
	}
	for _, cap := range w.Capabilities {
		set[cap] = true
	}
	for _, enc := range w.Encoders {
		set["encoder:"+enc] = true
	}
	for _, backend := range w.TranscriptBackends {
		set["transcript:"+backend] = true
	}
	if w.Role == QueueClassServer {
		set["metadata"] = true
	}
	return set
}

func WorkerSupports(w Worker, required []string) bool {
	set := workerCapabilitySet(w)
	for _, req := range required {
		if !set[req] {
			return false
		}
	}
	return true
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
