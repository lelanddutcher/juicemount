package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

type proxyTrackProbe func(string) (*farm.VideoTrack, error)

func shouldDispatchProxyPlan(job farmqueue.Job) bool {
	return job.PlanOnly && len(job.Kinds) == 1 && job.Kinds[0] == farmqueue.KindProxy
}

// dispatchProxyPlan probes source compatibility once on the metadata server,
// then publishes deterministic homogeneous batches. Unsupported codecs never
// consume a render lease, while compatible work enters that lane only with
// both verified decoder and encoder requirements attached.
func dispatchProxyPlan(ctx context.Context, q *farmqueue.Client, store *derivatives.Store, parent farmqueue.Job, targets []string, maxTargets int) (children, created int, err error) {
	if q == nil {
		return 0, 0, fmt.Errorf("proxy planner has no queue")
	}
	route, err := q.SnapshotRouter(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot live worker routes: %w", err)
	}
	planned, err := buildProxyPlan(ctx, func(_ context.Context, job *farmqueue.Job) { route(job) }, parent, targets, maxTargets, func(path string) (*farm.VideoTrack, error) {
		if track := knownVideoTrack(store, path); track != nil && strings.TrimSpace(track.Codec) != "" && strings.TrimSpace(track.Profile) != "" {
			return track, nil
		}
		return probeRenderVideoTrack(path)
	})
	if err != nil || len(planned) == 0 {
		return len(planned), 0, err
	}
	created, err = q.EnqueuePlannedChildren(ctx, planned)
	return len(planned), created, err
}

type proxyPlanSource struct {
	path   string
	track  *farm.VideoTrack
	reason string
}

func buildProxyPlan(ctx context.Context, route derivativeRouter, parent farmqueue.Job, targets []string, maxTargets int, probe proxyTrackProbe) ([]farmqueue.Job, error) {
	if route == nil {
		return nil, fmt.Errorf("proxy planner has no route function")
	}
	if maxTargets < 1 {
		return nil, fmt.Errorf("proxy planner batch size must be positive")
	}
	ordered := append([]string(nil), targets...)
	sort.Strings(ordered)
	groups := make(map[string][]proxyPlanSource)
	for _, target := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		track, probeErr := probe(target)
		if probeErr == nil && track == nil {
			continue // audio-only sources intentionally have no video proxy
		}
		source := proxyPlanSource{path: target, track: track}
		key := "probe-error|" + target
		if probeErr != nil {
			source.reason = "live video probe failed: " + probeErr.Error()
		} else {
			codec := strings.ToLower(strings.TrimSpace(track.Codec))
			profile := farmqueue.NormalizeVideoProfile(codec, track.Profile)
			key = strings.Join([]string{
				codec, profile, strings.ToLower(strings.TrimSpace(track.PixFmt)),
				strconv.Itoa(track.BitDepth), strconv.Itoa(track.Width), strconv.Itoa(track.Height),
			}, "|")
		}
		groups[key] = append(groups[key], source)
	}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var children []farmqueue.Job
	for _, key := range keys {
		sources := groups[key]
		for start := 0; start < len(sources); start += maxTargets {
			end := start + maxTargets
			if end > len(sources) {
				end = len(sources)
			}
			paths := make([]string, 0, end-start)
			for _, source := range sources[start:end] {
				paths = append(paths, source.path)
			}
			child := parent
			child.ID = plannedDerivativeID(parent.ID, "proxy", paths)
			child.ParentID = parent.ID
			child.PlanOnly = false
			child.ProcessedOffset = 0
			child.RetryTargets = paths
			child.RequiredCapabilities = nil
			child.SelectedBackend = ""
			child.SelectedWorker = ""
			child.QueueClass = ""
			child.RoutingReason = ""
			child.CPUFallbackLocked = false
			representative := sources[start]
			if representative.track == nil {
				routeProxyCPU(&child, representative.reason, true)
				children = append(children, child)
				continue
			}
			track := representative.track
			child.SourceVideoCodec = strings.ToLower(strings.TrimSpace(track.Codec))
			child.SourceVideoProfile = farmqueue.NormalizeVideoProfile(child.SourceVideoCodec, track.Profile)
			child.SourceBitDepth = track.BitDepth
			child.SourcePixelFormat = farmqueue.NormalizePixelFormat(track.PixFmt)
			child.SourceVideoWidth = track.Width
			child.SourceVideoHeight = track.Height
			codec := normalizedDecodeCodec(track.Codec)
			if codec == "" {
				routeProxyCPU(&child, fmt.Sprintf("codec %s has no verified hardware decoder", displayValue(track.Codec)), true)
				children = append(children, child)
				continue
			}
			if reason := unsupportedHardwareVideoFormatReason(codec, track); reason != "" {
				routeProxyCPU(&child, reason, true)
				children = append(children, child)
				continue
			}
			route(ctx, &child)
			if child.QueueClass != farmqueue.QueueClassRender {
				reason := child.RoutingReason
				if reason == "" {
					reason = "no verified hardware decode+encode path is online for " + child.SourceVideoCodec
				}
				routeProxyCPU(&child, reason, false)
				children = append(children, child)
				continue
			}
			// Planned children are portable to any worker proving the same full
			// path. Remove only the ephemeral worker pin, retaining encoder and
			// decoder/profile requirements.
			child.SelectedWorker = ""
			caps := child.RequiredCapabilities[:0]
			for _, capability := range child.RequiredCapabilities {
				if !strings.HasPrefix(capability, "worker:") {
					caps = append(caps, capability)
				}
			}
			child.RequiredCapabilities = caps
			children = append(children, child)
		}
	}
	for i := range children {
		children[i].ShardIndex = i + 1
		children[i].ShardCount = len(children)
	}
	return children, nil
}

func routeProxyCPU(child *farmqueue.Job, reason string, locked bool) {
	child.QueueClass = farmqueue.QueueClassCPU
	child.VCodec = "libx264"
	child.SelectedBackend = "libx264"
	child.SelectedWorker = ""
	child.RequiredCapabilities = []string{"cpu"}
	child.RoutingReason = strings.TrimSpace(reason)
	child.CPUFallbackLocked = locked
}
