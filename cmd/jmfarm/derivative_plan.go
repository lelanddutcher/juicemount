package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

const derivativeMetadataShardSize = 32

type derivativeTrackProbe func(string) (*farm.VideoTrack, error)
type derivativeRouter func(context.Context, *farmqueue.Job)

// dispatchDerivativePlan turns the historical composite derivatives request
// into independent ownership units: metadata/audio stays on the server, while
// every video preview gets its own verified decoder claim. A pathological or
// unsupported source can therefore delay only itself.
func dispatchDerivativePlan(ctx context.Context, q *farmqueue.Client, store *derivatives.Store, parent farmqueue.Job, targets []string) (children, created int, err error) {
	snapshotRoute, err := q.SnapshotRouter(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot live worker routes: %w", err)
	}
	planned, err := buildDerivativePlan(ctx, func(_ context.Context, job *farmqueue.Job) { snapshotRoute(job) }, parent, targets, func(path string) (*farm.VideoTrack, error) {
		// Existing tech is sufficient only when it includes the source profile.
		// Rows written before profile-aware admission must be refreshed from the
		// live bytes here, otherwise Baseline/Main/High collapse into the same
		// H.264 queue and the render node discovers the mismatch too late.
		if track := knownVideoTrack(store, path); track != nil {
			codec := normalizedDecodeCodec(track.Codec)
			if codec == "" || strings.TrimSpace(track.Profile) != "" {
				return track, nil
			}
		}
		return probeRenderVideoTrack(path)
	})
	if err != nil {
		return 0, 0, err
	}
	created, err = q.EnqueuePlannedChildren(ctx, planned)
	return len(planned), created, err
}

func buildDerivativePlan(ctx context.Context, route derivativeRouter, parent farmqueue.Job, targets []string, probe derivativeTrackProbe) ([]farmqueue.Job, error) {
	if route == nil {
		return nil, fmt.Errorf("derivative planner has no route function")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("derivative planner has no targets")
	}
	ordered := append([]string(nil), targets...)
	sort.Strings(ordered)
	enqueuedAt := time.Now().UTC().Format(time.RFC3339)

	baseChild := func(pass string, childTargets []string) farmqueue.Job {
		child := parent
		child.ID = plannedDerivativeID(parent.ID, pass, childTargets)
		child.ParentID = parent.ID
		child.EnqueuedAt = enqueuedAt
		child.PlanOnly = false
		child.DerivativePass = pass
		child.ProcessedOffset = 0
		child.RetryTargets = append([]string(nil), childTargets...)
		child.RequiredCapabilities = nil
		child.SelectedBackend = ""
		child.SelectedWorker = ""
		child.QueueClass = ""
		child.RoutingReason = ""
		return child
	}

	children := make([]farmqueue.Job, 0, len(ordered)+len(ordered)/derivativeMetadataShardSize+1)
	for start := 0; start < len(ordered); start += derivativeMetadataShardSize {
		end := start + derivativeMetadataShardSize
		if end > len(ordered) {
			end = len(ordered)
		}
		child := baseChild(farmqueue.DerivativePassMetadata, ordered[start:end])
		child.QueueClass = farmqueue.QueueClassServer
		child.SelectedBackend = "server-metadata"
		child.RequiredCapabilities = []string{"metadata"}
		children = append(children, child)
	}

	for _, target := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		track, probeErr := probe(target)
		if probeErr == nil && track == nil {
			continue // audio-only: metadata child owns tech + waveform
		}
		child := baseChild(farmqueue.DerivativePassPreviews, []string{target})
		if probeErr != nil {
			routePreviewCPU(&child, "live video probe failed: "+probeErr.Error())
			children = append(children, child)
			continue
		}
		child.SourceVideoCodec = normalizedDecodeCodec(track.Codec)
		child.SourceVideoProfile = farmqueue.NormalizeVideoProfile(child.SourceVideoCodec, track.Profile)
		child.SourceBitDepth = track.BitDepth
		child.SourcePixelFormat = farmqueue.NormalizePixelFormat(track.PixFmt)
		child.SourceVideoWidth = track.Width
		child.SourceVideoHeight = track.Height
		route(ctx, &child)
		if child.QueueClass != farmqueue.QueueClassRender {
			reason := child.RoutingReason
			if reason == "" {
				reason = fmt.Sprintf("codec %s has no verified hardware decoder", displayValue(track.Codec))
			}
			routePreviewCPU(&child, reason)
			children = append(children, child)
			continue
		}

		// RouteJob proves a live worker advertised this decoder. Enforce the
		// source's pixel-format/bit-depth contract before publishing the claim.
		admission := farmqueue.Worker{
			Role: farmqueue.QueueClassRender, Decoders: []string{child.SelectedBackend},
			Capabilities: append(append([]string(nil), child.RequiredCapabilities...),
				farmqueue.DecoderPixelFormatRequirement(child.SelectedBackend, child.SourcePixelFormat)),
			DecodeLimits: map[string]farmqueue.VideoDecodeLimit{
				child.SelectedBackend: {MaxWidth: track.Width, MaxHeight: track.Height},
			},
		}
		if reason := unsupportedHardwareDecodeReason(admission, child.SelectedBackend, track); reason != "" {
			routePreviewCPU(&child, reason)
		} else {
			// The backend capability is portable; never pin a directory plan to
			// one ephemeral worker identity.
			child.SelectedWorker = ""
			child.RequiredCapabilities = farmqueue.DecoderSourceRequirements(
				child.SelectedBackend, child.SourceVideoCodec, child.SourceVideoProfile, child.SourcePixelFormat,
			)
		}
		children = append(children, child)
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("derivative planner produced no execution children")
	}
	for i := range children {
		children[i].ShardIndex = i + 1
		children[i].ShardCount = len(children)
	}
	return children, nil
}

func routePreviewCPU(child *farmqueue.Job, reason string) {
	child.QueueClass = farmqueue.QueueClassCPU
	child.SelectedBackend = "cpu-decode"
	child.SelectedWorker = ""
	child.RequiredCapabilities = []string{"cpu"}
	child.RoutingReason = strings.TrimSpace(reason)
	// A lack of live hardware is temporary and can be re-evaluated when a node
	// returns. A source/probe/pixel-format rejection is durable and must not loop
	// back through the accelerator on every maintenance scan.
	normalizedReason := strings.ToLower(child.RoutingReason)
	child.CPUFallbackLocked = normalizedReason != "" &&
		!strings.HasPrefix(normalizedReason, "no verified hardware decoder is online for ") &&
		!strings.Contains(normalizedReason, "worker discovery unavailable")
}

func plannedDerivativeID(parentID, pass string, targets []string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(pass))
	for _, target := range targets {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(target))
	}
	sum := h.Sum(nil)
	return parentID + "-" + pass + "-" + hex.EncodeToString(sum[:6])
}
