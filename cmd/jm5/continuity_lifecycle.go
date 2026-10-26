package main

import (
	"context"
	"errors"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/internal/metrics"
)

func resolveContinuityManagerSettings(roots, legacyRoots, key, legacyKey, envKey string) (string, string) {
	if roots == "" {
		roots = legacyRoots
	}
	if key == "" {
		key = legacyKey
	}
	if key == "" {
		key = envKey
	}
	return roots, key
}

func prepareEmbeddedContinuity(ctx context.Context, roots string, cfg manager.ContinuityConfig) (*manager.Continuity, error) {
	if (cfg.CatalogPath != "" || cfg.LibraryID != "") && len(splitNonEmpty(roots, ",")) == 0 {
		return nil, errors.New("continuity requires enabled Manager source roots")
	}
	return manager.PrepareContinuity(ctx, cfg)
}

func closeEmbeddedContinuity(closeService func(context.Context) error) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := closeService(ctx)
		cancel()
		if !errors.Is(err, catalog.ErrDiscoveryDraining) {
			return err
		}
		jmlog.Warn("continuity shutdown incomplete; retaining catalog until requests drain")
	}
}

func stopEmbeddedMetrics(server *metrics.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return server.ShutdownAndWait(ctx)
}
