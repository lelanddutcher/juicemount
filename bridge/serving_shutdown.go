package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

var shutdownOwnerSocketPath = shutdownControlOwnerSocketPath
var observePriorServingShutdownNFS = priorServingShutdownNFSRetired

// Only replacement Shutdown uses this control address. General owner socket
// resolution continues through the full isolation admission path.
func shutdownControlOwnerSocketPath() (string, error) {
	scope, _, err := selectedOwnerControlScope()
	if err != nil {
		return "", err
	}
	path := filepath.Join(scope.Root, "o.sock")
	if len(path) > 103 {
		return "", errors.New("owner control socket path is too long")
	}
	if err := scope.RequireChild(path); err != nil {
		return "", err
	}
	return path, nil
}

// recoverServingShutdownOwner is control-only. An idle replacement has no
// globalOwner, but the original owner may still hold FUSE after StopMount.
// This path never selects, claims, attaches, or starts an owner.
func recoverServingShutdownOwner() (*ownerFacade, error) {
	desired, err := readServingDesired()
	if err != nil {
		return nil, err
	}
	if desired.Desired != "stopped" {
		return nil, errors.New("shutdown lacks durable stopped intent")
	}
	scope, _, err := selectedOwnerControlScope()
	if err != nil {
		return nil, err
	}
	current, intentErr := readOwnerIntent(scope)
	if desired.Owner == (volumeOwnerIntent{}) {
		if errors.Is(intentErr, os.ErrNotExist) && desired.NFSPhase == "none" && desired.NFSRecord == nil && desired.Config.MountPoint == "" {
			return nil, nil // fresh namespace which never selected an owner
		}
		// Selection can survive an engine exit before desired.Owner is bound.
		// Reconcile that exact original only in the pre-NFS phase; this grants
		// control-only Stop authority, never permission to select or claim.
		if intentErr != nil || desired.NFSPhase != "none" || desired.NFSRecord != nil {
			return nil, errors.New("shutdown owner intent missing or inconsistent")
		}
	} else if intentErr != nil || current != desired.Owner {
		return nil, errors.New("shutdown original owner intent changed or unavailable")
	}
	if err := validateOwnerIntent(current, scope.Root); err != nil {
		return nil, err
	}
	digest, err := servingConfigDigest(desired.Config)
	if err != nil {
		return nil, err
	}
	if current.ConfigDigest != digest {
		return nil, errors.New("shutdown original owner config changed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ready, err := observePriorServingShutdownNFS(ctx, desired)
	if err != nil {
		return nil, fmt.Errorf("shutdown original NFS retirement or absence unconfirmed: %w", err)
	}
	if !ready {
		return nil, errors.New("shutdown original NFS retirement or absence unconfirmed")
	}
	path, err := shutdownOwnerSocketPath()
	if err != nil {
		return nil, err
	}
	f := &ownerFacade{intent: current, client: &volumeOwnerClient{
		path: path, instance: current.Instance, digest: current.ConfigDigest, root: current.Root}}
	unchanged := func() error {
		selected, err := readOwnerIntent(scope)
		if err != nil || selected != current {
			return errors.New("shutdown original owner intent changed during reconciliation")
		}
		latest, err := readServingDesired()
		if err != nil || !reflect.DeepEqual(latest, desired) {
			return errors.New("shutdown desired state changed during owner reconciliation")
		}
		return nil
	}
	if err := confirmOwnerRetired(current); err == nil {
		if err := unchanged(); err != nil {
			return nil, err
		}
		return f, nil // terminal owner needs only exact retirement reconciliation
	}
	r, err := f.readReceipt()
	if err != nil {
		return nil, fmt.Errorf("shutdown original owner inspection unavailable: %w", err)
	}
	var stopID string
	for _, op := range r.Operations {
		if op.Method != "stop" {
			continue
		}
		decoded, decodeErr := hex.DecodeString(op.ID)
		if len(op.ID) != 32 || decodeErr != nil || hex.EncodeToString(decoded) != op.ID ||
			op.State != "pending" && op.State != "complete" && op.State != "error" {
			return nil, errors.New("shutdown original Stop operation ambiguous")
		}
		if op.State == "error" {
			continue // definitive prior failure permits one new Stop ID
		}
		if stopID != "" && stopID != op.ID {
			return nil, errors.New("shutdown original Stop operation ambiguous")
		}
		stopID = op.ID
	}
	if err := unchanged(); err != nil {
		return nil, err
	}
	if stopID != "" {
		globalMu.Lock()
		if globalOwnerStopID != "" && globalOwnerStopID != stopID {
			globalMu.Unlock()
			return nil, errors.New("shutdown Stop operation changed identity")
		}
		globalOwnerStopID = stopID
		globalMu.Unlock()
	}
	return f, nil
}
