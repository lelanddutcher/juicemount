package manager

// Round-3 review findings: route capture in service exposure, and the
// recorded exposure default.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestServicesConflictReportsRouteCapture: in service exposure a node
// holding an approved route over the NAS's LAN is reported. Mac apps from
// before linkDialPlanner followed such a route ahead of the tag:jm-nas peer.
func TestServicesConflictReportsRouteCapture(t *testing.T) {
	t.Setenv("JM_NET_NAS_LAN_ADDR", "")
	nodes := []HSNode{
		{ID: 1, NodeKey: "nodekey:nas", Name: "juicemount-nas", Tags: []string{LinkTagNAS}, Online: true},
		{ID: 2, NodeKey: "nodekey:other", Name: "evil", ApprovedRoutes: []string{"192.0.2.0/24"}, AvailableRoutes: []string{"192.0.2.0/24"}, Online: true},
	}
	err := linkNASConflicts(nodes, nil, 1, "")
	if err == nil || !strings.Contains(err.Error(), `node 2 ("evil")`) {
		t.Fatalf("services exposure: node 2's approved route over the NAS LAN not reported: %v", err)
	}
}

// TestRecordedExposureIsTheFirstRun: the record is the exposure the node
// first ran with, so dropping JM_NET_NAS_EXPOSURE=subnet later keeps it.
func TestRecordedExposureIsTheFirstRun(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_LAN_ADDR", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "subnet")
	mode, err := RecordLinkExposureDefault(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "nas-node"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_NET_NAS_EXPOSURE", "")
	if got, _, _ := LinkExposure(cfg); got != LinkExposureSubnet || mode != LinkExposureSubnet {
		t.Fatalf("recorded %q while running subnet; after unsetting JM_NET_NAS_EXPOSURE exposure is %q", mode, got)
	}
}

// TestLinkNASConflictsEndsWithItsContext: the NAS node's periodic conflict
// check runs the headscale CLI; a wedged control server must not hold the
// node's stop (which cancels ctx) or its watch loop.
func TestLinkNASConflictsEndsWithItsContext(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "headscale-hangs")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	err := LinkNASConflicts(ctx, bin, filepath.Join(dir, "config.yaml"), nodeKeyNAS, "")
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("LinkNASConflicts against a hung CLI returned %v after %v", err, time.Since(start))
	}
}

// TestConflictNamesStaleOwnRegistration: after the NAS node's state is
// reset it registers again under the same name, and its earlier record
// (offline, older) keeps tag:jm-nas. The report says so and gives the
// command to remove it.
func TestConflictNamesStaleOwnRegistration(t *testing.T) {
	t.Setenv("JM_NET_NAS_LAN_ADDR", "")
	nodes := []HSNode{
		{ID: 3, NodeKey: "nodekey:old", Name: "juicemount-nas", Tags: []string{LinkTagNAS}},
		{ID: 7, NodeKey: nodeKeyNAS, Name: "juicemount-nas", Tags: []string{LinkTagNAS}, Online: true},
		{ID: 9, NodeKey: nodeKeyMac, Name: "mac", Tags: []string{LinkTagMac, LinkTagNAS}, Online: true},
	}
	err := linkNASConflicts(nodes, nil, 7, "")
	if err == nil {
		t.Fatal("no conflict reported")
	}
	msg := err.Error()
	for _, want := range []string{
		`node 3 ("juicemount-nas") carries tag:jm-nas (offline and older than the NAS node under its name: most likely this NAS's own earlier registration`,
		"docker exec <Manager container> headscale nodes delete --identifier <id>",
		"Remove this NAS's earlier registration",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("conflict report lacks %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, `node 9 ("mac") carries tag:jm-nas (offline`) {
		t.Fatalf("an online Mac was called a stale NAS registration:\n%s", msg)
	}
}
