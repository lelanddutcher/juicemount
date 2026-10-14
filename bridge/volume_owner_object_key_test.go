//go:build !serviceclient

package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// Exercise the actual selected-owner dispatch and claim. Only generation
// reservation is replaced: the captured config is retained by an inert real
// manager, without starting a child or observing/mounting a filesystem.
func TestOwnerClaimForwardsObjectKeyWithoutPersistingIt(t *testing.T) {
	defer juicefsbin.SetForTest(testBundledJuiceFS)()
	for _, tc := range []struct {
		name string
		key  health.ObjectKey
	}{
		{"complete", health.ObjectKey{AccessKey: "claim-access", SecretKey: "claim-secret"}},
		{"empty", health.ObjectKey{}},
		{"access-only", health.ObjectKey{AccessKey: "claim-access"}},
		{"secret-only", health.ObjectKey{SecretKey: "claim-secret"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := privateIsolationTestRoot(t, "jm-owner-key-")
			t.Setenv("JM_APP_ISOLATION", "1")
			t.Setenv("JM_APP_ISOLATION_ROOT", root)
			t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
			t.Setenv("MINIO_ACCESS_KEY", "inherited-access-must-not-win")
			t.Setenv("MINIO_SECRET_KEY", "inherited-secret-must-not-win")
			cfg := privateStorageConfig(t, root)
			cfg.JuiceFSBin = testBundledJuiceFS
			cfg.ObjectAccessKey, cfg.ObjectSecretKey = tc.key.AccessKey, tc.key.SecretKey
			o, err := newVolumeOwner(root)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := servingConfigDigest(cfg)
			if err != nil {
				t.Fatal(err)
			}
			o.intentBound = true
			o.intent = volumeOwnerIntent{Instance: o.instance, ClaimID: strings.Repeat("1", 32), ConfigDigest: digest}
			var captured health.FUSEConfig
			var manager *health.FUSEManager
			calls := 0
			o.newFUSEManager = func(got health.FUSEConfig) (*health.FUSEManager, error) {
				calls++
				captured = got
				manager = health.NewFUSEManager(got)
				return manager, nil
			}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			request := servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
				Method: "claim", Strings: []string{o.instance, o.intent.ClaimID, string(raw)}, Ints: []int{0, int(netprofile.ClassFast)}}
			if response := o.dispatch(request); response.Error != "" {
				t.Fatalf("claim dispatch failed: %s", response.Error)
			}
			op := waitClaimIdentityOperation(t, o, o.intent.ClaimID)
			if op.err != nil {
				t.Fatalf("original claim failed: %v", op.err)
			}
			if calls != 1 || manager == nil || o.manager != manager {
				t.Fatal("claim did not retain its one constructed manager")
			}
			if captured.ObjectKey != tc.key {
				t.Fatal("owner claim dropped or substituted explicit object credentials")
			}
			if captured.JuiceFSBin != testBundledJuiceFS || captured.RedisURL != cfg.RedisURL || captured.MountPoint != cfg.FUSEPath {
				t.Fatal("claim changed the admitted executable, backend or mount path")
			}
			if response := o.dispatch(request); response.Error != "" || calls != 1 {
				t.Fatal("exact claim replay reconstructed its manager or failed")
			}
			persisted := persistableServerConfig(cfg)
			if persisted.ObjectAccessKey != "" || persisted.ObjectSecretKey != "" {
				t.Fatal("persistable config retained object credentials")
			}
			savedDigest, err := servingConfigDigest(persisted)
			if err != nil || savedDigest != digest || op.result.ConfigDigest != digest {
				t.Fatal("object credentials changed the selected owner identity")
			}
			withoutKey := captured
			withoutKey.ObjectKey = health.ObjectKey{}
			withJSON, err := json.Marshal(captured)
			if err != nil {
				t.Fatal(err)
			}
			withoutJSON, err := json.Marshal(withoutKey)
			if err != nil || string(withJSON) != string(withoutJSON) {
				t.Fatal("FUSE config serialized object credentials")
			}
			receipt, err := json.Marshal(op.result)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{tc.key.AccessKey, tc.key.SecretKey} {
				if key != "" && strings.Contains(string(receipt), key) {
					t.Fatal("owner receipt exposed object credentials")
				}
			}
		})
	}
}
