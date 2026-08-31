package juicemountmanager

import (
	"os"
	"strings"
	"testing"
)

func TestProductionComposeRequiresManagerAdminKeyAtRenderTime(t *testing.T) {
	src, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	compose := string(src)

	required := `JM_ADMIN_KEY: "${JM_ADMIN_KEY:?set JM_ADMIN_KEY to a 32+ character value before deploying Manager}"`
	if !strings.Contains(compose, required) {
		t.Fatalf("production compose must require JM_ADMIN_KEY before container reconciliation")
	}
	for _, unsafe := range []string{
		`JM_ADMIN_KEY: ""`,
		`JM_ADMIN_KEY: "${JM_ADMIN_KEY:-}"`,
		`JM_ADMIN_KEY: "${JM_ADMIN_KEY-}"`,
	} {
		if strings.Contains(compose, unsafe) {
			t.Fatalf("production compose contains unsafe Manager auth fallback %q", unsafe)
		}
	}
}

func TestProductionComposeUsesOnlyImmutableReleaseImages(t *testing.T) {
	src, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	compose := string(src)

	for _, forbidden := range []string{
		"juicemount-manager:latest",
		"juicefarm:local",
		`JM_FARM_SERVER_IMAGE: "${JM_FARM_SERVER_IMAGE:-}"`,
		`JM_FARM_RENDER_IMAGE: "${JM_FARM_RENDER_IMAGE:-}"`,
	} {
		if strings.Contains(compose, forbidden) {
			t.Fatalf("production compose retains mutable image contract %q", forbidden)
		}
	}
	for _, required := range []string{
		"juicemount-manager@${JM_MANAGER_DIGEST:?",
		"juicefarm@${JM_FARM_DIGEST:?",
		"juicefarm-gpu@${JM_FARM_RENDER_DIGEST:?",
		"redis:7.4-alpine@sha256:",
		"minio/minio:RELEASE.2025-01-20T14-49-07Z@sha256:",
		"juicedata/mount:ce-v1.3.1@sha256:",
	} {
		if !strings.Contains(compose, required) {
			t.Fatalf("production compose is missing immutable image contract %q", required)
		}
	}
}

func TestProductionComposeRequiresPhysicalFarmCapacity(t *testing.T) {
	src, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	compose := string(src)
	required := `JM_FARM_STORAGE_CAPACITY_BYTES: "${JM_FARM_STORAGE_CAPACITY_BYTES:?set to the integer output of zpool list -Hp -o size <pool>}"`
	if strings.Count(compose, required) != 2 {
		t.Fatalf("production compose must require exact physical capacity for Manager and server worker")
	}
	if strings.Contains(compose, `JM_FARM_STORAGE_CAPACITY_BYTES: "${JM_FARM_STORAGE_CAPACITY_BYTES:-0}"`) {
		t.Fatal("production compose silently falls back to dataset-scoped capacity")
	}
}

func TestProductionComposePinsFarmEncodeScratchOffJuiceFS(t *testing.T) {
	src, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	compose := string(src)
	if strings.Count(compose, `JM_FARM_ENCODE_SCRATCH: "/tmp"`) != 1 {
		t.Fatal("production compose must pin the server worker's encode scratch to node-local /tmp")
	}
	for _, forbidden := range []string{
		`JM_FARM_ENCODE_SCRATCH: "/jfs"`,
		`JM_FARM_ENCODE_SCRATCH: "/jfs/`,
	} {
		if strings.Contains(compose, forbidden) {
			t.Fatalf("production compose points seek-heavy MP4 scratch at JuiceFS: %q", forbidden)
		}
	}
}

func TestProductionComposeRequiresMinIOCredentialAtRenderTime(t *testing.T) {
	src, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	compose := string(src)
	if strings.Count(compose, `${MINIO_ROOT_PASSWORD:?`) != 2 {
		t.Fatal("production compose must require the same external MinIO credential for MinIO and JuiceFS format")
	}
	if strings.Contains(compose, "CHANGEME_MINIO_PASSWORD") {
		t.Fatal("production compose embeds a deployable placeholder MinIO password")
	}
}
