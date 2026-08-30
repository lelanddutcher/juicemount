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
