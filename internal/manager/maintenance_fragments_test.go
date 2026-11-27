package manager

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestEstimateCompactionRedis runs the counter against a real Redis
// (JM_TEST_JFS_META, a throwaway db: it writes fixture keys). Skips without.
func TestEstimateCompactionRedis(t *testing.T) {
	url := os.Getenv("JM_TEST_JFS_META")
	if url == "" {
		t.Skip("JM_TEST_JFS_META not set")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	slice := func(pos, l uint32) string {
		b := make([]byte, 24)
		binary.BigEndian.PutUint32(b[0:], pos)
		binary.BigEndian.PutUint64(b[4:], 7)
		binary.BigEndian.PutUint32(b[16:], l)
		binary.BigEndian.PutUint32(b[20:], l)
		return string(b)
	}
	// inode 10: chunk 0 in 48 slices of 1 MiB, chunk 1 in 1 slice.
	for i := uint32(0); i < 48; i++ {
		rdb.RPush(ctx, "c10_0", slice(i<<20, 1<<20))
	}
	rdb.RPush(ctx, "c10_1", slice(0, 64<<20))
	// inode 11: one chunk in 3 slices ending at 5 MiB.
	rdb.RPush(ctx, "c11_0", slice(0, 1<<20), slice(4<<20, 1<<20), slice(0, 16))
	rdb.Set(ctx, "counter", "x", 0) // not a chunk key
	est, err := estimateCompaction(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	// c10_0 repacks (48 slices, 48 MiB); c11_0's first 1 MiB slice is
	// overwritten by the 16 B one at 0, so all 3 are merged over 5 MiB.
	if est.ChunksScanned != 3 || est.ChunksMultiSlice != 2 || est.ChunksRepack != 2 || est.SlicesRepack != 51 ||
		est.FilesRepack != 2 || est.BytesRepack != 48<<20+5<<20 ||
		est.Buckets["20-98"] != 1 || est.Buckets["2-4"] != 1 {
		t.Fatalf("estimate = %+v", est)
	}
}

// TestEstimateCompactionLive prints the estimate for a real volume
// (JM_TEST_ESTIMATE_META, e.g. redis://bg:<pw>@<proxy>:6379/1). Read-only.
func TestEstimateCompactionLive(t *testing.T) {
	url := os.Getenv("JM_TEST_ESTIMATE_META")
	if url == "" {
		t.Skip("JM_TEST_ESTIMATE_META not set")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	opts.Protocol, opts.DisableIdentity = 2, true
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	est, err := estimateCompaction(context.Background(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("estimate: %+v", est)
}
