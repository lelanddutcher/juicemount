package teamstest

import (
	"bufio"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/redis/go-redis/v9"
)

// logBytes is the size of the audit log and its longest line.
func logBytes(t *testing.T, dir string) (total, longest int64) {
	segs, _ := filepath.Glob(filepath.Join(dir, "audit-*.jsonl"))
	for _, s := range segs {
		f, err := os.Open(s)
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReaderSize(f, 1<<20)
		for {
			line, err := r.ReadBytes('\n')
			total += int64(len(line))
			longest = max(longest, int64(len(line)))
			if err != nil {
				break
			}
		}
		f.Close()
	}
	return total, longest
}

// TestRefusedFloodIsCollapsed: a Mac sends refused writes in a loop
// (thousands a second, each with a long name). Each was a ~3 KB record,
// so size retention could drop the whole history within minutes. Now the
// first few per window are logged one by one, each bounded, and one
// collapsed record counts the rest.
func TestRefusedFloodIsCollapsed(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	john := e.rawClient("mac-john")
	B := inoKey("d", e.ino["/Clients/B"])
	entry := append([]byte{meta.TypeFile}, make([]byte, 8)...)
	binary.BigEndian.PutUint64(entry[1:], 1<<40)
	e.audit.Sync()
	before, _ := logBytes(t, e.auditDir)
	const n = 3000
	john.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < n; i++ {
			p.HSet(ctx, B, "x-"+strconv.Itoa(i)+"-"+string(make([]byte, 1000)), entry)
		}
		return nil
	})
	var refused []auditlog.Record
	collapsed := 0
	deadline := time.Now().Add(10 * time.Second)
	for {
		refused = e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}})
		collapsed = 0
		for _, r := range refused {
			if r.Action == "collapsed" {
				collapsed += r.Count
			}
		}
		if collapsed+len(refused) >= n+1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// At most 20 one by one per refused window (1.2 s here; a slow run
	// spans several), each window past that closed by one count.
	single, windows := 0, 0
	for _, r := range refused {
		if r.Action != "collapsed" {
			single++
		} else {
			windows++
		}
	}
	if single+collapsed != n || single > 20*(windows+1) {
		t.Fatalf("%d refusals: %d records one by one, %d collapsed in %d records", n, single, collapsed, windows)
	}
	total, longest := logBytes(t, e.auditDir)
	t.Logf("%d refused units: %d records, %d bytes of log (longest line %d)", n, len(refused), total-before, longest)
	if total-before > int64(len(refused)+2)*4<<10 || longest > 4<<10 {
		t.Errorf("the flood added %d bytes in %d records (longest line %d)", total-before, len(refused), longest)
	}
}
