package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func readRecentsCall(t *testing.T, d *DiscoveryService, bearer []byte, r RecentsReadRequest) RecentsResponse {
	t.Helper()
	var out bytes.Buffer
	result, err := d.QueryRecents(context.Background(), bearer, r, &out)
	if err != nil || !result.Started {
		t.Fatalf("recents: %+v %v", result, err)
	}
	var page RecentsResponse
	if err := json.Unmarshal(out.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Version != 2 || page.Kind != "recents" || !page.WindowComplete || page.CountScope != "frozen_authorized_recent" {
		t.Fatalf("recents envelope: %s", out.Bytes())
	}
	return page
}

func TestRecentsComplete256FullEscapedLabelsAndReplay(t *testing.T) {
	for _, pageSize := range []int{1, 100} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			f := newSavedReadFixture(t)
			label := strings.Repeat("<", 4096)
			changes := make([]Change, 256)
			for i := range changes {
				changes[i] = Change{CreateAsset: &CreateAsset{continuity.AssetID(fmt.Sprintf("asset_%032x", i+1)), label}}
			}
			// Full escaping expands these labels sixfold in canonical Apply JSON.
			// Eight distinct bounded requests preserve the entire maximal corpus.
			for start := 0; start < len(changes); start += 32 {
				f.apply(t, fmt.Sprintf("recents-max-assets-%d", start/32), changes[start:start+32]...)
			}
			f.now.Store(1700000000)
			for i := 256; i >= 129; i-- {
				f.remember(t, f.owner, fmt.Sprintf("old-%d", i), continuity.AssetID(fmt.Sprintf("asset_%032x", i)))
			}
			f.now.Store(1700001000)
			for i := 128; i >= 1; i-- {
				f.remember(t, f.owner, fmt.Sprintf("new-%d", i), continuity.AssetID(fmt.Sprintf("asset_%032x", i)))
			}
			f.now.Store(1700001001)
			before := savedQueryTestSnapshot(t, f.s, true)
			r := RecentsReadRequest{2, "list", readOracleLibrary, pageSize, ""}
			total, pages := 0, 0
			for {
				var out bytes.Buffer
				result, err := f.d.QueryRecents(context.Background(), f.owner, r, &out)
				if err != nil || !result.Started {
					t.Fatalf("max page: %+v %v", result, err)
				}
				var p RecentsResponse
				if err := json.Unmarshal(out.Bytes(), &p); err != nil {
					t.Fatal(err)
				}
				if p.MatchedCount != 256 || !p.WindowComplete {
					t.Fatal("truncated maximal recents")
				}
				for _, item := range p.Items {
					total++
					if item.AssetID != continuity.AssetID(fmt.Sprintf("asset_%032x", total)) || item.Label != label || item.HeadRevisionID != nil {
						t.Fatalf("full row %d: %+v", total, item)
					}
					wantSelected, wantExpiry := "1700001000", "1702593000"
					if total > 128 {
						wantSelected, wantExpiry = "1700000000", "1702592000"
					}
					if item.SelectedUnix != wantSelected || item.ExpiresUnix != wantExpiry {
						t.Fatal("foreign or inaccurate selection time")
					}
				}
				want := 25031
				if p.NextPageToken == nil {
					want = 24963
				}
				if pageSize == 100 {
					want = 1039986
					if p.NextPageToken == nil {
						want = 99228
					}
				}
				if out.Len() != want {
					t.Fatalf("independent page length: %d want %d", out.Len(), want)
				}
				if r.Kind == "page" {
					var replay bytes.Buffer
					if _, err := f.d.QueryRecents(context.Background(), f.owner, r, &replay); err != nil || !bytes.Equal(out.Bytes(), replay.Bytes()) {
						t.Fatalf("replay changed: %v", err)
					}
				}
				pages++
				if p.NextPageToken == nil {
					break
				}
				f.d.mu.Lock()
				w := f.d.tokens[*p.NextPageToken].window
				compact := w.recent
				if len(w.bodies) != 0 || len(compact.labels) != 1048576 || cap(compact.labels) != 1048576 || cap(compact.assets) != 256 || cap(compact.heads) != 256 || cap(compact.headFlags) != 256 || cap(compact.labelBounds) != 256 || cap(compact.times) != 256 || cap(compact.lengths) != 256 || w.charge != recentsCharge(w) || w.charge > 1226266 {
					t.Error("compact ownership/capacity charge")
				}
				f.d.mu.Unlock()
				r = RecentsReadRequest{2, "page", readOracleLibrary, 0, *p.NextPageToken}
			}
			wantPages := 256
			if pageSize == 100 {
				wantPages = 7
			}
			if total != 256 || pages != wantPages {
				t.Fatalf("coverage %d rows/%d pages", total, pages)
			}
			if !bytes.Equal(before, savedQueryTestSnapshot(t, f.s, true)) {
				t.Fatal("max paging wrote activity or pruning")
			}
		})
	}
}

func TestRecentsExactMiBRangeAndEscaping(t *testing.T) {
	for _, extra := range []bool{false, true} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			f := newSavedReadFixture(t)
			changes := make([]Change, 44)
			for i := range changes {
				label := ""
				if i < 42 {
					label = strings.Repeat("<", 4096)
				}
				if i == 42 {
					label = strings.Repeat("<", 1402)
					if extra {
						label += "a"
					}
				}
				changes[i] = Change{CreateAsset: &CreateAsset{continuity.AssetID(fmt.Sprintf("asset_%032x", i+1)), label}}
			}
			f.apply(t, "edge-assets", changes...)
			f.now.Store(1700001000)
			for i := 44; i >= 1; i-- {
				f.remember(t, f.owner, fmt.Sprintf("edge-%d", i), continuity.AssetID(fmt.Sprintf("asset_%032x", i)))
			}
			f.now.Store(1700001001)
			var out bytes.Buffer
			if _, err := f.d.QueryRecents(context.Background(), f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 100, ""}, &out); err != nil {
				t.Fatal(err)
			}
			var p RecentsResponse
			if err := json.Unmarshal(out.Bytes(), &p); err != nil {
				t.Fatal(err)
			}
			wantRows, wantBytes := 43, 1048576
			if extra {
				wantRows, wantBytes = 42, 1039985
			}
			if len(p.Items) != wantRows || out.Len() != wantBytes || p.MatchedCount != 44 || !p.WindowComplete || p.NextPageToken == nil {
				t.Fatalf("exact range: rows%d bytes%d", len(p.Items), out.Len())
			}
			// Repartition must preserve row43's full label and all44 literal IDs,
			// rather than produce a plausible first page after silently dropping it.
			next := RecentsReadRequest{2, "page", readOracleLibrary, 0, *p.NextPageToken}
			last := readRecentsCall(t, f.d, f.owner, next)
			if len(last.Items) != 44-wantRows || last.MatchedCount != 44 || !last.WindowComplete || last.NextPageToken != nil {
				t.Fatalf("terminal edge range: rows%d count%d complete%v next%v", len(last.Items), last.MatchedCount, last.WindowComplete, last.NextPageToken)
			}
			all := append(p.Items, last.Items...)
			if len(all) != 44 {
				t.Fatalf("edge traversal truncated: %d rows", len(all))
			}
			for i, item := range all {
				wantLabel := ""
				if i < 42 {
					wantLabel = strings.Repeat("<", 4096)
				} else if i == 42 {
					wantLabel = strings.Repeat("<", 1402)
					if extra {
						wantLabel += "a"
					}
				}
				if item.AssetID != continuity.AssetID(fmt.Sprintf("asset_%032x", i+1)) || item.Label != wantLabel || item.HeadRevisionID != nil || len(item.Reasons) != 1 || item.Reasons[0] != "declared_selection" || item.SelectedUnix != "1700001000" || item.ExpiresUnix != "1702593000" {
					t.Fatalf("literal edge row%d: %+v", i+1, item)
				}
			}
		})
	}
	for _, s := range []string{"\"\\\b\f\n\r\t<>&\u2028\u2029é", strings.Repeat("<", 4096)} {
		want, _ := json.Marshal(s)
		got := appendReadString(nil, []byte(s))
		if !bytes.Equal(got, want) {
			t.Fatalf("Go-compatible escaping: %s vs %s", got, want)
		}
	}
}

func TestRecentsClockFloorOwnGenerationAndForeignActivity(t *testing.T) {
	f := newSavedReadFixture(t)
	f.now.Store(1700000000)
	f.remember(t, f.owner, "own1", readOracleAssets[2])
	f.remember(t, f.owner, "own2", readOracleAssets[3])
	f.now.Store(1700000100)
	p := readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 1, ""})
	if p.NextPageToken == nil {
		t.Fatal("missing page")
	}
	r := RecentsReadRequest{2, "page", readOracleLibrary, 0, *p.NextPageToken}
	f.now.Store(1700000200)
	f.remember(t, f.reader, "foreign", readOracleAssets[0])
	f.now.Store(1700000201)
	_ = readRecentsCall(t, f.d, f.owner, r)
	// A fresh read above durable floor may go backwards relative to a prior read.
	f.now.Store(1700000300)
	_ = readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 100, ""})
	f.now.Store(1700000202)
	_ = readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 100, ""})
	f.now.Store(1700000199)
	var out bytes.Buffer
	result, err := f.d.QueryRecents(context.Background(), f.owner, r, &out)
	if DiscoveryErrorCode(err) != "unavailable" || result.Started || out.Len() != 0 {
		t.Fatalf("below floor: %+v %v", result, err)
	}
	f.now.Store(1700000202)
	p = readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 1, ""})
	f.remember(t, f.owner, "own3", readOracleAssets[1])
	r.Token = *p.NextPageToken
	result, err = f.d.QueryRecents(context.Background(), f.owner, r, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started || out.Len() != 0 {
		t.Fatalf("own generation: %+v %v", result, err)
	}
}

func TestRecentsWithinWindowRollbackAndSelectionExpiry(t *testing.T) {
	f := newSavedReadFixture(t)
	f.now.Store(1700000000)
	f.remember(t, f.owner, "first", readOracleAssets[0])
	f.remember(t, f.owner, "second", readOracleAssets[1])
	f.now.Store(1700000100)
	p := readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 1, ""})
	r := RecentsReadRequest{2, "page", readOracleLibrary, 0, *p.NextPageToken}
	f.now.Store(1700000099)
	var out bytes.Buffer
	result, err := f.d.QueryRecents(context.Background(), f.owner, r, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started || out.Len() != 0 {
		t.Fatalf("window rollback: %+v %v", result, err)
	}
	f.now.Store(1702591999)
	f.owner = f.issue(t, "renew-before-selection-expiry", readOracleA, readOracleActions)
	p = readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 1, ""})
	r.Token = *p.NextPageToken
	f.now.Add(1)
	result, err = f.d.QueryRecents(context.Background(), f.owner, r, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started || out.Len() != 0 {
		t.Fatalf("earliest expiry: %+v %v", result, err)
	}
	p = readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 100, ""})
	if p.MatchedCount != 0 || len(p.Items) != 0 {
		t.Fatal("expired rows were not filtered without pruning")
	}
}
