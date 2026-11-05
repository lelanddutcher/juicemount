package manager

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// A revoke that lands while an agent's edit is in flight (after its
// authentication and grant check, before the write) stops the edit: 401,
// nothing changes, and the audit log holds no intent for it.
func TestAgentRevokeStopsInFlightEdit(t *testing.T) {
	e := newAgentEnv(t)
	m := e.mint("Ann", map[string]any{"name": "agent"})
	col := string(e.write("Ann", "/api/collections", map[string]any{"name": "Picks"}, 200).Collection.ID)
	revoked := false
	collectionBeforeApply = func() {
		if _, err := e.people.revokeAgentToken(m.AgentToken.ID, ""); err != nil {
			t.Errorf("revoke: %v", err)
		}
		revoked = true
	}
	defer func() { collectionBeforeApply = nil }()
	rec := e.do(m.Token, http.MethodPost, "/api/collections/"+col+"/members/add", map[string]any{"members": pathRefs("Clients/A/one.mov")})
	collectionBeforeApply = nil
	if !revoked {
		t.Fatal("hook did not run")
	}
	if rec.Code != 401 {
		t.Fatalf("edit after its token was revoked: %d %s", rec.Code, rec.Body)
	}
	if n := len(e.members(catalog.CollectionID(col), "").Members); n != 0 {
		t.Fatalf("members after the refused edit: %d", n)
	}
	for _, r := range e.auditRecords() {
		if r.Type == auditlog.TypeIntent && r.Action == "collection-add-members" {
			t.Fatalf("intent recorded for a refused edit: %+v", r)
		}
	}
}

// asset/{id} shows a path hint (and a first name) only inside the
// caller's grants: a file moved into Ann's grants from outside, with no
// per-file event, keeps a hint naming its old folder.
func TestAgentAssetHidesStaleHint(t *testing.T) {
	e := newAgentEnv(t)
	e.vol.set(func(f map[string]uint64) { f["Secret/Q3-layoffs-list.mov"] = 900 })
	e.offer(binder.Event{Op: "create", Path: "Secret/Q3-layoffs-list.mov", Inode: 900})
	e.vol.set(func(f map[string]uint64) {
		delete(f, "Secret/Q3-layoffs-list.mov")
		f["Clients/A/inbox/renamed.mov"] = 900
	})
	tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
	id := string(binder.AssetID(e.vol.VolumeUUID(), 900))
	rec := e.do(tok, http.MethodGet, "/api/continuity/asset/"+id, nil)
	var v FileView
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &v) != nil {
		t.Fatalf("asset: %d %s", rec.Code, rec.Body)
	}
	if v.Path != "/Clients/A/inbox/renamed.mov" || v.PathHint != "" || v.Label != "" || strings.Contains(rec.Body.String(), "Secret") || strings.Contains(rec.Body.String(), "layoffs") {
		t.Fatalf("Ann's agent sees outside her grants: %s", rec.Body)
	}
	// The admin sees all of it.
	if rec := e.do("admin", http.MethodGet, "/api/continuity/asset/"+id, nil); !strings.Contains(rec.Body.String(), "Secret/Q3-layoffs-list.mov") {
		t.Fatalf("admin: %s", rec.Body)
	}
	// A file outside the grants whose path cannot be read is 404, not 503:
	// the answer does not say it exists.
	three := string(binder.AssetID(e.vol.VolumeUUID(), 44))
	e.vol.setFail(true)
	defer e.vol.setFail(false)
	if rec := e.do(tok, http.MethodGet, "/api/continuity/asset/"+three, nil); rec.Code != 404 {
		t.Fatalf("unreadable file outside the grants: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do(tok, http.MethodGet, "/api/continuity/asset/"+string(binder.AssetID(e.vol.VolumeUUID(), 42)), nil); rec.Code != 503 {
		t.Fatalf("unreadable file inside the grants: %d %s", rec.Code, rec.Body)
	}
}

type agentMembersReply struct {
	Collection CollectionView `json:"collection"`
	Members    []MemberView   `json:"members"`
	Next       string         `json:"next"`
	Withheld   int            `json:"withheld"`
}

// An agent reads collection members and saved-query results only inside
// its person's grants; what it may not see is withheld, counted without
// names. Macs and the admin read as before.
func TestAgentReadsWithinGrants(t *testing.T) {
	e := newAgentEnv(t)
	col := string(e.write("admin", "/api/collections", map[string]any{"name": "Mixed", "members": pathRefs("Clients/A/one.mov", "Clients/B/three.mov", "Clients/A/two.mov")}, 200).Collection.ID)
	qry := string(e.queryWrite("admin", "/api/queries", map[string]any{"name": "All", "folders": []string{"Clients"}}, 200).Query.ID)
	ann := e.mint("Ann", map[string]any{"name": "agent"}).Token
	nia := e.mint("Nia", map[string]any{"name": "agent"}).Token
	members := func(who, query string) agentMembersReply {
		t.Helper()
		rec := e.do(who, http.MethodGet, "/api/collections/"+col+"/members"+query, nil)
		var out agentMembersReply
		if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
			t.Fatalf("%.8s members: %d %s", who, rec.Code, rec.Body)
		}
		return out
	}
	for _, c := range []struct {
		who      string
		visible  int
		withheld int
	}{{ann, 2, 1}, {nia, 0, 3}, {"Ann", 3, 0}, {"Nia", 3, 0}, {"admin", 3, 0}} {
		r := members(c.who, "")
		if len(r.Members) != c.visible || r.Withheld != c.withheld {
			t.Errorf("%.8s: %d members, %d withheld: %+v", c.who, len(r.Members), r.Withheld, r)
		}
		for _, m := range r.Members {
			if strings.HasPrefix(c.who, "jma_") && !strings.HasPrefix(m.Path, "Clients/A/") {
				t.Errorf("%.8s sees %s", c.who, m.Path)
			}
		}
	}
	// Paging an agent: a page of one, never a hidden member's name.
	var seen []string
	withheld := 0
	for q := "?limit=1"; ; {
		r := members(ann, q)
		withheld += r.Withheld
		for _, m := range r.Members {
			seen = append(seen, m.Path)
		}
		if r.Next == "" {
			break
		}
		q = "?limit=1&after=" + r.Next
	}
	if len(seen) != 2 || withheld != 1 || !allUnder(seen, "Clients/A/") {
		t.Fatalf("paged: %v withheld %d", seen, withheld)
	}
	results := func(who string) (paths []string, withheld int) {
		t.Helper()
		rec := e.do(who, http.MethodGet, "/api/queries/"+qry+"/results", nil)
		var out struct {
			Results  []QueryResultView `json:"results"`
			Withheld int               `json:"withheld"`
		}
		if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
			t.Fatalf("%.8s results: %d %s", who, rec.Code, rec.Body)
		}
		for _, r := range out.Results {
			paths = append(paths, r.Path)
		}
		return paths, out.Withheld
	}
	if p, w := results(ann); len(p) != 3 || w != 1 || !allUnder(p, "Clients/A/") {
		t.Errorf("Ann's agent results: %v withheld %d", p, w)
	}
	if p, w := results(nia); len(p) != 0 || w != 4 {
		t.Errorf("Nia's agent results: %v withheld %d", p, w)
	}
	if p, w := results("Ann"); len(p) != 4 || w != 0 {
		t.Errorf("Ann's Mac results: %v withheld %d", p, w)
	}
	// find?collection= stays filtered too.
	for _, f := range e.find(ann, url.Values{"collection": {col}}).Files {
		if strings.Contains(f.Path, "Clients/B") {
			t.Errorf("find leaked %s", f.Path)
		}
	}
	// A refusal never names a file the agent may not see.
	three := string(binder.AssetID(e.vol.VolumeUUID(), 44))
	mine := string(e.write(ann, "/api/collections", map[string]any{"name": "Mine"}, 200).Collection.ID)
	rec := e.do(ann, http.MethodPost, "/api/collections/"+mine+"/members/add", map[string]any{"members": assetRefs(binder.AssetID(e.vol.VolumeUUID(), 44))})
	if rec.Code != 403 || strings.Contains(rec.Body.String(), "three") || strings.Contains(rec.Body.String(), "Clients/B") {
		t.Fatalf("refusal by ID %s: %d %s", three, rec.Code, rec.Body)
	}
}

// Agents have their own small slot pool: two people's agents busy finding
// files never hold up a Mac's collection work.
func TestAgentsDoNotStarveMacs(t *testing.T) {
	e := newDiscoveryEnv(t)
	saved := collectionBusyWait
	collectionBusyWait = 50 * time.Millisecond
	defer func() { collectionBusyWait = saved }()
	release := make(chan struct{})
	var inside atomic.Int32
	findBeforeStat = func() {
		inside.Add(1)
		<-release
	}
	defer func() { findBeforeStat = nil }()
	ann := e.mint("Ann", map[string]any{"name": "agent"}).Token
	bob := e.mint("Bob", map[string]any{"name": "agent"}).Token
	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i, tok := range []string{ann, bob} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = e.do(tok, http.MethodGet, "/api/continuity/find", nil).Code
		}()
		deadline := time.Now().Add(5 * time.Second)
		for int(inside.Load()) < 1 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	}
	time.Sleep(200 * time.Millisecond) // the second agent: waiting, refused or inside
	rec := e.do("Al", http.MethodPost, "/api/collections", map[string]any{"name": "Mac work", "members": pathRefs("Clients/A/one.mov")})
	close(release)
	wg.Wait()
	if rec.Code != 200 {
		t.Fatalf("Mac blocked by agents: %d %s", rec.Code, rec.Body)
	}
	if !(codes[0] == 200 && codes[1] == 503 || codes[0] == 503 && codes[1] == 200) {
		t.Fatalf("agents: %v (one finds, one is told the pool is busy)", codes)
	}
}

// A refused token that the store still knows (revoked or expired) is in
// the catalog audit log, at most once a minute per token, never the token.
func TestAgentRefusedTokenAudited(t *testing.T) {
	e := newAgentEnv(t)
	m := e.mint("Ann", map[string]any{"name": "agent"})
	e.do("Ann", http.MethodDelete, "/api/agent-tokens/"+m.AgentToken.ID, nil)
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	for i := 0; i < 3; i++ {
		if rec := e.do(m.Token, http.MethodGet, "/api/collections", nil); rec.Code != 401 {
			t.Fatalf("revoked: %d", rec.Code)
		}
	}
	n := 0
	for _, r := range e.auditRecords() {
		if r.Type == auditlog.TypeRefused && r.Agent == m.AgentToken.ID && r.Action == "agent-token-refused" {
			n++
			if r.Person != e.ids["Ann"] || !strings.Contains(r.Detail, "revoked") {
				t.Errorf("refusal record: %+v", r)
			}
		}
	}
	if n != 1 {
		t.Fatalf("refusal records: %d, want 1", n)
	}
	if strings.Contains(buf.String(), m.Token[len("jma_")+17:]) {
		t.Fatal("the log holds the token")
	}
	// Still revoked after a store restart; never listed, never counted.
	again, err := NewPrincipalStore(e.people.opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := again.agentPerson(m.Token); err == nil {
		t.Fatal("revoked token works after a reload")
	}
	if got := e.tokenList("Ann"); len(got) != 0 {
		t.Fatalf("revoked token listed: %+v", got)
	}
}
