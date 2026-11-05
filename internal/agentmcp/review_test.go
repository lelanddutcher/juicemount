package agentmcp

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

// slowManager answers each request only when release is closed (or the
// request is abandoned), and counts requests.
func slowManager(t *testing.T) (srv *httptest.Server, hits *atomic.Int32, arrived chan struct{}, release chan struct{}) {
	hits, arrived, release = &atomic.Int32{}, make(chan struct{}, 16), make(chan struct{})
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.Copy(io.Discard, r.Body)
		arrived <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"collection":{"id":"`+col1+`"},"operation":{"id":"x"},"collections":[]}`)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	return srv, hits, arrived, release
}

var opIDInText = regexp.MustCompile(`operation_id "(mcp-[0-9a-f]{24})"`)

var opIDInLog = regexp.MustCompile(`operation_id mcp-[0-9a-f]{24} retries it safely`)

func opIDLogged(logs string) bool { return opIDInLog.MatchString(logs) }

// A write whose answers never come before the deadline still tells the agent
// the operation_id it was sent with, whether each attempt timed out on its
// own or the whole call ran out of time.
func TestWriteDeadlineKeepsOperationID(t *testing.T) {
	for _, c := range []struct {
		name          string
		clientTimeout time.Duration // stands in for an attempt's timeout
		callTimeout   time.Duration // stands in for toolTimeout
		wantHits      int32
	}{
		{"attempts time out", 150 * time.Millisecond, 5 * time.Second, writeAttempts},
		{"call times out", 0, 300 * time.Millisecond, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv, hits, _, _ := slowManager(t)
			hc := NewHTTPClient()
			hc.Timeout = c.clientTimeout
			tools := &Tools{Mount: testMount, Log: log.New(io.Discard, "", 0), Sleep: func(time.Duration) {},
				Client: func() (*Client, error) { return NewAgentClient(srv.URL, testToken, hc), nil }}
			ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
			defer cancel()
			_, err := tools.createCollection(ctx, json.RawMessage(`{"name":"Selects"}`))
			msg := errorMessage(err)
			m := opIDInText.FindStringSubmatch(msg)
			if m == nil || !strings.Contains(msg, "may or may not have been made") || hits.Load() != c.wantHits {
				t.Fatalf("after %d attempts the agent sees %q", hits.Load(), msg)
			}
		})
	}
	// Two attempts and the pause fit in a tool call, which fits in the 60 s
	// a client commonly waits.
	if writeAttempts*writeAttemptTimeout+time.Second > toolTimeout || toolTimeout >= 60*time.Second {
		t.Fatalf("budget: %d x %v + 1 s vs %v", writeAttempts, writeAttemptTimeout, toolTimeout)
	}
}

// Invalid UTF-8 from the Manager never reaches stdout.
func TestInvalidUTF8IsReplaced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "{\"collections\":[{\"id\":\"col_x\",\"name\":\"bad \xff\xfe name\"}]}")
	}))
	defer srv.Close()
	s := startSession(t, srv.URL)
	s.initialize()
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": map[string]any{"name": "list_collections", "arguments": map[string]any{}}})
	s.sendRaw(string(b))
	line := <-s.lines
	if !utf8.ValidString(line) {
		t.Fatalf("the server wrote invalid UTF-8: %q", line)
	}
	var r struct {
		Result struct {
			Content           []struct{ Text string } `json:"content"`
			StructuredContent struct {
				Collections []struct{ Name string } `json:"collections"`
			} `json:"structuredContent"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &r); err != nil || r.Result.StructuredContent.Collections[0].Name != "bad \ufffd\ufffd name" ||
		!strings.Contains(r.Result.Content[0].Text, "bad \ufffd\ufffd name") {
		t.Fatalf("answer: %s", line)
	}
}

// A redirect is not followed, so the bearer goes nowhere else.
func TestRedirectIsNotFollowed(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/x", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	err := NewAgentClient(srv.URL, testToken, NewHTTPClient()).Do(context.Background(), "GET", "/api/collections", nil, nil, nil)
	if leaked.Load() || err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("redirect: %v (leaked %v)", err, leaked.Load())
	}
}

// Request ids round-trip exactly, large integers included.
func TestRequestIDsRoundTrip(t *testing.T) {
	_, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	for _, id := range []string{`9223372036854775808`, `123456789012345678901234567890`, `-1`, `0`, `"x"`, `""`, `"é\"q"`} {
		s.sendRaw(`{"jsonrpc":"2.0","id":` + id + `,"method":"ping"}`)
		if r := s.recv(); r.Error != nil || string(r.ID) != id {
			t.Errorf("id %s came back as %s (%+v)", id, r.ID, r.Error)
		}
	}
}

// While a tool call runs, ping is answered, a second tool call is told the
// server is busy, and cancelling the running write still tells the agent its
// operation_id.
func TestCancelAndBusyWhileToolRuns(t *testing.T) {
	srv, hits, arrived, _ := slowManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	s.sendRaw(`{"jsonrpc":"2.0","id":"w1","method":"tools/call","params":{"name":"create_collection","arguments":{"name":"Selects"}}}`)
	<-arrived
	s.sendRaw(`{"jsonrpc":"2.0","id":"p","method":"ping"}`)
	if r := s.recv(); string(r.ID) != `"p"` || r.Error != nil {
		t.Fatalf("ping while a tool runs: %+v", r)
	}
	s.sendRaw(`{"jsonrpc":"2.0","id":"w2","method":"tools/call","params":{"name":"list_collections","arguments":{}}}`)
	r := s.recv()
	var busy callResult
	if string(r.ID) != `"w2"` || json.Unmarshal(r.Result, &busy) != nil || !busy.IsError || !strings.Contains(busy.Content[0].Text, "still running") {
		t.Fatalf("second call: %s %s", r.ID, r.Result)
	}
	// A cancellation naming another request changes nothing.
	s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"w2"}}`)
	s.sendRaw(`{"jsonrpc":"2.0","id":"p2","method":"ping"}`)
	if r := s.recv(); string(r.ID) != `"p2"` {
		t.Fatalf("expected the ping's answer first: %+v", r)
	}
	s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"w1","reason":"user"}}`)
	// The cancelled call gets no answer; its operation_id goes to the log.
	deadline := time.Now().Add(5 * time.Second)
	for !opIDLogged(s.logs.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("no operation_id logged: %s", s.logs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(s.logs.String(), "cancelled by the client, not answered") {
		t.Fatalf("log: %s", s.logs)
	}
	if hits.Load() != 1 {
		t.Fatalf("a cancelled write was sent again: %d requests", hits.Load())
	}
	// The next message read is the ping's answer: nothing for w1.
	s.sendRaw(`{"jsonrpc":"2.0","id":"p3","method":"ping"}`)
	if r := s.recv(); string(r.ID) != `"p3"` {
		t.Fatalf("after cancel: %+v", r)
	}
	select {
	case l := <-s.lines:
		t.Fatalf("extra line: %s", l)
	case <-time.After(100 * time.Millisecond):
	}
	if !strings.Contains(s.logs.String(), "cancelled the running tool call") {
		t.Fatalf("log: %s", s.logs)
	}
}

// EOF while a tool call runs lets it end and be answered, then shuts down;
// ending the context instead cancels it (and a write still names its
// operation_id).
func TestShutdownDuringToolCall(t *testing.T) {
	srv, _, arrived, release := slowManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	call := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"add_to_collection","arguments":{"collection_id":"` + col1 + `","asset_ids":["` + asset1 + `"]}}}`
	s.sendRaw(call)
	<-arrived
	s.in.Close()
	select {
	case <-s.done:
		t.Fatal("Serve returned with a call running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	r := s.recv()
	var res callResult
	if string(r.ID) != "7" || json.Unmarshal(r.Result, &res) != nil || res.IsError {
		t.Fatalf("answer: %s", r.Result)
	}
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}

	// A signal (the context ends): the running write is cancelled.
	srv2, _, arrived2, _ := slowManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := &syncBuffer{}
	inR, inW := io.Pipe()
	defer inW.Close()
	out := &syncBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- (&Server{Tools: testTools(srv2.URL, log.New(logs, "", 0)), Log: log.New(logs, "", 0)}).Serve(ctx, inR, out)
	}()
	io.WriteString(inW, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`+"\n"+call+"\n")
	<-arrived2
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return on cancel")
	}
	if opIDInText.FindString(strings.ReplaceAll(out.String(), `\"`, `"`)) == "" {
		t.Fatalf("output: %s", out)
	}
	if !opIDLogged(logs.String()) {
		t.Fatalf("stderr: %s", logs)
	}
}

// Cancellations that name no running call change nothing: one for an
// unknown id, a malformed one, or one after the call was answered.
func TestCancelEdgeCases(t *testing.T) {
	f, srv := newFakeManager(t)
	f.on("GET /api/collections", 200, map[string]any{"collections": []any{}})
	s := startSession(t, srv.URL)
	s.initialize()
	for _, l := range []string{
		`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"nope"}}`,
		`{"jsonrpc":"2.0","method":"notifications/cancelled","params":"x"}`,
		`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":{"a":1}}}`,
		`{"jsonrpc":"2.0","method":"notifications/cancelled"}`,
	} {
		s.sendRaw(l)
	}
	s.sendRaw(`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_collections","arguments":{}}}`)
	if r := s.recv(); string(r.ID) != "5" || r.Error != nil {
		t.Fatalf("%+v", r)
	}
	s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":5}}`)
	s.sendRaw(`{"jsonrpc":"2.0","id":6,"method":"ping"}`)
	if r := s.recv(); string(r.ID) != "6" {
		t.Fatalf("after a late cancel: %+v", r)
	}
	select {
	case l := <-s.lines:
		t.Fatalf("extra line: %s", l)
	case <-time.After(200 * time.Millisecond):
	}
}
