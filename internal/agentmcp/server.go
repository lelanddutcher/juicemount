package agentmcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// The MCP stdio transport (protocol 2025-06-18): JSON-RPC 2.0 messages, one
// per line, UTF-8, on stdin and stdout; stdout carries nothing else, logs go
// to stderr.
//
// Concurrency. stdin is read on its own goroutine, so ping, tools/list and
// notifications/cancelled are answered while a tool call runs. Tool calls
// run one at a time: a second tools/call while one runs gets a busy tool
// error. notifications/cancelled for the running call cancels its context;
// its answer is still sent (a write that was sent then says which
// operation_id retries it safely).

const (
	// ProtocolVersion is the MCP version this server speaks.
	ProtocolVersion = "2025-06-18"
	// MaxLine bounds one incoming message (bytes, without the newline).
	MaxLine = 1 << 20
	// toolTimeout bounds one tool call, write retries included: under the
	// 60 s a client commonly waits for an answer (the TypeScript MCP SDK's
	// default request timeout).
	toolTimeout = 55 * time.Second
)

// supportedVersions are the versions initialize echoes. 2025-03-26 is not
// among them: it requires JSON-RPC batches, which this server refuses; a
// client asking for it (or anything else) is answered with 2025-06-18.
// structuredContent is sent under 2025-06-18 only.
var supportedVersions = map[string]bool{"2025-06-18": true, "2024-11-05": true}

// JSON-RPC error codes.
const (
	codeParse          = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

const instructions = "JuiceMount: find files on the shared video volume and curate collections, as the person who " +
	"minted this server's agent token and within their project grants. Tools return metadata only. To read a file, " +
	"use its mount_path (an absolute path on this Mac) with your own file tools. Find results come a page at a time: " +
	"follow next while you need more. Tool calls run one at a time."

// Server is the MCP server.
type Server struct {
	Tools   *Tools
	Log     *log.Logger
	Version string // serverInfo.version

	// protocol is the negotiated version ("" until initialize). Only the
	// message loop touches it.
	protocol string

	mu      sync.Mutex // the writer and running
	w       *bufio.Writer
	werr    error
	running *runningCall
	wg      sync.WaitGroup
}

// runningCall is the tool call in progress.
type runningCall struct {
	id     json.RawMessage
	cancel context.CancelFunc
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

var nullID = json.RawMessage("null")

// inLine is one line read from stdin.
type inLine struct {
	line    []byte
	tooLong bool
	err     error // set on the last item: io.EOF or a read error
}

// Serve reads messages from in until it ends (EOF: a clean shutdown once
// the running tool call has ended and been answered; ctx ending cancels it
// first) and writes answers to out.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	s.w = bufio.NewWriter(out)
	lines := make(chan inLine)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		r := bufio.NewReaderSize(in, 64<<10)
		for {
			line, tooLong, err := readLine(r, MaxLine)
			select {
			case lines <- inLine{line, tooLong, err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	// finish waits for the running tool call; cancel first stops it.
	finish := func(cancel bool) {
		s.mu.Lock()
		if cancel && s.running != nil {
			s.running.cancel()
		}
		s.mu.Unlock()
		s.wg.Wait()
	}
	for {
		var l inLine
		select {
		case l = <-lines:
		case <-ctx.Done():
			finish(true)
			return ctx.Err()
		}
		if l.tooLong {
			s.Log.Printf("refused a message larger than %d bytes", MaxLine)
			s.send(&rpcResponse{ID: nullID, Error: &rpcError{codeInvalidRequest, fmt.Sprintf("message larger than %d bytes", MaxLine)}})
		} else if line := bytes.TrimSpace(l.line); len(line) > 0 {
			s.send(s.handle(ctx, line))
		}
		if l.err != nil {
			// No more requests: the running call (bounded by toolTimeout)
			// still ends and is answered.
			finish(false)
			if errors.Is(l.err, io.EOF) {
				s.Log.Printf("stdin closed; shutting down")
				return s.writeErr()
			}
			return l.err
		}
		if err := s.writeErr(); err != nil {
			finish(true)
			return err
		}
	}
}

func (s *Server) writeErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.werr
}

// send writes one answer (nil: none). Output is valid UTF-8 whatever the
// Manager sent: an answer that is not is decoded and encoded again, which
// turns each invalid byte into U+FFFD.
func (s *Server) send(resp *rpcResponse) {
	if resp == nil {
		return
	}
	resp.JSONRPC = "2.0"
	b, err := json.Marshal(resp)
	if err == nil && !utf8.Valid(b) {
		b, err = validUTF8JSON(b)
	}
	if err != nil {
		b, _ = json.Marshal(rpcResponse{JSONRPC: "2.0", ID: nullID, Error: &rpcError{-32603, "internal error encoding the answer"}})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.werr != nil {
		return
	}
	if _, err := s.w.Write(append(b, '\n')); err != nil {
		s.werr = err
		return
	}
	s.werr = s.w.Flush()
}

// validUTF8JSON re-encodes a JSON document as valid UTF-8 (numbers kept
// exactly).
func validUTF8JSON(b []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// readLine reads one line of at most max bytes (the newline excluded). A
// longer line is read to its end and dropped (tooLong). err is io.EOF at the
// end of input, with the last unterminated line, if any.
func readLine(r *bufio.Reader, max int) (line []byte, tooLong bool, err error) {
	for {
		chunk, err := r.ReadSlice('\n')
		if !tooLong {
			if n := len(line) + len(bytes.TrimRight(chunk, "\r\n")); n > max {
				tooLong, line = true, nil
			} else {
				line = append(line, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, tooLong, err
	}
}

// handle answers one message; nil for a notification, a response, or a
// tool call that answers when it ends.
func (s *Server) handle(ctx context.Context, raw []byte) *rpcResponse {
	if raw[0] == '[' {
		return &rpcResponse{ID: nullID, Error: &rpcError{codeInvalidRequest, "batches are not supported"}}
	}
	var msg struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  *string         `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		var syn *json.SyntaxError
		if errors.As(err, &syn) || raw[0] != '{' {
			return &rpcResponse{ID: nullID, Error: &rpcError{codeParse, "parse error: not a JSON object"}}
		}
		return &rpcResponse{ID: nullID, Error: &rpcError{codeInvalidRequest, "invalid request: " + err.Error()}}
	}
	id, hasID := msg.ID, len(msg.ID) > 0
	if hasID && !validID(id) {
		return &rpcResponse{ID: nullID, Error: &rpcError{codeInvalidRequest, "invalid request: id must be a string or an integer"}}
	}
	if msg.JSONRPC != "2.0" {
		if !hasID {
			return nil
		}
		return &rpcResponse{ID: id, Error: &rpcError{codeInvalidRequest, `invalid request: jsonrpc must be "2.0"`}}
	}
	if msg.Method == nil {
		if hasID && (msg.Result != nil || msg.Error != nil) {
			return nil // a response; this server sends no requests
		}
		return &rpcResponse{ID: orNull(id), Error: &rpcError{codeInvalidRequest, "invalid request: no method"}}
	}
	if !hasID {
		// A notification: never answered. Only a cancellation needs
		// anything.
		if *msg.Method == "notifications/cancelled" {
			s.cancelled(msg.Params)
		}
		return nil
	}
	if *msg.Method == "tools/call" && s.protocol != "" {
		return s.startTool(ctx, id, msg.Params)
	}
	result, rerr := s.call(*msg.Method, msg.Params)
	if rerr != nil {
		return &rpcResponse{ID: id, Error: rerr}
	}
	return &rpcResponse{ID: id, Result: result}
}

// validID: a JSON string, or an integer of any size (kept as sent, so it
// is echoed unchanged).
func validID(id json.RawMessage) bool {
	switch id[0] {
	case '"':
		var s string
		return json.Unmarshal(id, &s) == nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		var n json.Number
		return json.Unmarshal(id, &n) == nil && !bytes.ContainsAny(id, ".eE")
	}
	return false
}

func orNull(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return nullID
	}
	return id
}

// cancelled cancels the running tool call when the notification names it.
func (s *Server) cancelled(params json.RawMessage) {
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(params, &p) != nil || len(p.RequestID) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running != nil && bytes.Equal(compactJSON(s.running.id), compactJSON(p.RequestID)) {
		s.Log.Printf("the client cancelled the running tool call")
		s.running.cancel()
	}
}

func compactJSON(b []byte) []byte {
	var out bytes.Buffer
	if json.Compact(&out, b) != nil {
		return b
	}
	return out.Bytes()
}

func (s *Server) call(method string, params json.RawMessage) (any, *rpcError) {
	switch method {
	case "initialize":
		return s.initialize(params)
	case "ping":
		return struct{}{}, nil
	}
	if s.protocol == "" {
		return nil, &rpcError{codeInvalidRequest, "the session is not initialized: send initialize first"}
	}
	switch method {
	case "tools/list":
		return map[string]any{"tools": toolList}, nil
	}
	return nil, &rpcError{codeMethodNotFound, "method not found: " + method}
}

func (s *Server) initialize(params json.RawMessage) (any, *rpcError) {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.ProtocolVersion == "" {
		return nil, &rpcError{codeInvalidParams, "initialize needs params.protocolVersion"}
	}
	// A version this server supports is answered as is; any other gets the
	// version it speaks, and the client decides whether to go on.
	s.protocol = ProtocolVersion
	if supportedVersions[p.ProtocolVersion] {
		s.protocol = p.ProtocolVersion
	}
	s.Log.Printf("initialize: client %q %q asked for protocol %q; using %q", trimLog(p.ClientInfo.Name), trimLog(p.ClientInfo.Version), trimLog(p.ProtocolVersion), s.protocol)
	return map[string]any{
		"protocolVersion": s.protocol,
		"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
		"serverInfo":      map[string]any{"name": "juicemount-mcp", "title": "JuiceMount", "version": s.Version},
		"instructions":    instructions,
	}, nil
}

// trimLog bounds a client-supplied string written to the log.
func trimLog(v string) string {
	if len(v) > 64 {
		v = v[:64]
	}
	return strings.ToValidUTF8(v, "?")
}

// toolResult is a tools/call result.
type toolResult struct {
	Content           []textContent `json:"content"`
	StructuredContent any           `json:"structuredContent,omitempty"`
	IsError           bool          `json:"isError"`
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func errorResult(msg string) toolResult {
	return toolResult{Content: []textContent{{"text", msg}}, IsError: true}
}

// startTool checks a tools/call and runs it on its own goroutine, which
// answers when it ends; nil then. A call while another runs is a busy tool
// error.
func (s *Server) startTool(ctx context.Context, id, params json.RawMessage) *rpcResponse {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Name == "" {
		return &rpcResponse{ID: id, Error: &rpcError{codeInvalidParams, "tools/call needs params.name"}}
	}
	tool := toolsByName[p.Name]
	if tool == nil {
		return &rpcResponse{ID: id, Error: &rpcError{codeInvalidParams, "unknown tool: " + trimLog(p.Name)}}
	}
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	s.mu.Lock()
	if s.running != nil {
		s.mu.Unlock()
		cancel()
		return &rpcResponse{ID: id, Result: errorResult("Another tool call of this session is still running; this server runs one at a time. Wait for it, then call again.")}
	}
	s.running = &runningCall{id: id, cancel: cancel}
	s.wg.Add(1)
	s.mu.Unlock()
	protocol := s.protocol
	go func() {
		defer s.wg.Done()
		res := s.runTool(ctx, tool, p.Arguments, protocol)
		cancel()
		s.mu.Lock()
		s.running = nil
		s.mu.Unlock()
		s.send(&rpcResponse{ID: id, Result: res})
	}()
	return nil
}

func (s *Server) runTool(ctx context.Context, tool *Tool, args json.RawMessage, protocol string) toolResult {
	start := time.Now()
	out, err := tool.run(s.Tools, ctx, args)
	if err != nil {
		var ae *APIError
		status := "error"
		if errors.As(err, &ae) {
			status = fmt.Sprintf("HTTP %d", ae.Status)
		}
		s.Log.Printf("tools/call %s: %s (%v)", tool.Name, status, time.Since(start).Round(time.Millisecond))
		return errorResult(errorMessage(err))
	}
	s.Log.Printf("tools/call %s: ok (%v)", tool.Name, time.Since(start).Round(time.Millisecond))
	b, err := json.Marshal(out)
	if err == nil && !utf8.Valid(b) {
		// The Manager's answer passes through as raw JSON in places; the
		// text and the structured copy must both be valid UTF-8.
		b, err = validUTF8JSON(b)
	}
	if err != nil {
		return errorResult("the result could not be encoded")
	}
	res := toolResult{Content: []textContent{{"text", string(b)}}}
	if protocol == "2025-06-18" {
		res.StructuredContent = json.RawMessage(b)
	}
	return res
}
