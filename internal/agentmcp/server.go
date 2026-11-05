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
	"time"
)

// The MCP stdio transport (protocol 2025-06-18): JSON-RPC 2.0 messages, one
// per line, UTF-8, on stdin and stdout; stdout carries nothing else, logs go
// to stderr. Requests are handled one at a time, in order.

const (
	// ProtocolVersion is the MCP version this server speaks.
	ProtocolVersion = "2025-06-18"
	// MaxLine bounds one incoming message (bytes, without the newline).
	MaxLine = 1 << 20
	// toolTimeout bounds one tool call, write retries included.
	toolTimeout = 2 * time.Minute
)

// supportedVersions are the versions initialize accepts. The tools use only
// what all of them have; structuredContent is sent under 2025-06-18 only.
var supportedVersions = map[string]bool{"2025-06-18": true, "2025-03-26": true, "2024-11-05": true}

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
	"follow next while you need more."

// Server is the MCP server.
type Server struct {
	Tools   *Tools
	Log     *log.Logger
	Version string // serverInfo.version

	// protocol is the negotiated version ("" until initialize).
	protocol string
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

// Serve reads messages from in until it ends (EOF: a clean shutdown) and
// writes answers to out.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	r := bufio.NewReaderSize(in, 64<<10)
	w := bufio.NewWriter(out)
	send := func(resp *rpcResponse) error {
		if resp == nil {
			return nil
		}
		resp.JSONRPC = "2.0"
		b, err := json.Marshal(resp)
		if err != nil {
			b, _ = json.Marshal(rpcResponse{JSONRPC: "2.0", ID: resp.ID, Error: &rpcError{-32603, "internal error encoding the answer"}})
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			return err
		}
		return w.Flush()
	}
	for {
		line, tooLong, rerr := readLine(r, MaxLine)
		if tooLong {
			s.Log.Printf("refused a message larger than %d bytes", MaxLine)
			if err := send(&rpcResponse{ID: nullID, Error: &rpcError{codeInvalidRequest, fmt.Sprintf("message larger than %d bytes", MaxLine)}}); err != nil {
				return err
			}
		} else if line = bytes.TrimSpace(line); len(line) > 0 {
			if err := send(s.handle(ctx, line)); err != nil {
				return err
			}
		}
		if errors.Is(rerr, io.EOF) {
			s.Log.Printf("stdin closed; shutting down")
			return nil
		}
		if rerr != nil {
			return rerr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
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

// handle answers one message; nil for a notification or a response.
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
		// A notification: never answered. notifications/initialized and
		// notifications/cancelled need nothing (requests are handled one
		// at a time, so there is nothing in flight to cancel).
		return nil
	}
	result, rerr := s.call(ctx, *msg.Method, msg.Params)
	if rerr != nil {
		return &rpcResponse{ID: id, Error: rerr}
	}
	return &rpcResponse{ID: id, Result: result}
}

func validID(id json.RawMessage) bool {
	var v any
	dec := json.NewDecoder(bytes.NewReader(id))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return false
	}
	switch v := v.(type) {
	case string:
		return true
	case json.Number:
		_, err := v.Int64()
		return err == nil
	}
	return false
}

func orNull(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return nullID
	}
	return id
}

func (s *Server) call(ctx context.Context, method string, params json.RawMessage) (any, *rpcError) {
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
	case "tools/call":
		return s.callTool(ctx, params)
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
	return v
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

func (s *Server) callTool(ctx context.Context, params json.RawMessage) (any, *rpcError) {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Name == "" {
		return nil, &rpcError{codeInvalidParams, "tools/call needs params.name"}
	}
	tool := toolsByName[p.Name]
	if tool == nil {
		return nil, &rpcError{codeInvalidParams, "unknown tool: " + trimLog(p.Name)}
	}
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	start := time.Now()
	out, err := tool.run(s.Tools, ctx, p.Arguments)
	if err != nil {
		var ae *APIError
		status := "error"
		if errors.As(err, &ae) {
			status = fmt.Sprintf("HTTP %d", ae.Status)
		}
		s.Log.Printf("tools/call %s: %s (%v)", tool.Name, status, time.Since(start).Round(time.Millisecond))
		return toolResult{Content: []textContent{{"text", errorMessage(err)}}, IsError: true}, nil
	}
	s.Log.Printf("tools/call %s: ok (%v)", tool.Name, time.Since(start).Round(time.Millisecond))
	b, err := json.Marshal(out)
	if err != nil {
		return toolResult{Content: []textContent{{"text", "the result could not be encoded"}}, IsError: true}, nil
	}
	res := toolResult{Content: []textContent{{"text", string(b)}}}
	if s.protocol == "2025-06-18" {
		res.StructuredContent = json.RawMessage(b)
	}
	return res, nil
}
