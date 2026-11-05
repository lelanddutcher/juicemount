// Package agentmcp is juicemount-mcp (continuity slice 6b): the stdio MCP
// server through which an agent on a Mac (Claude Desktop, Claude Code and
// others) finds files and edits collections through the Manager, and the
// commands that mint, list, revoke and import the agent token it uses.
//
// Identity. An agent token belongs to a Teams person and acts as that person
// (internal/manager agenttokens.go): it may find files and edit collections
// within the person's grants, never read file contents. A person mints it
// from their own enrolled Mac, which authenticates with the enrollment poll
// token, instance ID and device digest it already holds (as the app does
// for collections); the token is then sent as "Authorization: Bearer".
//
// Bytes. The server never streams file contents and never opens or stats a
// file under the mount: each result carries mount_path, the file's absolute
// path on this Mac (the mount point joined with the Manager's volume path),
// and the agent reads it with its own file tools.
//
// Transport. The Manager is reached as the app's enrollment reaches it over
// the operating system's network (internal/enrollclient, bridge
// managerHTTPClient): Go's default TLS verification against the system
// roots, nothing pinned or skipped, and no redirect followed (a redirect
// would replay the credential to whatever host it names).
package agentmcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const (
	// maxResponse bounds a Manager answer read (a find page of 200 files,
	// each with up to 50 collections, is well under 1 MiB).
	maxResponse = 8 << 20
	// requestTimeout bounds one Manager request: the Manager's own 30 s
	// deadline plus its 5 s wait for a busy slot, and margin.
	requestTimeout = 45 * time.Second
	// maxErrorText bounds the Manager's error text quoted to the agent.
	maxErrorText = 400
)

// NewHTTPClient is the Manager client: the default transport's TLS (system
// roots, full verification), bounded timeouts, and no redirects.
func NewHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSHandshakeTimeout = 10 * time.Second
	t.ResponseHeaderTimeout = requestTimeout
	t.MaxResponseHeaderBytes = 64 << 10
	return &http.Client{
		Timeout:       requestTimeout,
		Transport:     t,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// MacCredentials are what an enrolled Mac authenticates to the Manager with
// (internal/manager collections.go macPerson).
type MacCredentials struct {
	ManagerURL string
	InstanceID string
	PollToken  string
	// Device is the internal/deviceid digest ("" when unavailable).
	Device string
}

// Client sends requests to one Manager with one credential.
type Client struct {
	base string
	hc   *http.Client
	auth func(http.Header)
}

// NewAgentClient authenticates with an agent token.
func NewAgentClient(base, token string, hc *http.Client) *Client {
	return &Client{base: strings.TrimRight(base, "/"), hc: hc, auth: func(h http.Header) {
		h.Set("Authorization", "Bearer "+token)
	}}
}

// NewMacClient authenticates as this Mac (minting, listing and revoking
// tokens: no agent may).
func NewMacClient(m MacCredentials, hc *http.Client) *Client {
	return &Client{base: strings.TrimRight(m.ManagerURL, "/"), hc: hc, auth: func(h http.Header) {
		h.Set("X-JuiceMount-Mac-Instance", m.InstanceID)
		h.Set("X-JuiceMount-Mac-Token", m.PollToken)
		if m.Device != "" {
			h.Set("X-JuiceMount-Mac-Device", m.Device)
		}
	}}
}

// APIError is a Manager answer other than 200.
type APIError struct {
	Status int
	// Message is the Manager's error text (bounded, control characters
	// removed), or the status text.
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message) }

// errNoAnswer marks a request that may have reached the Manager but got no
// answer (a write's outcome is then unknown).
var errNoAnswer = errors.New("no answer from the Manager")

// Do sends one request: path is below the Manager base (it starts with
// "/api/"), body is JSON-encoded when not nil, and a 200 answer is decoded
// into out when not nil.
func (c *Client) Do(ctx context.Context, method, path string, q url.Values, body, out any) error {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return fmt.Errorf("the Manager address cannot be used: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	c.auth(req.Header)
	resp, err := c.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The error names the URL, never a header.
		return fmt.Errorf("%w (%v)", errNoAnswer, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return fmt.Errorf("%w (the answer was cut off: %v)", errNoAnswer, err)
	}
	if len(raw) > maxResponse {
		return fmt.Errorf("the Manager's answer is larger than %d bytes", maxResponse)
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return &APIError{Status: resp.StatusCode, Message: "the Manager answered with a redirect, which is not followed; check the Manager address"}
	}
	if resp.StatusCode != http.StatusOK {
		return &APIError{Status: resp.StatusCode, Message: errorText(resp.StatusCode, raw)}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("the Manager's answer could not be read: %v", err)
	}
	return nil
}

// errorText is the Manager's {"error": ...} text, bounded and without
// control characters, or the status text.
func errorText(status int, raw []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	msg := ""
	if json.Unmarshal(raw, &e) == nil {
		msg = e.Error
	}
	msg = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, strings.TrimSpace(msg))
	if len(msg) > maxErrorText {
		msg = strings.ToValidUTF8(msg[:maxErrorText], "") + "..."
	}
	if msg == "" {
		msg = http.StatusText(status)
	}
	return msg
}
