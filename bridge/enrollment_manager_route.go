package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
)

// The enrollment request to the Manager in the desktop archive (review 2
// finding F3, the remaining blocker of 752903b).
//
// The desktop archive's app process holds no Link node. It sends each Manager
// request to the serving process ("enroll-http"). In selected-owner mode the
// serving process forwards it to the owner holding Link; legacy local-Link
// mode uses its own node. Without Link the service uses its normal network.
//
// The caller does not choose the target (the shape of review 3 finding 4 for
// "enroll-verify"). The serving process sends only to the Manager that
// belongs to the Redis endpoint it was started with: that host, the
// Manager's default port, the enroll route, GET or POST. A Manager URL typed
// in Settings with another host or port is refused before any dial, and the
// app then sends it over macOS networking itself, as before.
//
// Bodies are bounded both ways, no redirect is followed, only the two
// response headers enrollclient reads are carried, and neither side logs a
// body (a POST body carries the enrollment secret).

const (
	enrollHTTPMethod = "enroll-http"
	// enrollHTTPPath is the only path "enroll-http" sends to.
	enrollHTTPPath = "/api/principals/enroll"
	// enrollHTTPManagerPort is the Manager's default published port
	// (server/docker-compose.yml; the app's Preferences.defaultManagerPort).
	enrollHTTPManagerPort = "30190"
	// enrollHTTPMaxRequest bounds a request body (an enroll request is a
	// few hundred bytes).
	enrollHTTPMaxRequest = 16 << 10
	// enrollHTTPMaxResponse is what enrollclient reads of an answer.
	enrollHTTPMaxResponse = 64 << 10
	// enrollHTTPTimeout bounds the serving process's request (a cold Link).
	enrollHTTPTimeout = 12 * time.Second
	// enrollHTTPDeadline is the app's wait for the answer: the request's
	// own bound plus margin.
	enrollHTTPDeadline = 15 * time.Second
)

// enrollHTTPAnswer is the "enroll-http" answer. Body is the Manager's answer
// (it carries a poll token or, once approved, the object key: the app's own
// enrollment result, the same data the app would read directly).
type enrollHTTPAnswer struct {
	// Route: "link", "system" (sent), or "refused" (nothing was sent).
	Route string `json:"route"`
	// Refused says why nothing was sent (fixed text).
	Refused string `json:"refused,omitempty"`
	// Error says the request may have been sent but no answer was read
	// (fixed text, never the URL's secret-bearing body).
	Error      string `json:"error,omitempty"`
	Status     int    `json:"status,omitempty"`
	RetryAfter string `json:"retry_after,omitempty"`
	ManagerID  string `json:"manager_id,omitempty"`
	Body       []byte `json:"body,omitempty"`
}

// managerHTTPClient is a Manager client over dial (nil = the operating
// system's dialer). It never follows a redirect: following a 307/308 replays
// the POST body, so the enrollment secret would go to whatever host the
// Location names. A 3xx is returned as is and enrollclient.Step reports it.
func managerHTTPClient(dial contextDial) *http.Client {
	c := &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	if dial != nil {
		c.Transport = &http.Transport{Proxy: nil, DialContext: dial, ForceAttemptHTTP2: false,
			TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second}
	}
	return c
}

// canonicalHost lower-cases a host, drops a trailing dot and brackets, and
// writes an IP literal in canonical form.
func canonicalHost(h string) string {
	h = strings.TrimSuffix(strings.ToLower(strings.Trim(h, "[]")), ".")
	if ip := net.ParseIP(h); ip != nil {
		return ip.String()
	}
	return h
}

// enrollHTTPTargetAllowed reports why rawURL is not the Manager of the Redis
// endpoint servingRedis ("" = allowed): http or https, no user, query or
// fragment, the Redis host, port enrollHTTPManagerPort, path enrollHTTPPath.
func enrollHTTPTargetAllowed(rawURL, servingRedis string) string {
	const notManager = "the Manager address is not the Redis host on port " + enrollHTTPManagerPort +
		", so the JuiceMount service does not carry it"
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return notManager
	}
	if u.EscapedPath() != enrollHTTPPath {
		return notManager
	}
	opts, err := redisurl.Parse(servingRedis)
	if err != nil {
		return "the JuiceMount service's Redis address could not be read"
	}
	redisHost, _, err := net.SplitHostPort(opts.Addr)
	if err != nil || redisHost == "" {
		return "the JuiceMount service's Redis address could not be read"
	}
	if u.Port() != enrollHTTPManagerPort || canonicalHost(u.Hostname()) != canonicalHost(redisHost) {
		return notManager
	}
	return ""
}

// enrollHTTPInServingProcess is the serving process's side of "enroll-http":
// one request to the Manager of its configured Redis endpoint over the
// selected owner's Link, local Link, or ordinary network route.
func enrollHTTPInServingProcess(method, rawURL, body string) string {
	answer := func(a enrollHTTPAnswer) string {
		b, _ := json.Marshal(a)
		return string(b)
	}
	refuse := func(why string) string { return answer(enrollHTTPAnswer{Route: "refused", Refused: why}) }
	switch {
	case method != http.MethodGet && method != http.MethodPost:
		return refuse("only GET and POST are sent to the Manager")
	case method == http.MethodGet && body != "":
		return refuse("a GET to the Manager carries no body")
	case len(body) > enrollHTTPMaxRequest:
		return refuse("the request to the Manager is too large")
	}
	configured := servingRedisURL.Load()
	configuredURL := ""
	if configured != nil {
		configuredURL = strings.TrimSpace(*configured)
	}
	globalMu.Lock()
	owner := globalOwner
	globalMu.Unlock()
	if owner != nil {
		// Owner mode leaves no Link node in this process. The selected owner
		// must carry the request itself; an uncertain claim cannot authorize
		// an ordinary network send to a NAS reachable only through Link.
		owner.mu.Lock()
		claimed, logical := owner.claimed, owner.logicalRedisURL
		owner.mu.Unlock()
		if claimed.Instance == "" || logical == "" ||
			configuredURL != "" && !sameRedisEndpoint(logical, configuredURL) {
			return answer(enrollHTTPAnswer{Route: "link", Error: "the selected owner Manager route is unavailable"})
		}
		if claimed.LinkEnabled {
			// A claim can finish before the serving engine publishes its
			// Redis URL. The owner's selected logical endpoint is already
			// fixed; use it without creating an OS-network fallback window.
			if why := enrollHTTPTargetAllowed(rawURL, logical); why != "" {
				return refuse(why)
			}
			return answer(owner.enrollHTTP(method, rawURL, body))
		}
	}
	if configuredURL == "" {
		return refuse("the JuiceMount service has not been started with a Redis address yet")
	}
	if why := enrollHTTPTargetAllowed(rawURL, configuredURL); why != "" {
		return refuse(why)
	}
	route, dial := "system", inProcessLinkDial()
	if dial != nil {
		route = "link"
	}
	return answer(sendEnrollHTTP(method, rawURL, body, route, dial))
}

// sendEnrollHTTP is called only after the serving process or selected owner
// has checked the fixed Manager target. It sends at most one request.
func sendEnrollHTTP(method, rawURL, body, route string, dial contextDial) enrollHTTPAnswer {
	ctx, cancel := context.WithTimeout(context.Background(), enrollHTTPTimeout)
	defer cancel()
	var rd io.Reader
	if method == http.MethodPost {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rd)
	if err != nil {
		return enrollHTTPAnswer{Route: route, Error: "the Manager address could not be read"}
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	// The host only: the body carries the enrollment secret.
	jmlog.Info("enrollment: sending the Manager request for the app", "route", route, "method", method, "host", req.URL.Host)
	resp, err := managerHTTPClient(dial).Do(req)
	if err != nil {
		return enrollHTTPAnswer{Route: route, Error: "the Manager could not be reached over " + routeWords(route)}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, enrollHTTPMaxResponse))
	if err != nil {
		return enrollHTTPAnswer{Route: route, Error: "the Manager's answer over " + routeWords(route) + " was cut off"}
	}
	return enrollHTTPAnswer{Route: route, Status: resp.StatusCode, Body: raw,
		RetryAfter: resp.Header.Get("Retry-After"), ManagerID: resp.Header.Get(enrollclient.ManagerIDHeader)}
}

func routeWords(route string) string {
	if route == "link" {
		return "JuiceMount Link"
	}
	return "the JuiceMount service's network"
}

// servingRequestNotRun reports a control-socket error that the serving
// process returns before it runs anything: an older or different build, or
// a method it does not know. Those strings come from dispatchServingRequest
// and have the same text in every build that has a control socket.
func servingRequestNotRun(errText string) bool {
	return errText == servingErrBuildDiffers || errText == servingErrBadOperation
}

// servingDoer is the desktop archive's Manager client: it hands each request
// to the serving process ("enroll-http"). It sends a request over macOS
// networking itself only when the serving process certainly did not send it
// (no control socket, an older build, or a refusal); once the request may
// have reached the serving process, a lost or late answer is a failure that
// enrollment retries later, never a second send by another route.
type servingDoer struct {
	direct enrollclient.Doer
	// note explains the route the last request took when it was not the
	// serving process's (fixed text, never a secret).
	note string
}

func (d *servingDoer) Do(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		b, err := io.ReadAll(io.LimitReader(req.Body, enrollHTTPMaxRequest+1))
		req.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(b) > enrollHTTPMaxRequest {
			return nil, errors.New("the request to the Manager is too large")
		}
		body = b
	}
	directly := func(why string) (*http.Response, error) {
		d.note = why
		jmlog.Info("enrollment: sending the Manager request over macOS networking", "why", why)
		again := req.Clone(req.Context())
		again.Body = io.NopCloser(bytes.NewReader(body))
		again.ContentLength = int64(len(body))
		again.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
		return d.direct.Do(again)
	}
	res, reached := forwardServiceCallReached(enrollHTTPMethod, []string{req.Method, req.URL.String(), string(body)}, nil)
	if !reached {
		return directly("no JuiceMount service is running, so it was sent over macOS networking, not JuiceMount Link")
	}
	if res.Error != "" {
		if servingRequestNotRun(res.Error) {
			return directly("the running JuiceMount service is an older build that cannot carry it over JuiceMount Link, so it was sent over macOS networking; restart JuiceMount to use Link")
		}
		d.note = "the JuiceMount service did not answer in time; it is sent again automatically"
		return nil, errors.New(d.note)
	}
	var a enrollHTTPAnswer
	if err := json.Unmarshal([]byte(res.Text), &a); err != nil {
		d.note = "the JuiceMount service's answer could not be read; it is sent again automatically"
		return nil, errors.New(d.note)
	}
	switch {
	case a.Refused != "":
		return directly(a.Refused + "; it was sent over macOS networking")
	case a.Error != "":
		d.note = a.Error
		return nil, errors.New(a.Error)
	case a.Status < 100 || a.Status > 599:
		d.note = "the JuiceMount service's answer had no status"
		return nil, errors.New(d.note)
	}
	if len(a.Body) > enrollHTTPMaxResponse {
		a.Body = a.Body[:enrollHTTPMaxResponse]
	}
	h := http.Header{}
	if a.RetryAfter != "" {
		h.Set("Retry-After", a.RetryAfter)
	}
	if a.ManagerID != "" {
		h.Set(enrollclient.ManagerIDHeader, a.ManagerID)
	}
	return &http.Response{StatusCode: a.Status, Status: http.StatusText(a.Status), Header: h, Request: req,
		Body: io.NopCloser(bytes.NewReader(a.Body)), ContentLength: int64(len(a.Body))}, nil
}

// enrollManagerDoer is the client for one enrollment step: the Link node in
// this process when there is one; else, in the desktop archive, the
// servingDoer (returned twice: its note explains the route); else the
// operating system's client.
func enrollManagerDoer() (enrollclient.Doer, *servingDoer) {
	if serviceClientBuild && inProcessLinkDial() == nil {
		d := &servingDoer{direct: enrollHTTPClient}
		return d, d
	}
	return enrollManagerClient(), nil
}
