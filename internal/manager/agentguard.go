package manager

// agentguard.go: continuity slice 6a, where an agent token may be used.
//
// An agent token (agenttokens.go) is accepted only on an allowlist, and is
// refused (403) everywhere else, closed by default at two levels:
//
//   - Routes. Register registers every Manager route through an
//     agentRouteMux, which wraps each handler so that a request with an
//     "Authorization: Bearer" header is refused before the handler runs,
//     unless its pattern is one of agentPatterns. A route added later is
//     therefore refused for agents until it is added there (and to the
//     route-walk test). No other Manager route takes a bearer credential, so
//     any bearer is refused, valid or not.
//   - Methods and paths. The allowed patterns are subtrees whose handlers
//     also serve admin and Mac requests; serveWith checks an agent's request
//     against agentRequests (method and path) and refuses anything else.
//     It checks the parts routeParts split (collections.go), which also
//     route the request and decide its slot, so the path checked is the
//     path routed; a non-canonical path is 404 for every caller.
//
// What an agent may do: list and read collections and their members,
// create a collection, rename one and add or remove members (each checked
// against its person's grants as for that person's Mac); list and read saved
// queries and their results; find files and read one by asset ID. It may not
// delete a collection: a delete takes the whole collection, curated by
// people over time, in one step with no undo route, while member edits are
// incremental and each is audited with what it changed. It may not create,
// rename or delete a saved query, and reaches nothing that moves or returns
// file bytes: agents read bytes through the mount.

import (
	"net/http"
	"strings"
)

// routeMux is what route registration needs from an http.ServeMux.
type routeMux interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
	Handle(pattern string, handler http.Handler)
}

// agentPatterns are the registered patterns (below the prefix) whose
// handlers accept an agent token; they check agentRequests themselves.
var agentPatterns = []string{
	"/api/collections", "/api/collections/",
	"/api/queries", "/api/queries/",
	"/api/continuity/find",
	"/api/continuity/asset/",
}

// agentRequests are the requests an agent may make (paths below the
// prefix; "*" is one path segment).
var agentRequests = []struct{ method, path string }{
	{http.MethodGet, "/api/collections"},
	{http.MethodPost, "/api/collections"},
	{http.MethodGet, "/api/collections/*"},
	{http.MethodGet, "/api/collections/*/members"},
	{http.MethodPost, "/api/collections/*/rename"},
	{http.MethodPost, "/api/collections/*/members/add"},
	{http.MethodPost, "/api/collections/*/members/remove"},
	{http.MethodGet, "/api/queries"},
	{http.MethodGet, "/api/queries/*"},
	{http.MethodGet, "/api/queries/*/results"},
	{http.MethodGet, "/api/continuity/find"},
	{http.MethodGet, "/api/continuity/asset/*"},
}

// agentMay reports whether an agent may make this request: base is the
// handler's route below the prefix and parts the path segments below it, as
// routeParts split them (the same parts the handler routes and the slot
// decision reads).
func agentMay(method, base string, parts []string) bool {
	got := append(strings.Split(strings.TrimPrefix(base, "/"), "/"), parts...)
	for _, a := range agentRequests {
		if a.method != method {
			continue
		}
		want := strings.Split(strings.TrimPrefix(a.path, "/"), "/")
		if len(want) != len(got) {
			continue
		}
		ok := true
		for i := range want {
			if want[i] != "*" && want[i] != got[i] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// bearerScheme reports whether the request carries an Authorization header
// of the Bearer scheme (an agent token, or an attempt at one).
func bearerScheme(r *http.Request) bool {
	scheme, _, _ := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
	return strings.EqualFold(scheme, "Bearer")
}

// agentRouteMux registers routes on a ServeMux, refusing agent requests on
// every pattern not in agentPatterns, and records the patterns (tests).
type agentRouteMux struct {
	mux      *http.ServeMux
	allowed  map[string]bool
	patterns []string
}

func newAgentRouteMux(mux *http.ServeMux, prefix string) *agentRouteMux {
	m := &agentRouteMux{mux: mux, allowed: map[string]bool{}}
	for _, p := range agentPatterns {
		m.allowed[prefix+p] = true
	}
	return m
}

func (m *agentRouteMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	m.Handle(pattern, http.HandlerFunc(handler))
}

func (m *agentRouteMux) Handle(pattern string, handler http.Handler) {
	m.patterns = append(m.patterns, pattern)
	if !m.allowed[pattern] {
		next := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if bearerScheme(r) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "agent tokens may not use this route"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	m.mux.Handle(pattern, handler)
}

// routesRegistered, when set, receives every pattern Register registered
// (tests).
var routesRegistered func(patterns []string)
