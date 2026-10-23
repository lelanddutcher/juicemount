package mutationgateway

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationbackend"
)

// Server is the only supported network listener configuration. There is no
// plaintext, proxy-identity-header, bearer-token or unauthenticated fallback.
func (g *Gateway) Server(cert tls.Certificate, clients *x509.CertPool) (*http.Server, error) {
	if len(cert.Certificate) == 0 || cert.PrivateKey == nil || clients == nil || len(clients.Subjects()) == 0 {
		return nil, mutationauthority.ErrInvalid
	}
	return &http.Server{
		Handler:           http.HandlerFunc(g.serveHTTP),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: clients, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13},
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192,
	}, nil
}

func identity(r *http.Request) (string, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains) == 0 ||
		len(r.TLS.VerifiedChains[0]) == 0 || !bytes.Equal(r.TLS.PeerCertificates[0].Raw, r.TLS.VerifiedChains[0][0].Raw) {
		return "", ErrUnauthorized
	}
	return hash(r.TLS.PeerCertificates[0].Raw), nil
}

func owns(principal, session string) bool {
	return validSession(session) && strings.HasPrefix(session, principal+".")
}

func decode(w http.ResponseWriter, r *http.Request, out any, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return mutationauthority.ErrInvalid
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return mutationauthority.ErrInvalid
	}
	return nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	code, msg := http.StatusInternalServerError, "custody_unavailable"
	switch {
	case errors.Is(err, ErrUnauthorized):
		code, msg = http.StatusForbidden, "unauthorized"
	case errors.Is(err, mutationauthority.ErrInvalid):
		code, msg = http.StatusBadRequest, "invalid_request"
	case errors.Is(err, mutationauthority.ErrNoSuchOperation):
		code, msg = http.StatusNotFound, "unknown_operation"
	case errors.Is(err, ErrIntegrity):
		code, msg = http.StatusConflict, "custody_integrity_unconfirmed"
	case errors.Is(err, mutationauthority.ErrOwnerActive):
		code, msg = http.StatusConflict, "owner_active"
	case errors.Is(err, mutationauthority.ErrUnsettled), errors.Is(err, mutationauthority.ErrUncertain):
		code, msg = http.StatusConflict, "reconciliation_required"
	case errors.Is(err, mutationauthority.ErrChangedRequest):
		code, msg = http.StatusConflict, "changed_request"
	case errors.Is(err, mutationauthority.ErrStaleEpoch), errors.Is(err, mutationauthority.ErrLeaseExpired):
		code, msg = http.StatusConflict, "stale_or_expired_epoch"
	case errors.Is(err, mutationauthority.ErrClosed):
		code, msg = http.StatusServiceUnavailable, "closed"
	case errors.Is(err, mutationbackend.ErrUnavailable):
		code, msg = http.StatusServiceUnavailable, "backend_unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{msg})
}

func (g *Gateway) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	principal, err := identity(r)
	if err != nil {
		fail(w, err)
		return
	}
	done, err := g.enter()
	if err != nil {
		fail(w, err)
		return
	}
	defer done()
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if r.URL.RawQuery != "" {
		fail(w, mutationauthority.ErrInvalid)
		return
	}
	want := http.MethodPost
	if r.URL.Path == "/v1/custody/upload" {
		want = http.MethodPut
	}
	if r.Method != want {
		w.Header().Set("Allow", want)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/images/") {
		g.serveImageHTTP(w, r, principal)
		return
	}
	switch r.URL.Path {
	case "/v1/claim":
		if err := decode(w, r, new(struct{}), 1024); err != nil {
			fail(w, err)
			return
		}
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			fail(w, err)
			return
		}
		l, err := g.journal.Claim(r.Context(), g.volume, principal+"."+hex.EncodeToString(nonce[:]), g.ttl)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, wireLease(l))
	case "/v1/renew":
		var l Lease
		if err := decode(w, r, &l, 2048); err != nil {
			fail(w, err)
			return
		}
		if l.Volume != g.volume || !owns(principal, l.Session) {
			fail(w, ErrUnauthorized)
			return
		}
		next, err := g.journal.Renew(r.Context(), l.authority(), g.ttl)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, wireLease(next))
	case "/v1/custody/upload":
		var u Upload
		if err := decode(w, r, &u, maxWireBytes); err != nil {
			fail(w, err)
			return
		}
		if !u.Chunk.valid() || int64(len(u.Data)) != u.Chunk.Size || hash(u.Data) != u.Chunk.SHA256 {
			fail(w, mutationauthority.ErrInvalid)
			return
		}
		if !owns(principal, u.Chunk.Session) {
			fail(w, ErrUnauthorized)
			return
		}
		_, err := g.journal.Execute(r.Context(), g.request(u.Chunk), stageBackend{g: g, chunk: u.Chunk, data: u.Data})
		if err != nil {
			fail(w, err)
			return
		}
		// Exact duplicate receipts still require fresh artifact integrity. A
		// committed journal row alone cannot acknowledge missing stage bytes.
		receipt, _, err := g.custody(r.Context(), u.Chunk)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, receipt)
	case "/v1/custody/status", "/v1/custody/readback":
		var c Chunk
		if err := decode(w, r, &c, 4096); err != nil {
			fail(w, err)
			return
		}
		if !c.valid() {
			fail(w, mutationauthority.ErrInvalid)
			return
		}
		if !owns(principal, c.Session) {
			fail(w, ErrUnauthorized)
			return
		}
		receipt, data, err := g.custody(r.Context(), c)
		if err != nil {
			fail(w, err)
			return
		}
		if r.URL.Path == "/v1/custody/status" {
			writeJSON(w, receipt)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-JM-Custody-SHA256", receipt.ArtifactSHA256)
		_, _ = w.Write(data)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}
