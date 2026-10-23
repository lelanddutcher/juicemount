// Package mutationclient provides the authenticated Gateway image wire client
// without depending on the Gateway server or its SDK-backed custody path.
package mutationclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

var ErrIntegrity = errors.New("custody artifact integrity could not be established")

type Client struct {
	base string
	http *http.Client
}

func NewClient(endpoint string, cert tls.Certificate, roots *x509.CertPool) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil ||
		len(cert.Certificate) == 0 || cert.PrivateKey == nil || roots == nil || len(roots.Subjects()) == 0 {
		return nil, fmt.Errorf("invalid authenticated gateway endpoint")
	}
	t := &http.Transport{TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: roots, MinVersion: tls.VersionTLS13}, MaxConnsPerHost: 4, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second}
	return &Client{base: "https://" + u.Host, http: &http.Client{Transport: t, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Transport is used by the historical Gateway custody facade so both APIs
// share one authenticated connection pool and redirect policy.
func (c *Client) Transport() (string, *http.Client) { return c.base, c.http }

func (c *Client) Close() { c.http.CloseIdleConnections() }
