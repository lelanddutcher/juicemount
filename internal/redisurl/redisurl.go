// Package redisurl parses saved Redis endpoints without exposing credentials
// in errors. It is dependency-light so metadata, health, bridge and Manager
// can share one connection contract without import cycles.
package redisurl

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

var (
	ErrEmpty   = errors.New("Redis URL is empty")
	ErrScheme  = errors.New("unsupported Redis URL scheme")
	ErrHost    = errors.New("Redis URL needs a valid host")
	ErrPort    = errors.New("Redis port must be between 1 and 65535")
	ErrOptions = errors.New("invalid Redis URL path or options")
)

// Parse preserves go-redis URL options while validating the address before
// go-redis's permissive localhost and port defaults can hide a typo. A bare
// host:port/db is accepted for the legacy CLI and saved preferences.
func Parse(raw string) (*redis.Options, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ErrEmpty
	}
	if !strings.Contains(raw, "://") {
		raw = "redis://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, ErrHost
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, ErrScheme
	}
	host := u.Hostname()
	if host == "" || (strings.Contains(host, ":") && !strings.HasPrefix(u.Host, "[")) {
		return nil, ErrHost
	}
	if strings.HasPrefix(u.Host, "[") {
		if _, err := netip.ParseAddr(host); err != nil {
			return nil, ErrHost
		}
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, ErrPort
	}
	if _, err := url.ParseQuery(u.RawQuery); err != nil {
		return nil, ErrOptions
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return nil, ErrPort
	}
	// go-redis's fallback for [IPv6] without an explicit port treats the
	// brackets as part of the hostname. Give it an exact normalized host:port.
	u.Host = net.JoinHostPort(host, port)
	opts, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, ErrOptions
	}
	if opts.DB < 0 {
		return nil, ErrOptions
	}
	return opts, nil
}

// Address returns only the effective destination and DB. Callers may use it
// for route policy, reachability and keyspace subscriptions, never to create
// an authenticated client pool.
func Address(raw string) (string, int, error) {
	opts, err := Parse(raw)
	if err != nil {
		return "", 0, err
	}
	return opts.Addr, opts.DB, nil
}
