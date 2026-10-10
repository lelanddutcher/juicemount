package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/credstore"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// loadServingCredential is the service-side secret source (the Keychain item
// the app wrote, or the isolated root's private file). Tests substitute it.
var loadServingCredential = credstore.Load

// persistableServerConfig returns cfg with every secret removed: the Redis
// password from redis_url's userinfo (the username stays), the object-store
// key and the Link pairing key. It is the only form of the config that may reach disk: the service's
// serving-desired.json sits in Application Support, which Time Machine backs
// up. Owner digests are computed over this form too (servingConfigDigest), so
// Start (live config) and Stop/recovery (saved config) agree.
func persistableServerConfig(cfg ServerConfig) ServerConfig {
	cfg.RedisURL, _ = health.SplitMetaPassword(cfg.RedisURL)
	cfg.NetAuthKey = ""
	cfg.ObjectAccessKey, cfg.ObjectSecretKey = "", ""
	return cfg
}

// canonicalServerConfig puts redis_url in its one canonical spelling
// (credstore.CanonicalRedisURL). Every config the service or an owner accepts
// passes through here first, so the app's Start and recovery's replay of the
// same configuration are byte-identical: the owner compares claim payloads
// exactly, and a password the app encoded as "%2B" but recovery rebuilt as "+"
// read as a different claim, held recovery, and was never retried.
func canonicalServerConfig(cfg ServerConfig) ServerConfig {
	cfg.RedisURL = credstore.CanonicalRedisURL(cfg.RedisURL)
	return cfg
}

// servingConfigDigest identifies a serving configuration without its secrets.
func servingConfigDigest(cfg ServerConfig) (string, error) {
	canonical, err := json.Marshal(persistableServerConfig(cfg))
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// hydrateServingConfig restores the secrets persistableServerConfig removed,
// for recovery after launchd restarts the service with no app running. Each
// secret is looked up for the endpoint in cfg, so it is only ever attached to
// the server it was saved for. A missing secret leaves the field empty: a
// password-less server still mounts, and an enforcing proxy then fails the
// mount with a named AUTH_REQUIRED error instead of a silent hang.
func hydrateServingConfig(cfg ServerConfig) ServerConfig {
	if _, pw := health.SplitMetaPassword(cfg.RedisURL); pw == "" {
		if account, ok := credstore.RedisAccount(cfg.RedisURL); ok {
			secret, err := loadServingCredential(credstore.RedisPassword, account)
			switch {
			case err == nil:
				cfg.RedisURL = credstore.WithPassword(cfg.RedisURL, secret)
			case !errors.Is(err, credstore.ErrNotFound):
				jmlog.Warn("recovery could not read the Redis password; mounting without it", "error", err.Error())
			}
		}
	}
	if cfg.ObjectAccessKey == "" && cfg.ObjectSecretKey == "" {
		// The object key is bound to the same endpoint account as the Redis
		// password (the proxy URL with this Mac's principal as the user).
		if account, ok := credstore.RedisAccount(cfg.RedisURL); ok {
			stored, err := loadServingCredential(credstore.ObjectKey, account)
			ak, sk, ok := credstore.DecodeObjectKey(stored)
			switch {
			case err == nil && ok:
				cfg.ObjectAccessKey, cfg.ObjectSecretKey = ak, sk
			case err == nil:
				jmlog.Warn("recovery found a malformed object-store key; mounting without it")
			case !errors.Is(err, credstore.ErrNotFound):
				jmlog.Warn("recovery could not read the object-store key; mounting without it", "error", err.Error())
			}
		}
	}
	if cfg.NetAuthKey == "" && strings.TrimSpace(cfg.NetControlURL) != "" {
		secret, err := loadServingCredential(credstore.LinkAuthKey, credstore.LinkAccount(cfg.NetControlURL))
		switch {
		case err == nil:
			cfg.NetAuthKey = secret
		case !errors.Is(err, credstore.ErrNotFound):
			jmlog.Warn("recovery could not read the Link pairing key", "error", err.Error())
		}
	}
	return cfg
}
