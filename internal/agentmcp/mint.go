package agentmcp

// Minting, listing and revoking agent tokens as this Mac: the requests and
// checks that `juicemount-mcp token` (TokenCommands) and the JuiceMount app
// (bridge agenttokens.go) share. Neither the app nor the CLI words its
// errors here; each explains a failure to its own reader.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// MaxTokenDays is the longest an agent token lives, and the Manager's
// default (internal/manager agentTokenMaxDays).
const MaxTokenDays = 90

// Refusals of a mint, list or revoke made before anything is sent.
var (
	ErrTokenName = errors.New("name the token, e.g. the agent that will use it")
	ErrTokenDays = errors.New("a token lasts 1-90 days (default 90)")
	ErrTokenID   = errors.New("a token ID is 16 hex digits")
)

// errMalformedMint: the Manager answered 200 without a well-formed token.
var errMalformedMint = errors.New("the Manager's answer did not carry a well-formed agent token")

// TokenView is the Manager's description of an agent token. It never holds
// the token.
type TokenView struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Person     string `json:"person"`
	PersonName string `json:"person_name"`
	Mac        string `json:"mac"`
	CreatedAt  int64  `json:"created_at_ms"`
	ExpiresAt  int64  `json:"expires_at_ms"`
}

// Minted is a token the Manager has just minted. Token is the secret: it is
// stored, never shown or logged.
type Minted struct {
	Token string
	View  TokenView
}

// CheckMint returns the name a token is minted with (trimmed), or why it
// cannot be minted: ErrTokenName or ErrTokenDays. days 0 is the default.
func CheckMint(name string, days int) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrTokenName
	}
	if days < 0 || days > MaxTokenDays {
		return "", ErrTokenDays
	}
	return name, nil
}

// MintToken mints a token named name, living days (0: the Manager's
// default), with c, a Mac client (NewMacClient). When MayHaveMinted(err),
// the Manager may hold a token whose answer was lost.
func MintToken(ctx context.Context, c *Client, name string, days int) (Minted, error) {
	name, err := CheckMint(name, days)
	if err != nil {
		return Minted{}, err
	}
	body := map[string]any{"name": name}
	if days > 0 {
		body["expires_in_days"] = days
	}
	var reply struct {
		Token      string    `json:"token"`
		AgentToken TokenView `json:"agent_token"`
	}
	if err := c.Do(ctx, http.MethodPost, "/api/agent-tokens", nil, body, &reply); err != nil {
		return Minted{}, err
	}
	if mm := agentTokenRe.FindStringSubmatch(reply.Token); mm == nil || mm[1] != reply.AgentToken.ID {
		return Minted{}, errMalformedMint
	}
	return Minted{Token: reply.Token, View: reply.AgentToken}, nil
}

// MayHaveMinted reports whether a MintToken failure may have left a token on
// the Manager: no answer was read, or the Manager failed (5xx) after it
// may have minted one.
func MayHaveMinted(err error) bool {
	if err == nil || errors.Is(err, ErrTokenName) || errors.Is(err, ErrTokenDays) || errors.Is(err, errMalformedMint) {
		return false
	}
	var ae *APIError
	return !errors.As(err, &ae) || ae.Status >= 500
}

// StoreError is a minted token that could not be stored.
type StoreError struct {
	// ID is the minted token's.
	ID string
	// Err is the store's failure.
	Err error
	// RevokeErr is nil when the token was revoked on the Manager (so no
	// live token is left that nobody holds).
	RevokeErr error
	// Prev is the record stored before; PrevStored says there was one.
	Prev       TokenRecord
	PrevStored bool
}

func (e *StoreError) Error() string {
	if e.RevokeErr != nil {
		return "the token could not be stored, and revoking it failed too"
	}
	return "the token could not be stored; it was revoked on the Manager"
}

func (e *StoreError) Unwrap() error { return e.Err }

// PrevRemoved reports whether the store removed the record it held before
// it failed: that token is no longer here but still live on the Manager.
func (e *StoreError) PrevRemoved() bool {
	return e.PrevStored && errors.Is(e.Err, ErrPreviousTokenRemoved)
}

// StoreMinted stores m, minted on the Manager at base, in store, replacing
// the record stored before. replaced is that record, when it was another
// token (it stays valid on the Manager until revoked). When m cannot be
// stored it is revoked with c, and the error is a *StoreError.
func StoreMinted(ctx context.Context, c *Client, store TokenStore, base string, m Minted) (replaced *TokenRecord, err error) {
	prev, perr := store.Load()
	rec := TokenRecord{ManagerURL: base, Token: m.Token, ID: m.View.ID, Name: m.View.Name, ExpiresAt: m.View.ExpiresAt}
	if err := store.Save(rec); err != nil {
		rerr := c.Do(ctx, http.MethodPost, "/api/agent-tokens/"+m.View.ID+"/revoke", nil, nil, nil)
		return nil, &StoreError{ID: m.View.ID, Err: err, RevokeErr: rerr, Prev: prev, PrevStored: perr == nil}
	}
	if perr == nil && prev.ID != m.View.ID {
		return &prev, nil
	}
	return nil, nil
}

// ListTokens lists this person's tokens with c, a Mac client.
func ListTokens(ctx context.Context, c *Client) ([]TokenView, error) {
	var reply struct {
		Tokens []TokenView `json:"agent_tokens"`
	}
	if err := c.Do(ctx, http.MethodGet, "/api/agent-tokens", nil, nil, &reply); err != nil {
		return nil, err
	}
	return reply.Tokens, nil
}

// RevokeToken revokes token id on the Manager with c, a Mac client. gone
// reports that the Manager does not have it (already revoked or expired).
func RevokeToken(ctx context.Context, c *Client, id string) (gone bool, err error) {
	if !agentTokenIDRe.MatchString(id) {
		return false, ErrTokenID
	}
	err = c.Do(ctx, http.MethodPost, "/api/agent-tokens/"+id+"/revoke", nil, nil, nil)
	var ae *APIError
	if errors.As(err, &ae) && ae.Status == http.StatusNotFound {
		return true, nil
	}
	return false, err
}

// StoredTokenID is the ID of the token stored in store ("" when none, or
// when it cannot be read).
func StoredTokenID(store TokenStore) string {
	if r, err := store.Load(); err == nil {
		return r.ID
	}
	return ""
}

// ForgetStored deletes the record in store when it is token id; deleted
// reports whether it was.
func ForgetStored(store TokenStore, id string) (deleted bool, err error) {
	r, err := store.Load()
	switch {
	case errors.Is(err, ErrNoToken):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading the stored token: %w", err)
	case r.ID != id:
		return false, nil
	}
	if err := store.Delete(); err != nil {
		return false, fmt.Errorf("deleting the stored token: %w", err)
	}
	return true, nil
}
