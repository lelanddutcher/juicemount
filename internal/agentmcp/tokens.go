package agentmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
)

var (
	agentTokenRe   = regexp.MustCompile(`^jma_([0-9a-f]{16})_[0-9a-f]{64}$`)
	agentTokenIDRe = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// ErrNoToken means no agent token is stored on this Mac.
var ErrNoToken = errors.New("no agent token is stored on this Mac")

// TokenRecord is the stored agent token: the secret and the Manager it was
// minted on (it is sent nowhere else).
type TokenRecord struct {
	ManagerURL string `json:"manager_url"`
	Token      string `json:"token"`
	ID         string `json:"id"`
	Name       string `json:"name,omitempty"`
	ExpiresAt  int64  `json:"expires_at_ms,omitempty"`
}

// TokenStore keeps one TokenRecord (internal/credstore: the Keychain on a
// Mac). Load returns ErrNoToken when none is stored.
type TokenStore interface {
	Save(TokenRecord) error
	Load() (TokenRecord, error)
	Delete() error
}

// ParseTokenRecord reads a stored record and checks it.
func ParseTokenRecord(raw string) (TokenRecord, error) {
	var r TokenRecord
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return TokenRecord{}, errors.New("the stored agent token record is unreadable; import or create the token again")
	}
	m := agentTokenRe.FindStringSubmatch(r.Token)
	base, err := enrollclient.NormalizeManagerURL(r.ManagerURL)
	if m == nil || m[1] != r.ID || err != nil {
		return TokenRecord{}, errors.New("the stored agent token record is malformed; import or create the token again")
	}
	r.ManagerURL = base
	return r, nil
}

// Encode is the stored form of r.
func (r TokenRecord) Encode() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// MacCredentialsFromState reads the app's enrollment record
// (enrollclient.State JSON, the Keychain slot "enrollment") and this Mac's
// device digest. Only an approved enrollment with a poll token can mint.
func MacCredentialsFromState(raw, device string) (MacCredentials, error) {
	var st enrollclient.State
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return MacCredentials{}, errors.New("this Mac's enrollment record is unreadable")
	}
	if st.Status != "approved" || st.PollToken == "" || st.InstanceID == "" {
		return MacCredentials{}, errors.New("this Mac is not approved by the Manager yet: request access in JuiceMount's settings and have an admin approve it")
	}
	base, err := enrollclient.NormalizeManagerURL(st.ManagerURL)
	if err != nil {
		return MacCredentials{}, fmt.Errorf("this Mac's enrollment names no usable Manager address: %v", err)
	}
	return MacCredentials{ManagerURL: base, InstanceID: st.InstanceID, PollToken: st.PollToken, Device: device}, nil
}

// TokenCommands are `juicemount-mcp token ...`. Nothing they write to Out
// or Err, and nothing they log, holds a token, except Create with print,
// which writes the token alone to Out.
type TokenCommands struct {
	Store TokenStore
	// Mac returns this Mac's Manager credentials (minting, listing and
	// revoking need them; no agent may).
	Mac  func() (MacCredentials, error)
	HTTP *http.Client
	Out  io.Writer
	Err  io.Writer
}

type tokenView struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Person     string `json:"person"`
	PersonName string `json:"person_name"`
	Mac        string `json:"mac"`
	CreatedAt  int64  `json:"created_at_ms"`
	ExpiresAt  int64  `json:"expires_at_ms"`
}

func msTime(ms int64) string { return time.UnixMilli(ms).Local().Format("2006-01-02 15:04") }

func (tc *TokenCommands) macClient() (*Client, error) {
	m, err := tc.Mac()
	if err != nil {
		return nil, err
	}
	return NewMacClient(m, tc.HTTP), nil
}

// managerFailure explains a failed token request to a person.
func managerFailure(what string, err error) error {
	var ae *APIError
	if errors.As(err, &ae) {
		switch ae.Status {
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("%s: the Manager refused this Mac (%s); it must be approved and assigned to a person on the People page", what, ae.Message)
		case http.StatusNotFound:
			return fmt.Errorf("%s: %s (is continuity, JM_CONTINUITY, on in the Manager?)", what, ae.Message)
		}
		return fmt.Errorf("%s: %s", what, ae.Message)
	}
	return fmt.Errorf("%s: %v", what, err)
}

// Create mints a token from this Mac. It is stored (replacing the one stored
// before, which stays valid on the Manager until revoked), or with print
// written to Out instead, for a machine that imports it.
func (tc *TokenCommands) Create(ctx context.Context, name string, days int, print bool) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("name the token (--name), e.g. the agent that will use it")
	}
	if days < 0 || days > 90 {
		return errors.New("--days is 1-90 (default 90)")
	}
	m, err := tc.Mac()
	if err != nil {
		return err
	}
	c := NewMacClient(m, tc.HTTP)
	body := map[string]any{"name": name}
	if days > 0 {
		body["expires_in_days"] = days
	}
	var reply struct {
		Token      string    `json:"token"`
		AgentToken tokenView `json:"agent_token"`
	}
	if err := c.Do(ctx, http.MethodPost, "/api/agent-tokens", nil, body, &reply); err != nil {
		return managerFailure("minting the token", err)
	}
	if mm := agentTokenRe.FindStringSubmatch(reply.Token); mm == nil || mm[1] != reply.AgentToken.ID {
		return errors.New("the Manager's answer did not carry a well-formed agent token")
	}
	v := reply.AgentToken
	if print {
		fmt.Fprintf(tc.Err, "Minted agent token %s (%q) for %s; it expires %s. It is not stored on this Mac.\n", v.ID, v.Name, v.PersonName, msTime(v.ExpiresAt))
		_, err := fmt.Fprintln(tc.Out, reply.Token)
		return err
	}
	prev, perr := tc.Store.Load()
	rec := TokenRecord{ManagerURL: m.ManagerURL, Token: reply.Token, ID: v.ID, Name: v.Name, ExpiresAt: v.ExpiresAt}
	if err := tc.Store.Save(rec); err != nil {
		// Do not leave a live token nobody holds.
		rerr := c.Do(ctx, http.MethodPost, "/api/agent-tokens/"+v.ID+"/revoke", nil, nil, nil)
		if rerr != nil {
			return fmt.Errorf("the token could not be stored (%v), and revoking it failed too (%v): revoke %s with juicemount-mcp token revoke %s", err, rerr, v.ID, v.ID)
		}
		return fmt.Errorf("the token could not be stored (%v); it was revoked on the Manager", err)
	}
	fmt.Fprintf(tc.Out, "Minted agent token %s (%q) for %s; it expires %s. It is stored on this Mac for juicemount-mcp serve.\n", v.ID, v.Name, v.PersonName, msTime(v.ExpiresAt))
	if perr == nil && prev.ID != v.ID {
		fmt.Fprintf(tc.Out, "It replaces token %s here, which still works on the Manager until you run: juicemount-mcp token revoke %s\n", prev.ID, prev.ID)
	}
	return nil
}

// List prints this person's tokens; "*" marks the one stored here.
func (tc *TokenCommands) List(ctx context.Context) error {
	c, err := tc.macClient()
	if err != nil {
		return err
	}
	var reply struct {
		Tokens []tokenView `json:"agent_tokens"`
	}
	if err := c.Do(ctx, http.MethodGet, "/api/agent-tokens", nil, nil, &reply); err != nil {
		return managerFailure("listing tokens", err)
	}
	local := ""
	if r, err := tc.Store.Load(); err == nil {
		local = r.ID
	}
	if len(reply.Tokens) == 0 {
		_, err := fmt.Fprintln(tc.Out, "No agent tokens.")
		return err
	}
	w := tabwriter.NewWriter(tc.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "\tID\tNAME\tPERSON\tCREATED\tEXPIRES")
	for _, v := range reply.Tokens {
		mark := ""
		if v.ID == local {
			mark = "*"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", mark, v.ID, v.Name, v.PersonName, msTime(v.CreatedAt), msTime(v.ExpiresAt))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if local != "" {
		fmt.Fprintln(tc.Out, "* stored on this Mac (used by juicemount-mcp serve)")
	}
	return nil
}

// Revoke revokes a token on the Manager (unless localOnly) and deletes it
// here when it is the one stored.
func (tc *TokenCommands) Revoke(ctx context.Context, id string, localOnly bool) error {
	if !agentTokenIDRe.MatchString(id) {
		return errors.New("a token ID is 16 hex digits (juicemount-mcp token list shows them)")
	}
	if !localOnly {
		c, err := tc.macClient()
		if err != nil {
			return fmt.Errorf("%v (use --local-only to delete the token from this machine without the Manager)", err)
		}
		err = c.Do(ctx, http.MethodPost, "/api/agent-tokens/"+id+"/revoke", nil, nil, nil)
		var ae *APIError
		switch {
		case errors.As(err, &ae) && ae.Status == http.StatusNotFound:
			fmt.Fprintf(tc.Out, "Token %s is not on the Manager (already revoked or expired).\n", id)
		case err != nil:
			return managerFailure("revoking the token", err)
		default:
			fmt.Fprintf(tc.Out, "Revoked token %s on the Manager.\n", id)
		}
	}
	r, err := tc.Store.Load()
	switch {
	case errors.Is(err, ErrNoToken):
		return nil
	case err != nil:
		return fmt.Errorf("reading the stored token: %v", err)
	case r.ID != id:
		return nil
	}
	if err := tc.Store.Delete(); err != nil {
		return fmt.Errorf("deleting the stored token: %v", err)
	}
	_, err = fmt.Fprintf(tc.Out, "Deleted token %s from this machine.\n", id)
	return err
}

// Import stores a token minted elsewhere (token create --print), read from
// in, for the Manager at managerURL. It is first checked with the Manager:
// a token the Manager refuses is not stored.
func (tc *TokenCommands) Import(ctx context.Context, in io.Reader, managerURL string) error {
	base, err := enrollclient.NormalizeManagerURL(managerURL)
	if err != nil {
		return fmt.Errorf("--manager: %v", err)
	}
	raw, err := io.ReadAll(io.LimitReader(in, 4097))
	if err != nil {
		return err
	}
	if len(raw) > 4096 {
		return errors.New("the input is not an agent token")
	}
	token := strings.TrimSpace(string(raw))
	m := agentTokenRe.FindStringSubmatch(token)
	if m == nil {
		return errors.New("the input is not an agent token (jma_<id>_<secret>)")
	}
	err = NewAgentClient(base, token, tc.HTTP).Do(ctx, http.MethodGet, "/api/collections", url.Values{"limit": {"1"}}, nil, nil)
	var ae *APIError
	switch {
	case errors.As(err, &ae) && ae.Status == http.StatusUnauthorized:
		return fmt.Errorf("the Manager refused token %s (%s); it was not stored", m[1], ae.Message)
	case err != nil:
		fmt.Fprintf(tc.Err, "warning: token %s could not be checked with the Manager (%v); storing it anyway\n", m[1], errorMessage(err))
	}
	if err := tc.Store.Save(TokenRecord{ManagerURL: base, Token: token, ID: m[1], Name: "imported"}); err != nil {
		return fmt.Errorf("storing the token: %v", err)
	}
	_, err = fmt.Fprintf(tc.Out, "Stored agent token %s for %s.\n", m[1], base)
	return err
}
