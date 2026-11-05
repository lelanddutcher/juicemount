package agentmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

// ErrPreviousTokenRemoved: a TokenStore's Save deleted the record stored
// before it and then failed, so no token is stored.
var ErrPreviousTokenRemoved = errors.New("the agent token stored before was removed, and the new one could not be stored")

// removedNote tells the person what a failed Save that removed prev means.
func removedNote(err error, prev TokenRecord, havePrev bool) string {
	if !errors.Is(err, ErrPreviousTokenRemoved) {
		return ""
	}
	if !havePrev || prev.ID == "" {
		return " A token stored here before was removed from this machine and may still be live on the Manager: run juicemount-mcp token list, and juicemount-mcp token revoke <id> for any you no longer need."
	}
	return fmt.Sprintf(" The token stored here before (%s) was removed from this machine but is still live on the Manager: run juicemount-mcp token list, and juicemount-mcp token revoke %s if you no longer need it.", prev.ID, prev.ID)
}

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

// CleartextWarning is a warning for a Manager address over plain http to a
// host other than this machine ("" otherwise): the token would cross the
// network unencrypted, as the Mac's enrollment poll token does. It is not
// refused.
func CleartextWarning(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "http" {
		return ""
	}
	h := u.Hostname()
	if ip := net.ParseIP(h); h == "localhost" || ip != nil && ip.IsLoopback() {
		return ""
	}
	return fmt.Sprintf("warning: the Manager address %s is plain http, so the agent token crosses the network unencrypted (as this Mac's enrollment does); use an https address where you can", base)
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
	switch _, err := CheckMint(name, days); {
	case errors.Is(err, ErrTokenName):
		return errors.New("name the token (--name), e.g. the agent that will use it")
	case err != nil:
		return errors.New("--days is 1-90 (default 90)")
	}
	m, err := tc.Mac()
	if err != nil {
		return err
	}
	c := NewMacClient(m, tc.HTTP)
	if w := CleartextWarning(m.ManagerURL); w != "" {
		fmt.Fprintln(tc.Err, w)
	}
	minted, err := MintToken(ctx, c, name, days)
	switch {
	case errors.Is(err, errMalformedMint):
		return err
	case MayHaveMinted(err):
		return fmt.Errorf("%v. A token may have been created: run juicemount-mcp token list and revoke any you don't recognise", managerFailure("minting the token", err))
	case err != nil:
		return managerFailure("minting the token", err)
	}
	v := minted.View
	if print {
		fmt.Fprintf(tc.Err, "Minted agent token %s (%q) for %s; it expires %s. It is not stored on this Mac.\n", v.ID, v.Name, v.PersonName, msTime(v.ExpiresAt))
		_, err := fmt.Fprintln(tc.Out, minted.Token)
		return err
	}
	replaced, err := StoreMinted(ctx, c, tc.Store, m.ManagerURL, minted)
	var se *StoreError
	if errors.As(err, &se) {
		// The minted token was revoked (or that failed): nobody holds it.
		note := removedNote(se.Err, se.Prev, se.PrevStored)
		if se.RevokeErr != nil {
			return fmt.Errorf("the token could not be stored (%v), and revoking it failed too (%v): revoke %s with juicemount-mcp token revoke %s.%s", se.Err, se.RevokeErr, v.ID, v.ID, note)
		}
		return fmt.Errorf("the token could not be stored (%v); it was revoked on the Manager.%s", se.Err, note)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(tc.Out, "Minted agent token %s (%q) for %s; it expires %s. It is stored on this Mac for juicemount-mcp serve.\n", v.ID, v.Name, v.PersonName, msTime(v.ExpiresAt))
	if replaced != nil {
		fmt.Fprintf(tc.Out, "It replaces token %s here, which still works on the Manager until you run: juicemount-mcp token revoke %s\n", replaced.ID, replaced.ID)
	}
	return nil
}

// List prints this person's tokens; "*" marks the one stored here.
func (tc *TokenCommands) List(ctx context.Context) error {
	c, err := tc.macClient()
	if err != nil {
		return err
	}
	tokens, err := ListTokens(ctx, c)
	if err != nil {
		return managerFailure("listing tokens", err)
	}
	local := StoredTokenID(tc.Store)
	if len(tokens) == 0 {
		_, err := fmt.Fprintln(tc.Out, "No agent tokens.")
		return err
	}
	w := tabwriter.NewWriter(tc.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "\tID\tNAME\tPERSON\tCREATED\tEXPIRES")
	for _, v := range tokens {
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
		gone, err := RevokeToken(ctx, c, id)
		switch {
		case err != nil:
			return managerFailure("revoking the token", err)
		case gone:
			fmt.Fprintf(tc.Out, "Token %s is not on the Manager (already revoked or expired).\n", id)
		default:
			fmt.Fprintf(tc.Out, "Revoked token %s on the Manager.\n", id)
		}
	}
	deleted, err := ForgetStored(tc.Store, id)
	if err != nil {
		return err
	}
	if deleted {
		_, err = fmt.Fprintf(tc.Out, "Deleted token %s from this machine.\n", id)
	}
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
	if w := CleartextWarning(base); w != "" {
		fmt.Fprintln(tc.Err, w)
	}
	err = NewAgentClient(base, token, tc.HTTP).Do(ctx, http.MethodGet, "/api/collections", url.Values{"limit": {"1"}}, nil, nil)
	var ae *APIError
	switch {
	case errors.As(err, &ae) && ae.Status == http.StatusUnauthorized:
		return fmt.Errorf("the Manager refused token %s (%s); it was not stored", m[1], ae.Message)
	case err != nil:
		fmt.Fprintf(tc.Err, "warning: token %s could not be checked with the Manager (%v); storing it anyway\n", m[1], errorMessage(err))
	}
	prev, perr := tc.Store.Load()
	if err := tc.Store.Save(TokenRecord{ManagerURL: base, Token: token, ID: m[1], Name: "imported"}); err != nil {
		return fmt.Errorf("storing the token: %v.%s", err, removedNote(err, prev, perr == nil))
	}
	_, err = fmt.Fprintf(tc.Out, "Stored agent token %s for %s.\n", m[1], base)
	return err
}
