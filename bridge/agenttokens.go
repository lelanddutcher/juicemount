package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/agentmcp"
	"github.com/lelanddutcher/juicemount/internal/credstore"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// Agent tokens from the app's Settings (continuity slice 6b,
// docs/agent-mcp.md). The app holds this Mac's enrollment record in its own
// Keychain slot and passes it here; the bridge mints, lists and revokes this
// person's agent tokens on the Manager as this Mac, through the code
// `juicemount-mcp token` uses (internal/agentmcp). A minted token goes
// straight into the Keychain item `juicemount-mcp serve` reads
// (credstore.AppAgentToken), whose access list trusts the app and the
// bundled juicemount-mcp. So minting never opens the enrollment item to
// another program, and `serve` reads the token without a prompt.
//
// The token itself never leaves this file: no output carries it and nothing
// logs it. Like the enrollment step, these run in the app's own process
// (both archive builds) and reach the Manager over the operating system's
// network, as `serve` does.

// agentTokenInput is the input of every agent-token export.
type agentTokenInput struct {
	// State is this Mac's enrollment as the app keeps it in its Keychain
	// slot "enrollment" (enrollclient.State JSON). The app sends only what
	// minting needs (macState); anything else, the enrollment secret
	// included, is dropped unread. Only an approved one can mint.
	State json.RawMessage `json:"state"`
	// Create: the token's name and lifetime (0: 90 days).
	Name string `json:"name,omitempty"`
	Days int    `json:"days,omitempty"`
	// Revoke: the token's ID.
	ID string `json:"id,omitempty"`
}

// Agent-token failure codes (agentTokenOutput.Error); the app words them.
const (
	agentErrInvalid       = "invalid"        // bad input; Detail says what
	agentErrNotEnrolled   = "not_enrolled"   // no approved enrollment to mint with
	agentErrRefused       = "refused"        // 401/403: this Mac is not approved or not assigned to a person
	agentErrContinuityOff = "continuity_off" // 404: the Manager has agent tokens off (JM_CONTINUITY)
	agentErrManager       = "manager"        // another refusal; Detail is the Manager's text
	agentErrUnreachable   = "unreachable"    // no answer from the Manager
	agentErrMaybeMinted   = "maybe_minted"   // create: no answer, so a token may exist
	agentErrStoreFailed   = "store_failed"   // create: not stored, and revoked on the Manager
	agentErrStoreLive     = "store_failed_live"
	agentErrKeychain      = "keychain" // revoke: the token stored here could not be deleted
)

// agentTokenOutput is the output of every agent-token export. It never
// holds a token.
type agentTokenOutput struct {
	OK bool `json:"ok"`
	// Continuity (list): the Manager answered the agent-token routes. A 404
	// there (JM_CONTINUITY off, or a Manager without agent tokens) is
	// OK with Continuity false.
	Continuity bool                 `json:"continuity"`
	Tokens     []agentmcp.TokenView `json:"tokens,omitempty"`
	// Token (create) is the minted token's description.
	Token *agentmcp.TokenView `json:"token,omitempty"`
	// StoredID is the token stored on this Mac for juicemount-mcp serve
	// (list, create).
	StoredID string `json:"stored_id,omitempty"`
	// ReplacedID (create) was stored here before, when its ID is known; it
	// stays valid on the Manager until revoked.
	ReplacedID string `json:"replaced_id,omitempty"`
	// PreviousRemoved (a failed create): storing the new token removed the
	// one stored here before, so agents on this Mac have none now, and that
	// token (ReplacedID, when known) is still live on the Manager. Set
	// whenever an item was removed, whether or not its ID could be told.
	PreviousRemoved bool `json:"previous_removed,omitempty"`
	// Gone (revoke): the Manager no longer had the token. Deleted: it was
	// the one stored here and was deleted.
	Gone    bool `json:"gone,omitempty"`
	Deleted bool `json:"deleted,omitempty"`
	// Warning is a plain-http Manager address to another host.
	Warning string `json:"warning,omitempty"`
	Error   string `json:"error,omitempty"`
	// Detail explains Error (never a secret).
	Detail string `json:"detail,omitempty"`
}

// agentTokenHTTP is the Manager client; tests substitute it.
var agentTokenHTTP = agentmcp.NewHTTPClient()

// agentTokenStore is where a minted token is kept; tests substitute it.
var agentTokenStore = func() agentmcp.TokenStore {
	return appTokenStore{credstore.AppAgentToken{Trusted: bundledMCP()}}
}

// bundledMCP is the juicemount-mcp beside this process's executable (the
// app's Contents/MacOS), when there is one: the program the token's
// Keychain item trusts besides the app. A build without it (swift run)
// stores the token for the app alone.
func bundledMCP() []string {
	exe, err := os.Executable()
	if err != nil {
		return nil
	}
	p := filepath.Join(filepath.Dir(exe), "juicemount-mcp")
	if st, err := os.Stat(p); err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0o111 == 0 {
		return nil
	}
	return []string{p}
}

// appTokenStore keeps the record with credstore.AppAgentToken.
type appTokenStore struct{ item credstore.AppAgentToken }

func (s appTokenStore) Save(r agentmcp.TokenRecord) error {
	err := s.item.Save(r.Encode(), r.ID)
	if errors.Is(err, credstore.ErrStoredRecordRemoved) {
		return fmt.Errorf("%w (%v)", agentmcp.ErrPreviousTokenRemoved, err)
	}
	return err
}

func (s appTokenStore) Load() (agentmcp.TokenRecord, error) {
	raw, err := s.item.Load()
	if errors.Is(err, credstore.ErrNotFound) {
		return agentmcp.TokenRecord{}, agentmcp.ErrNoToken
	}
	if err != nil {
		return agentmcp.TokenRecord{}, err
	}
	return agentmcp.ParseTokenRecord(raw)
}

func (s appTokenStore) Delete() error { return s.item.Delete() }

// StoredID tells the stored token's ID from the item's label, never from
// the token (agentmcp.TokenIDReader): listing, replacing and revoking never
// show the Keychain's prompt, even for an item another program made. An
// item labelled before IDs were kept there is unknown.
func (s appTokenStore) StoredID() (string, bool) {
	id, err := s.item.StoredID()
	return id, err == nil && id != ""
}

//export NFSServerAgentTokenCreate
func NFSServerAgentTokenCreate(input *C.char) *C.char {
	return C.CString(agentTokenCreate(C.GoString(input)))
}

//export NFSServerAgentTokenList
func NFSServerAgentTokenList(input *C.char) *C.char {
	return C.CString(agentTokenList(C.GoString(input)))
}

//export NFSServerAgentTokenRevoke
func NFSServerAgentTokenRevoke(input *C.char) *C.char {
	return C.CString(agentTokenRevoke(C.GoString(input)))
}

// agentTokenCall bounds one export's Manager requests (a create may mint
// and then revoke).
const agentTokenCall = 2 * time.Minute

// macState is what minting needs of the enrollment record: no secret but
// the poll token the Mac authenticates with.
type macState struct {
	ManagerURL string `json:"manager_url"`
	InstanceID string `json:"instance_id"`
	PollToken  string `json:"poll_token"`
	Status     string `json:"status"`
}

// agentTokenClient reads the input and makes a Manager client as this Mac.
func agentTokenClient(raw string) (agentTokenInput, agentmcp.MacCredentials, *agentmcp.Client, *agentTokenOutput) {
	var in agentTokenInput
	if err := json.Unmarshal([]byte(raw), &in); err != nil {
		return in, agentmcp.MacCredentials{}, nil, &agentTokenOutput{Error: agentErrInvalid, Detail: "invalid agent token input"}
	}
	var st macState
	if len(in.State) == 0 || string(in.State) == "null" || json.Unmarshal(in.State, &st) != nil {
		return in, agentmcp.MacCredentials{}, nil, &agentTokenOutput{Error: agentErrNotEnrolled, Detail: "this Mac has no Manager enrollment"}
	}
	in.State = nil // the rest of the record, if the app sent it, goes no further
	state, _ := json.Marshal(st)
	device, _ := deviceid.Fingerprint()
	m, err := agentmcp.MacCredentialsFromState(string(state), device)
	if err != nil {
		return in, m, nil, &agentTokenOutput{Error: agentErrNotEnrolled, Detail: err.Error()}
	}
	return in, m, agentmcp.NewMacClient(m, agentTokenHTTP), nil
}

// agentTokenFailure turns a failed request into an output.
func agentTokenFailure(err error) agentTokenOutput {
	var ae *agentmcp.APIError
	switch {
	case errors.Is(err, agentmcp.ErrTokenName), errors.Is(err, agentmcp.ErrTokenDays), errors.Is(err, agentmcp.ErrTokenID):
		return agentTokenOutput{Error: agentErrInvalid, Detail: err.Error()}
	case errors.As(err, &ae) && (ae.Status == http.StatusUnauthorized || ae.Status == http.StatusForbidden):
		return agentTokenOutput{Error: agentErrRefused, Detail: ae.Message}
	case errors.As(err, &ae) && ae.Status == http.StatusNotFound:
		return agentTokenOutput{Error: agentErrContinuityOff, Detail: ae.Message}
	case errors.As(err, &ae):
		return agentTokenOutput{Error: agentErrManager, Detail: ae.Message}
	}
	return agentTokenOutput{Error: agentErrUnreachable, Detail: err.Error()}
}

// agentTokenJSON encodes out with every Manager-supplied text cleaned
// (agentmcp.CleanText) for display.
func agentTokenJSON(out agentTokenOutput) string {
	clean := func(v *agentmcp.TokenView) {
		v.Name, v.PersonName = agentmcp.CleanText(v.Name), agentmcp.CleanText(v.PersonName)
	}
	for i := range out.Tokens {
		clean(&out.Tokens[i])
	}
	if out.Token != nil {
		v := *out.Token
		clean(&v)
		out.Token = &v
	}
	out.Detail, out.Warning = agentmcp.CleanText(out.Detail), agentmcp.CleanText(out.Warning)
	b, err := json.Marshal(out)
	if err != nil {
		return `{"ok":false,"error":"invalid","detail":"could not encode the result"}`
	}
	return string(b)
}

// agentTokenCreate mints a token and stores it for juicemount-mcp serve.
func agentTokenCreate(raw string) string {
	in, m, c, fail := agentTokenClient(raw)
	if fail != nil {
		return agentTokenJSON(*fail)
	}
	if _, err := agentmcp.CheckMint(in.Name, in.Days); err != nil {
		return agentTokenJSON(agentTokenFailure(err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), agentTokenCall)
	defer cancel()
	warning := agentmcp.CleartextWarning(m.ManagerURL)
	minted, err := agentmcp.MintToken(ctx, c, in.Name, in.Days)
	if err != nil {
		out := agentTokenFailure(err)
		if agentmcp.MayHaveMinted(err) {
			out.Error = agentErrMaybeMinted
		}
		out.Warning = warning
		jmlog.Warn("agent token: minting failed", "code", out.Error, "detail", out.Detail)
		return agentTokenJSON(out)
	}
	v := minted.View
	replaced, err := agentmcp.StoreMinted(ctx, c, agentTokenStore(), m.ManagerURL, minted)
	var se *agentmcp.StoreError
	if errors.As(err, &se) {
		out := agentTokenOutput{Error: agentErrStoreFailed, Detail: se.Err.Error(), Warning: warning}
		if se.RevokeErr != nil {
			out.Error = agentErrStoreLive
			out.Token = &v
		}
		if se.PrevRemoved() {
			out.PreviousRemoved = true
			if se.PrevStored {
				out.ReplacedID = se.Prev.ID
			}
		}
		jmlog.Warn("agent token: minted but not stored", "id", v.ID, "revoked", se.RevokeErr == nil, "error", se.Err.Error())
		return agentTokenJSON(out)
	}
	if err != nil {
		return agentTokenJSON(agentTokenOutput{Error: agentErrStoreFailed, Detail: err.Error(), Warning: warning})
	}
	out := agentTokenOutput{OK: true, Continuity: true, Token: &v, StoredID: v.ID, Warning: warning}
	if replaced != nil {
		out.ReplacedID = replaced.ID
	}
	jmlog.Info("agent token: minted and stored for juicemount-mcp", "id", v.ID, "name", v.Name, "replaced", out.ReplacedID)
	return agentTokenJSON(out)
}

// agentTokenList lists this person's tokens.
func agentTokenList(raw string) string {
	_, m, c, fail := agentTokenClient(raw)
	if fail != nil {
		return agentTokenJSON(*fail)
	}
	ctx, cancel := context.WithTimeout(context.Background(), agentTokenCall)
	defer cancel()
	tokens, err := agentmcp.ListTokens(ctx, c)
	if err != nil {
		out := agentTokenFailure(err)
		if out.Error == agentErrContinuityOff {
			return agentTokenJSON(agentTokenOutput{OK: true, Continuity: false})
		}
		return agentTokenJSON(out)
	}
	return agentTokenJSON(agentTokenOutput{OK: true, Continuity: true, Tokens: tokens,
		StoredID: agentmcp.StoredTokenID(agentTokenStore()), Warning: agentmcp.CleartextWarning(m.ManagerURL)})
}

// agentTokenRevoke revokes a token on the Manager and, when it is the one
// stored here, deletes it here too.
func agentTokenRevoke(raw string) string {
	in, _, c, fail := agentTokenClient(raw)
	if fail != nil {
		return agentTokenJSON(*fail)
	}
	ctx, cancel := context.WithTimeout(context.Background(), agentTokenCall)
	defer cancel()
	gone, err := agentmcp.RevokeToken(ctx, c, in.ID)
	if err != nil {
		return agentTokenJSON(agentTokenFailure(err))
	}
	deleted, err := agentmcp.ForgetStored(agentTokenStore(), in.ID)
	if err != nil {
		// Revoked on the Manager: serve's next call is refused anyway.
		return agentTokenJSON(agentTokenOutput{Error: agentErrKeychain, Detail: err.Error(), Gone: gone})
	}
	jmlog.Info("agent token: revoked", "id", in.ID, "already_gone", gone, "deleted_here", deleted)
	return agentTokenJSON(agentTokenOutput{OK: true, Continuity: true, Gone: gone, Deleted: deleted})
}
