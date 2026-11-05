package agentmcp

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// The pieces the CLI and the app share (bridge agenttokens.go): which
// failures may have left a token on the Manager, and storing a minted token
// (revoked when it cannot be stored).
func TestMintSharedPieces(t *testing.T) {
	for err, want := range map[error]bool{
		ErrTokenName: false, ErrTokenDays: false, errMalformedMint: false,
		&APIError{Status: 409}: false, &APIError{Status: 403}: false, &APIError{Status: 500}: true,
		fmt.Errorf("%w (dial: refused)", errNoAnswer): true, context.DeadlineExceeded: true,
	} {
		if got := MayHaveMinted(err); got != want {
			t.Errorf("MayHaveMinted(%v) = %v", err, got)
		}
	}
	e := newTokenEnv(t)
	c := NewMacClient(e.mustMac(t), e.tc.HTTP)
	e.f.on("POST /manager/api/agent-tokens", 200, mintAnswer())
	m, err := MintToken(context.Background(), c, " Claude Desktop ", 0)
	if err != nil || m.Token != mintedToken || m.View.ID != mintedID || m.View.PersonName != "Ann" {
		t.Fatalf("mint %+v %v", m.View, err)
	}
	if _, err := MintToken(context.Background(), c, "", 0); !errors.Is(err, ErrTokenName) || len(e.f.requests()) != 1 {
		t.Fatalf("no name: %v", err)
	}
	old := TokenRecord{ID: "1111111111111111", Token: "jma_1111111111111111_" + mintedSecret}
	e.store.rec = &old
	replaced, err := StoreMinted(context.Background(), c, e.store, "http://nas.example:30190", m)
	if err != nil || replaced == nil || replaced.ID != old.ID || e.store.rec.ID != mintedID || e.store.rec.ManagerURL != "http://nas.example:30190" {
		t.Fatalf("store: %+v %v", replaced, err)
	}
	if replaced, err := StoreMinted(context.Background(), c, e.store, "http://nas.example:30190", m); err != nil || replaced != nil {
		t.Fatalf("storing the same token again replaced %+v (%v)", replaced, err)
	}
	if got := StoredTokenID(e.store); got != mintedID {
		t.Fatalf("stored ID %q", got)
	}
	e.store.saveErr = fmt.Errorf("%w (keychain)", ErrPreviousTokenRemoved)
	e.f.on("POST /manager/api/agent-tokens/"+mintedID+"/revoke", 200, map[string]string{"revoked": mintedID})
	_, err = StoreMinted(context.Background(), c, e.store, "http://nas.example:30190", m)
	var se *StoreError
	if !errors.As(err, &se) || se.RevokeErr != nil || !se.PrevRemoved() || se.Prev.ID != mintedID || !errors.Is(err, ErrPreviousTokenRemoved) {
		t.Fatalf("unstored: %v %+v", err, se)
	}
	if deleted, err := ForgetStored(e.store, "aaaaaaaaaaaaaaaa"); err != nil || deleted || e.store.rec == nil {
		t.Fatalf("forget another: %v %v", deleted, err)
	}
	if deleted, err := ForgetStored(e.store, mintedID); err != nil || !deleted || e.store.rec != nil {
		t.Fatalf("forget stored: %v %v", deleted, err)
	}
	if gone, err := RevokeToken(context.Background(), c, "x"); !errors.Is(err, ErrTokenID) || gone {
		t.Fatalf("bad ID: %v", err)
	}
	e.noSecrets(t, false)
}

// idStore is a TokenIDReader: its token is never read.
type idStore struct {
	t       *testing.T
	id      string
	known   bool
	saveErr error
	deleted bool
}

func (s *idStore) Save(r TokenRecord) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.id, s.known = r.ID, true
	return nil
}

func (s *idStore) Load() (TokenRecord, error) {
	s.t.Error("the stored token was read; its ID is beside it")
	return TokenRecord{}, errors.New("read")
}

func (s *idStore) Delete() error { s.deleted = true; return nil }

func (s *idStore) StoredID() (string, bool) { return s.id, s.known }

// A store that tells the stored token's ID without reading it (the app,
// where reading an item another program made prompts) is never read: not to
// list, not to replace, not to forget.
func TestTokenIDReaderNeverRead(t *testing.T) {
	e := newTokenEnv(t)
	c := NewMacClient(e.mustMac(t), e.tc.HTTP)
	m := Minted{Token: mintedToken, View: TokenView{ID: mintedID, Name: "x"}}
	s := &idStore{t: t, id: "1111111111111111", known: true}
	if got := StoredTokenID(s); got != "1111111111111111" {
		t.Fatalf("stored ID %q", got)
	}
	replaced, err := StoreMinted(context.Background(), c, s, "http://nas.example:30190", m)
	if err != nil || replaced == nil || replaced.ID != "1111111111111111" || s.id != mintedID {
		t.Fatalf("store: %+v %v", replaced, err)
	}
	if deleted, err := ForgetStored(s, "aaaaaaaaaaaaaaaa"); err != nil || deleted || s.deleted {
		t.Fatalf("forget another: %v %v", deleted, err)
	}
	if deleted, err := ForgetStored(s, mintedID); err != nil || !deleted || !s.deleted {
		t.Fatalf("forget stored: %v %v", deleted, err)
	}
	// An item without a known ID (stored before IDs were kept beside it) is
	// unknown, never read, and never deleted on a guess.
	legacy := &idStore{t: t, known: false}
	if got := StoredTokenID(legacy); got != "" {
		t.Fatalf("legacy stored ID %q", got)
	}
	if deleted, err := ForgetStored(legacy, mintedID); err != nil || deleted || legacy.deleted {
		t.Fatalf("forget legacy: %v %v", deleted, err)
	}
	// A failed store that removed the unknown item before it says so.
	legacy.saveErr = fmt.Errorf("%w (keychain)", ErrPreviousTokenRemoved)
	e.f.on("POST /manager/api/agent-tokens/"+mintedID+"/revoke", 200, map[string]string{"revoked": mintedID})
	_, err = StoreMinted(context.Background(), c, legacy, "http://nas.example:30190", m)
	var se *StoreError
	if !errors.As(err, &se) || !se.PrevRemoved() || se.PrevStored {
		t.Fatalf("removed unknown: %v %+v", err, se)
	}
}

// Text the Manager supplies loses control and format characters (bidi
// overrides, zero-width), so it cannot reorder or hide what is around it.
func TestCleanText(t *testing.T) {
	if got := CleanText("Ann‮⁦gpj.exe​⁩\x07x"); got != "Anngpj.exe x" {
		t.Fatalf("CleanText = %q", got)
	}
	if got := errorText(400, []byte("{\"error\":\" name‮ too long‍ \"}")); got != "name too long" {
		t.Fatalf("errorText = %q", got)
	}
}
