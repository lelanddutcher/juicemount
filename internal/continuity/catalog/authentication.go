package catalog

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type AuthHandle struct {
	store                 *Store
	incarnation, selector string
	subject               SubjectID
	generation            int64
}
type credential struct {
	selector, incarnation       string
	subject                     SubjectID
	scope                       Scope
	actions                     []string
	verifier                    []byte
	issued, expires, generation int64
	revoked                     bool
}

func (s *Store) readSnapshot(ctx context.Context) (*sql.Tx, func() error, error) {
	db, err := openDB(s.path, true)
	if err != nil {
		return nil, nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, errors.Join(err, db.Close())
	}
	done := func() error {
		err := tx.Rollback()
		if errors.Is(err, sql.ErrTxDone) {
			err = nil
		}
		return errors.Join(err, db.Close())
	}
	if err := checkSettings(ctx, tx); err != nil {
		return nil, nil, errors.Join(err, done())
	}
	return tx, done, nil
}

func readCredential(ctx context.Context, q queryer, library continuity.LibraryID, selector string) (credential, error) {
	var c credential
	var lib continuity.LibraryID
	var actions []byte
	err := q.QueryRowContext(ctx, `SELECT selector,library_id,incarnation,subject_id,scope_kind,COALESCE(asset_id,''),CASE WHEN length(actions)<=256 THEN actions END,CASE WHEN length(verifier)=32 THEN verifier END,issued_unix,expires_unix,generation,revoked FROM catalog_credentials WHERE selector=?`, selector).Scan(&c.selector, &lib, &c.incarnation, &c.subject, &c.scope.Kind, &c.scope.AssetID, &actions, &c.verifier, &c.issued, &c.expires, &c.generation, &c.revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, corrupt("credential fields")
	}
	if lib != library || c.selector != selector || !validRandomID(c.selector, "cred_") || !validRandomID(c.incarnation, "auth_") || c.subject.Validate() != nil || !c.scope.valid() || decodeCanonicalValue(actions, &c.actions) != nil || !validActions(c.actions) || len(c.verifier) != 32 || c.expires <= c.issued || c.generation <= 0 {
		return credential{}, corrupt("credential fields")
	}
	return c, nil
}

func credentialStatus(ctx context.Context, q queryer, library continuity.LibraryID, selector string, now int64) (string, error) {
	c, err := readCredential(ctx, q, library, selector)
	if err != nil {
		return "", err
	}
	a, err := readAuthority(ctx, q, library)
	if err != nil {
		return "", err
	}
	if a.Incarnation != c.incarnation {
		return "invalidated", nil
	}
	if c.revoked {
		return "revoked", nil
	}
	if now < c.issued {
		return "not_yet_valid", nil
	}
	if now >= c.expires {
		return "expired", nil
	}
	s, err := readSubject(ctx, q, library, c.subject)
	if err != nil {
		return "", err
	}
	if !s.Value.Enabled {
		return "subject_disabled", nil
	}
	return "active", nil
}

func (s *Store) Authenticate(ctx context.Context, library continuity.LibraryID, bearer []byte) (result *AuthHandle, resultErr error) {
	if err := s.enter(); err != nil {
		if errors.Is(err, ErrClosed) {
			err = ErrUnauthenticated
		}
		return nil, err
	}
	defer s.leave()
	if s.version != authoritySchemaVersion {
		return nil, ErrUpgradeRequired
	}
	if library != s.library {
		return nil, ErrUnauthenticated
	}
	tx, done, err := s.readSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	handle, _, _, err := s.authenticateSnapshot(ctx, tx, library, bearer)
	return handle, err
}

// authenticateSnapshot shares the complete verifier/current-authority check
// with discovery without opening a second, inconsistent metadata snapshot.
func (s *Store) authenticateSnapshot(ctx context.Context, q queryer, library continuity.LibraryID, bearer []byte) (*AuthHandle, credential, Subject, error) {
	if len(bearer) != 102 || string(bearer[:5]) != "jmc1_" || bearer[37] != '.' || strings.ToLower(string(bearer)) != string(bearer) {
		return nil, credential{}, Subject{}, ErrUnauthenticated
	}
	selector := "cred_" + string(bearer[5:37])
	if !validRandomID(selector, "cred_") {
		return nil, credential{}, Subject{}, ErrUnauthenticated
	}
	secret, err := hex.DecodeString(string(bearer[38:]))
	if err != nil || len(secret) != 32 {
		return nil, credential{}, Subject{}, ErrUnauthenticated
	}
	c, err := readCredential(ctx, q, library, selector)
	if err != nil {
		return nil, credential{}, Subject{}, ErrUnauthenticated
	}
	verifier := credentialVerifier(library, c.incarnation, selector, secret)
	if subtle.ConstantTimeCompare(verifier[:], c.verifier) != 1 {
		return nil, credential{}, Subject{}, ErrUnauthenticated
	}
	status, err := credentialStatus(ctx, q, library, selector, s.now().Unix())
	if err != nil {
		return nil, credential{}, Subject{}, err
	}
	if status != "active" {
		return nil, credential{}, Subject{}, ErrUnauthenticated
	}
	sub, err := readSubject(ctx, q, library, c.subject)
	if err != nil {
		return nil, credential{}, Subject{}, err
	}
	return &AuthHandle{s, c.incarnation, selector, c.subject, c.generation}, c, sub, nil
}

func (s *Store) currentHandle(ctx context.Context, q queryer, handle *AuthHandle) (credential, Subject, error) {
	if handle == nil || handle.store != s || !validRandomID(handle.selector, "cred_") || handle.generation <= 0 {
		return credential{}, Subject{}, ErrUnauthenticated
	}
	c, err := readCredential(ctx, q, s.library, handle.selector)
	if err != nil {
		return credential{}, Subject{}, ErrUnauthenticated
	}
	if c.incarnation != handle.incarnation || c.subject != handle.subject || c.generation != handle.generation {
		return credential{}, Subject{}, ErrUnauthenticated
	}
	status, err := credentialStatus(ctx, q, s.library, handle.selector, s.now().Unix())
	if err != nil {
		return credential{}, Subject{}, err
	}
	if status != "active" {
		return credential{}, Subject{}, ErrUnauthenticated
	}
	sub, err := readSubject(ctx, q, s.library, c.subject)
	return c, sub, err
}

func (s *Store) CheckAccess(ctx context.Context, handle *AuthHandle, request AccessRequest) (result AccessDecision, resultErr error) {
	if err := s.enter(); err != nil {
		if errors.Is(err, ErrClosed) {
			err = ErrUnauthenticated
		}
		return AccessDecision{}, err
	}
	defer s.leave()
	if s.version != authoritySchemaVersion {
		return AccessDecision{}, ErrUpgradeRequired
	}
	if request.LibraryID != s.library {
		return AccessDecision{}, ErrUnauthenticated
	}
	if !validAction(request.Action) || request.AssetID.Validate() != nil {
		return AccessDecision{}, invalid("access request")
	}
	tx, done, err := s.readSnapshot(ctx)
	if err != nil {
		return AccessDecision{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	c, _, err := s.currentHandle(ctx, tx, handle)
	if err != nil {
		return AccessDecision{}, err
	}
	if !c.scope.matches(request.AssetID) || !containsAction(c.actions, request.Action) {
		return AccessDecision{}, nil
	}
	if _, err := readAsset(ctx, tx, s.library, request.AssetID); errors.Is(err, ErrNotFound) {
		return AccessDecision{}, nil
	} else if err != nil {
		return AccessDecision{}, err
	}
	allowed := false
	// The index constrains subject and the two exact scope matches; action sets
	// are bounded to five entries, never a library-wide grant scan.
	err = walkRows(ctx, tx, `SELECT `+grantColumns+` FROM policy_grants WHERE subject_id=? AND removed=0 AND (scope_kind='library' OR (scope_kind='asset' AND asset_id=?))`, func(rows *sql.Rows) error {
		g, err := scanGrant(rows, s.library)
		if err != nil {
			return err
		}
		if containsAction(g.Value.Actions, request.Action) {
			allowed = true
		}
		return nil
	}, c.subject, request.AssetID)
	return AccessDecision{allowed}, err
}

func (s *Store) WhoAmI(ctx context.Context, handle *AuthHandle) (result Identity, resultErr error) {
	if err := s.enter(); err != nil {
		if errors.Is(err, ErrClosed) {
			err = ErrUnauthenticated
		}
		return Identity{}, err
	}
	defer s.leave()
	if s.version != authoritySchemaVersion {
		return Identity{}, ErrUpgradeRequired
	}
	tx, done, err := s.readSnapshot(ctx)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	c, sub, err := s.currentHandle(ctx, tx, handle)
	if err != nil {
		return Identity{}, err
	}
	return Identity{c.subject, sub.Value.Kind, c.selector, c.expires}, nil
}

func (r IssuanceResult) String() string   { return "catalog issuance result (secret redacted)" }
func (r IssuanceResult) GoString() string { return r.String() }
