package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

var errPreferenceExpired = errors.New("expired_request")
var errPreferenceClock = errors.New("preference clock below durable floor")

type PreferenceRequest struct {
	Version       int                  `json:"version"`
	LibraryID     continuity.LibraryID `json:"library_id"`
	OperationID   string               `json:"operation_id"`
	Kind          string               `json:"kind"`
	AssetID       continuity.AssetID   `json:"asset_id"`
	ExpectedToken *string              `json:"expected_token"`
	ExpiresUnix   int64                `json:"expires_unix"`
}
type PreferenceStateRequest struct {
	Version   int
	LibraryID continuity.LibraryID
	AssetID   continuity.AssetID
}
type preferenceReceipt struct {
	Version     int     `json:"version"`
	OperationID string  `json:"operation_id"`
	Kind        string  `json:"kind"`
	Token       *string `json:"selection_token"`
	Scope       string  `json:"receipt_scope"`
}
type preferenceState struct {
	Version int                `json:"version"`
	AssetID continuity.AssetID `json:"asset_id"`
	Token   *string            `json:"selection_token"`
}
type preferenceControl struct{ floor, subjects, entries, receipts int64 }
type preferenceSubject struct{ generation, entries, receipts int64 }
type preferenceEntry struct {
	asset                    continuity.AssetID
	token                    string
	order, selected, expires int64
}
type preferenceBinding struct{ generation, lower, expiry, floor int64 }

func validSelectionToken(s string) bool {
	if len(s) != 69 || !strings.HasPrefix(s, "sel1_") || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s[5:])
	return err == nil
}
func randomSelectionToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "sel1_" + hex.EncodeToString(b[:]), nil
}
func canonicalPreferenceRequest(r PreferenceRequest) (PreferenceRequest, []byte, error) {
	if r.Version != 2 || r.LibraryID.Validate() != nil || !validOperationID(r.OperationID) || r.AssetID.Validate() != nil || r.ExpiresUnix <= 0 {
		return r, nil, invalid("preference request")
	}
	switch r.Kind {
	case "select":
		if r.ExpectedToken != nil {
			return r, nil, invalid("select expected token")
		}
	case "clear":
		if r.ExpectedToken == nil || !validSelectionToken(*r.ExpectedToken) {
			return r, nil, invalid("clear expected token")
		}
		v := *r.ExpectedToken
		r.ExpectedToken = &v
	default:
		return r, nil, invalid("preference kind")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return r, nil, err
	}
	if len(b) > 1024 {
		return r, nil, invalid("preference request bytes")
	}
	return r, b, nil
}
func decodePreferenceRequest(b []byte) (PreferenceRequest, error) {
	var r PreferenceRequest
	if len(b) > 1024 || strictJSON(b, &r) != nil {
		return r, corrupt("preference request JSON")
	}
	r, expected, err := canonicalPreferenceRequest(r)
	if err != nil || !bytes.Equal(b, expected) {
		return r, corrupt("preference request canonical form")
	}
	return r, nil
}
func decodePreferenceReceipt(b []byte, r PreferenceRequest) (preferenceReceipt, error) {
	var v preferenceReceipt
	if len(b) > 512 || strictJSON(b, &v) != nil {
		return v, corrupt("preference receipt JSON")
	}
	expected, err := json.Marshal(v)
	if err != nil || !bytes.Equal(expected, b) || v.Version != 2 || v.OperationID != r.OperationID || v.Kind != r.Kind || v.Scope != "operation_result" || (v.Kind == "select" && (v.Token == nil || !validSelectionToken(*v.Token))) || (v.Kind == "clear" && v.Token != nil) {
		return v, corrupt("preference receipt canonical form")
	}
	return v, nil
}
func readPreferenceControl(ctx context.Context, q queryer, library continuity.LibraryID) (preferenceControl, error) {
	var c preferenceControl
	var actual continuity.LibraryID
	err := q.QueryRowContext(ctx, `SELECT library_id,clock_floor_unix,subject_count,entry_count,receipt_count FROM preference_control WHERE singleton=1`).Scan(&actual, &c.floor, &c.subjects, &c.entries, &c.receipts)
	if err != nil {
		return c, err
	}
	if actual != library || c.floor < 0 || c.subjects < 0 || c.subjects > 1024 || c.entries < 0 || c.entries > 65536 || c.receipts < 0 || c.receipts > 32768 {
		return c, corrupt("preference control")
	}
	return c, nil
}
func readPreferenceSubject(ctx context.Context, q queryer, library continuity.LibraryID, subject SubjectID) (preferenceSubject, error) {
	var p preferenceSubject
	var actual continuity.LibraryID
	err := q.QueryRowContext(ctx, `SELECT library_id,generation,entry_count,receipt_count FROM preference_subjects WHERE subject_id=?`, subject).Scan(&actual, &p.generation, &p.entries, &p.receipts)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	if actual != library || p.generation <= 0 || p.entries < 0 || p.entries > 256 || p.receipts < 0 || p.receipts > 128 {
		return p, corrupt("preference subject")
	}
	return p, nil
}
func scanPreference(row interface{ Scan(...any) error }) (preferenceEntry, error) {
	var e preferenceEntry
	err := row.Scan(&e.asset, &e.token, &e.order, &e.selected, &e.expires)
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrNotFound
	}
	if err != nil {
		return e, err
	}
	if e.asset.Validate() != nil || !validSelectionToken(e.token) || e.order <= 0 || e.selected < 0 || e.selected > 9223372036852183807 || e.expires != e.selected+preferenceAge {
		return e, corrupt("preference fields")
	}
	return e, nil
}

const preferenceColumns = `asset_id,selection_token,person_order,selected_unix,expires_unix`

func readPreference(ctx context.Context, q queryer, subject SubjectID, asset continuity.AssetID) (preferenceEntry, error) {
	return scanPreference(q.QueryRowContext(ctx, `SELECT `+preferenceColumns+` FROM preferences WHERE subject_id=? AND asset_id=?`, subject, asset))
}
func preferenceClock(now, floor int64) error {
	if now < 0 || now < floor {
		return errPreferenceClock
	}
	return nil
}
func preferenceRequestTime(now, expiry int64, new bool) error {
	if now >= expiry {
		return errPreferenceExpired
	}
	if expiry-now > 900 || new && expiry-now < 60 {
		return invalid("preference request expiry")
	}
	return nil
}
func (b *preferenceBinding) checkTime(now int64) error {
	if err := preferenceClock(now, b.floor); err != nil {
		return err
	}
	if now < b.lower || b.expiry != 0 && now >= b.expiry {
		return errDiscoveryRestart
	}
	return nil
}
