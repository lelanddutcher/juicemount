package manager

// Teams/seats authentication (Tier-2 T2.3).
// SQLite user accounts + session tokens for small teams.

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type authUser struct {
	ID       int64
	Username string
	Role     string
}

type authStore struct {
	db *sql.DB
	mu sync.Mutex
}

func newAuthStore(dbPath string) (*authStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("auth open: %w", err)
	}
	schema := `
	CREATE TABLE IF NOT EXISTS auth_users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT UNIQUE NOT NULL,
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'member',
		created_at INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS auth_sessions (
		token TEXT PRIMARY KEY,
		user_id INTEGER NOT NULL,
		expires_at INTEGER NOT NULL
	);`
	if _, err = db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &authStore{db: db}, nil
}

func (a *authStore) hasUsers() bool {
	var n int
	a.db.QueryRow("SELECT COUNT(*) FROM auth_users").Scan(&n)
	return n > 0
}

func (a *authStore) createUser(username, pwHash, role string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.db.Exec(
		"INSERT OR IGNORE INTO auth_users(username,password_hash,role,created_at) VALUES(?,?,?,?)",
		username, pwHash, role, time.Now().Unix())
	return err
}

func (a *authStore) verifyLogin(username, password string) (string, string, error) {
	var hash, role string
	var id int64
	err := a.db.QueryRow(
		"SELECT id,password_hash,role FROM auth_users WHERE username=?",
		username).Scan(&id, &hash, &role)
	if err != nil {
		return "", "", fmt.Errorf("user not found")
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return "", "", fmt.Errorf("invalid password")
	}
	return username, role, nil
}

func (a *authStore) createSession(token string, expires int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.db.Exec(
		"INSERT OR REPLACE INTO auth_sessions(token,user_id,expires_at) VALUES(?,?,?)",
		token, 0, expires)
	return err
}

func (a *authStore) validSession(token string) bool {
	var exp int64
	err := a.db.QueryRow(
		"SELECT expires_at FROM auth_sessions WHERE token=?", token).Scan(&exp)
	return err == nil && time.Now().Unix() < exp
}

func randomTokenHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hexEncodeToString(b)
}

func hexEncodeToString(b []byte) string {
	const hexChars = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexChars[v>>4]
		out[i*2+1] = hexChars[v&0x0f]
	}
	return string(out)
}

var globalAuthStore *authStore

func InitAuth(dbPath string) error {
	store, err := newAuthStore(dbPath)
	if err != nil {
		return err
	}
	globalAuthStore = store
	if !store.hasUsers() {
		pw := randomTokenHex(16)
		hash, _ := bcrypt.GenerateFromPassword([]byte(pw), 10)
		store.createUser("admin", string(hash), "admin")
		fmt.Printf("*** JuiceMount Link: default admin created — user=admin pass=%s ***\n", pw)
	}
	return nil
}

func HandleAuthLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	username, role, err := globalAuthStore.verifyLogin(req.Username, req.Password)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid credentials"})
		return
	}
	token := randomTokenHex(32)
	expires := time.Now().Add(24 * time.Hour).Unix()
	globalAuthStore.createSession(token, expires)
	json.NewEncoder(w).Encode(map[string]any{
		"ok": true, "token": token,
		"username": username, "role": role, "expires_at": expires,
	})
}

func bearerAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if globalAuthStore != nil {
			auth := r.Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") &&
				globalAuthStore.validSession(strings.TrimPrefix(auth, "Bearer ")) {
				next(w, r)
				return
			}
		}
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	}
}
