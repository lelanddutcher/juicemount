package credstore

// Agent tokens (continuity slice 6b). juicemount-mcp, the MCP server agents
// run on a Mac, keeps one agent token: a JSON record holding the token and
// the Manager it was minted on. Unlike the app's slots it is written here,
// by juicemount-mcp itself, in an item of its own (service
// AgentTokenService), so it never touches the items the app owns.
//
//   - darwin with cgo: one generic-password item in the login Keychain,
//     stamped with this Mac's device digest like the app's items. It is
//     created with the default access list, which trusts the creating
//     program (juicemount-mcp), so `serve` reads it with user interaction
//     disabled.
//   - darwin without cgo: no storage (refused); a token is never written to a
//     plain file on a Mac.
//   - elsewhere: a private 0600 file, credential-agent-token.json, in
//     os.UserConfigDir()/juicemount (a 0700 directory), in the isolated
//     entry format.
//   - an isolated acceptance root (internal/appisolation), any platform: the
//     same file inside the root.
//
// The app's Manager enrollment record (slot Enrollment) is what mints a
// token: LoadEnrollment reads it. On a Mac the item's access list names the
// app and its service, not juicemount-mcp, so the read may show the macOS
// Keychain prompt (it is run by a person in a terminal).

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

const (
	// AgentTokenService is the Keychain service of juicemount-mcp's item.
	AgentTokenService = "com.juicemount.agent-token"
	// AgentToken is its slot (the Keychain account attribute, the file stem).
	AgentToken Slot = "agent-token"
	// agentTokenAccount is the item's fixed endpoint (generic attribute):
	// which Manager the token belongs to is in the record.
	agentTokenAccount = "juicemount-mcp"

	// Enrollment is the app's Manager enrollment record (EnrollmentRecord in
	// RedisCredentialStore.swift: enrollclient.State JSON), stored under the
	// fixed account EnrollmentAccount.
	Enrollment        Slot = "enrollment"
	EnrollmentAccount      = "manager-enrollment"
)

// ErrNoKeychain means this build cannot store a secret on this platform
// (a darwin build without cgo).
var ErrNoKeychain = errors.New("this build of juicemount-mcp has no Keychain support (built without cgo); use a release build")

// SaveAgentToken stores record (the token record JSON), replacing any.
func SaveAgentToken(record string) error {
	if record == "" {
		return errors.New("empty agent token record")
	}
	device, _ := deviceid.Fingerprint()
	if path, ok, err := agentTokenFile(); err != nil {
		return err
	} else if ok {
		return saveFile(path, isolatedEntry{Account: agentTokenAccount, Secret: record, Device: device})
	}
	return keychainSave(AgentTokenService, string(AgentToken), agentTokenAccount, record, device, agentTokenUI)
}

// LoadAgentToken returns the stored record (ErrNotFound when none; on a Mac
// build without cgo, ErrNoKeychain). It shows no UI unless
// AllowAgentTokenUI was called. A record stamped for another Mac is
// ErrOtherDevice.
func LoadAgentToken() (string, error) {
	if path, ok, err := agentTokenFile(); err != nil {
		return "", err
	} else if ok {
		return loadIsolated(path, agentTokenAccount)
	}
	if !keychainAvailable {
		return "", ErrNoKeychain
	}
	read := keychainRead
	if agentTokenUI {
		read = keychainReadInteractive
	}
	secret, stamp, err := read(AgentTokenService, string(AgentToken), agentTokenAccount)
	if err != nil {
		return "", err
	}
	if err := checkDevice(deviceFromComment(stamp)); err != nil {
		return "", err
	}
	return secret, nil
}

// DeleteAgentToken removes the stored record; none stored is not an error.
func DeleteAgentToken() error {
	if path, ok, err := agentTokenFile(); err != nil {
		return err
	} else if ok {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return keychainDelete(AgentTokenService, string(AgentToken), agentTokenUI)
}

// agentTokenUI: the agent token's Keychain calls may ask the person.
var agentTokenUI bool

// AllowAgentTokenUI lets the agent token's Keychain calls show the
// Keychain's prompts for the rest of the process: for juicemount-mcp's token
// commands, which a person runs in a terminal (after a rebuild or update,
// the Keychain asks before another build of the program may use the item).
// juicemount-mcp serve never calls it: it runs under an agent, with no one
// to answer.
func AllowAgentTokenUI() { agentTokenUI = true }

// LoadEnrollment returns the app's enrollment record (enrollclient.State
// JSON). On a Mac it may show the Keychain prompt: the item trusts the app
// and its service, and this is a person's command.
func LoadEnrollment() (string, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", err
	}
	if isolated {
		return loadIsolated(filepath.Join(scope.Root, isolatedLeaf(Enrollment)), EnrollmentAccount)
	}
	secret, stamp, err := keychainReadInteractive(keychainService, string(Enrollment), EnrollmentAccount)
	if err != nil {
		return "", err
	}
	if err := checkDevice(deviceFromComment(stamp)); err != nil {
		return "", err
	}
	return secret, nil
}

// agentTokenFile is the file that holds the record when it is kept in a
// file (ok): inside an isolated root, or off darwin.
func agentTokenFile() (string, bool, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", false, err
	}
	if isolated {
		return filepath.Join(scope.Root, isolatedLeaf(AgentToken)), true, nil
	}
	if !agentTokenInFile {
		return "", false, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", false, err
	}
	return filepath.Join(dir, "juicemount", isolatedLeaf(AgentToken)), true, nil
}

// keychainReadInteractive is the Keychain reader LoadEnrollment uses
// (tests substitute it).
var keychainReadInteractive = keychainLoadInteractive
