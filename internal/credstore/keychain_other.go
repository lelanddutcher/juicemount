//go:build !(darwin && cgo)

package credstore

import "runtime"

func keychainLoad(service, slot, account string) (secret, stamp string, err error) {
	return "", "", ErrNotFound
}

// keychainLoadInteractive: no Keychain. On a Mac (a build without cgo) the
// record exists but cannot be read.
func keychainLoadInteractive(service, slot, account string) (secret, stamp string, err error) {
	if runtime.GOOS == "darwin" {
		return "", "", ErrNoKeychain
	}
	return "", "", ErrNotFound
}

// keychainAvailable: this build has no Keychain.
const keychainAvailable = false

func keychainSave(service, slot, account, secret, device string, allowUI bool) error {
	return ErrNoKeychain
}

func keychainDelete(service, slot string, allowUI bool) error { return ErrNoKeychain }
