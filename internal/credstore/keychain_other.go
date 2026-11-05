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

// keychainLoadAsIs: as keychainLoadInteractive.
func keychainLoadAsIs(service, slot, account string) (secret, stamp string, err error) {
	return keychainLoadInteractive(service, slot, account)
}

// keychainAvailable: this build has no Keychain.
const keychainAvailable = false

func keychainSave(service, slot, account, secret, device string, allowUI bool) error {
	return ErrNoKeychain
}

func keychainDelete(service, slot string, allowUI bool) error { return ErrNoKeychain }

func keychainDeleteAsIs(service, slot string) error { return ErrNoKeychain }

func keychainReplaceTrusting(service, slot, account, secret, device string, trusted []string) error {
	return ErrNoKeychain
}
