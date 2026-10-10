//go:build !(darwin && cgo)

package credstore

func keychainLoad(service, slot, account string) (secret, stamp string, err error) {
	return "", "", ErrNotFound
}
