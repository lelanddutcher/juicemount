//go:build !(darwin && cgo)

package deviceid

func platformUUID() (string, error) { return "", ErrUnavailable }
