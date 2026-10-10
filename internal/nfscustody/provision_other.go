//go:build !darwin || !cgo

package nfscustody

func provisionACL(int, bool) error { return ErrUnsupported }

func nativeProvision(req provisionRequest) (provisionReceipt, bool, error) {
	return provisionReceipt{Request: req}, false, ErrUnsupported
}
