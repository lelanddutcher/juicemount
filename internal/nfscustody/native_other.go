//go:build !darwin || !cgo

package nfscustody

func nativeCapability() bool                             { return false }
func openNativeRoot(string, string) (rootCustody, error) { return nil, ErrUnsupported }
