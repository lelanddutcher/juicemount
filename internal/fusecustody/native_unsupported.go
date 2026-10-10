//go:build !darwin || !cgo

package fusecustody

func nativeCapability() bool { return false }

func openNativeRoot(string, int, int, string) (rootCustody, error) {
	return nil, ErrUnsupported
}
