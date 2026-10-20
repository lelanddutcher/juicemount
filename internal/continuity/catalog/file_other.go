//go:build !darwin && !linux

package catalog

import "os"

func checkFileIdentity(os.FileInfo) error {
	return corrupt("catalog file identity is unqualified on this platform")
}
func openRegular(string, bool) (*os.File, error) {
	return nil, corrupt("catalog file identity is unqualified on this platform")
}
