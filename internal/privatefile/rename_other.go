//go:build !darwin && !linux

package privatefile

import "errors"

func renameNoReplace(fromFD int,from string,toFD int,to string) error {
    return errors.New("managed no-replace exposure unsupported on this platform")
}
