//go:build h4startupacceptance

package main

/*
#include <stdlib.h>
*/
import "C"

import "unsafe"

// This tagged adapter lets Go tests call the exported production entry point.
// cgo cannot be used directly from a _test.go file.
func h4StartFromJSON(raw string) string {
	input := C.CString(raw)
	defer C.free(unsafe.Pointer(input))
	output := NFSServerStart(input)
	defer C.free(unsafe.Pointer(output))
	return C.GoString(output)
}
