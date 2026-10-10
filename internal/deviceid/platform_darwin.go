//go:build darwin && cgo

package deviceid

/*
#cgo CFLAGS: -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0 -framework IOKit -framework CoreFoundation
#include <IOKit/IOKitLib.h>
#include <CoreFoundation/CoreFoundation.h>

// Copies the IOPlatformUUID into buf. Returns 0 on success.
static int jm_platform_uuid(char *buf, int len) {
	io_service_t dev = IOServiceGetMatchingService(kIOMainPortDefault,
	                                               IOServiceMatching("IOPlatformExpertDevice"));
	if (dev == IO_OBJECT_NULL) return -1;
	CFTypeRef v = IORegistryEntryCreateCFProperty(dev, CFSTR(kIOPlatformUUIDKey), kCFAllocatorDefault, 0);
	IOObjectRelease(dev);
	if (v == NULL) return -2;
	int rc = -3;
	if (CFGetTypeID(v) == CFStringGetTypeID() &&
	    CFStringGetCString((CFStringRef)v, buf, len, kCFStringEncodingUTF8)) rc = 0;
	CFRelease(v);
	return rc;
}
*/
import "C"

import "unsafe"

func platformUUID() (string, error) {
	var buf [128]C.char
	if C.jm_platform_uuid(&buf[0], C.int(len(buf))) != 0 {
		return "", ErrUnavailable
	}
	return C.GoString((*C.char)(unsafe.Pointer(&buf[0]))), nil
}
