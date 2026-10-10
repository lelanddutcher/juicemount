//go:build darwin && cgo

package credstore

/*
#cgo CFLAGS: -mmacosx-version-min=26.0 -Wno-deprecated-declarations
#cgo LDFLAGS: -mmacosx-version-min=26.0 -framework Security -framework CoreFoundation
#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
#include <stdlib.h>
#include <string.h>

static char *jm_copy_data(CFDataRef data, int *len) {
	if (data == NULL || CFGetTypeID(data) != CFDataGetTypeID()) { *len = -1; return NULL; }
	CFIndex n = CFDataGetLength(data);
	char *out = malloc(n > 0 ? n : 1);
	if (out == NULL) { *len = -1; return NULL; }
	memcpy(out, CFDataGetBytePtr(data), n);
	*len = (int)n;
	return out;
}

// Reads one generic-password item with user interaction disabled: a legacy
// keychain ACL that does not list this binary is refused (errSecAuthFailed or
// errSecInteractionNotAllowed) instead of showing a SecurityAgent dialog from
// a background LaunchAgent.
//
// SecKeychainSetUserInteractionAllowed(false) is PROCESS-WIDE and sticky: after
// the first read, no Keychain call anywhere in this process may show UI. That
// is intended for the serving LaunchAgent and the volume owner, which have no
// one to answer a dialog; do not call Load from a process that needs Keychain
// UI (the app reads through RedisCredentialStore.swift, never this package).
// The item's comment attribute (kSecAttrComment) carries the device stamp; it
// is copied as UTF-8 into *comment (NULL when absent).
static OSStatus jm_keychain_read(const char *service, const char *account,
                                 char **generic, int *genericLen, char **secret, int *secretLen,
                                 char **comment) {
	SecKeychainSetUserInteractionAllowed(false);
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	const void *keys[] = { kSecClass, kSecAttrService, kSecAttrAccount, kSecReturnData,
	                       kSecReturnAttributes, kSecMatchLimit, kSecUseAuthenticationUI };
	const void *vals[] = { kSecClassGenericPassword, svc, acct, kCFBooleanTrue,
	                       kCFBooleanTrue, kSecMatchLimitOne, kSecUseAuthenticationUIFail };
	CFDictionaryRef q = CFDictionaryCreate(NULL, keys, vals, 7,
	                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
	CFTypeRef result = NULL;
	OSStatus st = SecItemCopyMatching(q, &result);
	CFRelease(q); CFRelease(svc); CFRelease(acct);
	*generic = NULL; *secret = NULL; *genericLen = -1; *secretLen = -1; *comment = NULL;
	if (st != errSecSuccess) return st;
	if (result == NULL || CFGetTypeID(result) != CFDictionaryGetTypeID()) {
		if (result) CFRelease(result);
		return errSecDecode;
	}
	CFDictionaryRef d = (CFDictionaryRef)result;
	*generic = jm_copy_data((CFDataRef)CFDictionaryGetValue(d, kSecAttrGeneric), genericLen);
	*secret = jm_copy_data((CFDataRef)CFDictionaryGetValue(d, kSecValueData), secretLen);
	CFTypeRef c = CFDictionaryGetValue(d, kSecAttrComment);
	if (c != NULL && CFGetTypeID(c) == CFStringGetTypeID()) {
		CFIndex max = CFStringGetMaximumSizeForEncoding(CFStringGetLength((CFStringRef)c), kCFStringEncodingUTF8) + 1;
		char *buf = malloc(max);
		if (buf != NULL && CFStringGetCString((CFStringRef)c, buf, max, kCFStringEncodingUTF8)) *comment = buf;
		else free(buf);
	}
	CFRelease(result);
	return errSecSuccess;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func keychainLoad(service, slot, account string) (secret, stamp string, err error) {
	cs, cslot := C.CString(service), C.CString(slot)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cslot))
	var generic, csecret, comment *C.char
	var genericLen, secretLen C.int
	st := C.jm_keychain_read(cs, cslot, &generic, &genericLen, &csecret, &secretLen, &comment)
	defer C.free(unsafe.Pointer(generic))
	defer C.free(unsafe.Pointer(csecret))
	defer C.free(unsafe.Pointer(comment))
	switch st {
	case C.errSecSuccess:
	case C.errSecItemNotFound:
		return "", "", ErrNotFound
	default:
		return "", "", fmt.Errorf("keychain read refused (OSStatus %d)", int(st))
	}
	if generic == nil || csecret == nil || genericLen < 0 || secretLen <= 0 {
		return "", "", ErrNotFound
	}
	if C.GoStringN(generic, genericLen) != account {
		return "", "", ErrNotFound
	}
	if comment != nil {
		stamp = C.GoString(comment)
	}
	return C.GoStringN(csecret, secretLen), stamp, nil
}
