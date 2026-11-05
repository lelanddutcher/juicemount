//go:build darwin && cgo

package credstore

/*
#cgo CFLAGS: -mmacosx-version-min=26.0 -Wno-deprecated-declarations
#cgo LDFLAGS: -mmacosx-version-min=26.0 -framework Security -framework CoreFoundation
#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
#include <stdlib.h>
#include <string.h>

static char *jm_agent_copy_data(CFDataRef data, int *len) {
	if (data == NULL || CFGetTypeID(data) != CFDataGetTypeID()) { *len = -1; return NULL; }
	CFIndex n = CFDataGetLength(data);
	char *out = malloc(n > 0 ? n : 1);
	if (out == NULL) { *len = -1; return NULL; }
	memcpy(out, CFDataGetBytePtr(data), n);
	*len = (int)n;
	return out;
}

// jm_keychain_read_ui is jm_keychain_read (keychain_darwin.go) for a person's
// command: it allows the Keychain's access prompt. It turns user interaction
// back on for the process (Load turns it off), so it is called only from a
// command a person runs (juicemount-mcp token), never from a service.
static OSStatus jm_keychain_read_ui(const char *service, const char *account,
                                    char **generic, int *genericLen, char **secret, int *secretLen,
                                    char **comment) {
	SecKeychainSetUserInteractionAllowed(true);
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	const void *keys[] = { kSecClass, kSecAttrService, kSecAttrAccount, kSecReturnData,
	                       kSecReturnAttributes, kSecMatchLimit };
	const void *vals[] = { kSecClassGenericPassword, svc, acct, kCFBooleanTrue,
	                       kCFBooleanTrue, kSecMatchLimitOne };
	CFDictionaryRef q = CFDictionaryCreate(NULL, keys, vals, 6,
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
	*generic = jm_agent_copy_data((CFDataRef)CFDictionaryGetValue(d, kSecAttrGeneric), genericLen);
	*secret = jm_agent_copy_data((CFDataRef)CFDictionaryGetValue(d, kSecValueData), secretLen);
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

// jm_access_refused: the Keychain refused this program the item (another
// build of it created the item, or the person denied it).
static int jm_access_refused(OSStatus st) {
	return st == errSecAuthFailed || st == errSecInteractionNotAllowed || st == errSecNoAccessForItem;
}

// jm_keychain_upsert replaces the value, generic attribute and comment of the
// item (service, account), adding it when there is none. An added item gets
// the default access list: the creating program only. With allow_ui the
// Keychain may ask the person (a command they run); an item this program may
// not update (one an earlier build created) is then deleted and added again.
// Without it no UI is shown.
static OSStatus jm_keychain_upsert(const char *service, const char *account, const char *label,
                                   const char *generic, int genericLen,
                                   const char *secret, int secretLen, const char *comment, int allow_ui) {
	SecKeychainSetUserInteractionAllowed(allow_ui ? true : false);
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	CFStringRef lbl = CFStringCreateWithCString(NULL, label, kCFStringEncodingUTF8);
	CFStringRef cmt = CFStringCreateWithCString(NULL, comment, kCFStringEncodingUTF8);
	CFDataRef gen = CFDataCreate(NULL, (const UInt8 *)generic, genericLen);
	CFDataRef val = CFDataCreate(NULL, (const UInt8 *)secret, secretLen);
	OSStatus st = errSecAllocate;
	if (svc != NULL && acct != NULL && lbl != NULL && cmt != NULL && gen != NULL && val != NULL) {
		const void *qkeys[] = { kSecClass, kSecAttrService, kSecAttrAccount };
		const void *qvals[] = { kSecClassGenericPassword, svc, acct };
		CFDictionaryRef q = CFDictionaryCreate(NULL, qkeys, qvals, 3,
		                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
		const void *ukeys[] = { kSecValueData, kSecAttrGeneric, kSecAttrComment };
		const void *uvals[] = { val, gen, cmt };
		CFDictionaryRef u = CFDictionaryCreate(NULL, ukeys, uvals, 3,
		                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
		st = SecItemUpdate(q, u);
		if (allow_ui && jm_access_refused(st)) {
			OSStatus del = SecItemDelete(q);
			st = (del == errSecSuccess || del == errSecItemNotFound) ? errSecItemNotFound : del;
		}
		CFRelease(q); CFRelease(u);
		if (st == errSecItemNotFound) {
			const void *akeys[] = { kSecClass, kSecAttrService, kSecAttrAccount, kSecAttrLabel,
			                        kSecValueData, kSecAttrGeneric, kSecAttrComment };
			const void *avals[] = { kSecClassGenericPassword, svc, acct, lbl, val, gen, cmt };
			CFDictionaryRef a = CFDictionaryCreate(NULL, akeys, avals, 7,
			                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
			st = SecItemAdd(a, NULL);
			CFRelease(a);
		}
	}
	if (svc) CFRelease(svc);
	if (acct) CFRelease(acct);
	if (lbl) CFRelease(lbl);
	if (cmt) CFRelease(cmt);
	if (gen) CFRelease(gen);
	if (val) CFRelease(val);
	return st;
}

// jm_keychain_delete removes the item (service, account); UI only with
// allow_ui.
static OSStatus jm_keychain_delete(const char *service, const char *account, int allow_ui) {
	SecKeychainSetUserInteractionAllowed(allow_ui ? true : false);
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	const void *keys[] = { kSecClass, kSecAttrService, kSecAttrAccount };
	const void *vals[] = { kSecClassGenericPassword, svc, acct };
	CFDictionaryRef q = CFDictionaryCreate(NULL, keys, vals, 3,
	                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
	OSStatus st = SecItemDelete(q);
	CFRelease(q); CFRelease(svc); CFRelease(acct);
	return st;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// keychainAvailable: this build reads and writes the Keychain.
const keychainAvailable = true

func cBool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

func keychainLoadInteractive(service, slot, account string) (secret, stamp string, err error) {
	cs, cslot := C.CString(service), C.CString(slot)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cslot))
	var generic, csecret, comment *C.char
	var genericLen, secretLen C.int
	st := C.jm_keychain_read_ui(cs, cslot, &generic, &genericLen, &csecret, &secretLen, &comment)
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

// keychainSave stores secret in the item (service, slot), bound to account
// (generic attribute) and stamped with device (comment attribute). allowUI
// lets the Keychain ask the person.
func keychainSave(service, slot, account, secret, device string, allowUI bool) error {
	cs, cslot := C.CString(service), C.CString(slot)
	clabel := C.CString("JuiceMount agent token (juicemount-mcp)")
	cgen, csecret := C.CString(account), C.CString(secret)
	ccomment := C.CString("")
	if device != "" {
		C.free(unsafe.Pointer(ccomment))
		ccomment = C.CString(DeviceStampPrefix + device)
	}
	defer func() {
		for _, p := range []*C.char{cs, cslot, clabel, cgen, csecret, ccomment} {
			C.free(unsafe.Pointer(p))
		}
	}()
	st := C.jm_keychain_upsert(cs, cslot, clabel, cgen, C.int(len(account)), csecret, C.int(len(secret)), ccomment, cBool(allowUI))
	if st != C.errSecSuccess {
		return fmt.Errorf("keychain write refused (OSStatus %d; is the login Keychain unlocked?)", int(st))
	}
	return nil
}

func keychainDelete(service, slot string, allowUI bool) error {
	cs, cslot := C.CString(service), C.CString(slot)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cslot))
	switch st := C.jm_keychain_delete(cs, cslot, cBool(allowUI)); st {
	case C.errSecSuccess, C.errSecItemNotFound:
		return nil
	default:
		return fmt.Errorf("keychain delete refused (OSStatus %d)", int(st))
	}
}
