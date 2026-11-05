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
// command or the app. With ui 1 it allows the Keychain's access prompt: it
// turns user interaction back on for the process (Load turns it off), which
// only a command a person runs (juicemount-mcp token) may do, never a
// service. With ui -1 it leaves the process's setting as it is (the app,
// whose setting is its own).
static OSStatus jm_keychain_read_ui(const char *service, const char *account,
                                    char **generic, int *genericLen, char **secret, int *secretLen,
                                    char **comment, int ui) {
	if (ui >= 0) SecKeychainSetUserInteractionAllowed(ui ? true : false);
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

// The OSStatus values keychainFallback (keychain_fallback.go) names.
_Static_assert(errSecInteractionNotAllowed == -25308, "errSecInteractionNotAllowed");
_Static_assert(errSecNoAccessForItem == -25243, "errSecNoAccessForItem");

// jm_keychain_write updates the value, generic attribute, comment and label
// of the item (service, account), or with add creates it. An added item gets the
// default access list: the creating program only. The caller has set user
// interaction (SecKeychainSetUserInteractionAllowed).
static OSStatus jm_keychain_write(const char *service, const char *account, const char *label,
                                  const char *generic, int genericLen,
                                  const char *secret, int secretLen, const char *comment, int add) {
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	CFStringRef lbl = CFStringCreateWithCString(NULL, label, kCFStringEncodingUTF8);
	CFStringRef cmt = CFStringCreateWithCString(NULL, comment, kCFStringEncodingUTF8);
	CFDataRef gen = CFDataCreate(NULL, (const UInt8 *)generic, genericLen);
	CFDataRef val = CFDataCreate(NULL, (const UInt8 *)secret, secretLen);
	OSStatus st = errSecAllocate;
	if (svc != NULL && acct != NULL && lbl != NULL && cmt != NULL && gen != NULL && val != NULL) {
		if (!add) {
			const void *qkeys[] = { kSecClass, kSecAttrService, kSecAttrAccount };
			const void *qvals[] = { kSecClassGenericPassword, svc, acct };
			CFDictionaryRef q = CFDictionaryCreate(NULL, qkeys, qvals, 3,
			                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
			const void *ukeys[] = { kSecValueData, kSecAttrGeneric, kSecAttrComment, kSecAttrLabel };
			const void *uvals[] = { val, gen, cmt, lbl };
			CFDictionaryRef u = CFDictionaryCreate(NULL, ukeys, uvals, 4,
			                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
			st = SecItemUpdate(q, u);
			CFRelease(q); CFRelease(u);
		} else {
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

static void jm_keychain_allow_ui(int allow_ui) {
	SecKeychainSetUserInteractionAllowed(allow_ui ? true : false);
}

// jm_keychain_delete removes the item (service, account); UI only with
// allow_ui (-1: the process's setting is left as it is).
static OSStatus jm_keychain_delete(const char *service, const char *account, int allow_ui) {
	if (allow_ui >= 0) SecKeychainSetUserInteractionAllowed(allow_ui ? true : false);
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

static char *jm_agent_copy_string(CFTypeRef v) {
	if (v == NULL || CFGetTypeID(v) != CFStringGetTypeID()) return NULL;
	CFIndex max = CFStringGetMaximumSizeForEncoding(CFStringGetLength((CFStringRef)v), kCFStringEncodingUTF8) + 1;
	char *buf = malloc(max);
	if (buf != NULL && CFStringGetCString((CFStringRef)v, buf, max, kCFStringEncodingUTF8)) return buf;
	free(buf);
	return NULL;
}

// jm_keychain_read_attrs reads the generic attribute, label and comment of
// the item (service, account), never its data: the legacy keychain checks
// an item's access list only to hand out its data, so this shows no prompt.
// It leaves the process's user-interaction setting as it is.
static OSStatus jm_keychain_read_attrs(const char *service, const char *account,
                                       char **generic, int *genericLen, char **label, char **comment) {
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	*generic = NULL; *genericLen = -1; *label = NULL; *comment = NULL;
	if (svc == NULL || acct == NULL) {
		if (svc) CFRelease(svc);
		if (acct) CFRelease(acct);
		return errSecAllocate;
	}
	const void *keys[] = { kSecClass, kSecAttrService, kSecAttrAccount, kSecReturnAttributes, kSecMatchLimit };
	const void *vals[] = { kSecClassGenericPassword, svc, acct, kCFBooleanTrue, kSecMatchLimitOne };
	CFDictionaryRef q = CFDictionaryCreate(NULL, keys, vals, 5,
	                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
	CFTypeRef result = NULL;
	OSStatus st = SecItemCopyMatching(q, &result);
	CFRelease(q); CFRelease(svc); CFRelease(acct);
	if (st != errSecSuccess) return st;
	if (result == NULL || CFGetTypeID(result) != CFDictionaryGetTypeID()) {
		if (result) CFRelease(result);
		return errSecDecode;
	}
	CFDictionaryRef d = (CFDictionaryRef)result;
	*generic = jm_agent_copy_data((CFDataRef)CFDictionaryGetValue(d, kSecAttrGeneric), genericLen);
	*label = jm_agent_copy_string(CFDictionaryGetValue(d, kSecAttrLabel));
	*comment = jm_agent_copy_string(CFDictionaryGetValue(d, kSecAttrComment));
	CFRelease(result);
	return errSecSuccess;
}

// jm_keychain_add_trusting adds the item (service, account) with an access
// list that trusts this process and each of the npaths executables in
// paths: the legacy (file-based) keychain's SecAccess, as the app's
// RedisCredentialStore makes for the service. It leaves the process's
// user-interaction setting as it is.
static OSStatus jm_keychain_add_trusting(const char *service, const char *account, const char *label,
                                         const char *generic, int genericLen,
                                         const char *secret, int secretLen, const char *comment,
                                         const char **paths, int npaths) {
	CFMutableArrayRef apps = CFArrayCreateMutable(NULL, 0, &kCFTypeArrayCallBacks);
	if (apps == NULL) return errSecAllocate;
	SecTrustedApplicationRef app = NULL;
	OSStatus st = SecTrustedApplicationCreateFromPath(NULL, &app);
	if (st == errSecSuccess) { CFArrayAppendValue(apps, app); CFRelease(app); }
	for (int i = 0; st == errSecSuccess && i < npaths; i++) {
		app = NULL;
		st = SecTrustedApplicationCreateFromPath(paths[i], &app);
		if (st == errSecSuccess) { CFArrayAppendValue(apps, app); CFRelease(app); }
	}
	CFStringRef lbl = CFStringCreateWithCString(NULL, label, kCFStringEncodingUTF8);
	SecAccessRef access = NULL;
	if (st == errSecSuccess) st = lbl != NULL ? SecAccessCreate(lbl, apps, &access) : errSecAllocate;
	CFRelease(apps);
	CFStringRef svc = CFStringCreateWithCString(NULL, service, kCFStringEncodingUTF8);
	CFStringRef acct = CFStringCreateWithCString(NULL, account, kCFStringEncodingUTF8);
	CFStringRef cmt = CFStringCreateWithCString(NULL, comment, kCFStringEncodingUTF8);
	CFDataRef gen = CFDataCreate(NULL, (const UInt8 *)generic, genericLen);
	CFDataRef val = CFDataCreate(NULL, (const UInt8 *)secret, secretLen);
	if (st == errSecSuccess) {
		if (svc != NULL && acct != NULL && lbl != NULL && cmt != NULL && gen != NULL && val != NULL) {
			const void *akeys[] = { kSecClass, kSecAttrService, kSecAttrAccount, kSecAttrLabel,
			                        kSecValueData, kSecAttrGeneric, kSecAttrComment, kSecAttrAccess };
			const void *avals[] = { kSecClassGenericPassword, svc, acct, lbl, val, gen, cmt, access };
			CFDictionaryRef a = CFDictionaryCreate(NULL, akeys, avals, 8,
			                                       &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
			st = a != NULL ? SecItemAdd(a, NULL) : errSecAllocate;
			if (a) CFRelease(a);
		} else {
			st = errSecAllocate;
		}
	}
	if (access) CFRelease(access);
	if (svc) CFRelease(svc);
	if (acct) CFRelease(acct);
	if (lbl) CFRelease(lbl);
	if (cmt) CFRelease(cmt);
	if (gen) CFRelease(gen);
	if (val) CFRelease(val);
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
	return keychainLoadUI(service, slot, account, 1)
}

// keychainLoadAsIs reads like keychainLoadInteractive but leaves the
// process's user-interaction setting as it is: for the app (bridge), whose
// setting is its own.
func keychainLoadAsIs(service, slot, account string) (secret, stamp string, err error) {
	return keychainLoadUI(service, slot, account, -1)
}

// keychainReadAttributes returns the label and comment of the item
// (service, slot) bound to account, without its data (no prompt).
func keychainReadAttributes(service, slot, account string) (label, comment string, err error) {
	cs, cslot := C.CString(service), C.CString(slot)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cslot))
	var generic, clabel, ccomment *C.char
	var genericLen C.int
	st := C.jm_keychain_read_attrs(cs, cslot, &generic, &genericLen, &clabel, &ccomment)
	defer C.free(unsafe.Pointer(generic))
	defer C.free(unsafe.Pointer(clabel))
	defer C.free(unsafe.Pointer(ccomment))
	switch st {
	case C.errSecSuccess:
	case C.errSecItemNotFound:
		return "", "", ErrNotFound
	default:
		return "", "", fmt.Errorf("keychain attribute read refused (OSStatus %d)", int(st))
	}
	if generic == nil || genericLen < 0 || C.GoStringN(generic, genericLen) != account {
		return "", "", ErrNotFound
	}
	if clabel != nil {
		label = C.GoString(clabel)
	}
	if ccomment != nil {
		comment = C.GoString(ccomment)
	}
	return label, comment, nil
}

func keychainLoadUI(service, slot, account string, ui C.int) (secret, stamp string, err error) {
	cs, cslot := C.CString(service), C.CString(slot)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cslot))
	var generic, csecret, comment *C.char
	var genericLen, secretLen C.int
	st := C.jm_keychain_read_ui(cs, cslot, &generic, &genericLen, &csecret, &secretLen, &comment, ui)
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
// lets the Keychain ask the person; an item this build may not update (one
// another build made) is then deleted and added again, but only for the
// statuses keychainFallback names, never after the person denied access.
func keychainSave(service, slot, account, label, secret, device string, allowUI bool) error {
	cs, cslot := C.CString(service), C.CString(slot)
	clabel := C.CString(label)
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
	write := func(add bool) int {
		return int(C.jm_keychain_write(cs, cslot, clabel, cgen, C.int(len(account)), csecret, C.int(len(secret)), ccomment, cBool(add)))
	}
	C.jm_keychain_allow_ui(cBool(allowUI))
	st, removed := write(false), false
	if allowUI && keychainFallback(st) {
		switch del := int(C.jm_keychain_delete(cs, cslot, cBool(allowUI))); del {
		case C.errSecSuccess:
			removed, st = true, C.errSecItemNotFound
		case C.errSecItemNotFound:
			st = C.errSecItemNotFound
		default:
			return fmt.Errorf("keychain write refused (OSStatus %d), and the stored item could not be replaced (OSStatus %d)", st, del)
		}
	}
	if st == C.errSecItemNotFound {
		st = write(true)
	}
	switch {
	case st == C.errSecSuccess:
		return nil
	case removed:
		return fmt.Errorf("%w; storing the new one failed (OSStatus %d)", ErrStoredRecordRemoved, st)
	}
	return fmt.Errorf("keychain write refused (OSStatus %d; is the login Keychain unlocked?)", st)
}

func keychainDelete(service, slot string, allowUI bool) error {
	return keychainDeleteUI(service, slot, cBool(allowUI))
}

// keychainDeleteAsIs deletes like keychainDelete but leaves the process's
// user-interaction setting as it is (the app).
func keychainDeleteAsIs(service, slot string) error { return keychainDeleteUI(service, slot, -1) }

func keychainDeleteUI(service, slot string, ui C.int) error {
	cs, cslot := C.CString(service), C.CString(slot)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cslot))
	switch st := C.jm_keychain_delete(cs, cslot, ui); st {
	case C.errSecSuccess, C.errSecItemNotFound:
		return nil
	default:
		return fmt.Errorf("keychain delete refused (OSStatus %d)", int(st))
	}
}

// keychainReplaceTrusting stores secret in the item (service, slot) like
// keychainSave, but always as a new item whose access list trusts this
// process and the executables in trusted: any item stored before is deleted
// first, because an update keeps the old item's access list (one made by
// `juicemount-mcp token create` trusts only that build of the CLI). It leaves
// the process's user-interaction setting as it is: the app, where a person
// can answer the Keychain's prompt for an item another program made.
func keychainReplaceTrusting(service, slot, account, label, secret, device string, trusted []string) error {
	cs, cslot := C.CString(service), C.CString(slot)
	clabel := C.CString(label)
	cgen, csecret := C.CString(account), C.CString(secret)
	ccomment := C.CString("")
	if device != "" {
		C.free(unsafe.Pointer(ccomment))
		ccomment = C.CString(DeviceStampPrefix + device)
	}
	paths := C.malloc(C.size_t(len(trusted)+1) * C.size_t(unsafe.Sizeof(uintptr(0))))
	cpaths := unsafe.Slice((**C.char)(paths), len(trusted)+1)
	for i, p := range trusted {
		cpaths[i] = C.CString(p)
	}
	defer func() {
		for _, p := range []*C.char{cs, cslot, clabel, cgen, csecret, ccomment} {
			C.free(unsafe.Pointer(p))
		}
		for _, p := range cpaths[:len(trusted)] {
			C.free(unsafe.Pointer(p))
		}
		C.free(paths)
	}()
	removed := false
	switch del := int(C.jm_keychain_delete(cs, cslot, -1)); del {
	case C.errSecSuccess:
		removed = true
	case C.errSecItemNotFound:
	default:
		return fmt.Errorf("the agent token stored before could not be replaced (OSStatus %d)", del)
	}
	st := int(C.jm_keychain_add_trusting(cs, cslot, clabel, cgen, C.int(len(account)), csecret, C.int(len(secret)),
		ccomment, (**C.char)(paths), C.int(len(trusted))))
	switch {
	case st == C.errSecSuccess:
		return nil
	case removed:
		return fmt.Errorf("%w; storing the new one failed (OSStatus %d)", ErrStoredRecordRemoved, st)
	}
	return fmt.Errorf("keychain write refused (OSStatus %d; is the login Keychain unlocked?)", st)
}
