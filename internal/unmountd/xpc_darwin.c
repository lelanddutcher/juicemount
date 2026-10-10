// libxpc glue for juicemount-unmountd. It moves bytes and connection events
// to Go (xpc_darwin.go) and applies the peer code-signing requirement that Go
// chooses for each listener. It holds no policy.
//
// Peer lifecycle: the listener handler retains each new peer, sets its event
// handler, lets Go admit it (Go sets the requirement through
// jm_ud_set_requirement BEFORE activation), then activates it. A refused
// peer is activated and cancelled at once. The peer's reference is dropped on
// its final CONNECTION_INVALID event.

#include <xpc/xpc.h>
#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
#include <stdint.h>
#include <string.h>
#include "_cgo_export.h"

#define JM_KEY_VERSION "jm.v"
#define JM_KEY_BODY "jm.body"

static uint64_t jm_ud_next_peer;

int jm_ud_listen(const char *name, int listener) {
	xpc_connection_t l = xpc_connection_create_mach_service(name, NULL, XPC_CONNECTION_MACH_SERVICE_LISTENER);
	if (l == NULL) return -1;
	xpc_connection_set_event_handler(l, ^(xpc_object_t ev) {
		if (xpc_get_type(ev) != XPC_TYPE_CONNECTION) {
			if (ev == XPC_ERROR_CONNECTION_INVALID) jmUnmountdListenerFailed(listener);
			return;
		}
		xpc_connection_t peer = (xpc_connection_t)ev;
		uint64_t id = __atomic_add_fetch(&jm_ud_next_peer, 1, __ATOMIC_RELAXED);
		xpc_retain(peer);
		xpc_connection_set_event_handler(peer, ^(xpc_object_t msg) {
			if (xpc_get_type(msg) == XPC_TYPE_DICTIONARY) {
				size_t n = 0;
				const void *b = xpc_dictionary_get_data(msg, JM_KEY_BODY, &n);
				xpc_retain(msg); // released by jm_ud_reply / jm_ud_drop
				jmUnmountdMessage(id, (uintptr_t)msg, xpc_dictionary_get_int64(msg, JM_KEY_VERSION), (void *)b, b ? n : 0);
				return;
			}
			if (msg == XPC_ERROR_PEER_CODE_SIGNING_REQUIREMENT) {
				jmUnmountdPeerRejected(id);
				xpc_connection_cancel(peer);
				return;
			}
			if (msg == XPC_ERROR_CONNECTION_INVALID) {
				jmUnmountdPeerClosed(id);
				xpc_release(peer);
				return;
			}
			xpc_connection_cancel(peer);
		});
		int ok = jmUnmountdPeerOpen(listener, id, (uintptr_t)peer,
			(uint32_t)xpc_connection_get_euid(peer), (int)xpc_connection_get_asid(peer));
		xpc_connection_activate(peer);
		if (!ok) xpc_connection_cancel(peer);
	});
	xpc_connection_activate(l);
	return 0;
}

int jm_ud_set_requirement(uintptr_t peer, const char *requirement) {
	return xpc_connection_set_peer_code_signing_requirement((xpc_connection_t)peer, requirement);
}

void jm_ud_reply(uintptr_t msg, int64_t version, const void *body, size_t len) {
	xpc_object_t m = (xpc_object_t)msg;
	xpc_object_t r = xpc_dictionary_create_reply(m);
	if (r != NULL) {
		xpc_connection_t c = xpc_dictionary_get_remote_connection(m);
		xpc_dictionary_set_int64(r, JM_KEY_VERSION, version);
		xpc_dictionary_set_data(r, JM_KEY_BODY, body, len);
		if (c != NULL) xpc_connection_send_message(c, r);
		xpc_release(r);
	}
	xpc_release(m);
}

void jm_ud_drop(uintptr_t msg) { xpc_release((xpc_object_t)msg); }

static CFStringRef jm_ud_cfstr(const char *s) {
	return CFStringCreateWithCString(kCFAllocatorDefault, s, kCFStringEncodingUTF8);
}

int jm_ud_requirement_valid(const char *requirement) {
	CFStringRef s = jm_ud_cfstr(requirement);
	if (s == NULL) return -1;
	SecRequirementRef req = NULL;
	OSStatus st = SecRequirementCreateWithString(s, kSecCSDefaultFlags, &req);
	CFRelease(s);
	if (req != NULL) CFRelease(req);
	return (int)st;
}

int jm_ud_static_satisfies(const char *path, int is_dir, const char *requirement) {
	CFURLRef url = CFURLCreateFromFileSystemRepresentation(kCFAllocatorDefault,
		(const UInt8 *)path, (CFIndex)strlen(path), is_dir ? true : false);
	if (url == NULL) return -1;
	SecStaticCodeRef code = NULL;
	OSStatus st = SecStaticCodeCreateWithPath(url, kSecCSDefaultFlags, &code);
	CFRelease(url);
	if (st != errSecSuccess) return (int)st;
	CFStringRef s = jm_ud_cfstr(requirement);
	SecRequirementRef req = NULL;
	st = s ? SecRequirementCreateWithString(s, kSecCSDefaultFlags, &req) : -1;
	if (s != NULL) CFRelease(s);
	if (st == errSecSuccess) {
		st = SecStaticCodeCheckValidity(code, kSecCSDefaultFlags | kSecCSCheckAllArchitectures, req);
	}
	if (req != NULL) CFRelease(req);
	CFRelease(code);
	return (int)st;
}
