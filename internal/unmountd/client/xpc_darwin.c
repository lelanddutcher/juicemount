// libxpc glue for the unmount-helper client (juicemount-service side).
// Every connection requires the daemon to satisfy the peer code-signing
// requirement the Go side passes (client.DaemonRequirement).

#include <xpc/xpc.h>
#include <stdint.h>
#include <stddef.h>
#include "_cgo_export.h"

#define JM_KEY_VERSION "jm.v"
#define JM_KEY_BODY "jm.body"

static int jm_uc_error_kind(xpc_object_t ev) {
	if (ev == XPC_ERROR_CONNECTION_INTERRUPTED) return 1;
	if (ev == XPC_ERROR_CONNECTION_INVALID) return 2;
	if (ev == XPC_ERROR_PEER_CODE_SIGNING_REQUIREMENT) return 3;
	return 4;
}

uintptr_t jm_uc_open(const char *service, const char *requirement, uint64_t conn_id, int *err) {
	xpc_connection_t c = xpc_connection_create_mach_service(service, NULL, XPC_CONNECTION_MACH_SERVICE_PRIVILEGED);
	if (c == NULL) {
		*err = -1;
		return 0;
	}
	int rc = xpc_connection_set_peer_code_signing_requirement(c, requirement);
	if (rc != 0) {
		// Never release an inactive connection: activate, cancel, release.
		xpc_connection_set_event_handler(c, ^(xpc_object_t ev) { (void)ev; });
		xpc_connection_activate(c);
		xpc_connection_cancel(c);
		xpc_release(c);
		*err = rc;
		return 0;
	}
	xpc_connection_set_event_handler(c, ^(xpc_object_t ev) {
		if (xpc_get_type(ev) == XPC_TYPE_ERROR) {
			jmUnmountdClientEvent(conn_id, jm_uc_error_kind(ev));
		}
		// Unsolicited messages are ignored: the daemon only replies.
	});
	xpc_connection_activate(c);
	return (uintptr_t)c;
}

void jm_uc_send(uintptr_t conn, int64_t version, const void *body, size_t len, uint64_t token) {
	xpc_connection_t c = (xpc_connection_t)conn;
	xpc_object_t m = xpc_dictionary_create(NULL, NULL, 0);
	xpc_dictionary_set_int64(m, JM_KEY_VERSION, version);
	xpc_dictionary_set_data(m, JM_KEY_BODY, body, len);
	xpc_connection_send_message_with_reply(c, m, NULL, ^(xpc_object_t r) {
		if (xpc_get_type(r) == XPC_TYPE_DICTIONARY) {
			size_t n = 0;
			const void *b = xpc_dictionary_get_data(r, JM_KEY_BODY, &n);
			jmUnmountdClientReply(token, 0, xpc_dictionary_get_int64(r, JM_KEY_VERSION), (void *)b, b ? n : 0);
		} else {
			jmUnmountdClientReply(token, jm_uc_error_kind(r), 0, NULL, 0);
		}
	});
	xpc_release(m);
}

// jm_uc_close cancels and drops Go's reference. libxpc keeps the connection
// alive until it has delivered the final CONNECTION_INVALID event and every
// outstanding reply handler.
void jm_uc_close(uintptr_t conn) {
	xpc_connection_t c = (xpc_connection_t)conn;
	xpc_connection_cancel(c);
	xpc_release(c);
}
