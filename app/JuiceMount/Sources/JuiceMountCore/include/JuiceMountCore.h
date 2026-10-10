#ifndef JUICEMOUNT_CORE_H
#define JUICEMOUNT_CORE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Server lifecycle
// Packaging/readiness checks; neither starts or tears down storage.
int NFSServerUsesServingService(void);
int NFSServerServiceReady(void);
int NFSServerAutoStartEligible(void);
char* NFSServerStart(char* configJSON);
char* NFSServerStartAutomatic(char* configJSON);
// Join/refresh JuiceMount Link and probe the NAS backend route. Returns JSON.
char* NFSServerLinkTest(char* configJSON);
// H-8B Manager enrollment: one step (at most one HTTP request) of this Mac's
// enrollment. Input and output are JSON (bridge/enrollment.go); the output
// carries secrets for the Keychain and must never be logged.
char* NFSServerEnrollStep(char* inputJSON);
// Soft stop: tears down server/sync/cache/monitor/metrics, but leaves
// FUSE and NFS mounted so the next Start is fast and prompt-free.
int NFSServerStop(void);
// Middle-ground stop (QA-7): unmounts NFS so /Volumes/<name> disappears
// and tears down the server, but leaves FUSE + JuiceFS daemon alive so
// the next Start is fast (no admin password re-prompt for re-mount).
int NFSServerStopMount(void);
// Hard stop: soft stop, then unmount NFS and FUSE. Use on app Quit.
int NFSServerShutdown(void);
// Explicit user choice to quit with unfinished uploads; still quiesces NFS.
int NFSServerShutdownForced(void);
// Typed reason behind the last Shutdown/ShutdownForced result, as JSON
// {"complete":bool,"reason":string,"detail":string}, or "error: ..." when the
// serving service cannot be asked. The int result above stays authoritative.
char* NFSServerLastShutdownOutcome(void);
int NFSServerIsRunning(void);
// Live macOS Network.framework path-cost signal. Expensive or constrained
// paths force metered policy; clearing both restores passive classification.
void NFSServerSetNetworkCost(int expensive, int constrained);

// Status
char* NFSServerStats(void);
char* NFSServerSyncNow(void);

// Search (FTS5)
char* NFSServerSearch(char* query, int limit, char* parentPath);

// Pin / offline-mode (offline-pin prototype)
char* NFSServerPin(char* rootPath);
char* NFSServerUnpin(char* rootPath);
char* NFSServerCacheStatus(void);
char* NFSServerSetOffline(int on);
int NFSServerIsOffline(void);

// Logging — bridge a Swift-side line into the Go rotating file log
// (~/Library/Logs/JuiceMount/juicemount.log), prefixed "[swift]".
void NFSServerLog(char* msg);

// Memory management — every char* returned by the above must be freed with this
void NFSServerFreeString(char* s);

#ifdef __cplusplus
}
#endif

#endif /* JUICEMOUNT_CORE_H */
