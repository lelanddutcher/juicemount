#ifndef JUICEMOUNT_CORE_H
#define JUICEMOUNT_CORE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Server lifecycle
char* NFSServerStart(char* configJSON);
// Soft stop: tears down server/sync/cache/monitor/metrics, but leaves
// FUSE and NFS mounted so the next Start is fast and prompt-free.
void NFSServerStop(void);
// Hard stop: soft stop, then unmount NFS and FUSE. Use on app Quit.
void NFSServerShutdown(void);
int NFSServerIsRunning(void);

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

// Memory management — every char* returned by the above must be freed with this
void NFSServerFreeString(char* s);

#ifdef __cplusplus
}
#endif

#endif /* JUICEMOUNT_CORE_H */
