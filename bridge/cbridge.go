package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// globalServer is the singleton NFS server instance.
var (
	globalMu      sync.Mutex
	globalServer  *jmnfs.Server
	globalRC      *metadata.RedisClient
	globalStore   *metadata.Store
	globalCache   *cache.Reader
	globalMonitor *health.HealthMonitor
	globalRDB     *redis.Client
)

// ServerConfig is the JSON configuration passed from Swift.
type ServerConfig struct {
	RedisURL   string `json:"redis_url"`
	FUSEPath   string `json:"fuse_path"`
	MountPoint string `json:"mount_point"`
	ListenAddr string `json:"listen_addr"`
	DBPath     string `json:"db_path"`
	CacheSize  string `json:"cache_size"`
}

//export NFSServerStart
func NFSServerStart(configJSON *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalServer != nil {
		return C.CString("error: server already running")
	}

	var cfg ServerConfig
	if err := json.Unmarshal([]byte(C.GoString(configJSON)), &cfg); err != nil {
		return C.CString(fmt.Sprintf("error: parse config: %v", err))
	}

	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:11049"
	}

	// Open metadata store
	store, err := metadata.Open(cfg.DBPath)
	if err != nil {
		return C.CString(fmt.Sprintf("error: open store: %v", err))
	}
	globalStore = store

	// Connect to Redis
	rc, err := metadata.NewRedisClient(cfg.RedisURL, store)
	if err != nil {
		store.Close()
		return C.CString(fmt.Sprintf("error: redis: %v", err))
	}
	globalRC = rc

	// Initial sync
	if err := rc.SyncOnce(); err != nil {
		log.Printf("WARNING: initial sync: %v", err)
	}
	rc.Start()

	// Cache reader
	cacheDir := cache.DetectCacheDir()
	if cacheDir != "" {
		addr, db, _ := metadata.ParseRedisURL(cfg.RedisURL)
		rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
		globalRDB = rdb
		cr := cache.NewReader(cacheDir, cache.DefaultBlockSize, rdb)
		if err := cr.Verify(); err == nil {
			globalCache = cr
		}
	}

	// NFS server
	srv := jmnfs.NewServer(jmnfs.Config{
		ListenAddr: cfg.ListenAddr,
		FUSEPath:   cfg.FUSEPath,
	}, store)

	if err := srv.Start(); err != nil {
		rc.Stop()
		store.Close()
		return C.CString(fmt.Sprintf("error: start: %v", err))
	}

	if globalCache != nil {
		srv.Handler().SetCacheReader(globalCache)
	}
	srv.Handler().SetRedisClient(rc)
	globalServer = srv

	// Health monitor
	redisAddr, _, _ := metadata.ParseRedisURL(cfg.RedisURL)
	globalMonitor = health.New(health.Config{
		RedisURL:      redisAddr,
		MinIOURL:      "", // TODO: make configurable
		FUSEPath:      cfg.FUSEPath,
		NFSMountPoint: cfg.MountPoint,
	})
	globalMonitor.Start()

	return C.CString(srv.Addr())
}

//export NFSServerStop
func NFSServerStop() {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalMonitor != nil {
		globalMonitor.Stop()
		globalMonitor = nil
	}
	if globalServer != nil {
		globalServer.Handler().StopHandler()
		globalServer.Stop()
		globalServer = nil
	}
	if globalCache != nil {
		globalCache.Stop()
		globalCache = nil
	}
	if globalRC != nil {
		globalRC.Stop()
		globalRC = nil
	}
	if globalStore != nil {
		globalStore.Close()
		globalStore = nil
	}
	if globalRDB != nil {
		globalRDB.Close()
		globalRDB = nil
	}
}

//export NFSServerIsRunning
func NFSServerIsRunning() C.int {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalServer != nil {
		return 1
	}
	return 0
}

// StatsResult is the JSON stats returned to Swift.
type StatsResult struct {
	Running        bool    `json:"running"`
	EntryCount     int     `json:"entry_count"`
	LastSyncMs     int64   `json:"last_sync_ms"`
	LastSyncTime   string  `json:"last_sync_time"`
	ServerAddr     string  `json:"server_addr"`
	HealthRedis    bool    `json:"health_redis"`
	HealthMinIO    bool    `json:"health_minio"`
	HealthFUSE     bool    `json:"health_fuse"`
}

//export NFSServerStats
func NFSServerStats() *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	stats := StatsResult{Running: globalServer != nil}

	if globalRC != nil {
		stats.LastSyncMs = globalRC.LastSyncDuration().Milliseconds()
		stats.LastSyncTime = globalRC.LastSyncTime().Format(time.RFC3339)
		stats.EntryCount = globalRC.LastSyncEntries()
	}
	if globalServer != nil {
		stats.ServerAddr = globalServer.Addr()
	}
	if globalMonitor != nil {
		status := globalMonitor.Status()
		stats.HealthRedis = status.Redis.Healthy
		stats.HealthMinIO = status.MinIO.Healthy
		stats.HealthFUSE = status.FUSE.Healthy
	}

	data, _ := json.Marshal(stats)
	return C.CString(string(data))
}

//export NFSServerFreeString
func NFSServerFreeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

// SyncNow triggers an immediate metadata reconciliation.
//export NFSServerSyncNow
func NFSServerSyncNow() *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalRC == nil {
		return C.CString("error: not running")
	}

	if err := globalRC.SyncOnce(); err != nil {
		return C.CString(fmt.Sprintf("error: %v", err))
	}
	return C.CString("ok")
}

func main() {} // required for c-archive build
