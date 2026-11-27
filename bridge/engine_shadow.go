package main

import (
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/enginehost"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// globalEngine is the engine helper of level shadow (engine-direct step 1),
// guarded by globalMu. It lives until a full shutdown, like the FUSE
// manager; a soft stop and start keeps it. If the app process dies, the
// helper's start pipe closes and it exits by itself.
var globalEngine *enginehost.Supervisor

// engineLevel resolves the switch: the config (the app's hidden
// preference) wins over the JM_ENGINE environment. Anything it cannot use
// is today's behaviour.
func engineLevel(cfg ServerConfig) enginehost.Level {
	raw := cfg.EngineLevel
	if raw == "" {
		raw = os.Getenv("JM_ENGINE")
	}
	level, err := enginehost.ParseLevel(raw)
	if err != nil {
		jmlog.Warn("engine level ignored; no engine helper", "error", err.Error())
		return enginehost.LevelFUSE
	}
	return level
}

// engineHelperLogPath puts the helper's log beside the FUSE log.
func engineHelperLogPath(fuseLog string) string {
	dir := ""
	if fuseLog != "" {
		dir = filepath.Dir(fuseLog)
	} else if home, err := os.UserHomeDir(); err == nil {
		dir = filepath.Join(home, ".juicefs")
	}
	if dir == "" || os.MkdirAll(dir, 0o700) != nil {
		return ""
	}
	return filepath.Join(dir, "juicefs-engine.log")
}

// startEngineShadowLocked starts the helper once per serving lifetime when
// the level is shadow. With the level unset it does nothing at all.
// Caller holds globalMu.
func startEngineShadowLocked(cfg ServerConfig, fuseLog string) {
	if globalEngine != nil || engineLevel(cfg) != enginehost.LevelShadow {
		return
	}
	bin := cfg.JuiceFSBin
	if bin == "" {
		bin = health.ResolveJuiceFSBinary()
	}
	args, metaPassword := health.EngineHelperArgs(cfg.RedisURL, cfg.BucketOverride)
	globalEngine = enginehost.StartShadow(enginehost.Config{
		Bin: bin, Args: args, Env: health.EngineHelperEnvironment(),
		MetaPassword: metaPassword,
		ObjectKey:    enginehost.ObjectKey{AccessKey: cfg.ObjectAccessKey, SecretKey: cfg.ObjectSecretKey},
		LogPath:      engineHelperLogPath(fuseLog),
	})
}

// stopEngineShadow stops the helper (bounded by the supervisor's 30 s stop
// bound plus SIGTERM and SIGKILL) and forgets it.
func stopEngineShadow() {
	globalMu.Lock()
	s := globalEngine
	globalEngine = nil
	globalMu.Unlock()
	enginehost.StopShadow(s)
}
