package main

import (
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/enginehost"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// globalEngine is the engine helper of level shadow (engine-direct step 1),
// guarded by globalMu. It lives until a full shutdown, like the FUSE
// manager; a soft stop and start keeps it while its inputs stay the same.
// If the app process dies, the helper's start pipe closes and it exits by
// itself.
var globalEngine *enginehost.Supervisor

// globalEngineInputs are what globalEngine was started with (guarded by
// globalMu). A start with other inputs (the level, the Redis URL, the
// bucket, the binary or the per-Mac object key after a rotation) replaces
// the helper (step-1 review F4).
var globalEngineInputs engineInputs

type engineInputs struct {
	bin, metaPassword string
	args              []string
	key               enginehost.ObjectKey
}

func (a engineInputs) equal(b engineInputs) bool {
	return a.bin == b.bin && a.metaPassword == b.metaPassword && a.key == b.key && slices.Equal(a.args, b.args)
}

// globalEngineDoor is the engine socket inside the juicefs mount at level
// reads (engine-direct step 2). It lives as long as the FUSE manager that
// uses it: a soft stop and start keeps both, and the new NFS handler reads
// through the same door. A change of level needs a full restart (the FUSE
// reuse check compares it).
var globalEngineDoor atomic.Pointer[enginehost.Door]

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

// startEngineShadowLocked starts the helper when the level is shadow, and
// replaces a running one whose inputs changed (step-1 review F4: a level
// change or a key rotation must not leave the old helper running). With the
// level unset it starts nothing and stops a helper left from an earlier
// start. Caller holds globalMu.
func startEngineShadowLocked(cfg ServerConfig, fuseLog string) {
	if engineLevel(cfg) != enginehost.LevelShadow {
		if globalEngine != nil {
			retireEngineShadowLocked("the engine level is no longer shadow")
		}
		return
	}
	bin := cfg.JuiceFSBin
	if bin == "" {
		bin = health.ResolveJuiceFSBinary()
	}
	args, metaPassword := health.EngineHelperArgs(cfg.RedisURL, cfg.BucketOverride)
	in := engineInputs{bin: bin, metaPassword: metaPassword, args: args,
		key: enginehost.ObjectKey{AccessKey: cfg.ObjectAccessKey, SecretKey: cfg.ObjectSecretKey}}
	if globalEngine != nil {
		if globalEngineInputs.equal(in) {
			return
		}
		retireEngineShadowLocked("the helper's inputs changed (URL, bucket, binary or object key)")
	}
	globalEngineInputs = in
	globalEngine = enginehost.StartShadow(enginehost.Config{
		Bin: bin, Args: args, Env: health.EngineHelperEnvironment(),
		MetaPassword: metaPassword,
		ObjectKey:    in.key,
		LogPath:      engineHelperLogPath(fuseLog),
	})
}

// retireEngineShadowLocked stops the running helper in the background (it
// can take seconds; the caller holds globalMu). The shadow helper carries no
// data, so a new one may start before the old one is gone.
func retireEngineShadowLocked(why string) {
	old := globalEngine
	globalEngine = nil
	globalEngineInputs = engineInputs{}
	jmlog.Info("engine helper replaced", "why", why)
	go enginehost.StopShadow(old)
}

// engineDoorForMount returns the engine socket the next juicefs mount child
// opens, or nil (a nil interface) below level reads.
func engineDoorForMount(cfg ServerConfig) health.EngineDoor {
	if engineLevel(cfg) != enginehost.LevelReads {
		return nil
	}
	d := globalEngineDoor.Load()
	if d == nil {
		d = enginehost.NewDoor(enginehost.DoorConfig{
			ExpectVersion: enginehost.ExpectedJuiceFSVersion,
			OnEvent:       enginehost.LogDoorEvents,
		})
		if !globalEngineDoor.CompareAndSwap(nil, d) {
			d = globalEngineDoor.Load()
		}
	}
	jmlog.Info("engine reads: the juicefs mount opens the engine socket", "level", enginehost.LevelReads.String())
	return d
}

// engineFUSEKey is the part of the engine level that selects the FUSE
// child: only level reads changes the mount (it opens the socket).
func engineFUSEKey(cfg ServerConfig) string {
	if engineLevel(cfg) == enginehost.LevelReads {
		return enginehost.LevelReads.String()
	}
	return ""
}

// attachEngineReads points a new NFS handler at the door, when there is one.
func attachEngineReads(h *jmnfs.JuiceMountHandler) {
	if d := globalEngineDoor.Load(); d != nil && h != nil {
		h.SetEngineReads(doorReadSource(d))
	}
}

// closeEngineDoor ends the socket once the mount that served it is gone.
func closeEngineDoor() {
	if d := globalEngineDoor.Swap(nil); d != nil {
		d.Close()
	}
}

// stopEngineShadow stops the helper (bounded by the supervisor's 30 s stop
// bound plus SIGTERM and SIGKILL) and forgets it.
func stopEngineShadow() {
	globalMu.Lock()
	s := globalEngine
	globalEngine = nil
	globalEngineInputs = engineInputs{}
	globalMu.Unlock()
	enginehost.StopShadow(s)
}

// doorReadSource adapts a door to the NFS front's read path.
func doorReadSource(d *enginehost.Door) jmnfs.EngineReadSource {
	return func() (jmnfs.EngineReadClient, uint64, bool) {
		cl, epoch, ok := d.Current()
		if !ok {
			return nil, 0, false
		}
		return cl, epoch, true
	}
}
