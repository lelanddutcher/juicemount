package health

import "os"

// Engine helper (engine-direct step 1, `JM_ENGINE=shadow`): the flags and
// environment of `juicefs jm-engine`. The supervisor lives in
// internal/enginehost; this file only shares the juicefs child policy of
// this package with it, so the helper gets exactly the environment rules
// of `juicefs mount`.

// EngineHelperEnvironment is the helper's whole environment: the allow-list
// a juicefs mount child gets and the closed AWS credential chain. It holds
// no secret; the helper reads the Redis password and the object key from
// its private start pipe (wire.StartMessage).
func EngineHelperEnvironment() []string {
	return append(juiceFSChildEnvironment(), juiceFSNoAWSCredentialChain...)
}

// EngineHelperArgs returns the arguments after the juicefs binary for a
// step-1 helper and the Redis password split out of redisURL (it goes to
// the start message, never argv).
//
// Step 1 runs read-only with no block cache: --read-only and
// --cache-size 0 (the helper also forces both). It never shares the
// mount's cache folder, so the mount's cache has one owner. Later steps take
// over the mount's full flag set. --no-agent: no pprof debug listener on
// 127.0.0.1:6060-6099 (a port outside the socket's peer and token checks).
func EngineHelperArgs(redisURL, bucketOverride string) (args []string, metaPassword string) {
	metaURL, metaPassword := SplitMetaPassword(redisURL)
	args = []string{
		"jm-engine", metaURL,
		"--no-usage-report",
		"--no-agent",
		"--no-bgjob",
		"--max-deletes", "0",
		"--read-only",
		"--cache-size", "0",
	}
	if bucketOverride != "" {
		args = append(args, "--bucket", bucketOverride)
	}
	return args, metaPassword
}

// EngineDoor is the engine socket inside a juicefs mount child
// (enginehost.Door, engine-direct step 2). Prepare is called before each
// child starts and returns the mount flags and the descriptors to inherit
// from firstFD on; Started is called once the child started or failed.
type EngineDoor interface {
	Prepare(firstFD int) (args []string, files []*os.File, err error)
	Started(err error)
}
