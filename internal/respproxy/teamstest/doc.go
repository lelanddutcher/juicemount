// Package teamstest holds the end-to-end tests of person grants and the
// per-person audit log (Teams slice 1): a real jm-respproxy with the lease
// engine in front of Redis 7, driven by the JuiceFS metadata client the way
// a Mac's juicefs drives it. It has no code of its own; it lives apart from
// internal/respproxy so that package's tests do not link the JuiceFS client.
//
// The tests need a disposable Redis (JM_RESPPROXY_TEST_REDIS=host:port) and
// use its database 10, which they flush.
package teamstest
