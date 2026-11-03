package respproxy

// accounting.go: the volume-wide accounting hashes (dir stats and quota
// usage): the values a write may store in them, and how far a person
// limited to grants may move the quota usage of a folder above them.

import (
	"strconv"
)

// DefaultProjectDepth is the project depth when ConfigKey sets none: a
// project is /<client>/<project>.
const DefaultProjectDepth = 2

// ProjectDepth parses the "depth" field of ConfigKey (jmlease:v1:cfg) as
// the lease engine reads it: 1 to 16, else DefaultProjectDepth. The
// Manager reads the same field with it, so its quota scope check and the
// proxy agree on what is above the project depth.
func ProjectDepth(v string) int {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 || n > 16 {
		return DefaultProjectDepth
	}
	return int(n)
}

// counterHash: a dir-stat or quota-usage hash. juicefs stores a decimal
// int64 in every field and parses it back with strconv.ParseInt
// (doLoadQuotas, doGetDirStat).
func counterHash(shape string) bool {
	switch shape {
	case "dirDataLength", "dirUsedSpace", "dirUsedInodes", "dirQuotaUsedSpace", "dirQuotaUsedInodes":
		return true
	}
	return false
}

// canonicalInt: s is an int64 as strconv.FormatInt writes it (and as
// go-redis sends juicefs's int64 arguments).
func canonicalInt(s []byte) bool {
	v, err := strconv.ParseInt(string(s), 10, 64)
	return err == nil && strconv.FormatInt(v, 10) == string(s)
}

// accountingValueDenial refuses a write that would leave a value juicefs
// cannot parse in a dir-stat, quota-usage or quota hash: HINCRBYFLOAT on any
// of them, and an HINCRBY delta or an HSET / HSETNX / HMSET value on a
// counter that is not a decimal int64. juicefs never sends such a write
// (pkg/meta/redis.go: doFlushQuotas and doUpdateDirStat send HINCRBY with
// int64 deltas; doSyncDirStat, mknod and HandleQuota HSET int64 values). One
// float in dirQuotaUsedSpace made every new juicefs client fail "Load
// quotas" (no quota was enforced anywhere), and the flush MULTI of every
// running client partially applied and was sent again, counting the other
// quotas' usage several times. It holds for every principal but bg, in
// every mode, with or without people.
//
// An increment of a quota definition (dirQuota holds packed limits, which
// juicefs only HSETs) is refused for every principal, bg included: it
// would make a definition, or a junk row, out of nothing.
func accountingValueDenial(p Principal, u *Unit) *Denial {
	for _, w := range u.Writes {
		if n := u.Cmds[w.Cmd].Info.Name; w.Key.Shape == "dirQuota" && (n == "HINCRBY" || n == "HINCRBYFLOAT") {
			return &Denial{Reason: ReasonAdminOnly, Detail: n + " on " + w.Raw + ": quota definitions are only set (juicefs quota set)"}
		}
	}
	if p.Kind == KindBG {
		return nil
	}
	bad := func(w KeyWrite, what string) *Denial {
		return &Denial{Reason: ReasonBGOnly, Detail: what + " on " + w.Raw + ": juicefs stores only int64 values there"}
	}
	for _, w := range u.Writes {
		if w.Key.Group != "dirstat" && w.Key.Group != "quota" {
			continue
		}
		c := u.Cmds[w.Cmd]
		switch c.Info.Name {
		case "HINCRBYFLOAT":
			return bad(w, "HINCRBYFLOAT")
		case "HINCRBY":
			if len(c.Args) != 4 || !canonicalInt(c.Args[3]) {
				return bad(w, "HINCRBY with a delta that is not an int64")
			}
		case "HSET", "HMSET", "HSETNX":
			if !counterHash(w.Key.Shape) {
				continue // dirQuota holds packed limits (admin only)
			}
			for i := 3; i < len(c.Args); i += 2 {
				if !canonicalInt(c.Args[i]) {
					return bad(w, c.Info.Name+" of a value that is not an int64")
				}
			}
		}
	}
	return nil
}

// quotaScopeDenial enforces the quota scope rule while grants are in
// force: a quota (dirQuota) and its usage rows exist only on project
// folders and below. A write that sets one of those rows (juicefs quota
// set, or quota check --repair: doSetQuota HSETs dirQuota and the usage
// hashes) for a folder above the project depth is refused for every
// principal but bg, admin included; removing one (juicefs quota delete,
// HDEL) is not. Above the project depth a quota would sum the writes of
// several projects: a person limited to one of them could not flush its
// usage without being able to move it for all of them, so the proxy
// refuses a limited person's flush that names it (grantCheck), and the
// Manager does not put grants in force while such a quota exists.
// Increments of an existing row by writers that are not limited (their
// juicefs flushes) still pass. A folder the proxy cannot place is refused.
// So is every write to these hashes as a whole (COPY, RENAME, DEL, EXPIRE,
// and the admin's RESTORE and MOVE of them): it could put a row above the
// project depth without naming its field. Left to the admin principal:
// its raw maintenance commands that name no key (FLUSHDB, SWAPDB, FCALL,
// ...), which can rewrite any key of the volume.
func (e *LeaseEngine) quotaScopeDenial(p Principal, u *Unit) *Denial {
	if p.Kind == KindBG {
		return nil
	}
	for _, c := range u.Cmds {
		if (c.Info.Name == "RESTORE" || c.Info.Name == "MOVE") && len(c.Args) > 1 {
			switch string(c.Args[1]) {
			case "dirQuota", "dirQuotaUsedSpace", "dirQuotaUsedInodes":
				return &Denial{Reason: ReasonNotGranted, Detail: c.Info.Name + " of " + string(c.Args[1]) +
					": while per-person grants are in force, quota rows are changed one field at a time"}
			}
		}
	}
	locked := false
	defer func() {
		if locked {
			e.topo.RUnlock()
		}
	}()
	for _, w := range u.Writes {
		switch w.Key.Shape {
		case "dirQuota", "dirQuotaUsedSpace", "dirQuotaUsedInodes":
		default:
			continue
		}
		c := u.Cmds[w.Cmd]
		switch c.Info.Name {
		case "HSET", "HSETNX", "HMSET", "HDEL", "HINCRBY", "HINCRBYFLOAT":
		default:
			return &Denial{Reason: ReasonNotGranted, Detail: c.Info.Name + " of " + w.Raw +
				" as a whole: while per-person grants are in force, quota rows are changed one field at a time"}
		}
		for _, r := range statRowsOf(c.Info.Name, w.Key, c.Args) {
			if r.op != "set" {
				continue
			}
			if !locked {
				e.topo.RLock()
				locked = true
			}
			loc, err := e.anc.resolveDir(r.dir)
			if err == nil && loc.kind != nsRoot {
				continue
			}
			what := "folder " + inoStr(r.dir)
			if lbl := e.anc.label(r.dir); lbl != "" {
				what += " (" + lbl + ")"
			}
			if err != nil {
				what += ", which the proxy cannot place (" + err.Error() + ")"
			}
			return &Denial{Reason: ReasonNotGranted, Detail: c.Info.Name + " " + w.Raw + " of " + what +
				": while per-person grants are in force, quotas are supported only on project folders and below"}
		}
	}
	return nil
}
