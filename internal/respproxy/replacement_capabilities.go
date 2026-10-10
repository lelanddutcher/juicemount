package respproxy

// REPLACEMENT_CAPABILITIES is a read-only snapshot. A positive reply only
// permits attempting a new create; its birth receipt still has to prove the
// exact operation and proxy incarnation before any content can be written.
// The fixed RESP2 array is [version, selected DB, principal, incarnation,
// ready], with integer version/DB/ready and bulk identity fields.
func (c *conn) replacementCapabilities() []byte {
	if c.principal == nil || c.principal.Kind != KindMac || c.db != c.p.cfg.VolumeDB {
		return errFrame("replacement capabilities require the volume's Mac writer")
	}
	incarnation := c.p.Incarnation()
	if len(c.principal.Name) == 0 || len(c.principal.Name) > 256 || len(incarnation) > 256 || c.p.fenced() {
		return errFrame("replacement capabilities unavailable")
	}
	ready := int64(0)
	_, leaseEngine := c.p.policy.(*LeaseEngine)
	if c.p.stageBirth.Load() && c.p.Mode() == ModeEnforce && c.p.cfg.Singleton && leaseEngine && incarnation != "" {
		// A heartbeat timestamp alone cannot establish current ownership. A
		// stale or foreign singleton must never advertise fresh birth support.
		v, err := c.p.side.Do(c.p.cfg.VolumeDB, strArgs("GET", SingletonKey)...)
		if err != nil || v.Err() != nil || v.Null || v.Type != '$' || string(v.Str) != incarnation || c.p.fenced() {
			return errFrame("replacement capabilities ownership unavailable")
		}
		ready = 1
	}
	var out respBuf
	out.arr(5)
	out.integer(1)
	out.integer(int64(c.db))
	out.bulk(c.principal.Name)
	out.bulk(incarnation)
	out.integer(ready)
	return out.Bytes()
}
