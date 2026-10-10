package metadata

import (
	"database/sql"
	"time"
)

// NULL preserves the distinction between a legacy coarse observation and an
// authoritative timestamp whose nanoseconds happen to be exactly zero.
func (e *Entry) mtimeNsecSQL() any {
	if !e.MtimeNsecKnown {
		return nil
	}
	return e.Mtime.Nanosecond()
}

func (e *Entry) restoreMtime(seconds int64, fraction sql.NullInt64) {
	e.MtimeNsecKnown = fraction.Valid && fraction.Int64 >= 0 && fraction.Int64 < 1e9
	var ns int64
	if e.MtimeNsecKnown {
		ns = fraction.Int64
	}
	e.Mtime = time.Unix(seconds, ns)
}
