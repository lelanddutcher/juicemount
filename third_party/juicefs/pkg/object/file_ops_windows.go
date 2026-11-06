/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"os"
	"time"
)

func lchtimesPath(p string, mtime time.Time) error { return os.Chtimes(p, time.Time{}, mtime) }
