package nfs

import (
	"context"
)

func onRmDir(ctx context.Context, w *response, userHandle Handler) error {
	return removeEntry(w, userHandle, true)
}
