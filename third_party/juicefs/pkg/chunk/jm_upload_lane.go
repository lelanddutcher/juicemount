/*
 * JuiceMount: small-upload lane (patch 0024).
 *
 * Upstream uploads every block through one semaphore of --max-uploads tokens,
 * and blocked uploads take it in arrival order. On a thin far link a large
 * file keeps all tokens busy with whole 4 MiB blocks, so the PUT of a small
 * file that an fsync waits for queues behind them: about 38 s per small save
 * in the lab (200 ms RTT, 1% loss, --max-uploads 4).
 *
 * The lane adds --small-uploads tokens that only small saves can take. A
 * small save is a slice that fileWriter.flush (fsync, close) freezes while
 * the whole file is at most SmallUploadBytes (pkg/vfs/jm_upload_lane.go
 * marks it with SetSmallSave). The background flushes (a slice older than
 * 5 s or idle for 1 s, a full chunk) never mark a slice, so a large file
 * written slower than 1 MiB per 5 s, whose slices are each under 1 MiB,
 * stays in the shared tokens. A small save's block takes a lane token if
 * one is free, otherwise whichever token, lane or shared, frees first.
 * Every other block uses the shared tokens only, in arrival order as before.
 *
 * Bounds: at most MaxUpload + SmallUploads PUTs run at once, and at most
 * SmallUploads * SmallUploadBytes bytes of them belong to the lane beyond the
 * shared pool. Durability and ordering do not change: a block's result still
 * reaches the slice's error channel only after its PUT, and Finish still waits
 * for every block of the slice.
 */

package chunk

// SmallUploadBytes is the largest file whose fsync or close takes the
// small-upload lane.
const SmallUploadBytes = 1 << 20

// SetSmallSave marks the slice as part of an fsync or close of a file of at
// most SmallUploadBytes. Blocks that start uploading afterwards may take the
// lane; blocks already queued keep their pool.
func (s *wSlice) SetSmallSave() { s.smallSave.Store(true) }

// smallLane reports whether the slice's blocks may take the small-upload lane.
// Called from FlushTo/Finish on the writer's goroutine, before the upload
// goroutine starts. The length check keeps the lane bounded even if a caller
// marks a larger slice.
func (s *wSlice) smallLane() bool {
	return s.store.smallUpload != nil && s.smallSave.Load() && s.length <= SmallUploadBytes
}

// acquireUpload takes one upload token and returns its release. A small block
// prefers a free lane token, so it leaves the shared tokens to large blocks;
// when none is free it waits for the first token of either pool.
func (store *cachedStore) acquireUpload(small bool) (release func()) {
	if small && store.smallUpload != nil {
		select {
		case store.smallUpload <- true:
			return store.releaseSmallUpload
		default:
		}
		select {
		case store.smallUpload <- true:
			return store.releaseSmallUpload
		case store.currentUpload <- true:
			return store.releaseSharedUpload
		}
	}
	store.currentUpload <- true
	return store.releaseSharedUpload
}

func (store *cachedStore) releaseSmallUpload()  { <-store.smallUpload }
func (store *cachedStore) releaseSharedUpload() { <-store.currentUpload }

// uploadsInFlight counts the tokens held in both pools.
func (store *cachedStore) uploadsInFlight() int {
	return len(store.currentUpload) + len(store.smallUpload)
}
