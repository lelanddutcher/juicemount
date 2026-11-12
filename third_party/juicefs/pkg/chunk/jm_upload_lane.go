/*
 * JuiceMount: small-upload lane (patch 0024).
 *
 * Upstream uploads every block through one semaphore of --max-uploads tokens,
 * and blocked uploads take it in arrival order. On a thin far link a large
 * file keeps all tokens busy with whole 4 MiB blocks, so the PUT of a small
 * file that an fsync waits for queues behind them: about 30 s per small save
 * in the lab (200 ms RTT, 1% loss, --max-uploads 4).
 *
 * The lane adds --small-uploads tokens that only small slices can take. A
 * small slice is one being finished (fsync, close, or chunk end) whose whole
 * length is at most smallUploadBytes. Its block takes a lane token if one is
 * free, otherwise whichever token, lane or shared, frees first. Large blocks
 * use the shared tokens only, in arrival order as before.
 *
 * Bounds: at most MaxUpload + SmallUploads PUTs run at once, and at most
 * SmallUploads * smallUploadBytes bytes of them belong to the lane beyond the
 * shared pool. Durability and ordering do not change: a block's result still
 * reaches the slice's error channel only after its PUT, and Finish still waits
 * for every block of the slice.
 */

package chunk

// smallUploadBytes is the largest slice that takes the small-upload lane.
const smallUploadBytes = 1 << 20

// smallLane reports whether the slice's blocks may take the small-upload lane.
// Called from FlushTo/Finish on the writer's goroutine, before the upload
// goroutine starts.
func (s *wSlice) smallLane() bool {
	return s.finishing && s.length <= smallUploadBytes && s.store.smallUpload != nil
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
