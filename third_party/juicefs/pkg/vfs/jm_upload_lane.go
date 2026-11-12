package vfs

// JuiceMount: small-upload lane (patch 0024, pkg/chunk/jm_upload_lane.go).

import "github.com/juicedata/juicefs/pkg/chunk"

// smallSaveMarker is the chunk writer's lane mark (*chunk.wSlice).
type smallSaveMarker interface{ SetSmallSave() }

// jmMarkSmallSave marks a slice that fileWriter.flush (fsync, close) is
// flushing, when the whole file is at most chunk.SmallUploadBytes. Only
// flush calls it: the background flushes never do. A slice already frozen by
// a background flush is marked too, so its blocks take the lane if they have
// not started uploading yet. Called with the file locked.
func jmMarkSmallSave(f *fileWriter, s *sliceWriter) {
	if f.length > chunk.SmallUploadBytes || s.done {
		return
	}
	if m, ok := s.writer.(smallSaveMarker); ok {
		m.SetSmallSave()
	}
}
