package nfs

import (
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

const replacementUnavailableWindow = 2 * time.Minute
const replacementUnavailableCap = 256

type replacementUnavailable struct {
	operation string
	revision  int64
	phase     string
	at        time.Time
}

func (q *replacementDispatch) clearUnavailable(id int64) {
	q.availabilityMu.Lock()
	delete(q.unavailable, id)
	q.availabilityMu.Unlock()
}

func (q *replacementDispatch) noteUnavailable(p metadata.SpoolReplacementPublication) {
	if p.Phase != metadata.ReplacementPublicationQueued && p.Phase != metadata.ReplacementPublicationIntent &&
		p.Phase != metadata.ReplacementPublicationArming && p.Phase != metadata.ReplacementPublicationArmed {
		return
	}
	q.availabilityMu.Lock()
	defer q.availabilityMu.Unlock()
	if q.unavailable == nil {
		q.unavailable = make(map[int64]replacementUnavailable)
	}
	now := time.Now()
	var oldest int64
	for id, v := range q.unavailable {
		if now.Sub(v.at) >= replacementUnavailableWindow {
			delete(q.unavailable, id)
			continue
		}
		if oldest == 0 || v.at.Before(q.unavailable[oldest].at) {
			oldest = id
		}
	}
	if len(q.unavailable) >= replacementUnavailableCap && oldest != 0 {
		delete(q.unavailable, oldest)
	}
	q.unavailable[p.SourceRowID] = replacementUnavailable{p.OperationID, p.Revision, p.Phase, now}
}

func (q *replacementDispatch) recentlyUnavailable(p metadata.ReplacementPublicationStatus) bool {
	if q == nil {
		return false
	}
	q.availabilityMu.Lock()
	defer q.availabilityMu.Unlock()
	v, ok := q.unavailable[p.RowID]
	if ok && time.Since(v.at) >= replacementUnavailableWindow {
		delete(q.unavailable, p.RowID)
		return false
	}
	return ok && v.operation == p.OperationID && v.revision == p.Revision && v.phase == p.Phase
}
