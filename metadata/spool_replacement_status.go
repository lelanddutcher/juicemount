package metadata

import "strings"

// ReplacementPublicationStatus is display-only local journal identity. It
// never authorizes a dispatch, stage operation, settlement or cleanup.
type ReplacementPublicationStatus struct {
	RowID       int64
	OperationID string
	Revision    int64
	Phase       string
}

// ReplacementPublicationsForStatus reads only journal entries in the listed
// spool rows. The publication's durable checks remain in the coordinator.
func (s *SpoolStore) ReplacementPublicationsForStatus(list []*SpoolRow) (map[int64]ReplacementPublicationStatus, error) {
	out := make(map[int64]ReplacementPublicationStatus)
	listed := make(map[int64]DrainState, len(list))
	ids := make([]int64, 0, len(list))
	for _, row := range list {
		if row != nil && (row.DrainState == DrainReady || row.DrainState == DrainDone) {
			if _, seen := listed[row.ID]; !seen {
				ids = append(ids, row.ID)
			}
			listed[row.ID] = row.DrainState
		}
	}
	// Query only the bounded status list by the publication's primary key.
	// A global phase scan would re-read every historical payload on each poll.
	for start := 0; start < len(ids); start += 100 {
		end := start + 100
		if end > len(ids) {
			end = len(ids)
		}
		placeholders := make([]string, end-start)
		args := make([]any, end-start)
		for i, id := range ids[start:end] {
			placeholders[i], args[i] = "?", id
		}
		rows, err := s.db.Query(`SELECT p.spool_id, p.operation_id, p.phase, p.payload, s.drain_state
 FROM spool_replacement_publications p JOIN spool_entries s ON s.id=p.spool_id
 WHERE p.spool_id IN (`+strings.Join(placeholders, ",")+`) AND s.retired_by=0
 AND p.phase IN ('queued','intent','arming','armed','committed','settled','cleaning')`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var operation, phase, currentState string
			var raw []byte
			if err := rows.Scan(&id, &operation, &phase, &raw, &currentState); err != nil {
				rows.Close()
				return nil, err
			}
			state, ok := listed[id]
			if !ok {
				rows.Close()
				return nil, ErrMirrorConflict
			}
			// ListForStatus and this query are separate read snapshots. A row
			// settled between them belongs to the next status poll.
			if string(state) != currentState {
				continue
			}
			p, err := decodeReplacementPublication(raw)
			if err != nil || p.SourceRowID != id || p.OperationID != operation || p.Phase != phase {
				rows.Close()
				return nil, ErrMirrorConflict
			}
			if (currentState == string(DrainDone)) != (phase == ReplacementPublicationSettled || phase == ReplacementPublicationCleaning) {
				rows.Close()
				return nil, ErrMirrorConflict
			}
			out[id] = ReplacementPublicationStatus{RowID: id, OperationID: operation, Revision: p.Revision, Phase: phase}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
