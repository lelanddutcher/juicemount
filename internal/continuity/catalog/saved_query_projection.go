package catalog

import (
	"context"
	"database/sql"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type savedQueryDefinitionProjection struct {
	LibraryID         continuity.LibraryID `json:"library_id"`
	CollectionID      CollectionID         `json:"collection_id"`
	Kind              string               `json:"kind"`
	Name              string               `json:"name"`
	Archived          bool                 `json:"archived"`
	DefinitionVersion CollectionVersion    `json:"definition_version"`
	CreatedSequence   int64                `json:"created_sequence"`
	UpdatedSequence   int64                `json:"updated_sequence"`
	Definition        SavedQueryDefinition `json:"definition"`
}

// Query definitions are the final table in domain4. BLOB bounds are enforced
// before Scan; D is decoded canonically, never emitted as unchecked raw JSON.
func walkSavedQueryProjections(ctx context.Context, q queryer, library continuity.LibraryID, emit func(byte, any) error) error {
	return walkRows(ctx, q, `SELECT i.library_id,d.collection_id,d.kind,CASE WHEN length(CAST(d.name AS BLOB)) BETWEEN 1 AND 256 THEN d.name END,d.archived,d.definition_version,d.created_sequence,d.updated_sequence,CASE WHEN length(d.definition) BETWEEN 1 AND 8192 THEN d.definition END FROM saved_query_definitions d JOIN collection_identities i USING(collection_id) ORDER BY d.collection_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p savedQueryDefinitionProjection
		var data []byte
		if err := rows.Scan(&p.LibraryID, &p.CollectionID, &p.Kind, &p.Name, &p.Archived, &p.DefinitionVersion, &p.CreatedSequence, &p.UpdatedSequence, &data); err != nil {
			return err
		}
		var err error
		p.Definition, err = DecodeSavedQueryDefinition(data)
		if err != nil {
			return err
		}
		if p.LibraryID != library || p.CollectionID.Validate() != nil || p.Kind != "saved_query" || !validText(p.Name, 256, true) || p.DefinitionVersion.Validate() != nil || p.CreatedSequence <= 0 || p.UpdatedSequence < p.CreatedSequence {
			return corrupt("query definition projection")
		}
		return emit(0x21, p)
	})
}
