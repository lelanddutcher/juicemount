package catalog

import (
	"context"
	"database/sql"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type savedQueryDefinition struct {
	collectionDefinition
	definition       SavedQueryDefinition
	canonical        []byte
	created, updated int64
}

// Public manual readers remain manual-only. This reader is for shared policy.
func readAnyCollectionDefinition(ctx context.Context, q queryer, id CollectionID) (collectionDefinition, error) {
	kind, err := collectionStoredKind(ctx, q, id)
	if err != nil {
		return collectionDefinition{}, err
	}
	if kind == "manual" {
		return readCollectionDefinition(ctx, q, id)
	}
	d, err := readSavedQueryDefinition(ctx, q, id)
	return d.collectionDefinition, err
}

func collectionStoredKind(ctx context.Context, q queryer, id CollectionID) (string, error) {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return "", err
	}
	if !supportsSavedQueries(version) {
		_, err := readCollectionDefinition(ctx, q, id)
		return "manual", err
	}
	var identity, manual, saved int
	err = q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM collection_identities WHERE collection_id=?),(SELECT count(*) FROM collection_definitions WHERE collection_id=?),(SELECT count(*) FROM saved_query_definitions WHERE collection_id=?)`, id, id, id).Scan(&identity, &manual, &saved)
	if err != nil {
		return "", err
	}
	if identity == 0 && manual == 0 && saved == 0 {
		return "", ErrNotFound
	}
	if identity != 1 || manual+saved != 1 {
		return "", corrupt("collection immutable kind")
	}
	if saved == 1 {
		return "saved_query", nil
	}
	return "manual", nil
}

func readSavedQueryDefinition(ctx context.Context, q queryer, id CollectionID) (savedQueryDefinition, error) {
	var d savedQueryDefinition
	d.id = id
	err := q.QueryRowContext(ctx, `SELECT i.creator_subject_id,CASE WHEN length(CAST(d.name AS BLOB)) BETWEEN 1 AND 256 THEN d.name END,d.archived,d.definition_version,i.policy_generation,d.created_sequence,d.updated_sequence,CASE WHEN length(d.definition) BETWEEN 1 AND 8192 THEN d.definition END FROM saved_query_definitions d JOIN collection_identities i USING(collection_id) WHERE d.collection_id=?`, id).Scan(&d.creator, &d.name, &d.archived, &d.version, &d.generation, &d.created, &d.updated, &d.canonical)
	if errors.Is(err, sql.ErrNoRows) {
		return d, ErrNotFound
	}
	if err != nil {
		return d, err
	}
	d.definition, err = DecodeSavedQueryDefinition(d.canonical)
	if err != nil || d.id.Validate() != nil || d.creator.Validate() != nil || !validText(d.name, 256, true) || d.version.Validate() != nil || d.generation < 1 || d.created <= 0 || d.updated < d.created {
		return savedQueryDefinition{}, corrupt("saved query definition")
	}
	return d, nil
}

func savedQueryAnchors(ctx context.Context, q queryer, library continuity.LibraryID, definition SavedQueryDefinition) error {
	for _, id := range definition.ContextAssetIDs {
		a, err := readAsset(ctx, q, library, id)
		if err != nil {
			return err
		}
		if a.LibraryID != library {
			return corrupt("saved query anchor library")
		}
	}
	return nil
}

// Filter by established kind before manual counts or publication bindings.
func manualDirectoryIdentity(ctx context.Context, q queryer, id CollectionID) (bool, error) {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return false, err
	}
	if !supportsSavedQueries(version) {
		return true, nil
	}
	kind, err := collectionStoredKind(ctx, q, id)
	return kind == "manual", err
}
