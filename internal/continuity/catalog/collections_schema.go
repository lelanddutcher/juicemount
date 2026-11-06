package catalog

// Format 3 adds collections: named, tombstoned sets of assets. Membership rows
// are never deleted; removal and reactivation keep their creation sequence.
func collectionSchema() []schemaObject {
	return []schemaObject{
		{"table", "collections", "collections", `CREATE TABLE collections (
 collection_id TEXT PRIMARY KEY NOT NULL CHECK(length(collection_id)=36 AND substr(collection_id,1,4)='col_' AND substr(collection_id,5) NOT GLOB '*[^0-9a-f]*' AND substr(collection_id,5)<>'00000000000000000000000000000000'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 256 AND instr(name,char(0))=0),
 created_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)),
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence)
)`},
		{"table", "collection_members", "collection_members", `CREATE TABLE collection_members (
 collection_id TEXT NOT NULL REFERENCES collections(collection_id),
 asset_id TEXT NOT NULL REFERENCES assets(asset_id),
 created_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)),
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence),
 PRIMARY KEY(collection_id,asset_id)
)`},
		{"index", "collection_members_active", "collection_members", "CREATE INDEX collection_members_active ON collection_members(collection_id,removed,asset_id)"},
		{"index", "collection_members_asset", "collection_members", "CREATE INDEX collection_members_asset ON collection_members(asset_id,removed,collection_id)"},
		{"trigger", "collections_no_delete", "collections", "CREATE TRIGGER collections_no_delete BEFORE DELETE ON collections BEGIN SELECT RAISE(ABORT,'catalog tombstone required'); END"},
		{"trigger", "collections_identity", "collections", "CREATE TRIGGER collections_identity BEFORE UPDATE ON collections WHEN NEW.collection_id<>OLD.collection_id OR NEW.library_id<>OLD.library_id OR NEW.created_sequence<>OLD.created_sequence OR NEW.updated_sequence<OLD.updated_sequence BEGIN SELECT RAISE(ABORT,'immutable collection identity'); END"},
		{"trigger", "collection_members_no_delete", "collection_members", "CREATE TRIGGER collection_members_no_delete BEFORE DELETE ON collection_members BEGIN SELECT RAISE(ABORT,'catalog tombstone required'); END"},
		{"trigger", "collection_members_identity", "collection_members", "CREATE TRIGGER collection_members_identity BEFORE UPDATE ON collection_members WHEN NEW.collection_id<>OLD.collection_id OR NEW.asset_id<>OLD.asset_id OR NEW.created_sequence<>OLD.created_sequence OR NEW.updated_sequence<OLD.updated_sequence BEGIN SELECT RAISE(ABORT,'immutable collection member identity'); END"},
	}
}
