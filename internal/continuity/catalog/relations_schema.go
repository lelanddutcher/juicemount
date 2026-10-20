package catalog

func relationalSchema() []schemaObject {
	return []schemaObject{
		{"table", "relationships", "relationships", `CREATE TABLE relationships (
 relationship_id TEXT PRIMARY KEY NOT NULL CHECK(length(relationship_id) BETWEEN 1 AND 128 AND relationship_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 source_asset_id TEXT NOT NULL REFERENCES assets(asset_id),
 source_revision TEXT,
 target_asset_id TEXT NOT NULL REFERENCES assets(asset_id),
 target_revision TEXT,
 predicate TEXT NOT NULL CHECK(length(CAST(predicate AS BLOB)) BETWEEN 1 AND 128 AND instr(predicate,char(0))=0),
 provenance TEXT NOT NULL CHECK(provenance IN ('observed','claimed','inferred')),
 source TEXT NOT NULL CHECK(length(CAST(source AS BLOB)) BETWEEN 1 AND 256 AND instr(source,char(0))=0),
 created_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)),
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence),
 FOREIGN KEY(source_asset_id,source_revision) REFERENCES revisions(asset_id,revision_id),
 FOREIGN KEY(target_asset_id,target_revision) REFERENCES revisions(asset_id,revision_id)
)`},
		{"table", "locations", "locations", `CREATE TABLE locations (
 location_id TEXT PRIMARY KEY NOT NULL CHECK(length(location_id) BETWEEN 1 AND 128 AND location_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 asset_id TEXT NOT NULL REFERENCES assets(asset_id),
 revision TEXT,
 source TEXT NOT NULL CHECK(length(CAST(source AS BLOB)) BETWEEN 1 AND 256 AND instr(source,char(0))=0),
 locator TEXT NOT NULL CHECK(length(CAST(locator AS BLOB)) BETWEEN 1 AND 8192 AND instr(locator,char(0))=0),
 created_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)),
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence),
 FOREIGN KEY(asset_id,revision) REFERENCES revisions(asset_id,revision_id)
)`},
		{"index", "relationships_source_active", "relationships", "CREATE INDEX relationships_source_active ON relationships(source_asset_id,removed,relationship_id)"},
		{"index", "relationships_target_active", "relationships", "CREATE INDEX relationships_target_active ON relationships(target_asset_id,removed,relationship_id)"},
		{"index", "locations_asset_active", "locations", "CREATE INDEX locations_asset_active ON locations(asset_id,removed,location_id)"},
		{"trigger", "relationships_no_delete", "relationships", "CREATE TRIGGER relationships_no_delete BEFORE DELETE ON relationships BEGIN SELECT RAISE(ABORT,'catalog tombstone required'); END"},
		{"trigger", "relationships_identity", "relationships", "CREATE TRIGGER relationships_identity BEFORE UPDATE ON relationships WHEN NEW.relationship_id<>OLD.relationship_id OR NEW.library_id<>OLD.library_id OR NEW.created_sequence<>OLD.created_sequence OR NEW.updated_sequence<OLD.updated_sequence BEGIN SELECT RAISE(ABORT,'immutable relationship identity'); END"},
		{"trigger", "locations_no_delete", "locations", "CREATE TRIGGER locations_no_delete BEFORE DELETE ON locations BEGIN SELECT RAISE(ABORT,'catalog tombstone required'); END"},
		{"trigger", "locations_identity", "locations", "CREATE TRIGGER locations_identity BEFORE UPDATE ON locations WHEN NEW.location_id<>OLD.location_id OR NEW.library_id<>OLD.library_id OR NEW.asset_id<>OLD.asset_id OR NEW.created_sequence<>OLD.created_sequence OR NEW.updated_sequence<OLD.updated_sequence BEGIN SELECT RAISE(ABORT,'immutable location identity'); END"},
	}
}
