package catalog

// Format 4 adds saved queries: named, tombstoned, never deleted, each with a
// definition fixed at creation (stored canonical JSON).
func querySchema() []schemaObject {
	return []schemaObject{
		{"table", "queries", "queries", `CREATE TABLE queries (
 query_id TEXT PRIMARY KEY NOT NULL CHECK(length(query_id)=36 AND substr(query_id,1,4)='qry_' AND substr(query_id,5) NOT GLOB '*[^0-9a-f]*' AND substr(query_id,5)<>'00000000000000000000000000000000'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 256 AND instr(name,char(0))=0),
 definition TEXT NOT NULL CHECK(length(CAST(definition AS BLOB)) BETWEEN 2 AND 131072 AND instr(definition,char(0))=0),
 created_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)),
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence)
)`},
		{"trigger", "queries_no_delete", "queries", "CREATE TRIGGER queries_no_delete BEFORE DELETE ON queries BEGIN SELECT RAISE(ABORT,'catalog tombstone required'); END"},
		{"trigger", "queries_identity", "queries", "CREATE TRIGGER queries_identity BEFORE UPDATE ON queries WHEN NEW.query_id<>OLD.query_id OR NEW.library_id<>OLD.library_id OR NEW.definition<>OLD.definition OR NEW.created_sequence<>OLD.created_sequence OR NEW.updated_sequence<OLD.updated_sequence BEGIN SELECT RAISE(ABORT,'immutable query identity'); END"},
	}
}
