CREATE TABLE IF NOT EXISTS owners (
		volume TEXT PRIMARY KEY, session TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch > 0),
		expires_ns INTEGER NOT NULL, admission_closed INTEGER NOT NULL DEFAULT 0 CHECK(admission_closed IN (0,1))
	);
	CREATE TABLE IF NOT EXISTS operations (
		volume TEXT NOT NULL, id TEXT NOT NULL, session TEXT NOT NULL, epoch INTEGER NOT NULL,
		digest TEXT NOT NULL, expected_revision TEXT NOT NULL, state TEXT NOT NULL
		  CHECK(state IN ('PREPARED','COMMITTED','NO_EFFECT')),
		revision TEXT NOT NULL DEFAULT '', evidence TEXT NOT NULL DEFAULT '', committed_ns INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(volume,id), FOREIGN KEY(volume) REFERENCES owners(volume)
	);
	CREATE INDEX IF NOT EXISTS operations_unsettled ON operations(volume,epoch,state);