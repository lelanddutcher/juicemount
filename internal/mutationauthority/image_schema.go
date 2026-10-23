package mutationauthority

var imageSchema = map[string]string{
	"image_store": `CREATE TABLE image_store (
 id INTEGER PRIMARY KEY CHECK(id=1), store_id TEXT NOT NULL UNIQUE, volume TEXT NOT NULL,
 max_images INTEGER NOT NULL CHECK(max_images BETWEEN 1 AND 1024), max_logical INTEGER NOT NULL CHECK(max_logical BETWEEN 0 AND 17179869184),
 max_metadata INTEGER NOT NULL CHECK(max_metadata BETWEEN 1 AND 67108864),
 images INTEGER NOT NULL CHECK(images>=0 AND images<=max_images), logical INTEGER NOT NULL CHECK(logical>=0 AND logical<=max_logical),
 metadata INTEGER NOT NULL CHECK(metadata>=0 AND metadata<=max_metadata), active INTEGER NOT NULL CHECK(active IN (0,1)), activation INTEGER NOT NULL CHECK(activation>0))`,
	"image_seals": `CREATE TABLE image_seals (
 image_id TEXT PRIMARY KEY, store INTEGER NOT NULL DEFAULT 1 CHECK(store=1), producer TEXT NOT NULL, operation_id TEXT NOT NULL,
 incarnation TEXT NOT NULL, library_id TEXT NOT NULL, envelope BLOB NOT NULL, digest TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('RESERVED','SEALED')), logical INTEGER NOT NULL CHECK(logical BETWEEN 0 AND 1073741824),
 metadata INTEGER NOT NULL CHECK(metadata>0), receipt BLOB,
 CHECK((state='RESERVED' AND receipt IS NULL) OR (state='SEALED' AND receipt IS NOT NULL)),
 UNIQUE(producer,operation_id), UNIQUE(producer,incarnation), FOREIGN KEY(store) REFERENCES image_store(id))`,
	"image_parts": `CREATE TABLE image_parts (
 image_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 127), volume TEXT NOT NULL, custody_id TEXT NOT NULL, receipt BLOB NOT NULL,
 PRIMARY KEY(image_id,ordinal), UNIQUE(image_id,volume,custody_id),
 FOREIGN KEY(image_id) REFERENCES image_seals(image_id), FOREIGN KEY(volume,custody_id) REFERENCES operations(volume,id))`,
	"image_readers": `CREATE TABLE image_readers (
 image_id TEXT PRIMARY KEY, reader TEXT, generation INTEGER NOT NULL CHECK(generation>0), FOREIGN KEY(image_id) REFERENCES image_seals(image_id))`,
	"image_reader_events": `CREATE TABLE image_reader_events (
 image_id TEXT NOT NULL, operation_id TEXT NOT NULL, request BLOB NOT NULL, digest TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>1), receipt BLOB NOT NULL, charge INTEGER NOT NULL CHECK(charge>0),
 PRIMARY KEY(image_id,operation_id), UNIQUE(image_id,generation), FOREIGN KEY(image_id) REFERENCES image_seals(image_id))`,
}
