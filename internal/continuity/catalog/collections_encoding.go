package catalog

import (
	"bytes"
	"encoding/json"
)

func generatedCollectionPolicyID(id CollectionOperationID) CollectionPolicyOperationID {
	return CollectionPolicyOperationID("cpo_" + string(id)[4:])
}

func validCollectionEntries(entries []NewCollectionEntry) bool {
	if entries == nil || len(entries) > 64 {
		return false
	}
	assets := make(map[string]bool, len(entries))
	for i, e := range entries {
		if e.ID.Validate() != nil || e.AssetID.Validate() != nil || i > 0 && entries[i-1].ID >= e.ID || assets[string(e.AssetID)] {
			return false
		}
		assets[string(e.AssetID)] = true
	}
	return true
}

func collectionValue(op CollectionRequest) (any, error) {
	var value any
	switch op.Kind {
	case "create":
		value = &CreateCollection{}
	case "rename":
		value = &RenameCollection{}
	case "entries":
		value = &EditCollectionEntries{}
	case "archive":
		value = &SetCollectionArchived{}
	case "grants":
		value = &EditCollectionGrants{}
	default:
		return nil, invalid("collection kind")
	}
	if decodeCanonicalValue(op.Value, value) != nil {
		return nil, invalid("collection value JSON")
	}
	valid := false
	switch v := value.(type) {
	case *CreateCollection:
		valid = validText(v.Name, 256, true) && v.CreatorGrantID.Validate() == nil && validCollectionEntries(v.Entries)
	case *RenameCollection:
		valid = validText(v.Name, 256, true)
	case *SetCollectionArchived:
		valid = true
	case *EditCollectionEntries:
		valid = len(v.Changes) > 0 && len(v.Changes) <= 64
		ids, assets := make(map[CollectionEntryID]bool), make(map[string]bool)
		for _, c := range v.Changes {
			if (c.Kind != "add" && c.Kind != "remove" && c.Kind != "reactivate") || c.ID.Validate() != nil || c.AssetID.Validate() != nil || ids[c.ID] || assets[string(c.AssetID)] {
				valid = false
			}
			ids[c.ID], assets[string(c.AssetID)] = true, true
		}
	case *EditCollectionGrants:
		valid = validCollectionGrantChanges(v.Changes, "collection", op.CollectionID)
	}
	if !valid {
		return nil, invalid("collection value")
	}
	return value, nil
}

func CanonicalCollectionRequest(op CollectionRequest) ([]byte, string, error) {
	if op.Version != 1 || op.LibraryID.Validate() != nil || op.ID.Validate() != nil || op.CollectionID.Validate() != nil || (op.Kind == "create" && op.ExpectedVersion != "") || (op.Kind != "create" && op.ExpectedVersion.Validate() != nil) {
		return nil, "", invalid("collection envelope")
	}
	if _, err := collectionValue(op); err != nil {
		return nil, "", err
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > MaxCollectionRequestBytes {
		return nil, "", invalid("collection request bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.operation\x00\x01", b), nil
}

func DecodeCollectionRequest(data []byte) (CollectionRequest, error) {
	var op CollectionRequest
	if len(data) == 0 || len(data) > MaxCollectionRequestBytes || decodeCanonicalValue(data, &op) != nil {
		return CollectionRequest{}, invalid("collection request JSON")
	}
	b, _, err := CanonicalCollectionRequest(op)
	if err != nil || !bytes.Equal(b, data) {
		return CollectionRequest{}, invalid("collection request")
	}
	return op, nil
}

func CanonicalCollectionRecord(record CollectionRecord) ([]byte, string, error) {
	if record.Version != 1 || record.ActorSubjectID.Validate() != nil || record.ResultVersion.Validate() != nil || record.ResultVersion == record.Request.ExpectedVersion || record.RecordedUnixNano <= 0 {
		return nil, "", invalid("collection record")
	}
	if _, _, err := CanonicalCollectionRequest(record.Request); err != nil {
		return nil, "", err
	}
	generated := record.Request.Kind == "create" || record.Request.Kind == "grants"
	if generated && record.GeneratedPolicyOperationID != generatedCollectionPolicyID(record.Request.ID) || !generated && record.GeneratedPolicyOperationID != "" {
		return nil, "", invalid("collection record policy binding")
	}
	b, err := json.Marshal(record)
	if err != nil || len(b) > MaxCollectionRecordBytes {
		return nil, "", invalid("collection record bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.record\x00\x01", b), nil
}

func DecodeCollectionRecord(data []byte) (CollectionRecord, error) {
	var record CollectionRecord
	if len(data) == 0 || len(data) > MaxCollectionRecordBytes || decodeCanonicalValue(data, &record) != nil {
		return CollectionRecord{}, invalid("collection record JSON")
	}
	b, _, err := CanonicalCollectionRecord(record)
	if err != nil || !bytes.Equal(b, data) {
		return CollectionRecord{}, invalid("collection record")
	}
	return record, nil
}

func CanonicalCollectionReceipt(receipt CollectionReceipt) ([]byte, error) {
	if receipt.Version != 1 || receipt.OperationID.Validate() != nil || receipt.CollectionID.Validate() != nil || receipt.ResultVersion.Validate() != nil {
		return nil, invalid("collection receipt")
	}
	b, err := json.Marshal(receipt)
	if err != nil || len(b) > MaxCollectionReceiptBytes {
		return nil, invalid("collection receipt bytes")
	}
	return b, nil
}

func DecodeCollectionReceipt(data []byte) (CollectionReceipt, error) {
	var receipt CollectionReceipt
	if len(data) == 0 || len(data) > MaxCollectionReceiptBytes || decodeCanonicalValue(data, &receipt) != nil {
		return CollectionReceipt{}, invalid("collection receipt JSON")
	}
	b, err := CanonicalCollectionReceipt(receipt)
	if err != nil || !bytes.Equal(b, data) {
		return CollectionReceipt{}, invalid("collection receipt")
	}
	return receipt, nil
}
