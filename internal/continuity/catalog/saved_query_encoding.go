package catalog

import (
	"bytes"
	"encoding/json"
)

func CanonicalSavedQueryDefinition(definition SavedQueryDefinition) ([]byte, string, error) {
	if definition.Version != 1 || definition.Kind != "ranked_assets" || !validText(definition.Text, 256, false) || definition.ContextAssetIDs == nil || len(definition.ContextAssetIDs) > 64 {
		return nil, "", invalid("saved query definition")
	}
	for i, id := range definition.ContextAssetIDs {
		if id.Validate() != nil || i > 0 && definition.ContextAssetIDs[i-1] >= id {
			return nil, "", invalid("saved query anchors")
		}
	}
	b, err := json.Marshal(definition)
	if err != nil || len(b) > MaxSavedQueryDefinitionBytes {
		return nil, "", invalid("saved query definition bytes")
	}
	return b, domainDigest("juicemount.catalog.saved_query.definition\x00\x01", b), nil
}

func DecodeSavedQueryDefinition(data []byte) (SavedQueryDefinition, error) {
	var definition SavedQueryDefinition
	if len(data) == 0 || len(data) > MaxSavedQueryDefinitionBytes || decodeCanonicalValue(data, &definition) != nil {
		return SavedQueryDefinition{}, invalid("saved query definition JSON")
	}
	b, _, err := CanonicalSavedQueryDefinition(definition)
	if err != nil || !bytes.Equal(b, data) {
		return SavedQueryDefinition{}, invalid("saved query definition")
	}
	return definition, nil
}

func savedQueryValue(op SavedQueryRequest) (any, error) {
	var value any
	switch op.Kind {
	case "create":
		value = &CreateSavedQuery{}
	case "replace_definition":
		value = &ReplaceSavedQueryDefinition{}
	case "rename":
		value = &RenameCollection{}
	case "archive":
		value = &SetCollectionArchived{}
	case "grants":
		value = &EditCollectionGrants{}
	default:
		return nil, invalid("saved query kind")
	}
	if len(op.Value) == 0 || len(op.Value) > MaxSavedQueryRequestBytes || decodeCanonicalValue(op.Value, value) != nil {
		return nil, invalid("saved query value JSON")
	}
	valid := false
	switch v := value.(type) {
	case *CreateSavedQuery:
		_, _, err := CanonicalSavedQueryDefinition(v.Definition)
		valid = validText(v.Name, 256, true) && v.CreatorGrantID.Validate() == nil && err == nil
	case *ReplaceSavedQueryDefinition:
		_, _, err := CanonicalSavedQueryDefinition(v.Definition)
		valid = err == nil
	case *RenameCollection:
		valid = validText(v.Name, 256, true)
	case *SetCollectionArchived:
		valid = true
	case *EditCollectionGrants:
		valid = validCollectionGrantChanges(v.Changes, "collection", op.CollectionID)
	}
	if !valid {
		return nil, invalid("saved query value")
	}
	return value, nil
}

func CanonicalSavedQueryRequest(op SavedQueryRequest) ([]byte, string, error) {
	if op.Version != 2 || op.LibraryID.Validate() != nil || op.ID.Validate() != nil || op.CollectionID.Validate() != nil || op.Kind == "create" && op.ExpectedVersion != "" || op.Kind != "create" && op.ExpectedVersion.Validate() != nil {
		return nil, "", invalid("saved query envelope")
	}
	if _, err := savedQueryValue(op); err != nil {
		return nil, "", err
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > MaxSavedQueryRequestBytes {
		return nil, "", invalid("saved query request bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.operation\x00\x02", b), nil
}

func DecodeSavedQueryRequest(data []byte) (SavedQueryRequest, error) {
	var op SavedQueryRequest
	if len(data) == 0 || len(data) > MaxSavedQueryRequestBytes || decodeCanonicalValue(data, &op) != nil {
		return SavedQueryRequest{}, invalid("saved query request JSON")
	}
	b, _, err := CanonicalSavedQueryRequest(op)
	if err != nil || !bytes.Equal(b, data) {
		return SavedQueryRequest{}, invalid("saved query request")
	}
	return op, nil
}

func CanonicalSavedQueryRecord(record SavedQueryRecord) ([]byte, string, error) {
	if record.Version != 2 || record.ActorSubjectID.Validate() != nil || record.ResultVersion.Validate() != nil || record.ResultVersion == record.Request.ExpectedVersion || record.RecordedUnixNano <= 0 {
		return nil, "", invalid("saved query record")
	}
	if _, _, err := CanonicalSavedQueryRequest(record.Request); err != nil {
		return nil, "", err
	}
	generated := record.Request.Kind == "create" || record.Request.Kind == "grants"
	if generated && record.GeneratedPolicyOperationID != generatedCollectionPolicyID(record.Request.ID) || !generated && record.GeneratedPolicyOperationID != "" {
		return nil, "", invalid("saved query record policy binding")
	}
	b, err := json.Marshal(record)
	if err != nil || len(b) > MaxSavedQueryRecordBytes {
		return nil, "", invalid("saved query record bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.record\x00\x02", b), nil
}

func DecodeSavedQueryRecord(data []byte) (SavedQueryRecord, error) {
	var record SavedQueryRecord
	if len(data) == 0 || len(data) > MaxSavedQueryRecordBytes || decodeCanonicalValue(data, &record) != nil {
		return SavedQueryRecord{}, invalid("saved query record JSON")
	}
	b, _, err := CanonicalSavedQueryRecord(record)
	if err != nil || !bytes.Equal(b, data) {
		return SavedQueryRecord{}, invalid("saved query record")
	}
	return record, nil
}

func CanonicalSavedQueryReceipt(receipt SavedQueryReceipt) ([]byte, error) {
	if receipt.Version != 2 || receipt.OperationID.Validate() != nil || receipt.CollectionID.Validate() != nil || receipt.ResultVersion.Validate() != nil {
		return nil, invalid("saved query receipt")
	}
	b, err := json.Marshal(receipt)
	if err != nil || len(b) > MaxSavedQueryReceiptBytes {
		return nil, invalid("saved query receipt bytes")
	}
	return b, nil
}

func DecodeSavedQueryReceipt(data []byte) (SavedQueryReceipt, error) {
	var receipt SavedQueryReceipt
	if len(data) == 0 || len(data) > MaxSavedQueryReceiptBytes || decodeCanonicalValue(data, &receipt) != nil {
		return SavedQueryReceipt{}, invalid("saved query receipt JSON")
	}
	b, err := CanonicalSavedQueryReceipt(receipt)
	if err != nil || !bytes.Equal(b, data) {
		return SavedQueryReceipt{}, invalid("saved query receipt")
	}
	return receipt, nil
}
