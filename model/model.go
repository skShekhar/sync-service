package model

type InternalObjType string
type ExternalObjType string
type Provider string
type TenantId string
type RecordId string
type ConnectionId string
type FieldsMapping map[string]string
type InternalRecord map[string]any
type ExternalRecord map[string]any
type Action string

const (
	CreateAction Action = "create"
	UpdateAction Action = "update"
	DeleteAction Action = "delete"
)

type ChangeEvent struct {
	TenantId
	RecordId
	InternalObjType
	Before InternalRecord
	After  InternalRecord
}

func (ce ChangeEvent) Action() Action {
	if ce.After == nil {
		return DeleteAction
	}
	if ce.Before == nil {
		return CreateAction
	}
	return UpdateAction
}

type SyncCommand struct {
	ChangeEvent
	Provider
	ObjectMapping
}

func NewSyncCommand(changeEvent ChangeEvent, provider Provider, mapping ObjectMapping) SyncCommand {
	return SyncCommand{
		ChangeEvent:   changeEvent,
		Provider:      provider,
		ObjectMapping: mapping,
	}
}

type ExternalSync struct {
	Action
	ConnectionId
	RecordId
	InternalObjType
	ExternalObjType
	Payload ExternalRecord
}

func NewExternalSync(action Action, connectionId ConnectionId, recordId RecordId, internalObj InternalObjType, externalObj ExternalObjType, payload ExternalRecord) ExternalSync {
	return ExternalSync{
		Action: action,
		ConnectionId: connectionId,
		RecordId: recordId,
		InternalObjType: internalObj,
		ExternalObjType: externalObj,
		Payload: payload,
	}
}

type ObjectMapping struct {
	InternalObjType
	ExternalObjType
	FieldsMapping
}

func NewObjectMapping(internalObjType InternalObjType, externalObjType ExternalObjType, fieldsMapping FieldsMapping) ObjectMapping {
	return ObjectMapping{
		InternalObjType: internalObjType,
		ExternalObjType: externalObjType,
		FieldsMapping:   fieldsMapping,
	}
}

type Connection struct {
	Id ConnectionId
	Provider
	Mappings []ObjectMapping
}

func NewConnection(id ConnectionId, provider Provider, mappings []ObjectMapping) Connection {
	return Connection{
		Id:       id,
		Provider: provider,
		Mappings: mappings,
	}
}
