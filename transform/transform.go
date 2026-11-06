package transform

import (
	"context"
	"log"
	"reflect"
	"sync-service/model"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type Sink interface {
	Put(ec model.ExternalSync)
}

type Transformer struct {
	providerSchemas map[model.Provider]map[model.ExternalObjType]*jsonschema.Schema
	sink             Sink
}

func New(schemas map[model.Provider]map[model.ExternalObjType]*jsonschema.Schema, sink Sink) *Transformer {
	return &Transformer{
		sink:             sink,
		providerSchemas: schemas,
	}
}

func (tf *Transformer) Run(ctx context.Context, cmds <-chan model.SyncCommand) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd, ok := <-cmds:
			if !ok {
				return
			}
			es, ok, err := tf.transform(cmd)
			if err != nil {
				log.Printf("Processing event: tenant_id=%s record_id=%s err=%v", cmd.TenantId, cmd.RecordId, err)
				continue
			}
			if ok {
				tf.sink.Put(es)
			}
		}
	}
}

func (tf *Transformer) transform(cmd model.SyncCommand) (model.ExternalSync, bool, error) {
	if cmd.Action() == model.DeleteAction {
		return model.ExternalSync{}, true, nil
	}
	afterRecord := toExternalRecord(cmd.FieldsMapping, cmd.After)
	var payload model.ExternalRecord
	if cmd.Action() == model.UpdateAction {
		payload := diff(toExternalRecord(cmd.FieldsMapping, cmd.Before), afterRecord)
		if len(payload) == 0 {
			return model.ExternalSync{}, false, nil
		}
	}
	err := tf.providerSchemas[cmd.Provider][cmd.ExternalObjType].Validate(map[string]any(cmd.After))
	if err != nil {
		return model.ExternalSync{}, false, err
	}
	return model.ExternalSync{
		Action:  cmd.Action(),
		Payload: payload,
	}, true, nil
}

func toExternalRecord(fieldsMapping model.FieldsMapping, record model.InternalRecord) model.ExternalRecord {
	result := make(model.ExternalRecord, len(fieldsMapping))
	for extField, inField := range fieldsMapping {
		if val, ok := record[inField]; ok {
			result[extField] = val
		}
	}
	return result
}

func diff(before, after model.ExternalRecord) model.ExternalRecord {
	result := model.ExternalRecord{}
	for k, v := range after {
		if bv, ok := before[k]; !ok || !reflect.DeepEqual(bv, v) {
			result[k] = v
		}
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			result[k] = nil
		}
	}
	return result
}
