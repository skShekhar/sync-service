package ingest

import (
	"context"
	"log"
	"sync-service/model"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type Ingester struct {
	connections map[model.TenantId][]model.Connection
	schemas     map[model.InternalObjType]*jsonschema.Schema
	out         chan<- model.SyncCommand
}

func New(connections map[model.TenantId][]model.Connection, schemas map[model.InternalObjType]*jsonschema.Schema, out chan<- model.SyncCommand) *Ingester {
	return &Ingester{connections, schemas, out}
}

func (in *Ingester) Run(ctx context.Context, events <-chan model.ChangeEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			cmds, err := in.process(ev)
			if err != nil {
				log.Printf("Processing event: tenant_id=%s record_id=%s err=%v", ev.TenantId, ev.RecordId, err)
				continue
			}
			for _, cmd := range cmds {
				select {
				case <-ctx.Done():
					return
				case in.out <- cmd:
				}
			}
		}
	}
}

func (in *Ingester) process(ev model.ChangeEvent) ([]model.SyncCommand, error) {
	var result []model.SyncCommand
	for _, conn := range in.connections[ev.TenantId] {
		for _, mapping := range conn.Mappings {
			result = append(result, model.NewSyncCommand(ev, conn.Provider, mapping))
		}
	}
	if len(result) > 0 && ev.After != nil {
		if err := in.schemas[ev.InternalObjType].Validate(map[string]any(ev.After)); err != nil {
			return nil, err
		}
	}
	return result, nil
}
