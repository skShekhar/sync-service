package delivery

import (
	"context"
	"sync-service/model"
)

type Adapter interface {
	Create(ctx context.Context, obj model.ExternalObjType, rec model.ExternalRecord) (id string, err error)
	Update(ctx context.Context, obj model.ExternalObjType, rec model.ExternalRecord, id string) error
	Delete(ctx context.Context, obj model.ExternalObjType, id string) error
}

type IdStore interface {
	Get(es model.ExternalSync) (string, bool)
	Add(es model.ExternalSync, externalId string)
	Remove(es model.ExternalSync)
}

type Deliverer struct {
	adapters map[model.Provider]Adapter
	idStore  IdStore
}

func New(adapters map[model.Provider]Adapter, idStore IdStore) *Deliverer {
	return &Deliverer{
		adapters: adapters,
		idStore:  idStore,
	}
}

func (d *Deliverer) Deliver(ctx context.Context, connection *model.Connection, es model.ExternalSync) error {
	adapter := d.adapters[connection.Provider]
	externalId, _ := d.idStore.Get(es)
	if es.Action == model.DeleteAction {
		err := adapter.Delete(ctx, es.ExternalObjType, externalId)
		if err != nil {
			return err
		}
		d.idStore.Remove(es)
		return nil
	}
	if es.Action == model.UpdateAction {
		return adapter.Update(ctx, es.ExternalObjType, es.Payload, externalId)
	}
	newId, err := adapter.Create(ctx, es.ExternalObjType, es.Payload)
	if err != nil {
		return err
	}
	d.idStore.Add(es, newId)
	return nil
}
