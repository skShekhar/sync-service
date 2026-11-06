package idmap

import (
	"sync"
	"sync-service/model"
)

type IdMap struct {
	mu sync.Mutex
	ids map[mappingKey]string
}

func New() *IdMap{
	return &IdMap{ids: map[mappingKey]string{}}
}

func (idm *IdMap) Get(es model.ExternalSync) (string, bool) {
	idm.mu.Lock()
	defer idm.mu.Unlock()
	val, ok := idm.ids[key(es)]
	return val, ok
}

func (idm *IdMap) Add(es model.ExternalSync, externalId string) {
	idm.mu.Lock()
	defer idm.mu.Unlock()
	idm.ids[key(es)] = externalId
}

func (idm *IdMap) Remove(es model.ExternalSync) {
	idm.mu.Lock()
	defer idm.mu.Unlock()
	delete(idm.ids, key(es))
}

type mappingKey struct {
	connectionId model.ConnectionId
	internalObject model.InternalObjType
	recordId model.RecordId
}

func key(es model.ExternalSync) mappingKey {
	return mappingKey{connectionId: es.ConnectionId, internalObject: es.InternalObjType, recordId: es.RecordId}
}
