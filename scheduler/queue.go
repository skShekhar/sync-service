package scheduler

import (
	"sync"
	"sync-service/model"
)

type InMemoryQueue struct {
	mu    sync.Mutex
	items map[model.ConnectionId][]model.ExternalSync
}

func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		items: make(map[model.ConnectionId][]model.ExternalSync),
	}
}

func (q *InMemoryQueue) Put(es model.ExternalSync) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items[es.ConnectionId] = append(q.items[es.ConnectionId], es)
}

func (q *InMemoryQueue) HasPending(connectionId model.ConnectionId) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items[connectionId]) > 0
}

func (q *InMemoryQueue) Dequeue(connectionID model.ConnectionId) (model.ExternalSync, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.items[connectionID]
	if len(items) == 0 {
		return model.ExternalSync{}, false
	}
	es := items[0]
	items[0] = model.ExternalSync{}
	items = items[1:]
	if len(items) == 0 {
		delete(q.items, connectionID)
	} else {
		q.items[connectionID] = items
	}
	return es, true
}

func (q *InMemoryQueue) Requeue(es model.ExternalSync) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items[es.ConnectionId] = append([]model.ExternalSync{es}, q.items[es.ConnectionId]...)
}
