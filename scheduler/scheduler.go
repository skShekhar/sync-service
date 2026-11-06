package scheduler

import (
	"context"
	"sync"
	"sync-service/model"
	"time"
)

type Queue interface {
	HasPending(connectionID model.ConnectionId) bool
	Dequeue(connectionID model.ConnectionId) (model.ExternalSync, bool)
	Requeue(es model.ExternalSync)
}

type Limiter interface {
	Allow(now time.Time) bool
}

type Deliver func(ctx context.Context, connection *model.Connection, es model.ExternalSync) error

type Connection struct {
	connection *model.Connection
	limiter    Limiter
}

func NewConnection(connection *model.Connection, limiter Limiter) *Connection {
	return &Connection{
		connection: connection,
		limiter:    limiter,
	}
}

type Scheduler struct {
	queue       Queue
	connections []*Connection
	deliver     Deliver
	now         func() time.Time
}

func New(queue Queue, connections []*Connection, deliver Deliver, now func() time.Time) *Scheduler {
	return &Scheduler{
		queue:   queue,
		deliver: deliver,
		now:     now,
		connections: connections,
	}
}

func (s *Scheduler) Run(ctx context.Context, tick time.Duration) {
	var wg sync.WaitGroup
	for _, connection := range s.connections {
		wg.Add(1)
		go func() {
				defer wg.Done()
				s.work(ctx, connection, tick)
		}()
	}
	wg.Wait()
}

func (s *Scheduler) work(ctx context.Context, connection *Connection, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.queue.HasPending(connection.connection.Id) && connection.limiter.Allow(s.now()) {
				work, ok := s.queue.Dequeue(connection.connection.Id)
				if ok {
					err := s.deliver(ctx, connection.connection, work)
					if err != nil {
						s.queue.Requeue(work)
					}
				}
			}
		}
	}
}
