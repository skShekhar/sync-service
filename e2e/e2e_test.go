package e2e

import (
	"context"
	"sync"
	"sync-service/delivery"
	"sync-service/idmap"
	"sync-service/ingest"
	"sync-service/model"
	"sync-service/scheduler"
	"sync-service/transform"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const TenantId = "tenant_id_1"
const InternalContactObject = "internal_conact"

func TestPipelineE2E(t *testing.T) {
	events := make(chan model.ChangeEvent, 10)
	cmds := make(chan model.SyncCommand, 10)
	ingester, connections := buildIngester(t, cmds)
	queue := scheduler.NewInMemoryQueue()
	transformer := buildTransformer(t, queue)
	scheduler := buildScheduler(connections, queue)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, run := range []func(){
		func() { ingester.Run(ctx, events) },
		func() { transformer.Run(ctx, cmds) },
		func() { scheduler.Run(ctx, time.Millisecond) },
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run()
		}()
	}

	john := model.InternalRecord{"email": "john@gmail.com", "name": "John"}
	doe := model.InternalRecord{"email": "doe@gmail.com", "name": "Doe"}
	sam := model.InternalRecord{"email": "sam@gmail.com", "name": "Sam"}

	for _, e := range []model.ChangeEvent{
		{ RecordId: "record_id_1", After: john},
		{ RecordId: "record_id_1", Before: john, After: doe},
		{ RecordId: "record_id_2", Before: john},
		{ RecordId: "record_id_2", Before: sam},
	} {
		e.TenantId = TenantId
		e.InternalObjType = InternalContactObject
		events <-e
	}

	cancel()
	stopped := make(chan struct{})
	go func() {
		wg.Wait()
		close(stopped)
	}()
	select{
	case <-stopped:
	case <-time.After(5*time.Second):
		t.Fatal("test didn't complete in 5 second")	
	}
}

func buildTransformer(t *testing.T, sink transform.Sink) *transform.Transformer {
	return transform.New(map[model.Provider]map[model.ExternalObjType]*jsonschema.Schema{
		hubSpotProvider: {
			hubspotContactObject: getInternalSchema(t, "hs_email"),
		},
		salesforceProvider: {
			salesforceContactObject: getInternalSchema(t, "sf_email"),
		},
	}, sink)
}

func buildIngester(t *testing.T, cmds chan<- model.SyncCommand) (*ingest.Ingester, []model.Connection) {
	hsConnection := model.NewConnection(model.ConnectionId("connection_id_1"), hubSpotProvider, hubspotMappings)
	sfConnection := model.NewConnection(model.ConnectionId("connection_id_2"), salesforceProvider, salesforceMappings)
	tenantConnections := map[model.TenantId][]model.Connection{
		TenantId: {hsConnection, sfConnection},
	}
	objectSchemas := map[model.InternalObjType]*jsonschema.Schema{
		InternalContactObject: getInternalSchema(t, "internal_email"),
	}
	return ingest.New(
		tenantConnections,
		objectSchemas,
		cmds,
	), []model.Connection{hsConnection, sfConnection}
}

func buildScheduler(connections []model.Connection, queue scheduler.Queue) *scheduler.Scheduler {
	limiter := scheduler.NewTokenBucket(100, 10, time.Now())
	configs := make([]scheduler.Connection, len(connections))
	for i, c := range connections {
		configs[i] = scheduler.NewConnection(c, limiter)
	}
	adpaters := map[model.Provider]delivery.Adapter{
		hubSpotProvider:    &noOpAdapter{},
		salesforceProvider: &noOpAdapter{},
	}
	idMap := idmap.New()
	deliverer := delivery.New(adpaters, idMap)
	return scheduler.New(queue, configs, deliverer.Deliver, time.Now, 5)
}

func getInternalSchema(t *testing.T, fields ...any) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	err := c.AddResource("schema.json", map[string]any{
		"type":     "object",
		"required": fields,
	})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile("schema.json")
	if err != nil {
		t.Fatal(err)
	}
	return schema
}
