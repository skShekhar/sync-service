package e2e

import (
	"context"
	"fmt"
	"reflect"
	"sort"
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
	delivered := make(chan deliveryCall, 8)
	scheduler := buildScheduler(connections, queue, delivered)

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

	john := model.InternalRecord{"internal_email": "john@gmail.com", "internal_name": "John"}
	doe := model.InternalRecord{"internal_email": "doe@gmail.com", "internal_name": "Doe"}
	sam := model.InternalRecord{"internal_email": "sam@gmail.com", "internal_name": "Sam"}

	changeEvents := []model.ChangeEvent{
		{ RecordId: "record_id_1", After: john},
		{ RecordId: "record_id_1", Before: john, After: doe},
		{ RecordId: "record_id_2", Before: john},
		{ RecordId: "record_id_2", After: sam},
	}
	for _, e := range changeEvents {
		e.TenantId = TenantId
		e.InternalObjType = InternalContactObject
		events <-e
	}
	assertDeliveries(t, delivered)
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

func buildScheduler(connections []model.Connection, queue scheduler.Queue, delivered chan <-deliveryCall) *scheduler.Scheduler {
	limiter := scheduler.NewTokenBucket(100, 10, time.Now())
	configs := make([]*scheduler.Connection, len(connections))
	for i, c := range connections {
		configs[i] = scheduler.NewConnection(&c, limiter)
	}
	adpaters := map[model.Provider]delivery.Adapter{
		hubSpotProvider:    &noOpAdapter{delivered: delivered},
		salesforceProvider: &noOpAdapter{delivered: delivered},
	}
	idMap := idmap.New()
	deliverer := delivery.New(adpaters, idMap)
	return scheduler.New(queue, configs, deliverer.Deliver, time.Now)
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

func assertDeliveries(t *testing.T, delivered chan deliveryCall) {
	expected := []deliveryCall{
    {
        Action: model.CreateAction,
        Object: hubspotContactObject,
        Record: model.ExternalRecord{
            "hs_email": "john@gmail.com",
						"hs_name": "John",
        },
    },
    {
        Action: model.UpdateAction,
        Object: hubspotContactObject,
        Record: model.ExternalRecord{
            "hs_email": "doe@gmail.com",
						"hs_name": "Doe",
        },
    },
    {
        Action: model.DeleteAction,
        Object: hubspotContactObject,
    },
    {
        Action: model.CreateAction,
        Object: hubspotContactObject,
        Record: model.ExternalRecord{
            "hs_email": "sam@gmail.com",
						"hs_name": "Sam",
        },
    },
		{
        Action: model.CreateAction,
        Object: salesforceContactObject,
        Record: model.ExternalRecord{
            "sf_email": "john@gmail.com",
						"sf_name": "John",
        },
    },
		{
        Action: model.UpdateAction,
        Object: salesforceContactObject,
        Record: model.ExternalRecord{
            "sf_email": "doe@gmail.com",
						"sf_name": "Doe",
        },
    },
		{
        Action: model.DeleteAction,
        Object: salesforceContactObject,
    },
		{
        Action: model.CreateAction,
        Object: salesforceContactObject,
        Record: model.ExternalRecord{
            "sf_email": "sam@gmail.com",
						"sf_name": "Sam",
        },
    },
	}
	got := make([]deliveryCall, 0, len(expected))
	for i := 0; i < len(expected); i++ {
		select {
		case call := <-delivered:
			got = append(got, call)
		case <-time.After(5 * time.Second):
			t.Fatalf("expected %d deliveries, got %d, %v", len(expected), len(got), got)
		}
	}
	sort.Slice(got, func(i, j int) bool {
			return fmt.Sprintf("%+v", got[i]) < fmt.Sprintf("%+v", got[j])
	})
	sort.Slice(expected, func(i, j int) bool {
			return fmt.Sprintf("%+v", expected[i]) < fmt.Sprintf("%+v", expected[j])
	})
	if !reflect.DeepEqual(expected, got) {
    t.Errorf("deliveries mismatch: want: %+v\ngot:  %+v", expected, got)
	}
}
