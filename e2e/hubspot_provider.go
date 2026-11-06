package e2e

import (
	"context"
	"sync-service/model"
)

const hubSpotProvider = model.Provider("HubSpot")

const hubspotContactObject = "hs_contact"

var hubspotContactMapping = model.FieldsMapping{
	"hs_email": "internal_email",
	"hs_name":  "internal_name",
}

var hubspotContactMappings = model.NewObjectMapping("internal_contact", hubspotContactObject, hubspotContactMapping)

var hubspotMappings = []model.ObjectMapping{hubspotContactMappings}

type deliveryCall struct {
  Action model.Action
  Object model.ExternalObjType
  Record model.ExternalRecord
}

type noOpAdapter struct{
	delivered chan <-deliveryCall
}

func (ad *noOpAdapter) Create(ctx context.Context, obj model.ExternalObjType, rec model.ExternalRecord) (id string, err error) {
	ad.delivered <- deliveryCall{
		Action: model.CreateAction,
		Object: obj,
		Record: rec,
	}
	return "", nil
}

func (ad *noOpAdapter) Update(ctx context.Context, obj model.ExternalObjType, rec model.ExternalRecord, id string) error {
	ad.delivered <- deliveryCall{
		Action: model.UpdateAction,
		Object: obj,
		Record: rec,
	}
	return nil
}

func (ad *noOpAdapter) Delete(ctx context.Context, obj model.ExternalObjType, id string) error {
	ad.delivered <- deliveryCall{
		Action: model.DeleteAction,
		Object: obj,
	}
	return nil
}
