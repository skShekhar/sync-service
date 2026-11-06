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

type noOpAdapter struct{}

func (ad *noOpAdapter) Create(ctx context.Context, obj model.ExternalObjType, rec model.ExternalRecord) (id string, err error) {
	return "", nil
}

func (ad *noOpAdapter) Update(ctx context.Context, obj model.ExternalObjType, rec model.ExternalRecord, id string) error {
	return nil
}

func (ad *noOpAdapter) Delete(ctx context.Context, obj model.ExternalObjType, id string) error {
	return nil
}
