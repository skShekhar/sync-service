package e2e

import "sync-service/model"

const salesforceProvider = model.Provider("Salesforce")

const salesforceContactObject = "sf_contact"

var salesForceMapping = model.FieldsMapping{
	"sf_email": "internal_email",
	"sf_name":  "internal_name",
}

var salesforceContactMappings = model.NewObjectMapping("internal_contact", salesforceContactObject, salesForceMapping)

var salesforceMappings = []model.ObjectMapping{salesforceContactMappings}
