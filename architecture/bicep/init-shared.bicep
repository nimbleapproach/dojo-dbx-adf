param pm_location string = 'westeurope'
param pm_environment string = 'dev'
param pm_technicalOwner string = 'jon godfrey'
param pm_businessOwner string = 'stephan hassenbach'

var v_tags = {
  technicalOwner: pm_technicalOwner
  businessOwner: pm_businessOwner
  environment: pm_environment
  project : 'EDW'
}
var v_namingSuffix = pm_location

resource sharedDataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: 'adf-ig-shared-${v_namingSuffix}'
  location: pm_location
  identity : {
    type: 'SystemAssigned'
  }
  tags: v_tags
}
