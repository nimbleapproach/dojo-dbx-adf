param pm_location string = 'westeurope'
param pm_environment string = 'dev'
param pm_technicalOwner string = 'jon godfrey'
param pm_businessOwner string = 'stephan hassenbach'

module initResourceGroups 'init-rgs.bicep' = {
  scope: subscription()
  name: '${deployment().name}-initResourceGroups'
  params: {
    pm_location : pm_location
    pm_environment: pm_environment
    pm_technicalOwner: pm_technicalOwner
    pm_businessOwner : pm_businessOwner
  }
}


module initCommon 'init-common.bicep' = {
  scope: resourceGroup('rg-ig-common-${pm_environment}-${pm_location}')
  name: '${deployment().name}-initCommon'
  params: {
    pm_location : pm_location
    pm_environment: pm_environment
    pm_technicalOwner: pm_technicalOwner
    pm_businessOwner : pm_businessOwner
    pm_synapseID: initLakehouse.outputs.synapseID
    pm_adfID: initLakehouse.outputs.adfID
  }
}

module initLakehouse 'init-lakehouse.bicep' = {
  scope: resourceGroup('rg-ig-lakehouse-${pm_environment}-${pm_location}')
  name: '${deployment().name}-initLakehouse'
  params: {
    pm_location : pm_location
    pm_environment: pm_environment
    pm_technicalOwner: pm_technicalOwner
    pm_businessOwner : pm_businessOwner
    pm_managedResourceGroupID: '/subscriptions/${subscription().subscriptionId}/resourceGroups/mrg-ig-databricks-${pm_environment}-${pm_location}'
  }
}

module initShared 'init-shared.bicep' =  if (pm_environment == 'prod') {
  scope: resourceGroup('rg-ig-shared-${pm_location}')
  name: '${deployment().name}-initShared'
  params: {
    pm_location : pm_location
    pm_environment: pm_environment
    pm_technicalOwner: pm_technicalOwner
    pm_businessOwner : pm_businessOwner
  }
}

