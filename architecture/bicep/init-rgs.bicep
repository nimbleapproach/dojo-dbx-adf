
targetScope = 'subscription'

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
var v_namingSuffix = '${pm_environment}-${pm_location}'

resource commonRG 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: 'rg-ig-common-${v_namingSuffix}'
  location: pm_location
  tags: v_tags
}


resource lakehouseRG 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: 'rg-ig-lakehouse-${v_namingSuffix}'
  location: pm_location
  tags: v_tags
}

  resource sharedRG 'Microsoft.Resources/resourceGroups@2022-09-01' =  if (pm_environment == 'prod') {
  name: 'rg-ig-shared-${pm_location}'
  location: pm_location
  tags: v_tags
}


output commonRG string = commonRG.name
output lakehouseRG string = lakehouseRG.name
