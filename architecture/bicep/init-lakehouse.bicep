param pm_location string = 'westeurope'
param pm_environment string = 'dev'
param pm_technicalOwner string = 'jon godfrey'
param pm_businessOwner string = 'stephan hassenbach'
param pm_managedResourceGroupID string


var v_storageAccountSKU = pm_environment == 'prod' ? 'Standard_ZRS' : 'Standard_LRS'
var v_tags = {
  technicalOwner: pm_technicalOwner
  businessOwner: pm_businessOwner
  environment: pm_environment
  project : 'EDW'
}
var v_namingSuffix = '${pm_environment}-${pm_location}'
var v_namingSuffixURL = '${pm_environment}0${pm_location}'

resource storage 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: 'adls0ig0${v_namingSuffixURL}'
  location: pm_location
  tags: v_tags
  properties: {
    accessTier : 'Hot'
    isHnsEnabled : true
  }
  kind: 'StorageV2'
  sku: {
    name: v_storageAccountSKU
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2022-09-01' = {
  name: 'default'
  parent: storage
  properties: {
    deleteRetentionPolicy: {
      allowPermanentDelete: true
      days: 7
      enabled: true
    }
    containerDeleteRetentionPolicy: {
      allowPermanentDelete: true
      days: 7
      enabled: true
    }
  }
}

resource synapseContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  name: 'synapse'
  parent: blobService
}

resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
    name: 'abd-ig-lakehouse-${v_namingSuffix}'
    location: pm_location
    tags: v_tags
    sku : {
      name: 'Premium'
      tier: 'Premium'
    }
    properties: {
      managedResourceGroupId: pm_managedResourceGroupID
    }
  }

  @description('Allows for read, write and delete access to Azure Storage blob containers and data')
resource ADLScontributorRoleDefinition 'Microsoft.Authorization/roleDefinitions@2018-01-01-preview' existing = {
  scope: storage
  name: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
}


resource synapse 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: 'syn-ig-ingestion-${v_namingSuffix}'
  location: pm_location
  tags: v_tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    azureADOnlyAuthentication: true
    defaultDataLakeStorage: {
      accountUrl: storage.properties.primaryEndpoints.dfs
      filesystem: 'synapse'
      resourceId: storage.id
    }
    managedResourceGroupName: 'mrg-ig-synapse-${pm_environment}-${pm_location}'
  }
}

resource integrationRuntime 'Microsoft.Synapse/workspaces/integrationRuntimes@2021-06-01' = {
  name: 'ir-ig-managed-${v_namingSuffix}'
  parent: synapse
  properties: {
    type: 'Managed'
    typeProperties: {
      computeProperties: {
        dataFlowProperties: {
          cleanup: true
          computeType: 'General'
          coreCount: 8
          timeToLive: 10
        }
        location: pm_location
      }
    }
  }
}

resource roleSynapseStorage 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(resourceGroup().id, 'syn-ig-ingestion-${v_namingSuffix}', ADLScontributorRoleDefinition.id)
  scope: storage
  properties: {
    roleDefinitionId: ADLScontributorRoleDefinition.id
    principalId: synapse.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: 'adf-ig-${v_namingSuffix}'
  location: pm_location
  identity : {
    type: 'SystemAssigned'
  }
  tags: v_tags
}

resource roleDataFactoryStorage 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(resourceGroup().id, 'adf-ig-${v_namingSuffix}', ADLScontributorRoleDefinition.id)
  scope: storage
  properties: {
    roleDefinitionId: ADLScontributorRoleDefinition.id
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource integrationRuntimeADF 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  name: 'ir-ig-managed-${v_namingSuffix}'
  parent: dataFactory
  properties: {
    type: 'Managed'
    typeProperties: {
      computeProperties: {
        dataFlowProperties: {
          cleanup: true
          computeType: 'General'
          coreCount: 8
          timeToLive: 10
        }
        location: pm_location
      }
    }
  }
}

output storageName string = storage.name
output synapseID string = synapse.identity.principalId
output adfID string = dataFactory.identity.principalId
