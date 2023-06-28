param pm_location string = 'westeurope'
param pm_environment string = 'dev'
param pm_technicalOwner string = 'jon godfrey'
param pm_businessOwner string = 'stephan hassenbach'
param pm_synapseID string 
param pm_adfID string 


var v_tags = {
  technicalOwner: pm_technicalOwner
  businessOwner: pm_businessOwner
  environment: pm_environment
  project : 'EDW'
}
var v_namingSuffix = '${pm_environment}-${pm_location}'
var v_namingSuffixURL = '${pm_environment}0${pm_location}'

var v_storageAccountSKU = pm_environment == 'prod' ? 'Premium_ZRS' : 'Premium_ZRS'

resource unityCatalog 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: 'unity0ig0${v_namingSuffixURL}'
  location: pm_location
  tags: v_tags
  properties: {
    isHnsEnabled : true
  }
  kind: 'BlockBlobStorage'
  sku: {
    name: v_storageAccountSKU
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2022-09-01' = {
  name: 'default'
  parent: unityCatalog
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

resource dataverseExport 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: 'dv0ig0${v_namingSuffixURL}'
  location: pm_location
  tags: v_tags
  properties: {
    accessTier : 'Hot'
    isHnsEnabled : true
  }
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
}

resource blobServiceDataverse 'Microsoft.Storage/storageAccounts/blobServices@2022-09-01' = {
  name: 'default'
  parent: dataverseExport
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

resource sqlServer 'Microsoft.Sql/servers@2022-11-01-preview' = {
  name: 'sql-ig-metadata-${v_namingSuffix}'
  location: pm_location
  tags: v_tags
  properties: {
    administrators: {
      administratorType: 'ActiveDirectory'
      azureADOnlyAuthentication: true
      principalType: 'User'
      login: 'adminksa-com@infinigate.com'
      tenantId: '375313bf-8b9b-44af-97cd-fc2e258a968e'
      sid: '8a6b1e89-4069-4f1e-af5c-d5ba1b95c3f4'
    }
  }
}

resource sqlServerDatabase 'Microsoft.Sql/servers/databases@2022-11-01-preview' = {
  name: 'sqldb-ig-metadata-${v_namingSuffix}'
  location: pm_location
  parent: sqlServer
  tags: v_tags
  sku: {
    name: 'Basic'
    tier: 'Basic'
    capacity: 5
  }
}



resource keyVault 'Microsoft.KeyVault/vaults@2019-09-01' = {
  name: 'kv-ig-${v_namingSuffix}'
  location: pm_location
  tags: v_tags
  properties: {
    enabledForDeployment: true
    enabledForTemplateDeployment: true
    enabledForDiskEncryption: true
    tenantId: '375313bf-8b9b-44af-97cd-fc2e258a968e'
    enableRbacAuthorization : true
    sku: {
      name: 'standard'
      family: 'A'
    }
  }
}


@description('This is the built-in Contributor role for Key Vault.')
resource KVcontributorRoleDefinition 'Microsoft.Authorization/roleDefinitions@2018-01-01-preview' existing = {
  scope: keyVault
  name: 'f25e0fa2-a7c8-4377-a976-54943a77a395'
}

@description('Allows for read, write and delete access to Azure Storage blob containers and data')
resource ADLScontributorRoleDefinition 'Microsoft.Authorization/roleDefinitions@2018-01-01-preview' existing = {
  scope: unityCatalog
  name: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
}


resource roleSynapseKv 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(resourceGroup().id, 'syn-ig-ingestion-${v_namingSuffix}', KVcontributorRoleDefinition.id, 'kv-ig-${v_namingSuffix}')
  scope: keyVault
  properties: {
    roleDefinitionId: KVcontributorRoleDefinition.id
    principalId: pm_synapseID
    principalType: 'ServicePrincipal'
  }
}

resource roleADFKv 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(resourceGroup().id, 'adf-ig-${v_namingSuffix}', KVcontributorRoleDefinition.id, 'kv-ig-${v_namingSuffix}')
  scope: keyVault
  properties: {
    roleDefinitionId: KVcontributorRoleDefinition.id
    principalId: pm_adfID
    principalType: 'ServicePrincipal'
  }
}

resource acDatabricks 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: 'dac-lakehouse-${v_namingSuffix}'
  location: pm_location
  tags: v_tags
  identity: {
    type: 'SystemAssigned'
  }
}

resource roleDacADLS 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: guid(resourceGroup().id, 'dac-lakehouse-${v_namingSuffix}', ADLScontributorRoleDefinition.id,'unity0ig0${v_namingSuffixURL}')
  scope: unityCatalog
  properties: {
    roleDefinitionId: ADLScontributorRoleDefinition.id
    principalId: acDatabricks.identity.principalId
    principalType: 'ServicePrincipal'
  }
}


output keyVaultName string = keyVault.name
output sqlServerDatabaseName string = sqlServerDatabase.name
output sqlServerName string = sqlServer.name
