
@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Globally unique storage account name')
@minLength(3)
@maxLength(24)
param storageAccountName string

@description('Globally unique Data Factory name')
param dataFactoryName string

var containerNames = [
  'bronze'
  'silver'
  'gold'
  'config'
]

var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

// Azure Data Lake Storage Gen2
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

// Blob service
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storage
  name: 'default'
}

// Medallion architecture and configuration containers
resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for name in containerNames: {
  parent: blobService
  name: name
  properties: {
    publicAccess: 'None'
  }
}]

// Azure Data Factory
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
}

// Managed identity access to storage
resource factoryStorageAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, dataFactory.id, storageBlobDataContributorRoleId)
  scope: storage
  properties: {
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
  }
}

// Deployment outputs
output storageAccountId string = storage.id
output deployedDataFactoryName string = dataFactory.name
