// ============================================================
// NYC Taxi Medallion Pipeline — Infrastructure as Code
// Author  : Sri (Sriyadharshini Ravi)
// Purpose : Spin up / tear down full pipeline infra in one command
// Usage   : az deployment group create \
//             --resource-group rg-nyc-taxi-pipeline \
//             --template-file main.bicep \
//             --parameters @parameters.json
// ============================================================

targetScope = 'resourceGroup'

// ── Parameters ───────────────────────────────────────────────
@description('Environment tag — keep as dev for free trial')
@allowed(['dev', 'staging', 'prod'])
param env string = 'dev'

@description('Azure region — must match your resource group region')
param location string = resourceGroup().location

@description('Short unique suffix to avoid global name collisions (3-5 chars, lowercase)')
@maxLength(5)
param suffix string = 'sri01'

@description('Your GitHub repo URL for ADF source control integration')
param githubRepoUrl string = 'https://github.com/<your-username>/nyc-taxi-medallion'

@description('Your GitHub account name')
param githubAccountName string = '<your-github-username>'

@description('GitHub repo name')
param githubRepoName string = 'nyc-taxi-medallion'

@description('GitHub collaboration branch')
param githubBranch string = 'main'

@description('Azure SQL admin username')
param sqlAdminUsername string = 'sqladmin'

@description('Azure SQL admin password — pass via parameters.json, never hardcode')
@secure()
param sqlAdminPassword string

// ── Variables ─────────────────────────────────────────────────
var projectName        = 'nyctaxi'
var storageAccountName = 'st${projectName}${suffix}'          // st + project + suffix
var adfName            = 'adf-${projectName}-${env}-${suffix}'
var sqlServerName      = 'sql-${projectName}-${env}-${suffix}'
var sqlDbName          = 'sqldb-watermark-${env}'
var databricksName     = 'dbw-${projectName}-${env}-${suffix}'

var commonTags = {
  project     : 'nyc-taxi-medallion'
  environment : env
  owner       : 'sri'
  managedBy   : 'bicep'
  costCenter  : 'free-trial'
}

// ── 1. Storage Account (ADLS Gen2) ───────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name     : storageAccountName
  location : location
  tags     : commonTags
  sku: {
    name: 'Standard_LRS'   // Cheapest — fine for dev/free trial
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled            : true    // Hierarchical namespace = ADLS Gen2
    accessTier              : 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion       : 'TLS1_2'
    allowBlobPublicAccess   : false   // Production security default
  }
}

// ── 1a. Bronze Container ──────────────────────────────────────
resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name      : '${storageAccount.name}/default/bronze'
  properties: {
    publicAccess: 'None'
  }
}

// ── 1b. Silver Container ──────────────────────────────────────
resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name      : '${storageAccount.name}/default/silver'
  properties: {
    publicAccess: 'None'
  }
}

// ── 1c. Gold Container ───────────────────────────────────────
resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name      : '${storageAccount.name}/default/gold'
  properties: {
    publicAccess: 'None'
  }
}

// ── 1d. Config Container (pipeline configs, watermark backups)
resource configContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name      : '${storageAccount.name}/default/config'
  properties: {
    publicAccess: 'None'
  }
}

// ── 2. Azure SQL Server (for watermark table) ─────────────────
resource sqlServer 'Microsoft.Sql/servers@2022-05-01-preview' = {
  name     : sqlServerName
  location : location
  tags     : commonTags
  properties: {
    administratorLogin        : sqlAdminUsername
    administratorLoginPassword: sqlAdminPassword
    version                   : '12.0'
    publicNetworkAccess       : 'Enabled'   // Needed for ADF to reach it
  }
}

// ── 2a. Allow Azure services to access SQL Server ─────────────
resource sqlFirewallAzureServices 'Microsoft.Sql/servers/firewallRules@2022-05-01-preview' = {
  parent: sqlServer
  name  : 'AllowAllAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress  : '0.0.0.0'
  }
}

// ── 2b. Azure SQL Database (free / serverless tier) ───────────
resource sqlDatabase 'Microsoft.Sql/servers/databases@2022-05-01-preview' = {
  parent  : sqlServer
  name    : sqlDbName
  location: location
  tags    : commonTags
  sku: {
    name    : 'GP_S_Gen5_1'   // Serverless Gen5 — auto-pause when idle = cost saving
    tier    : 'GeneralPurpose'
    family  : 'Gen5'
    capacity: 1
  }
  properties: {
    autoPauseDelay          : 60      // Auto-pause after 60 min idle — saves cost
    minCapacity             : '0.5'
    requestedBackupStorageRedundancy: 'Local'
  }
}

// ── 3. Azure Data Factory ─────────────────────────────────────
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name    : adfName
  location: location
  tags    : commonTags
  identity: {
    type: 'SystemAssigned'   // Managed identity — no credentials needed for ADLS access
  }
  properties: {
    repoConfiguration: {
      type               : 'FactoryGitHubConfiguration'
      accountName        : githubAccountName
      repositoryName     : githubRepoName
      collaborationBranch: githubBranch
      rootFolder         : '/adf'          // ADF JSON lives in /adf folder in your repo
      lastCommitId       : ''
    }
    globalParameters: {
      environment: {
        type : 'String'
        value: env
      }
      storageAccountName: {
        type : 'String'
        value: storageAccountName
      }
      bronzeContainer: {
        type : 'String'
        value: 'bronze'
      }
      silverContainer: {
        type : 'String'
        value: 'silver'
      }
      goldContainer: {
        type : 'String'
        value: 'gold'
      }
    }
  }
}

// ── 4. Azure Databricks Workspace ─────────────────────────────
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name    : databricksName
  location: location
  tags    : commonTags
  sku: {
    name: 'trial'   // Free 14-day trial tier — use this for free trial
  }
  properties: {
    managedResourceGroupId: '${subscription().id}/resourceGroups/rg-dbw-managed-${suffix}'
  }
}

// ── 5. Role Assignment: ADF Managed Identity → ADLS Gen2 ──────
// Grants ADF identity Storage Blob Data Contributor on ADLS
// So ADF can read/write without any storage keys
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource adfStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name : guid(storageAccount.id, dataFactory.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId     : dataFactory.identity.principalId
    principalType   : 'ServicePrincipal'
  }
}

// ── Outputs (use these when configuring linked services) ───────
output storageAccountName   string = storageAccount.name
output storageAccountId     string = storageAccount.id
output adfName              string = dataFactory.name
output adfPrincipalId       string = dataFactory.identity.principalId
output sqlServerFqdn        string = sqlServer.properties.fullyQualifiedDomainName
output sqlDatabaseName      string = sqlDatabase.name
output databricksWorkspaceUrl string = databricksWorkspace.properties.workspaceUrl
output databricksWorkspaceId  string = databricksWorkspace.id
