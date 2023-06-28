az account set -s 'HLD 03 Invoice EDW PROD'
az configure --defaults location=westeurope

az deployment group create `
--resource-group rg-ig-common-prod-westeurope `
--mode Incremental `
--confirm-with-what-if `
-f bicep/main.bicep `
--parameters pm_environment=prod 
#-p bicep/main.bicepparam
