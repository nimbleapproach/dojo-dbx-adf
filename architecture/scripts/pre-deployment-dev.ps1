az account set -s 'HLD 03 Invoice EDW DEV'
az configure --defaults location=westeurope

az deployment group create `
--resource-group rg-ig-common-dev-westeurope `
--mode Incremental `
--confirm-with-what-if `
-f bicep/main.bicep `
--parameters pm_environment=dev 
#-p bicep/main.bicepparam