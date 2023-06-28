az account set -s 'HLD 03 Invoice EDW UAT'
az configure --defaults location=westeurope

az deployment group create `
--resource-group rg-ig-common-uat-westeurope `
--mode Incremental `
--confirm-with-what-if `
-f bicep/main.bicep `
--parameters pm_environment=uat 
#-p bicep/main.bicepparam