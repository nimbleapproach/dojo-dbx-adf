az account set -s 'KSA DEMO MPN'
az configure --defaults location=westeurope

az group create --name rg-common-demo-euw --tags OWNER=KSA ENVIRONMENT=DEMO
az group create --name rg-lakehouse-demo-euw --tags OWNER=KSA ENVIRONMENT=DEMO
az group create --name rg-adf-demo-euw --tags OWNER=KSA ENVIRONMENT=DEMO