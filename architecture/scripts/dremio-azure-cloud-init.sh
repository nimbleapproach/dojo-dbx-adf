# Create local variables for your subscription, region, resource group name, and storage account name. Note: The East US and West Europe regions are supported.
  SUBSCRIPTION_ID=4b6b9329-18aa-4bf0-83f2-090c052e7fd3
  REGION=westeurope
  RESOURCE_GROUP=mrg-ig-dremio-shared-westeurope
  STORAGE_ACCOUNT=adls0ig0dremio0shared

# Log into your Azure account and set the subscription
az login
az account set --subscription $SUBSCRIPTION_ID

# Enable Disk Encryption
az feature register --name EncryptionAtHost  --namespace Microsoft.Compute

# Create your Dremio resource group
az group create -l $REGION -n $RESOURCE_GROUP

# Create a virtual network, subnet and network security group
az network vnet create -g $RESOURCE_GROUP -n dremio-cloud-vnet-$REGION --address-prefix 10.0.0.0/16 --subnet-name dremio-cloud-sn-$REGION --subnet-prefix 10.0.0.0/16
az network nsg create -g $RESOURCE_GROUP -n dremio-cloud-nsg-$REGION

# Create a storage account and container with hierarchical namespace enabled and disable soft deletes. Then retrieve the account access key.
az storage account create -n $STORAGE_ACCOUNT -g $RESOURCE_GROUP -l $REGION --sku Standard_GRS --enable-hierarchical-namespace true --min-tls-version TLS1_2 --allow-blob-public-access false --https-only true
az storage blob service-properties delete-policy update --account-name $STORAGE_ACCOUNT --auth-mode login --enable false
az storage container create --name dremio-cloud --account-name $STORAGE_ACCOUNT --auth-mode login

# Create an application, grant the appropriate access then create and retrieve a key secret
az ad app create --display-name dremio-cloud-$REGION
APP_ID=$(az ad app list --display-name dremio-cloud-$REGION --query "[].[appId]" --output tsv)
az ad sp create --id $APP_ID
az role assignment create --role "Virtual Machine Contributor" --assignee $APP_ID -g $RESOURCE_GROUP
az role assignment create --role "Avere Contributor" --assignee $APP_ID -g $RESOURCE_GROUP
APP_PWD_O=$(az ad app credential reset --id $APP_ID --years 1 --display-name dremio-cloud-secret --append --output tsv)
APP_PWD=$(echo $APP_PWD_O | awk '{print $2}')

# Retrieve your tenant ID
TENANT_ID=$(echo $APP_PWD_O | awk '{print $3}')