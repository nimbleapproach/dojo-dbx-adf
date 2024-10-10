# Setting Up Secrets in Databricks with Azure Key Vault

This guide provides detailed steps to set up secrets in Databricks using Azure Key Vault. It includes creating a Key Vault, setting up a secret scope in Databricks, and linking the Key Vault to Databricks.

## Prerequisites
- Azure subscription
- Databricks workspace
- Azure Key Vault

## Steps 🚀

### 1. Create an Azure Key Vault
1. **Navigate to the Azure Portal**: Go to Azure Portal.
2. **Create a Key Vault**:
   - Search for "Key Vault" in the search bar and select "Key Vaults".
   - Click on "Create" and fill in the necessary details:
     - **Resource Group**: Select your resource group.
     - **Key Vault Name**: `kv-ig-westeurope`
     - **Region**: West Europe
   - Click "Review + create" and then "Create".

3. **Get the DNS Name and Resource ID**:
   - Go to your Key Vault and navigate to the "Properties" section.
   - Note down the **DNS Name** and **Resource ID**:
     - **DNS Name**: `https://kv-ig-dev-westeurope.vault.azure.net/`
     - **Resource ID**: `/subscriptions/4b6b9329-18aa-4bf0-83f2-090c052e7fd3/resourceGroups/rg-ig-shared-westeurope/providers/Microsoft.KeyVault/vaults/kv-ig-shared-westeurope`

### 2. Create a Secret Scope in Databricks
1. **Open Databricks Workspace**: Go to your Databricks workspace URL, e.g., `https://adb-2715169315282147.7.azuredatabricks.net/?o=7878361988304649`.
2. **Create a Secret Scope**:
   - Navigate to the URL: `https://adb-2715169315282147.7.azuredatabricks.net/?o=7878361988304649#secrets/createScope`.
   - Fill in the required details:
     - **Scope Name**: Choose a name for your secret scope.
     - **DNS Name**: `https://kv-ig-dev-westeurope.vault.azure.net/`
     - **Resource ID**: `/subscriptions/4b6b9329-18aa-4bf0-83f2-090c052e7fd3/resourceGroups/rg-ig-shared-westeurope/providers/Microsoft.KeyVault/vaults/kv-ig-shared-westeurope`
   - Click "Create".

### 3. Add Secrets to the Key Vault
1. **Navigate to the Key Vault**: Go to your Key Vault in the Azure Portal.
2. **Add a Secret**:
   - Go to the "Secrets" section and click on "Generate/Import".
   - Enter a name for the secret and the value you want to store.
   - Click "Create".

### 4. Access Secrets in Databricks
1. **Use Secrets in Notebooks**:
   - You can access the secrets in your Databricks notebooks using the `dbutils.secrets` utility.
   - Example:
     ```python
     secret_value = dbutils.secrets.get(scope="your-scope-name", key="your-secret-name")
     print(secret_value)
     ```

### Additional Resources
- Azure Key Vault Documentation
- Databricks Secrets Documentation

By following these steps, you can securely manage and access secrets in your Databricks environment using Azure Key Vault.
