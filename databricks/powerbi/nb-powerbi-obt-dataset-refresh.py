# Databricks notebook source
import os
import time
ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

# MAGIC %run  ./nb-powerbi-api-utils

# COMMAND ----------

try:
    DATASET_ID = dbutils.widgets.get("wg_datasetID")
except:
    dbutils.widgets.text(name = "wg_datasetID", defaultValue = 'secret-powerbi-dataset-id')
    DATASET_ID = dbutils.widgets.get("wg_datasetID")

# COMMAND ----------

try:
    WORKSPACE_ID = dbutils.widgets.get("wg_workspaceID")
except:
    dbutils.widgets.text(name = "wg_workspaceID", defaultValue = 'secret-powerbi-workspace-id')
    WORKSPACE_ID = dbutils.widgets.get("wg_workspaceID")

# COMMAND ----------

workspaceId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"{WORKSPACE_ID}-{ENVIRONMENT}")
datasetId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"{DATASET_ID}-{ENVIRONMENT}")
dataflowWorkspaceId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"secret-powerbi-dataflow-workspace-id-{ENVIRONMENT}")
dataflowId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"secret-powerbi-dataflow-id-{ENVIRONMENT}")
client_id = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-app-id")
client_secret = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-app-secret")
infinigateSession = requests.Session()

# COMMAND ----------

PowerBIHandler = PowerBIHandler(infinigateSession, client_id, client_secret)

# COMMAND ----------


## Refresh OBT dataflow

PowerBIHandler.postRefreshDataflow(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)
currentStatus = PowerBIHandler.getLatestDataflowAPIRefreshStatus(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)

while currentStatus == 'InProgress':
    print('Checking current Dataflow status:')
    currentStatus = PowerBIHandler.getLatestDataflowAPIRefreshStatus(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)
    print(f'Status is sill {currentStatus}')
    time.sleep(60)

if not currentStatus == 'Success':
  raise Exception(
    f'ERROR: Refreshing dataset {dataflowId} in workspace {dataflowWorkspaceId} with status {currentStatus}.'
    )
print(f'FINISHED: Refreshing dataset {dataflowId} in workspace {dataflowWorkspaceId} with status {currentStatus}.')

# COMMAND ----------

## Refresh OBT Dataset

PowerBIHandler.postRefreshDataset(datasetId=datasetId,workspaceId=workspaceId)
currentStatus = PowerBIHandler.getLatestDatasetAPIRefreshStatus(datasetId=datasetId,workspaceId=workspaceId)

while currentStatus == 'Unknown':
    print('Checking current Power BI Dataset status:')
    currentStatus = PowerBIHandler.getLatestDatasetAPIRefreshStatus(datasetId=datasetId,workspaceId=workspaceId)
    print(f'Status is sill {currentStatus}')
    time.sleep(60)

if not currentStatus == 'Completed':
  raise Exception(
    f'ERROR: Refreshing dataset {datasetId} in workspace {workspaceId} with status {currentStatus}.'
    )
print(f'FINISHED: Refreshing dataset {datasetId} in workspace {workspaceId} with status {currentStatus}.')
