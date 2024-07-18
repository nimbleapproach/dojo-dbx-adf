# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

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

import requests
import base64
import json

class InfinigateAuth(requests.auth.AuthBase):
    def __init__(self):
        base_url = "https://login.microsoftonline.com/375313bf-8b9b-44af-97cd-fc2e258a968e"
        get_token_url =  f"{base_url}/oauth2/token"
        client_id = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key='secret-powerbi-app-id')
        secret = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key='secret-powerbi-app-secret')
        grantType = 'client_credentials'
        resource = "https://analysis.windows.net/powerbi/api"
        
        request_body = json.loads(json.dumps({"client_id" : client_id,
                                   "client_secret": secret ,
                                   "grant_type" : grantType,
                                   "resource" : "https://analysis.windows.net/powerbi/api"}, indent=2))
        
        
        response = requests.post(get_token_url
              , data = request_body,
                               headers={
                                   'Content-Type':'application/x-www-form-urlencoded'
                               }
                              )
        result = response.json()["access_token"]
        self.authorization = f"Bearer {result}"
        
    def __call__(self, r):
        # modify and return the request
        r.headers['Authorization'] = self.authorization
        r.headers['Content-Type'] = "application/json"
        return r

# COMMAND ----------

InfinigateHandle = InfinigateAuth()

# COMMAND ----------

class PowerBIHandle():
    def __init__(self, session : requests.Session):
        self.baseURL = "https://api.powerbi.com/v1.0/myorg/"
        self.session = session

    def getRefreshDataset(self, datasetId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/datasets/{datasetId}/refreshes' , auth=InfinigateAuth())
        return json.loads(metaData.text)
    
    def getLatestDatasetAPIRefreshStatus(self, datasetId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/datasets/{datasetId}/refreshes' , auth=InfinigateAuth())
        response = json.loads(metaData.text)
        refreshHistory = response['value']
        return sorted([refresh for refresh in refreshHistory if refresh['refreshType'] == 'ViaApi'], key=lambda x: x['startTime'],reverse=True)[0]["status"]
    
    def postRefreshDataset(self, datasetId : int , workspaceId : str):
        print(f'BEGIN: Refreshing dataset {datasetId} in workspace {workspaceId}.')
        self.session.post(f'{self.baseURL}groups/{workspaceId}/datasets/{datasetId}/refreshes' , auth=InfinigateAuth())

    def getDataflowTransactions(self, dataflowId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/dataflows/{dataflowId}/transactions' , auth=InfinigateAuth())
        return json.loads(metaData.text)

    def postRefreshDataflow(self, dataflowId : int , workspaceId : str):
        print(f'BEGIN: Refreshing dataflow {dataflowId} in workspace {workspaceId}.')
        request_body = json.dumps(
            {"refreshRequest": 'y'})
        
        requestHeader = {}
        requestHeader["Content-Type"] = "application/json"
                                  
        return self.session.post(f'{self.baseURL}groups/{workspaceId}/dataflows/{dataflowId}/refreshes', headers=requestHeader,data=request_body, auth=InfinigateAuth())
    
    def getLatestDataflowAPIRefreshStatus(self, dataflowId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/dataflows/{dataflowId}/transactions' , auth=InfinigateAuth())
        response = json.loads(metaData.text)
        refreshHistory = response['value']
        return sorted([refresh for refresh in refreshHistory if refresh['refreshType'] == 'ViaApi'], key=lambda x: x['startTime'],reverse=True)[0]["status"]

# COMMAND ----------

infinigateSession = requests.Session()

# COMMAND ----------

PowerBIHandler = PowerBIHandle(infinigateSession)

# COMMAND ----------

workspaceId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"{WORKSPACE_ID}-{ENVIRONMENT}")
datasetId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"{DATASET_ID}-{ENVIRONMENT}")
dataflowWorkspaceId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"secret-powerbi-dataflow-workspace-id-{ENVIRONMENT}")
dataflowId = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key=f"secret-powerbi-dataflow-id-{ENVIRONMENT}")

# COMMAND ----------

response = PowerBIHandler.postRefreshDataflow(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)

# COMMAND ----------

response = PowerBIHandler.getDataflowTransactions(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)

# COMMAND ----------

currentStatus = PowerBIHandler.getLatestDataflowAPIRefreshStatus(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)

# COMMAND ----------

import time

while currentStatus == 'InProgress':
    print('Checking current Dataflow status:')
    currentStatus = currentStatus = PowerBIHandler.getLatestDataflowAPIRefreshStatus(dataflowId=dataflowId,workspaceId=dataflowWorkspaceId)
    print(f'Status is sill {currentStatus}')
    time.sleep(60)

# COMMAND ----------

if not currentStatus == 'Success':
  raise Exception(
    f'ERROR: Refreshing dataset {dataflowId} in workspace {dataflowWorkspaceId} with status {currentStatus}.'
    )
print(f'FINISHED: Refreshing dataset {dataflowId} in workspace {dataflowWorkspaceId} with status {currentStatus}.')

# COMMAND ----------

response_post = PowerBIHandler.postRefreshDataset(datasetId=datasetId,workspaceId=workspaceId)

# COMMAND ----------

currentStatus = PowerBIHandler.getLatestDatasetAPIRefreshStatus(datasetId=datasetId,workspaceId=workspaceId)

# COMMAND ----------

while currentStatus == 'Unknown':
    print('Checking current Power BI Dataset status:')
    currentStatus = PowerBIHandler.getLatestDatasetAPIRefreshStatus(datasetId=datasetId,workspaceId=workspaceId)
    print(f'Status is sill {currentStatus}')
    time.sleep(60)

# COMMAND ----------

if not currentStatus == 'Completed':
  raise Exception(
    f'ERROR: Refreshing dataset {datasetId} in workspace {workspaceId} with status {currentStatus}.'
    )
print(f'FINISHED: Refreshing dataset {datasetId} in workspace {workspaceId} with status {currentStatus}.')
