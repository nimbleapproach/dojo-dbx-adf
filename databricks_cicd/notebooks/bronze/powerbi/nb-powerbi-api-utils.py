# Databricks notebook source
import requests
import base64
import json

# COMMAND ----------

class InfinigateAuth(requests.auth.AuthBase):
    def __init__(self, client_id : str, client_secret : str):
        base_url = "https://login.microsoftonline.com/375313bf-8b9b-44af-97cd-fc2e258a968e"
        get_token_url =  f"{base_url}/oauth2/token"
        grantType = 'client_credentials'
        resource = "https://analysis.windows.net/powerbi/api"
        
        request_body = json.loads(json.dumps({"client_id" : client_id,
                                   "client_secret": client_secret ,
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

class PowerBIHandler():
    def __init__(self, session : requests.Session, client_id : str, client_secret : str):
        self.baseURL = "https://api.powerbi.com/v1.0/myorg/"
        self.session = session
        self.client_id = client_id
        self.client_secret = client_secret

    def getDailyReportViews(self, intial_run : bool, date: str, continuation_uri: str):
        if intial_run == True:
            metaData = self.session.get(f"{self.baseURL}admin/activityevents?startDateTime='{date}T00:00:00.000Z'&endDateTime='{date}T23:59:59.000Z'&$filter=Activity eq 'viewreport'" , auth=InfinigateAuth(self.client_id, self.client_secret))
        else:
            metaData = self.session.get(continuation_uri , auth=InfinigateAuth(self.client_id, self.client_secret))
        return metaData.json()
    
    def getWorkspaces(self):
        group_call = self.session.get(f"{self.baseURL}groups", auth=InfinigateAuth(self.client_id, self.client_secret))
        return group_call.json()
    
    def getDatasets(self, workspace_id : str):
        dataset_call = self.session.get(f"{self.baseURL}groups/{workspace_id}/datasets", auth=InfinigateAuth(self.client_id, self.client_secret))
        return dataset_call.json()

    def getRefreshDataset(self, datasetId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/datasets/{datasetId}/refreshes' , auth=InfinigateAuth(self.client_id, self.client_secret))
        return metaData.json()
    
    def getLatestDatasetAPIRefreshStatus(self, datasetId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/datasets/{datasetId}/refreshes' , auth=InfinigateAuth(self.client_id, self.client_secret))
        response = metaData.json()
        refreshHistory = response.get("value")
        return sorted([refresh for refresh in refreshHistory if refresh['refreshType'] == 'ViaApi'], key=lambda x: x['startTime'],reverse=True)[0]["status"]
    
    def postRefreshDataset(self, datasetId : int , workspaceId : str):
        print(f'BEGIN: Refreshing dataset {datasetId} in workspace {workspaceId}.')
        self.session.post(f'{self.baseURL}groups/{workspaceId}/datasets/{datasetId}/refreshes' , auth=InfinigateAuth(self.client_id, self.client_secret))

    def getDataflowTransactions(self, dataflowId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/dataflows/{dataflowId}/transactions' , auth=InfinigateAuth(self.client_id, self.client_secret))
        return metaData.json()

    def postRefreshDataflow(self, dataflowId : int , workspaceId : str):
        print(f'BEGIN: Refreshing dataflow {dataflowId} in workspace {workspaceId}.')
        request_body = json.dumps(
            {"refreshRequest": 'y'})
        
        requestHeader = {}
        requestHeader["Content-Type"] = "application/json"
                                  
        return self.session.post(f'{self.baseURL}groups/{workspaceId}/dataflows/{dataflowId}/refreshes', headers=requestHeader,data=request_body, auth=InfinigateAuth(self.client_id, self.client_secret))
    
    def getLatestDataflowAPIRefreshStatus(self, dataflowId : int , workspaceId : str):
        metaData = self.session.get(f'{self.baseURL}groups/{workspaceId}/dataflows/{dataflowId}/transactions' , auth=InfinigateAuth(self.client_id, self.client_secret))
        response = metaData.json()
        refreshHistory = response.get("value")
        return sorted([refresh for refresh in refreshHistory if refresh['refreshType'] == 'ViaApi'], key=lambda x: x['startTime'],reverse=True)[0]["status"]
