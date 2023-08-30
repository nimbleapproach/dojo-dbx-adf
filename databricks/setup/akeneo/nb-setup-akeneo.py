# Databricks notebook source
# MAGIC %md
# MAGIC # Library for connecting to Akeneo REST API

# COMMAND ----------

from pyspark.sql.functions import *
import requests
import base64
import json
from pprint import pprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## AkeneoAuth
# MAGIC This class is our custom authentification.
# MAGIC
# MAGIC ### __init__

# COMMAND ----------

BASE_URL = dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-akeneo-connector-baseurl")
AKENEO_SESSION = requests.Session()

# COMMAND ----------

class AkeneoAuth(requests.auth.AuthBase):
    def __init__(self):
        get_token_url =  f"{BASE_URL}/api/oauth/v1/token"
        client_id = dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-akeneo-connector-clientid")
        secret = dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-akeneo-connector-secret")
        raw_string = client_id + ":" + secret
        token_string = 'Basic {}'.format(str(base64.b64encode(raw_string.encode('ascii')),encoding="UTF-8"))
        request_body = json.dumps({"username" : dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-akeneo-connector-username"),
                                   "password": dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-akeneo-connector-password") ,
                                   "grant_type" : "password"}, indent=2)
        response = requests.post(get_token_url, data = request_body,
                               headers={
                                   'Content-Type':'application/json',
                                   'Authorization': token_string
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

class Products:
        
    def getData(self,indexSku : str = "", updatedAfter : str = "1900-01-01 10:00:00", limit : int = 100):
        searchString  = f'&search={{"updated":[{{"operator":">","value":"{updatedAfter}"}}]}}'
        endpoint =  f'?limit={limit}&pagination_type=search_after&search_after={indexSku}{searchString}'
        response = AKENEO_SESSION.get(f'{BASE_URL}/api/rest/v1/products{endpoint}', auth=AkeneoAuth())
        return json.loads(response.text)

    
    def getProductsDF(self,updatedAfter : str = "1900-01-01 10:00:00",limit : int = 100, onlyFirstPage : bool =  False):
        jsonDataRaw = self.getData(updatedAfter = updatedAfter,limit=limit)
        

        if '_embedded' in list(jsonDataRaw.keys()):
            jsonData = jsonDataRaw["_embedded"]["items"]
            
            if jsonData != []:
                jsonDataUnion = [json.dumps(jsonData)]        
                currentSku = jsonData[-1]['identifier']

                jsonData = self.getData(indexSku = currentSku,updatedAfter = updatedAfter,limit=limit)["_embedded"]["items"]

                if onlyFirstPage == False:
                    while jsonData != []:

                        jsonDataUnion.append(json.dumps(jsonData))

                        currentSku =  currentSku = jsonData[-1]['identifier']
                        jsonData = self.getData(indexSku = currentSku,updatedAfter = updatedAfter,limit=limit)["_embedded"]["items"]

    def writeProducts(self,updatedAfter : str = "1900-01-01 10:00:00",limit : int = 100, onlyFirstPage : bool =  False):
        jsonDataRaw = self.getData(updatedAfter = updatedAfter,limit=limit)
        

        if '_embedded' in list(jsonDataRaw.keys()):
            jsonData = jsonDataRaw["_embedded"]["items"]
            
            if jsonData != []:      
                currentSku = jsonData[-1]['identifier']

                jsonData = self.getData(indexSku = currentSku,updatedAfter = updatedAfter,limit=limit)["_embedded"]["items"]

                if onlyFirstPage == False:
                    while jsonData != []:
                        spark.read.json(sc.parallelize([json.dumps(jsonData)])).write.mode('append').option("mergeSchema", "true").saveAsTable('products')

                        currentSku =  currentSku = jsonData[-1]['identifier']
                        jsonData = self.getData(indexSku = currentSku,updatedAfter = updatedAfter,limit=limit)["_embedded"]["items"]


