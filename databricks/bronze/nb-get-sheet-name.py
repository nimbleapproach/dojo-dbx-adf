# Databricks notebook source
!pip install openpyxl --quiet

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

file_name = dbutils.widgets.get('file_name')

# COMMAND ----------

class MountHandle:
    def __init__(self):
        self.configs = {"fs.azure.account.auth.type": "OAuth",
                        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                        "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="kv-ig-westeurope",key='secret-databricks-app-id'),
                        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="kv-ig-westeurope",key='secret-databricks-app-value'),
                        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/375313bf-8b9b-44af-97cd-fc2e258a968e/oauth2/token"}
        
    def mount(self,container : str):
        if any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
                print("{}".format(container) + " from {}-ADLS already mounted to".format(container) + " DBFS at /mnt/{}".format(container) +".")
        else:
                dbutils.fs.mount(
                source = f"abfss://{container}@dv0ig0dev0westeurope.dfs.core.windows.net/",
                mount_point = "/mnt/{}".format(container),
                extra_configs = self.configs)
        
    def unmount(self,container :str):
        if any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount("/mnt/{}".format(container))
        else:
            print("DBFS at /mnt/{}".format(container) + " is not mounted.")

# COMMAND ----------

mountHandler = MountHandle()
mountHandler.mount("external")

# COMMAND ----------

dbutils.fs.ls("mnt/external/")

# COMMAND ----------

import pandas as pd


# COMMAND ----------

# file_path = f"/dbfs/mnt/external/vuzion/monthly_export/pending/{file_name}"
# file_path = f"/dbfs/mnt/external/vuzion/monthly_export/archived/FY24 Vuzion Revenue FY24 budget margin.xlsx"

# COMMAND ----------

df = spark.sql(f"""
select distinct Sys_SheetName from silver_{ENVIRONMENT}.vuzion_monthly.vuzion_monthly_revenue""")
target_sheetname = df.select("Sys_SheetName").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

def get_sheet_names(p):

    if len(dbutils.fs.ls(p))>0:
        f = dbutils.fs.ls(p)[0].path
        f = f.replace('dbfs:', '/dbfs')
        source_sheetname =  pd.ExcelFile(f).sheet_names
        sheet_names = list(set(source_sheetname) - set(target_sheetname) )
    else:
        pass

    return sheet_names


# COMMAND ----------

sheet_names_to_load = get_sheet_names(p = '/mnt/external/vuzion/monthly_export/pending/')
dbutils.notebook.exit(sheet_names_to_load)
