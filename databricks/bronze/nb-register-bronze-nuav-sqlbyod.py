# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

try:
    REREGISTER = bool(dbutils.widgets.get("wg_reregister") == 'true')
except:
    dbutils.widgets.dropdown(name = "wg_reregister", defaultValue = 'false', choices =  ['false','true'])
    REREGISTER = bool(dbutils.widgets.get("wg_reregister") == 'true')

# COMMAND ----------

ROOT_PATH = 'nuaz-sqlserver-01'

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

def registerTable(rootPath : str, tableName : str,schemaName : str,databaseName: str, recreate : bool = False):
    tableName_uc = f"{schemaName}_{tableName}"
    
    spark.sql(f"""
            use schema {databaseName}
            """)

    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {tableName_uc}
                  """)
    
    if not spark.catalog.tableExists(tableName_uc):
        spark.sql(f"""
        Create table {tableName_uc}
        using DELTA
        Location 'abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/{rootPath}/{databaseName}/{schemaName}/{tableName}/'
        """)

# COMMAND ----------


for databaseName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01'):
    for schemaName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/{databaseName.name[:-1]}'):
        for tableName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/{databaseName.name[:-1]}/{schemaName.name[:-1]}'):
            print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{databaseName.name[:-1]}' .")

            registerTable(rootPath = ROOT_PATH, tableName = tableName.name[:-1],schemaName=schemaName.name[:-1],databaseName=databaseName.name[:-1], recreate=REREGISTER)

            print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{databaseName.name[:-1]}' .")
