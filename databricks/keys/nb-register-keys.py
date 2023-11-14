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

try:
    KEYS_SCHEMA = dbutils.widgets.get("wg_keysSchema")
except:
    dbutils.widgets.text(name = "wg_keysSchema", defaultValue = 'igsql03')
    KEYS_SCHEMA = dbutils.widgets.get("wg_keysSchema")

# COMMAND ----------

try:
    ROOT_PATH = dbutils.widgets.get("wg_rootPath")
except:
    dbutils.widgets.text(name = "wg_rootPath", defaultValue = 'igsql03')
    ROOT_PATH = dbutils.widgets.get("wg_rootPath")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"keys_{ENVIRONMENT}")

# COMMAND ----------

def registerTable(rootPath : str, tableName : str,schemaName : str, recreate : bool = False):
    
    spark.sql(f"""
            use schema {schemaName}
            """)

    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {tableName}
                  """)
    
    if not spark.catalog.tableExists(tableName):
        spark.sql(f"""
        Create table {tableName}
        using parquet
        Location 'abfss://keys@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/{rootPath}/{tableName}/'
        """)

# COMMAND ----------

for tableName in dbutils.fs.ls(f'mnt/keys/{ROOT_PATH}'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'keys_{ENVIRONMENT}' in schema '{KEYS_SCHEMA}' as parquet.")

    registerTable(rootPath = ROOT_PATH, tableName = tableName.name[:-1], schemaName=KEYS_SCHEMA, recreate=REREGISTER)

    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'keys_{ENVIRONMENT}' in schema '{KEYS_SCHEMA}' as parquet.")
