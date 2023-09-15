# Databricks notebook source
try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
except:
    dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices =  ['dev','uat','prod'])
    ENVIRONMENT = dbutils.widgets.get("wg_environment")

# COMMAND ----------

try:
    REREGISTER = bool(dbutils.widgets.get("wg_reregister"))
except:
    dbutils.widgets.dropdown(name = "wg_reregister", defaultValue = 'false', choices =  ['false','true'])
    REREGISTER = bool(dbutils.widgets.get("wg_reregister"))

# COMMAND ----------

try:
    BRONZE_SCHEMA = dbutils.widgets.get("wg_bronzeSchema")
except:
    dbutils.widgets.text(name = "wg_bronzeSchema", defaultValue = 'igsql03')
    BRONZE_SCHEMA = dbutils.widgets.get("wg_bronzeSchema")

# COMMAND ----------

try:
    ROOT_PATH = dbutils.widgets.get("wg_rootPath")
except:
    dbutils.widgets.text(name = "wg_rootPath", defaultValue = 'igsql03')
    ROOT_PATH = dbutils.widgets.get("wg_rootPath")

# COMMAND ----------

try:
    TABLE_FORMAT = dbutils.widgets.get("wg_tableFormat")
except:
    dbutils.widgets.dropdown(name = "wg_tableFormat", defaultValue = 'parquet', choices =  ['parquet','delta'])
    TABLE_FORMAT = dbutils.widgets.get("wg_tableFormat")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

def registerTable(rootPath : str, tableName : str,schemaName : str, recreate : bool = False, tableFormat : str = 'parquet'):
    
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
        using {tableFormat}
        Location 'abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/{rootPath}/{tableName}/'
        """)

# COMMAND ----------

for tableName in dbutils.fs.ls(f'mnt/bronze/{ROOT_PATH}'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{BRONZE_SCHEMA}' as {TABLE_FORMAT}.")

    registerTable(rootPath = ROOT_PATH, tableName = tableName.name[:-1], schemaName=BRONZE_SCHEMA, recreate=REREGISTER, tableFormat=TABLE_FORMAT)

    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{BRONZE_SCHEMA}' as {TABLE_FORMAT}.")
