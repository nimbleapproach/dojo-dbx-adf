# Databricks notebook source
try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
except:
    dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices =  ['dev','uat','prod'])
    ENVIRONMENT = dbutils.widgets.get("wg_environment")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema nuvias_operations;

# COMMAND ----------

def registerTable(tableName : str, recreate : bool = False):
    cleanedTableName = tableName.replace(" ", "")
    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {cleanedTableName}
                  """)
    if not spark.catalog.tableExists(cleanedTableName):
        #df = spark.read.parquet(f'/mnt/bronze/igsql03/{tableName}/')
        #schema = df.schema
        spark.sql(f"""
        Create table {cleanedTableName}
        Location 'abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/nuvias.operations.dynamics.com/{tableName}/'
        """)

# COMMAND ----------

for tableName in dbutils.fs.ls('mnt/bronze/nuvias.operations.dynamics.com'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema 'nuvias_operations'.")
    registerTable(tableName.name[:-1])
    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema 'nuvias_operations'.")
