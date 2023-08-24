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
# MAGIC use schema netsuite;

# COMMAND ----------

def registerTable(tableName : str, recreate : bool = False):
    cleanedTableName = tableName.replace(" ", "")
    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {cleanedTableName}
                  """)
    if not spark.catalog.tableExists(cleanedTableName):
        df = spark.read.parquet(f'/mnt/bronze/netsuite/{tableName}/')
        schema = df.schema
        spark.catalog.createTable(tableName = cleanedTableName,
                                path=f"abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/netsuite/{tableName}/",
                                source='parquet',
                                schema=schema)

# COMMAND ----------

for tableName in dbutils.fs.ls('mnt/bronze/netsuite'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'netsuite'.")
    registerTable(tableName.name[:-1],recreate = True)
    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'netsuite'.")
