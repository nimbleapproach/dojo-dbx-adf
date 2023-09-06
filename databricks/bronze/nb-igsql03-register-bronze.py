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
# MAGIC use schema igsql03;

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
        using parquet
        Location 'abfss://bronze@adls0ig0dev0westeurope.dfs.core.windows.net/igsql03/{tableName}/'
        """)

# COMMAND ----------

for tableName in dbutils.fs.ls('mnt/bronze/igsql03'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema 'igsql03'.")
    registerTable(tableName.name[:-1])
    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema 'igsql03'.")
