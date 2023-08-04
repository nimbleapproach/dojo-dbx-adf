# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog bronze;
# MAGIC use schema tag02;

# COMMAND ----------

def registerTable(tableName : str, recreate : bool = False):
    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {tableName}
                  """)
    if not spark.catalog.tableExists(tableName):
        df = spark.read.parquet(f'/mnt/bronze/tag02/{tableName}/*/*/*')
        schema = df.schema
        spark.catalog.createTable(tableName = f"bronze.tag02.{tableName}",
                                path=f"abfss://bronze@adls0ig0dev0westeurope.dfs.core.windows.net/tag02/{tableName}/",
                                source='parquet',
                                schema=schema)

# COMMAND ----------

for tableName in dbutils.fs.ls('mnt/bronze/tag02'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'tag02'.")
    registerTable(tableName.name[:-1],recreate = False)
    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'tag02'.")
