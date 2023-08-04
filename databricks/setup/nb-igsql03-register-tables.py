# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog bronze;
# MAGIC use schema igsql03;

# COMMAND ----------

def registerTable(tableName : str, recreate : bool = False):
    cleanedTableName = tableName.replace(" ", "")
    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {cleanedTableName}
                  """)
    spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS bronze.igsql03.`{cleanedTableName}`
                USING PARQUET
                LOCATION 'abfss://bronze@adls0ig0dev0westeurope.dfs.core.windows.net/igsql03/{tableName}/'
              """)

# COMMAND ----------

for tableName in dbutils.fs.ls('mnt/bronze/igsql03'):
    print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'igsql03'.")
    registerTable(tableName.name[:-1])
    print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'igsql03'.")
