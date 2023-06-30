# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create an example catalog and schema to contain the new table
# MAGIC CREATE CATALOG IF NOT EXISTS bronze;
# MAGIC USE CATALOG bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS igsql03;
# MAGIC USE igsql03;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES on catalog bronze to az_edw_data_engineers

# COMMAND ----------

dbutils.fs.ls('mnt/bronze/igsql03')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog bronze;
# MAGIC use schema igsql03;

# COMMAND ----------

df = spark.read.parquet('/mnt/bronze/igsql03/DefaultDimension/')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bronze.igsql03.DefaultDimension
# MAGIC USING PARQUET
# MAGIC LOCATION 'abfss://bronze@adls0ig0dev0westeurope.dfs.core.windows.net/igsql03/DefaultDimension/'

# COMMAND ----------

def registerTable(tableName : str):
    spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS bronze.igsql03.{tableName}
                USING PARQUET
                LOCATION 'abfss://bronze@adls0ig0dev0westeurope.dfs.core.windows.net/igsql03/{tableName}/'
              """)

# COMMAND ----------

registerTable("CompanyInformation")

# COMMAND ----------

for tableName in dbutils.fs.ls('mnt/bronze/igsql03'):
    registerTable(tableName.name[:-1])
