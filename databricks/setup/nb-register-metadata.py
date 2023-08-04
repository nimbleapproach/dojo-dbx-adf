# Databricks notebook source
serverName = dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-sql-server-name")
databaseName = dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-sql-db-name")
password = dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-sql-databricks-connector")

# COMMAND ----------

spark.sql(f"""
    CREATE CONNECTION metadata_sqlserver
    TYPE SQLSERVER
    OPTIONS (
      host '{serverName}',
      port '1433',
      user 'az_edw_adb_connector',
      password '{password}')
      """)

# COMMAND ----------

spark.sql(f"""
    CREATE FOREIGN CATALOG  IF NOT EXISTS  metadata
    USING CONNECTION metadata_sqlserver
    options (database='{databaseName}')
          """)
