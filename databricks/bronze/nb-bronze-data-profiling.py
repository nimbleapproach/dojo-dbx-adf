# Databricks notebook source
try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
except:
    dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices =  ['dev','uat','prod'])
    ENVIRONMENT = dbutils.widgets.get("wg_environment")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

schemaChoices = [row['table_schema'] for row in spark.read.table('INFORMATION_SCHEMA.tables').select('table_schema').distinct().collect()]

# COMMAND ----------

try:
    SCHEMA_NAME = dbutils.widgets.get("wg_schemaName")
except:
    dbutils.widgets.dropdown(name = "wg_schemaName", defaultValue = 'igsql03', choices =  schemaChoices)
    SCHEMA_NAME = dbutils.widgets.get("wg_schemaName")

# COMMAND ----------

spark.catalog.setCurrentDatabase(SCHEMA_NAME)

# COMMAND ----------

from pyspark.sql.functions import col

tableChoices = [row['table_name'] for row in spark.read.table('INFORMATION_SCHEMA.tables').where(col('table_schema')==SCHEMA_NAME).select('table_name').distinct().collect()]

# COMMAND ----------

try:
    TABLE_NAME = dbutils.widgets.get("wg_tableName")
except:
    dbutils.widgets.dropdown(name = "wg_tableName", defaultValue = 'customer', choices =  tableChoices)
    TABLE_NAME = dbutils.widgets.get("wg_tableName")

# COMMAND ----------

df = spark.read.table(TABLE_NAME)

# COMMAND ----------

dbutils.data.summarize(df)
