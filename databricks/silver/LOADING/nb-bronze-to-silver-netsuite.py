# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

try:
    FULL_LOAD = dbutils.widgets.get("wg_fullload") == 'true'
except:
    dbutils.widgets.dropdown(name = "wg_fullload", defaultValue = 'false', choices =  ['false','true'])
    FULL_LOAD = dbutils.widgets.get("wg_fullload")== 'true'

# COMMAND ----------

try:
    TRUNCATE = dbutils.widgets.get("wg_truncate") == 'true'
except:
    dbutils.widgets.dropdown(name = "wg_truncate", defaultValue = 'false', choices =   ['false','true'])
    TRUNCATE = dbutils.widgets.get("wg_truncate") == 'true'

# COMMAND ----------

from pyspark.sql.functions import col

tableNames = spark.read.table('INFORMATION_SCHEMA.TABLES').where(col('table_schema')=='netsuite').select('table_name').collect()

# COMMAND ----------

for table in tableNames:
    dbutils.notebook.run(
    "nb-bronze-to-silver",
    60,
    {
        "wg_tableName": table['table_name'],
        "wg_tableSchema": "netsuite",
        "wg_watermarkColumn": "Last_Modified",
        "wg_fullload": FULL_LOAD,
        "wg_truncate": TRUNCATE
     })
