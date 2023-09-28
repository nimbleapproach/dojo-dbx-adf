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

try:
    TABLE_SCHEMA = dbutils.widgets.get("wg_tableSchema")
except:
    dbutils.widgets.text(name = "wg_tableSchema", defaultValue = 'tag02')
    TABLE_SCHEMA = dbutils.widgets.get("wg_tableSchema")

# COMMAND ----------

try:
    WATERMARK_COLUMN = dbutils.widgets.get("wg_watermarkColumn")
except:
    dbutils.widgets.text(name = "wg_watermarkColumn", defaultValue = 'DATEUPD')
    WATERMARK_COLUMN = dbutils.widgets.get("wg_watermarkColumn")

# COMMAND ----------

from pyspark.sql.functions import col

tableNames = spark.read.table('INFORMATION_SCHEMA.TABLES').where(col('table_schema')==f'{TABLE_SCHEMA}').select('table_name').collect()

# COMMAND ----------

for table in tableNames:
    dbutils.notebook.run(
    "nb-bronze-to-silver",
    600,
    {
        "wg_tableName": table['table_name'],
        "wg_tableSchema": f"{TABLE_SCHEMA}",
        "wg_watermarkColumn": f"{WATERMARK_COLUMN}",
        "wg_fullload": FULL_LOAD,
        "wg_truncate": TRUNCATE
     })
