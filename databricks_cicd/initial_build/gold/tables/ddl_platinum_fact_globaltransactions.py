# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'obt'

# COMMAND ----------

# REMOVE ONCE SOLUTION IS LIVE
# if ENVIRONMENT == 'dev':
#     spark.sql(f"""
#               DROP TABLE IF EXISTS {catalog}.{schema}.globaltransactions_arr
#               """)

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {catalog}.{schema}.globaltransactions ADD COLUMN
  matched_arr_type STRING,
  matched_type STRING,
  is_matched INT
""")
