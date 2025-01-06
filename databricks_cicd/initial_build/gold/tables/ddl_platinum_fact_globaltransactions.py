# Databricks notebook source
# MAGIC %run ./../../nb-build-common

# COMMAND ----------

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


add_column_if_not_exists(catalog, schema, "globaltransactions", "matched_arr_type", "STRING")
add_column_if_not_exists(catalog, schema, "globaltransactions", "matched_type", "STRING")
add_column_if_not_exists(catalog, schema, "globaltransactions", "is_matched", "INT")
