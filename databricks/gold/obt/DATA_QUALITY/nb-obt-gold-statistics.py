# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE OBT

# COMMAND ----------

obt_df = spark.read.table('globaltransactions')

# COMMAND ----------

dbutils.data.summarize(obt_df)
