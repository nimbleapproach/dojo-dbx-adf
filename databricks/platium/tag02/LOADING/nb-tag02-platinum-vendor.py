# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

spark.conf.set("tableObject.environment", ENVIRONMENT)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Use SCHEMA tag02

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW vendor AS
# MAGIC
# MAGIC SELECT vendor_code,
# MAGIC        vendor_name
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_vendor
# MAGIC WHERE is_current = 1
# MAGIC
