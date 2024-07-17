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
# MAGIC CREATE OR REPLACE VIEW account AS
# MAGIC
# MAGIC SELECT account_code,
# MAGIC        account_description,
# MAGIC        account_description_extended
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_account
# MAGIC WHERE is_current = 1
# MAGIC
