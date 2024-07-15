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
# MAGIC CREATE OR REPLACE VIEW cost_centre AS
# MAGIC
# MAGIC SELECT cost_centre_code,
# MAGIC        cost_centre_name
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_cost_centre
# MAGIC WHERE is_current = 1
# MAGIC
