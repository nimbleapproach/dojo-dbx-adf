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
# MAGIC CREATE OR REPLACE VIEW region AS
# MAGIC
# MAGIC SELECT region_code,
# MAGIC        region_name,
# MAGIC        country_code,
# MAGIC        country,
# MAGIC        country_detail,
# MAGIC        country_visuals,
# MAGIC        region_group
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_region
# MAGIC WHERE is_current = 1
# MAGIC
