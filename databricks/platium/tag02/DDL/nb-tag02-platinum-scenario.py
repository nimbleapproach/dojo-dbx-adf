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
# MAGIC CREATE OR REPLACE VIEW scenario AS
# MAGIC
# MAGIC SELECT scenario_code,
# MAGIC        scenario_type,
# MAGIC        scenario_group,
# MAGIC        original_scenario_code,
# MAGIC        scenario_description,
# MAGIC        scenario_version_description
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_scenario
# MAGIC WHERE is_current = 1
# MAGIC
