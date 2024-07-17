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
# MAGIC CREATE OR REPLACE VIEW exchange_rate AS
# MAGIC
# MAGIC SELECT exchange_rate_code,
# MAGIC        scenario_code,
# MAGIC        period,
# MAGIC        currency_code
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_exchange_rate
# MAGIC WHERE is_current = 1
# MAGIC
