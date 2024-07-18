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
# MAGIC CREATE OR REPLACE VIEW entity AS
# MAGIC
# MAGIC SELECT entity_code,
# MAGIC        entity_description,
# MAGIC        entity_type,
# MAGIC        legal_headquarters,
# MAGIC        administrative_city,
# MAGIC        date_established,
# MAGIC        entity_local_currency
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_entity
# MAGIC WHERE is_current = 1
# MAGIC
