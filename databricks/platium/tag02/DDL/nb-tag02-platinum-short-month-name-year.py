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
# MAGIC CREATE OR REPLACE VIEW short_month_name_year AS
# MAGIC
# MAGIC SELECT ROW_NUMBER() OVER(ORDER BY x.year, x.month) AS sort_id,
# MAGIC        x.short_month_name_year
# MAGIC FROM (
# MAGIC SELECT DISTINCT CONCAT(short_month_name,' - ',CAST(year AS STRING)) AS short_month_name_year, month, year
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_date
# MAGIC ) x
# MAGIC
# MAGIC
# MAGIC
