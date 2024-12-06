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
# MAGIC CREATE OR REPLACE VIEW reporting_date_selection AS
# MAGIC
# MAGIC SELECT DISTINCT last_day_of_month, CONCAT(MONTHNAME(last_day_of_month),' - ',YEAR(last_day_of_month)) AS reporting_month_description
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_date
# MAGIC WHERE last_day_of_month <= LAST_DAY(NOW())
# MAGIC ORDER BY last_day_of_month DESC
