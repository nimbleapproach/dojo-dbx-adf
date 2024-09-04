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
# MAGIC SELECT DISTINCT last_day_of_month
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_date
# MAGIC WHERE last_day_of_month <= 
# MAGIC (
# MAGIC SELECT (CASE WHEN (
# MAGIC   SELECT date
# MAGIC FROM (SELECT *, ROW_NUMBER() OVER(ORDER BY date) AS working_day
# MAGIC       FROM gold_${tableObject.environment}.tag02.dim_date
# MAGIC       WHERE YEAR(NOW()) = year
# MAGIC         AND MONTH(NOW()) = month
# MAGIC         AND day_of_week NOT IN (6,7))
# MAGIC WHERE working_day = 10) <= NOW() THEN last_day(DATE_ADD(MONTH,-1,NOW())) ELSE last_day(DATE_ADD(MONTH,-2,NOW())) END)
# MAGIC )
# MAGIC ORDER BY last_day_of_month
# MAGIC
