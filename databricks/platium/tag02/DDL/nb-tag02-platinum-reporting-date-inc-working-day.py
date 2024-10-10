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
# MAGIC SELECT (CASE WHEN ( SELECT COALESCE(b.date,a.date) AS date
# MAGIC                     FROM ( SELECT date
# MAGIC                           FROM ( SELECT *, ROW_NUMBER() OVER(ORDER BY date) AS working_day
# MAGIC                                   FROM gold_dev.tag02.dim_date
# MAGIC                                   WHERE YEAR(NOW()) = year
# MAGIC                                     AND MONTH(NOW()) = month
# MAGIC                                     AND day_of_week NOT IN (1,7) )
# MAGIC                           WHERE working_day = 10 ) a
# MAGIC                     LEFT OUTER JOIN (SELECT CAST('2024-10-01' AS DATE) AS date) b -- This table should contain dates for all overridden months, use the 1st of the month to ensure the comparison to NOW() returns true
# MAGIC                         ON MONTH(a.date) = MONTH(b.date)
# MAGIC                       AND YEAR(a.date) = YEAR(b.date)) <= NOW() THEN last_day(DATE_ADD(MONTH,-1,NOW())) ELSE last_day(DATE_ADD(MONTH,-2,NOW())) END)
# MAGIC )
# MAGIC ORDER BY last_day_of_month DESC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
