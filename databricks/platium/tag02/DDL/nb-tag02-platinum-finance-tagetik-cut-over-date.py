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
# MAGIC CREATE OR REPLACE VIEW finance_tagetik_cut_over_date AS
# MAGIC
# MAGIC SELECT (CASE WHEN CAST(NOW() AS DATE) < (
# MAGIC SELECT COALESCE(b.date,a.date) AS date
# MAGIC FROM ( SELECT date
# MAGIC       FROM ( SELECT *, ROW_NUMBER() OVER(ORDER BY date) AS working_day
# MAGIC               FROM gold_${tableObject.environment}.tag02.dim_date
# MAGIC               WHERE YEAR(NOW()) = year
# MAGIC                 AND MONTH(NOW()) = month
# MAGIC                 AND day_of_week NOT IN (1,7) )
# MAGIC       WHERE working_day = 10 ) a
# MAGIC LEFT OUTER JOIN (SELECT CAST('2024-10-01' AS DATE) AS date) b -- This table should contain dates for all overridden months, use the 1st of the in the comparison to NOW() to ensure current date is after it I.E. it's been overridden and use the end of the previous month as the Tagetik cut over 
# MAGIC     ON MONTH(a.date) = MONTH(b.date)
# MAGIC   AND YEAR(a.date) = YEAR(b.date)
# MAGIC ) THEN LAST_DAY(DATE_ADD(MONTH,-2,NOW())) ELSE LAST_DAY(DATE_ADD(MONTH,-1,NOW())) END) AS tagetik_data_cut_off_date
