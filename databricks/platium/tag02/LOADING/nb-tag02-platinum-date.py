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
# MAGIC CREATE OR REPLACE VIEW date AS
# MAGIC
# MAGIC SELECT date,
# MAGIC        date_id,
# MAGIC        day_name,
# MAGIC        day_of_month,
# MAGIC        day_of_week,
# MAGIC        day_of_year,
# MAGIC        first_day_of_month,
# MAGIC        first_day_of_quarter,
# MAGIC        first_day_of_year,
# MAGIC        last_day_of_month,
# MAGIC        last_day_of_quarter,
# MAGIC        last_day_of_year,
# MAGIC        month,
# MAGIC        month_name,
# MAGIC        quarter,
# MAGIC        short_month_name,
# MAGIC        week_end_date,
# MAGIC        week_of_year,
# MAGIC        week_start_date,
# MAGIC        year
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_date
# MAGIC
