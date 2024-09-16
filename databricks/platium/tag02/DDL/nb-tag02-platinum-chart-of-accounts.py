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
# MAGIC CREATE OR REPLACE VIEW chart_of_accounts AS
# MAGIC
# MAGIC SELECT account_desc,
# MAGIC        account_id,
# MAGIC        level04_desc,
# MAGIC        level07_desc,
# MAGIC        level08_desc,
# MAGIC        level09_desc,
# MAGIC        (CASE WHEN LEFT(account_id, 1) IN ('5','6')
# MAGIC                   AND level08_desc in ('Operating Expense','Personnel Expense') 
# MAGIC              THEN 1 ELSE 0 END) AS opex,
# MAGIC        (CASE WHEN LEFT(account_id, 1) IN ('3','4') 
# MAGIC                   AND level08_desc in ('Gross profit') 
# MAGIC              THEN 1 ELSE 0 END) AS margin
# MAGIC from gold_${tableObject.environment}.tag02.chart_of_accounts  
