# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace VIEW globaltransactions_tagetik_revenue_reconciliation_by_year AS
# MAGIC
# MAGIC SELECT 
# MAGIC GroupEntityCode,
# MAGIC EntityCode,
# MAGIC Year_No,
# MAGIC SUM(Globaltransactions_RevenueAmount_Euro) AS Globaltransactions_RevenueAmount_Euro,
# MAGIC SUM(Tagetik_RevenueAmount_Euro) AS Tagetik_RevenueAmount_Euro,
# MAGIC SUM(Globaltransactions_RevenueAmount_Euro - Tagetik_RevenueAmount_Euro) AS Amount_Euro_Diff,
# MAGIC ROUND(SUM(Globaltransactions_RevenueAmount_Euro - Tagetik_RevenueAmount_Euro)/SUM(Globaltransactions_RevenueAmount_Euro)*100,2) AS Percent_Diff
# MAGIC FROM 
# MAGIC     platinum_dev.obt.globaltransactions_tagetik_revenue_reconciliation
# MAGIC GROUP BY
# MAGIC     GroupEntityCode,
# MAGIC     EntityCode,
# MAGIC     Year_No
