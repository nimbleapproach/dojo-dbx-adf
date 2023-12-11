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
# MAGIC CREATE OR Replace VIEW globaltransactions_revenue_reconciliation AS
# MAGIC
# MAGIC WITH initial_query
# MAGIC AS
# MAGIC (
# MAGIC SELECT
# MAGIC     GroupEntityCode,
# MAGIC     EntityCode,
# MAGIC     year(TransactionDate) AS Year_No,
# MAGIC     right(concat('0',cast(month(TransactionDate) as string)),2) AS Month_No,
# MAGIC     coalesce(SUM(RevenueAmount_Euro),0.00) AS RevenueAmount_Euro,
# MAGIC     coalesce(SUM(GP1_Euro),0.00) AS GP1_Euro
# MAGIC FROM 
# MAGIC     platinum_dev.obt.globaltransactions p
# MAGIC GROUP BY 
# MAGIC     GroupEntityCode,
# MAGIC     EntityCode,
# MAGIC     year(TransactionDate),
# MAGIC     right(concat('0',cast(month(TransactionDate) as string)),2)
# MAGIC )
# MAGIC SELECT
# MAGIC     GroupEntityCode,
# MAGIC     EntityCode,
# MAGIC     Year_No,
# MAGIC     Month_No,
# MAGIC     RevenueAmount_Euro,
# MAGIC     GP1_Euro
# MAGIC FROM 
# MAGIC     initial_query
# MAGIC WHERE 
# MAGIC     RevenueAmount_Euro != 0
