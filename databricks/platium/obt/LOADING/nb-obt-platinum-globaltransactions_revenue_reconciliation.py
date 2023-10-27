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
# MAGIC WITH YearMonth
# MAGIC AS
# MAGIC (
# MAGIC SELECT DISTINCT cast(Year_No as string) AS Year_No, cast(Month_No as string) AS Month_No
# MAGIC FROM gold_dev.obt.tagetik_revenue_reconciliation
# MAGIC )
# MAGIC SELECT
# MAGIC     GroupEntityCode,
# MAGIC     EntityCode,
# MAGIC     year(TransactionDate) AS Year_No,
# MAGIC     right(concat('0',cast(month(TransactionDate) as string)),2) AS Month_No,
# MAGIC     coalesce(SUM(RevenueAmount_Euro),0.00) AS RevenueAmount_Euro
# MAGIC FROM 
# MAGIC     gold_dev.obt.globaltransactions_platinum p
# MAGIC INNER JOIN
# MAGIC     YearMonth ym
# MAGIC ON
# MAGIC     ym.Year_No = year(p.TransactionDate)
# MAGIC AND
# MAGIC     ym.Month_No = right(concat('0',cast(month(p.TransactionDate) as string)),2)
# MAGIC GROUP BY 
# MAGIC     GroupEntityCode,
# MAGIC     EntityCode,
# MAGIC     year(TransactionDate),
# MAGIC     right(concat('0',cast(month(TransactionDate) as string)),2)
