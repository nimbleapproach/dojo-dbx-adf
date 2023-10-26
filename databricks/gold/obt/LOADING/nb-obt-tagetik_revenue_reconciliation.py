# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace VIEW tagetik_revenue_reconciliation AS
# MAGIC
# MAGIC SELECT 
# MAGIC Entity_ID,
# MAGIC year(Date_ID) AS Year_No,
# MAGIC right(concat('0',cast(month(Date_ID) as string)),2) AS Month_No,
# MAGIC sum(Amount_LCY_in_Euro) AS Amount_Euro
# MAGIC FROM 
# MAGIC   gold_dev.obt.tagetik_consolidation
# MAGIC WHERE
# MAGIC   RevenueAccounts = 'TotalRevenue'
# MAGIC GROUP BY
# MAGIC Entity_ID,
# MAGIC year(Date_ID),
# MAGIC right(concat('0',cast(month(Date_ID) as string)),2)
