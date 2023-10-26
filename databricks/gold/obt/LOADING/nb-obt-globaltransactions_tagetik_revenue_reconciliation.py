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
# MAGIC CREATE OR Replace VIEW globaltransactions_tagetik_revenue_reconciliation AS
# MAGIC
# MAGIC SELECT 
# MAGIC g.GroupEntityCode,
# MAGIC g.EntityCode,
# MAGIC g.Year_No,
# MAGIC g.Month_No,
# MAGIC g.RevenueAmount_Euro AS Globaltransactions_RevenueAmount_Euro,
# MAGIC t.Amount_Euro AS Tagetik_RevenueAmount_Euro,
# MAGIC g.RevenueAmount_Euro - t.Amount_Euro AS Amount_Euro_Diff,
# MAGIC round((t.amount_euro - g.RevenueAmount_Euro)/g.RevenueAmount_Euro,2) AS Percent_Diff
# MAGIC FROM 
# MAGIC     gold_dev.obt.globaltransactions_revenue_reconciliation g
# MAGIC INNER JOIN 
# MAGIC     gold_dev.obt.tagetik_revenue_reconciliation t
# MAGIC ON
# MAGIC     g.entitycode = t.entity_id
# MAGIC AND
# MAGIC     g.year_no = t.year_no
# MAGIC AND
# MAGIC     g.month_no = t.month_no
