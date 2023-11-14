# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace view silver_dev.igsql03.g_l_consolidate as
# MAGIC SELECT
# MAGIC  right(gl.Sys_DatabaseName, 2 )as EntityCode,
# MAGIC   GL.DocumentNo_,
# MAGIC   cast (PostingDate as date) PostingDate,
# MAGIC   SUM(GL.Amount) AS Revenue_LCY
# MAGIC FROM
# MAGIC   silver_dev.igsql03.g_l_entry AS GL
# MAGIC   LEFT JOIN silver_dev.igsql03.g_l_account AS GA ON GL.G_LAccountNo_ = GA.No_
# MAGIC   and gl.Sys_DatabaseName = ga.Sys_DatabaseName
# MAGIC   and ga.Sys_Silver_IsCurrent = 1
# MAGIC   and gl.Sys_Silver_IsCurrent = 1
# MAGIC where
# MAGIC   GA.Consol_CreditAcc_ IN(
# MAGIC     '309988',
# MAGIC     '310188',
# MAGIC     '310288',
# MAGIC     '310388',
# MAGIC     '310488',
# MAGIC     '310588',
# MAGIC     '310688',
# MAGIC     '310788',
# MAGIC     '310888',
# MAGIC     '310988',
# MAGIC     '311088',
# MAGIC     '312088',
# MAGIC     '313088',
# MAGIC     '314088',
# MAGIC     '320988',
# MAGIC     '322988',
# MAGIC     '370988'
# MAGIC   )
# MAGIC group by
# MAGIC   DocumentNo_,
# MAGIC   PostingDate,
# MAGIC    right(gl.Sys_DatabaseName, 2 )
