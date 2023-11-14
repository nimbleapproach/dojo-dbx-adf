# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA igsql03

# COMMAND ----------

spark.sql(f"""create or replace view silver_{ENVIRONMENT}.igsql03.g_l_consolidate as
                
        SELECT
        right(gl.Sys_DatabaseName, 2 )as EntityCode,
        GL.DocumentNo_,
        cast (PostingDate as date) PostingDate,
        SUM(GL.Amount) AS Revenue_LCY
        FROM
        silver_{ENVIRONMENT}.igsql03.g_l_entry AS GL
        LEFT JOIN silver_{ENVIRONMENT}.igsql03.g_l_account AS GA ON GL.G_LAccountNo_ = GA.No_
        and gl.Sys_DatabaseName = ga.Sys_DatabaseName
        and ga.Sys_Silver_IsCurrent = 1
        and gl.Sys_Silver_IsCurrent = 1
        where
        GA.Consol_CreditAcc_ IN(
            '309988',
            '310188',
            '310288',
            '310388',
            '310488',
            '310588',
            '310688',
            '310788',
            '310888',
            '310988',
            '311088',
            '312088',
            '313088',
            '314088',
            '320988',
            '322988',
            '370988'
        )
        group by
        DocumentNo_,
        PostingDate,
        right(gl.Sys_DatabaseName, 2 )""")
