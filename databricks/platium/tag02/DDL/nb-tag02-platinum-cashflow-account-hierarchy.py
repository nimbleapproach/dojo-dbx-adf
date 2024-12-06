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

spark.sql(f"""

CREATE OR REPLACE VIEW cash_flow_accounts AS


select distinct
  child.COD_CONTO as AccountCode,
  child.DESC_CONTO0 as AccountName,
  parent.ParentAccountCode,
  ParentAccountName
from
  silver_{ENVIRONMENT}.tag02.CONTO child

  inner join (
    SELECT
      link.COD_CONTO,
      ParentAccountCode,
      ParentAccountName
    FROM
      silver_{ENVIRONMENT}.tag02.CONTO_GERARCHIA_ABBI link
      left join (
        SELECT
          COD_CONTO_ELEGER as ParentAccountCode,
          DESC_CONTO_ELEGER0 as ParentAccountName
        FROM
          silver_{ENVIRONMENT}.tag02.CONTO_GERARCHIA

        where
          COD_CONTO_GERARCHIA = '53'
          and Sys_Silver_IsCurrent=1
          and Sys_Silver_IsDeleted=0
      ) parent on link.COD_CONTO_ELEGER = parent.ParentAccountCode
    where
      link.COD_CONTO_GERARCHIA = '53'
      and Sys_Silver_IsCurrent=1
      and Sys_Silver_IsDeleted=0
  ) parent on child.COD_CONTO = parent.COD_CONTO
  and child.Sys_Silver_IsCurrent=1
  and child.Sys_Silver_IsDeleted=0
  --where ParentAccountName='Receivables from Goods and Services'
  """)


