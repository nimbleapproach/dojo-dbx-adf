# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

spark.sql(f"""
CREATE
OR Replace VIEW globaltransactions_tagetik_reconciliation_ytd AS with obt as (
  select
    'OBT' AS Source,
    GroupEntityCode,
    case
      when GroupEntityCode = 'VU' THEN 'VU'
      ELSE EntityCode
    END AS EntityCode,
    YEAR(TransactionDate) AS Year,
    MONTH(TransactionDate) AS Month,
    sum(RevenueAmount_Euro) Revenue_EUR,
    sum(GP1_Euro) GP1_EUR
  from
    platinum_{ENVIRONMENT}.obt.globaltransactions
  where
    TransactionDate >= '2022-04-01'
  group by
    MONTH(TransactionDate),
    YEAR(TransactionDate),
    GroupEntityCode,
    case
      when GroupEntityCode = 'VU' THEN 'VU'
      ELSE EntityCode
    END
),
TAG AS (
  select
    'TAG' AS Source,
    '' AS GroupEntityCode,
    case
      when Entity_ID = 'AT1' THEN 'DE1'
      when Entity_ID = 'BE1' THEN 'NL1'
      when Entity_ID = 'FR2' THEN 'FR1'
      WHEN Entity_ID IN('IE1', 'UK2') THEN 'VU'
      ELSE Entity_ID
    END AS EntityCode,
    Year(Date_ID) as Year,
    month(Date_ID) as Month,
    'Revenue_EUR' as Type,
    sum(Amount_LCY_in_Euro) as Amount_EUR
  from
    gold_{ENVIRONMENT}.obt.tagetik_consolidation
  where
    RevenueAccounts = 'TotalRevenue'
    and Scenario_ID like '%ACT-PFA-04'
  GROUP BY
    year(Date_ID),
    month(Date_ID),
    case
      when Entity_ID = 'AT1' THEN 'DE1'
      when Entity_ID = 'BE1' THEN 'NL1'
      when Entity_ID = 'FR2' THEN 'FR1'
      WHEN Entity_ID IN('IE1', 'UK2') THEN 'VU'
      ELSE Entity_ID
    END
  union ALL
  select
    'TAG' AS Source,
    '' AS GroupEntityCode,
    case
      when Entity_ID = 'AT1' THEN 'DE1'
      when Entity_ID = 'BE1' THEN 'NL1'
      when Entity_ID = 'FR2' THEN 'FR1'
      WHEN Entity_ID IN('IE1', 'UK2') THEN 'VU'
      ELSE Entity_ID
    END AS EntityCode,
    year(Date_ID) as Year,
    month(Date_ID) as Month,
    'GP1_EUR' as Type,
    sum(Amount_LCY_in_Euro) as Amount_EUR
  from
    gold_{ENVIRONMENT}.obt.tagetik_consolidation
  where
    GP1Accounts = 'GP1'
    and Scenario_ID like '%ACT-PFA-04'
  GROUP BY
    year(Date_ID),
    month(Date_ID),
    case
      when Entity_ID = 'AT1' THEN 'DE1'
      when Entity_ID = 'BE1' THEN 'NL1'
      when Entity_ID = 'FR2' THEN 'FR1'
      WHEN Entity_ID IN('IE1', 'UK2') THEN 'VU'
      ELSE Entity_ID
    END
),
tag_rev_gp as (
  SELECT
    *
  FROM
    (
      select
        *
      FROM
        TAG
    ) PIVOT(
      SUM(Amount_EUR) FOR Type in ('Revenue_EUR', 'GP1_EUR')
    )
),
result as(
  select
    *,
    CAST (concat_ws('-', Year, Month, '01') AS DATE) DATE_ID
  from
    obt
  union all
  select
    Source,
    entity.GroupEntityCode,
    tag_rev_gp.EntityCode,
    Year,
    Month,
    Revenue_EUR,
    GP1_EUR,
    CAST (concat_ws('-', Year, Month, '01') AS DATE) DATE_ID
  from
    tag_rev_gp
    left join (select distinct GroupEntityCode,EntityCode from
    obt) entity on tag_rev_gp.EntityCode = entity.EntityCode
)
select
  *
from
  result
where
  DATE_ID between concat_ws('-', year(now()) -2, '04', '01')
  and last_day(add_months(now(), -1))""")
