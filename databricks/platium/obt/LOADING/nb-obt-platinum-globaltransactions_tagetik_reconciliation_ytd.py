# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------


# Original Date [27/02/2024]
# Created BY [YZ]
# Generate a view for tagetik vs obt reconciliation use as source for reconciliation report ib powerbi

spark.sql(f"""
CREATE
OR Replace VIEW globaltransactions_tagetik_reconciliation_ytd AS 
/*with obt as(
select
    'OBT' AS Source,
    GroupEntityCode,
    case
      when GroupEntityCode = 'VU' THEN 'VU'
      ELSE EntityCode
    END AS EntityCode,
    case
      when GroupEntityCode = 'VU' THEN 'VU'
      ELSE EntityCode
    END  AS Region_ID ,
    VendorCode,
    VendorNameInternal as VendorName,
    VendorNameMaster,
    ''AS Category,
    YEAR(TransactionDate) AS Year,
    MONTH(TransactionDate) AS Month,
    Sum(coalesce(RevenueAmount_Euro,0 )) Revenue_EUR,
    Sum(coalesce(GP1_Euro,0 )) GP1_EUR
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
    END,
        VendorCode,
    VendorNameInternal,
    VendorNameMaster
    HAVING sum(coalesce(RevenueAmount_Euro,0 ))+ sum(coalesce(GP1_Euro,0 )) <>0
  ),*/
with 
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
    Region_ID,
    Vendor_ID as VendorCode,
    Vendor_Name as  VendorName,
    '' as VendorNameMaster,
    Category,
    Year(Date_ID) as Year,
    month(Date_ID) as Month,
    'Revenue_EUR' as Type,
    sum(Amount_LCY_in_Euro) as Amount_EUR
  from
    gold_{ENVIRONMENT}.obt.tagetik_consolidation
  where
    RevenueAccounts = 'TotalRevenue'
    and (Scenario_ID like '%ACT-PFA-04'
    or Scenario_ID='2025ACT-PFA-01')
  GROUP BY
    year(Date_ID),
    month(Date_ID),
    case
      when Entity_ID = 'AT1' THEN 'DE1'
      when Entity_ID = 'BE1' THEN 'NL1'
      when Entity_ID = 'FR2' THEN 'FR1'
      WHEN Entity_ID IN('IE1', 'UK2') THEN 'VU'
      ELSE Entity_ID
    END,
    Region_ID,

    Vendor_ID ,
    Vendor_Name,
    Category

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
    Region_ID,

    Vendor_ID  as VendorCode,
    Vendor_Name as  VendorName,
    '' as VendorNameMaster,
    Category,
    year(Date_ID) as Year,
    month(Date_ID) as Month,
    'GP1_EUR' as Type,
    sum(Amount_LCY_in_Euro) as Amount_EUR
  from
    gold_{ENVIRONMENT}.obt.tagetik_consolidation
  where
    GP1Accounts = 'GP1'
    and  (Scenario_ID like '%ACT-PFA-04'
    or Scenario_ID='2025ACT-PFA-01')
  GROUP BY
    year(Date_ID),
    month(Date_ID),
    case
      when Entity_ID = 'AT1' THEN 'DE1'
      when Entity_ID = 'BE1' THEN 'NL1'
      when Entity_ID = 'FR2' THEN 'FR1'
      WHEN Entity_ID IN('IE1', 'UK2') THEN 'VU'
      ELSE Entity_ID
    END,
    Region_ID,

    Vendor_ID,
    Vendor_Name,
    Category
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
  --select
  --  *,
  --  CAST (concat_ws('-', Year, Month, '01') AS DATE) DATE_ID
  --from
  --  obt
  --union all
  select
    Source,
    entity.GroupEntityCode,
    tag_rev_gp.EntityCode,
    tag_rev_gp.Region_ID,
    tag_rev_gp.VendorCode,
    tag_rev_gp.VendorName,
    tag_rev_gp.VendorNameMaster,
    Category,
    Year,
    Month,
    Revenue_EUR,
    GP1_EUR,
    CAST (concat_ws('-', Year, Month, '01') AS DATE) DATE_ID
  from
    tag_rev_gp
    left join (select distinct GroupEntityCode,case
      when GroupEntityCode = 'VU' THEN 'VU'
      ELSE EntityCode
    END AS EntityCode from
    platinum_{ENVIRONMENT}.obt.globaltransactions) entity on tag_rev_gp.EntityCode = entity.EntityCode
    where Revenue_EUR + GP1_EUR<>0
)
select
  *
from
  result
where
  DATE_ID between concat_ws('-', year(now()) -2, '04', '01')
  and last_day(add_months(now(), -1))""")
