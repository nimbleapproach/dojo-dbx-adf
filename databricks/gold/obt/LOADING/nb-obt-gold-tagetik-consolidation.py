# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 0,Gold Transactions Platinum
spark.sql(f"""
          

CREATE OR Replace VIEW tagetik_consolidation AS


with FX AS(
  /****** FX Rate  ******/

--[(21.02.2024) yz: Adjust FX from gold exchange rate table]
select distinct
Scenario,
Period,
Month,
Currency,
Period_FX_rate
 from gold_{ENVIRONMENT}.obt.exchange_rate
 ),
--[(17.02.2024) yz: Tagetik consolidation Vendor]
  vendor as (select 
  Code as Vendor_ID,
  Name as Vendor_Name

  from silver_{ENVIRONMENT}.igsql03.dimension_value
  where Sys_Silver_IsCurrent =1
  and DimensionCode = 'VENDOR'
  and Sys_DatabaseName='ReportsDE'),
  -- base table with original entity amounts
  base as(
  SELECT
    fb.COD_PERIODO AS Period,
    COD_VALUTA AS Currency_ID,
    COD_CONTO AS Account_ID,
    COD_DEST2 AS Region_ID,
    COD_DEST4 AS SpecialDeal_ID,CASE
      WHEN COD_DEST1 IS NULL
      OR COD_DEST1 = ''
      OR COD_DEST1 = 'NULL'
      OR COD_DEST1 IS NULL THEN 'N/A'
      ELSE COD_DEST1
    END AS Vendor_ID,
    fb.COD_SCENARIO AS Scenario_ID,
    COD_AZIENDA AS Entity_ID,
    COD_CATEGORIA as Category,
    CAST( sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
  FROM
    silver_{ENVIRONMENT}.tag02.dati_saldi_lordi FB
  WHERE
    (COD_SCENARIO like '%ACT%')
    and (COD_SCENARIO  like '%04')
    and (COD_SCENARIO not like '%OB%')
    AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4'))
 -- [yz] 2024.02.21 include all categories for finance report 
    AND (COD_CATEGORIA  like"%AMOUNT" or 
COD_CATEGORIA in (
'ADJ01'
,'ADJ02'
,'ADJ03'
,'CF_GROUP'
,'CF_LOCAL'
,'CF_TDA'
,'INP_HQ03'
,'INP_HQ04'
,'INP_HQ05'
,'INP_HQ06'
,'INP_HQ07'
,'INP_MSP2018'
,'SYN2'
 ) )
    AND Sys_Silver_IsCurrent = 1
    AND ( Sys_Silver_IsDeleted =0 or Sys_Silver_IsDeleted is null)
  group by
    COD_PERIODO,
    COD_VALUTA,
    COD_CONTO,
    COD_DEST2,
    COD_SCENARIO,
    COD_AZIENDA,CASE
      WHEN COD_DEST1 IS NULL
      OR COD_DEST1 = ''
      OR COD_DEST1 = 'NULL'
      OR COD_DEST1 IS NULL THEN 'N/A'
      ELSE COD_DEST1
    END,
    COD_DEST4,
    COD_CATEGORIA
),
adjustments as(
  SELECT
    fb.COD_PERIODO AS Period,
    COD_VALUTA AS Currency_ID,
    COD_CONTO AS Account_ID,
    COD_DEST2 AS Region_ID,
    COD_DEST4 AS SpecialDeal_ID,CASE
      WHEN COD_DEST1 IS NULL
      OR COD_DEST1 = ''
      OR COD_DEST1 = 'NULL'
      OR COD_DEST1 IS NULL THEN 'N/A'
      ELSE COD_DEST1
    END AS Vendor_ID,
    fb.COD_SCENARIO AS Scenario_ID,
    COD_AZIENDA AS Entity_ID,
    COD_CATEGORIA AS Category,
    CAST( sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
  FROM
    silver_{ENVIRONMENT}.tag02.dati_rett_riga FB
  WHERE
    (COD_SCENARIO like '%ACT%')
    and (COD_SCENARIO  like '%04')
    and (COD_SCENARIO not like '%OB%')
    AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4'))
-- [yz] 2024.02.21 include all manual journals categories for finance report 
    AND COD_CATEGORIA like '%ADJ%'
    AND Sys_Silver_IsCurrent = 1
    AND ( Sys_Silver_IsDeleted =0 or Sys_Silver_IsDeleted is null)
  group by
    COD_PERIODO,
    COD_VALUTA,
    COD_CONTO,
    COD_DEST2,
    COD_SCENARIO,
    COD_AZIENDA,CASE
      WHEN COD_DEST1 IS NULL
      OR COD_DEST1 = ''
      OR COD_DEST1 = 'NULL'
      OR COD_DEST1 IS NULL THEN 'N/A'
      ELSE COD_DEST1
    END,
    COD_DEST4,
    COD_CATEGORIA
),
cte as(
  select *from base
  union all
  select * from adjustments
),
cte_2 as (
  select
    *,
    MAX(Period) OVER (
      Partition by Currency_ID,
      Account_ID,
      SpecialDeal_ID,
      Region_ID,
      Vendor_ID,
      Scenario_ID,
      Entity_ID,
      Category
    ) as maxMonth
  FROM
    cte as base 
),
cte_3 as(
  select
    '12' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '11'
    and Period = '11'
  UNION ALL
    select
    '11' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '10'
    and Period = '10'
  UNION ALL
    select
    '10' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '09'
    and Period = '09'
  UNION ALL
    select
    '09' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '08'
    and Period = '08'
  UNION ALL
    select
    '08' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '07'
    and Period = '07'
  UNION ALL
    select
    '07' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '06'
    and Period = '06'
  UNION ALL
    select
    '06' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '05'
    and Period = '05'
  UNION ALL
    select
    '05' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '04'
    and Period = '04'
  UNION ALL
    select
    '04' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '03'
    and Period = '03'
  UNION ALL
    select
    '03' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '02'
    and Period = '02'
  UNION ALL
    select
    '02' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
     0 AS Amount_LCY_Original
  from
    cte_2
  where
    maxMonth = '01'
    and Period = '01'
UNION ALL
  SELECT
    Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
    Amount_LCY_Original
  from
    cte
),
cta as(
  SELECT
    *,
    LAG(Amount_LCY_Original, 1, 0) OVER(
      PARTITION BY Currency_ID,
      Account_ID,
      SpecialDeal_ID,
      Region_ID,
      Vendor_ID,
      Scenario_ID,
      Entity_ID,
      Category
      ORDER BY
        Period ASC
    ) AS Amount_LCY_Original_Prev
  FROM
    cte_3
),
act as(
  select
    case
      when CTA.Period between '01'
      and '09' then DATEADD(
        YEAR,
        -1,( DATEADD(month, 3,CONCAT( LEFT(Scenario_ID, 4) , '-' , CAST(CTA.Period AS STRING),'-01') )
        )
      )
      when CTA.Period between '10'
      and '12' then  DATEADD(month, -9,CONCAT( LEFT(Scenario_ID, 4) , '-' , CAST(CTA.Period AS STRING),'-01') )
    end AS Date_ID ,
    cta.Period,
    Currency_ID,
    Account_ID,
    case when Account_ID IN(
                '309988'
                ,'310188'
                ,'310288'
                ,'310388'
                ,'310488'
                ,'310588'
                ,'310688'
                ,'310788'
                ,'310888'
                ,'310988'
                ,'311088'
                ,'312088'
                ,'313088'
                ,'314088'
                ,'320988'
                ,'322988'
                ,'370988'
                -- ,'371988' #these two are IC accounts shouldn't be included
                -- ,'391988'
                )
     THEN 'TotalRevenue' 
     WHEN Account_ID IN (
                '400988'
                ,'401988'
                ,'402988'
                ,'409999_REF'
                ,'420988'
                ,'420999_REF'
                ,'421988'
                ,'422988'
                ,'440188'
                ,'440288'
                ,'440388'
                ,'440488'
                ,'440588'
                ,'440688'
                ,'440788'
                ,'440888'
                ,'449988'
                ,'450888'
                ,'450988'
                ,'452888'
                ,'452988'
                ,'468988'
                ,'471988'
     )THEN 'Total COGS'
     ELSE 'Others' end as RevenueAccounts,
     case when ACCOUNT_ID IN (
        '309988' ,'310188' ,'310288' ,'310388' ,'310488' ,'310588' ,'310688' ,'310788' ,'310888' ,'310988' ,'311088' ,'312088' ,'313088' ,'314088' ,'320988' ,'322988' ,'350988' ,'351988' ,'370988' ,'371988' ,'391988' ,'400988' ,'401988' ,'402988' ,'409999_REF' ,'420988' ,'420999_REF' ,'421988' ,'422988' ,'440188' ,'440288' ,'440388' ,'440488' ,'440588' ,'440688' ,'440788' ,'440888' ,'449988' ,'450888' ,'450988' ,'451988' ,'452788' ,'452888' ,'452988' ,'468988' ,'469988' ,'471988' ,'499988'
     ) THEN 'GP1' ELSE 'Others' end as GP1Accounts,
    SpecialDeal_ID,
    Region_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
    Category,
    (Amount_LCY_Original - Amount_LCY_Original_Prev) AS Amount_LCY_Original,
    ROUND(
     
       CAST((Amount_LCY_Original - Amount_LCY_Original_Prev) * 1 / fxr.Period_FX_rate AS  DECIMAL(18, 2)
      ),
      3
    ) AS Amount_LCY_in_Euro
  from
    cta
    LEFT JOIN (
      select
        *
      from
        FX
      where
        (
          Scenario LIKE '%ACT%' 
        )
    ) AS fxr ON Currency_ID = fxr.Currency
    AND CONCAT(Scenario_ID ,CAST(CTA.Period AS STRING)) = CONCAT(fxr.Scenario , fxr.Period)
)
select act.*, coalesce(vendor.Vendor_Name,act.Vendor_ID)  as Vendor_Name
from act
left join vendor on act.Vendor_ID = vendor.Vendor_ID
where act.Amount_LCY_Original <>0

""")
