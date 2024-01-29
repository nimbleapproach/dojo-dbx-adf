# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Gold Transactions Platinum
spark.sql(f"""
          

CREATE OR Replace VIEW tagetik_consolidation AS

with FX AS(
  /****** FX Rate  ******/
    SELECT
    distinct 
    COD_SCENARIO as Scenario,
    COD_PERIODO as Period,case
      when COD_PERIODO between '10'
      and '12' then LEFT(COD_SCENARIO, 4)
      else cast(LEFT(COD_SCENARIO, 4) as int) -1
    end as Calendar_Year,case
      when COD_PERIODO between '01'
      and '09' then right(CONCAT('0' , CAST((COD_PERIODO + 3) AS INT)),2)
      when COD_PERIODO between '10'
      and '12' then right(CONCAT('0' , CAST((COD_PERIODO - 9) AS INT)),2)
    end as Month,
    COD_VALUTA as Currency,
    CAST(CAMBIO_PERIODO AS decimal(18, 4)) as Period_FX_rate
  FROM
    silver_{ENVIRONMENT}.tag02.dati_cambio
  where
    LEFT(COD_SCENARIO, 4) RLIKE '[0-9]'
    and (
      COD_SCENARIO LIKE '%ACT%'

    )
    AND COD_SCENARIO not like '%AUD%'
    AND COD_SCENARIO not like '%OB%'
    AND Sys_Silver_IsCurrent =1
),
cte as(
  SELECT
    fb.COD_PERIODO AS Period,
    COD_VALUTA AS Currency_ID,
    COD_CONTO AS Account_ID,
    COD_DEST2 AS RPTRegion_ID,
    COD_DEST4 AS SpecialDeal_ID,CASE
      WHEN COD_DEST1 IS NULL
      OR COD_DEST1 = ''
      OR COD_DEST1 = 'NULL'
      OR COD_DEST1 IS NULL THEN 'N/A'
      ELSE COD_DEST1
    END AS Vendor_ID,
    fb.COD_SCENARIO AS Scenario_ID,
    COD_AZIENDA AS Entity_ID,
    CAST( sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
  FROM
    silver_{ENVIRONMENT}.tag02.dati_saldi_lordi FB
  WHERE
    (COD_SCENARIO like '%ACT%')
    and (COD_SCENARIO  like '%04')
    and (COD_SCENARIO not like '%OB%')
    AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4'))
    AND COD_CATEGORIA LIKE '%AMOUNT'
    AND Sys_Silver_IsCurrent = 1
    AND Sys_Silver_IsDeleted = 0
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
    COD_DEST4
),
cte_2 as (
  select
    *,
    MAX(Period) OVER (
      Partition by Currency_ID,
      Account_ID,
      SpecialDeal_ID,
      RPTRegion_ID,
      Vendor_ID,
      Scenario_ID,
      Entity_ID
    ) as maxMonth
  FROM
    cte as base --where  Account_ID ='309988'
),
cte_3 as(
  select
    '12' AS Period,
    Currency_ID,
    Account_ID,
    SpecialDeal_ID,
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
      RPTRegion_ID,
      Vendor_ID,
      Scenario_ID,
      Entity_ID
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
    RPTRegion_ID,
    Vendor_ID,
    Scenario_ID,
    Entity_ID,
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
select * from act

""")
