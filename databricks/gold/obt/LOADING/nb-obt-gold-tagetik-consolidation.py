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
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace VIEW tagetik_consolidation AS
# MAGIC
# MAGIC with FX AS(
# MAGIC   /****** FX Rate  ******/
# MAGIC     SELECT
# MAGIC     distinct COD_SCENARIO as Scenario,
# MAGIC     COD_PERIODO as Period,case
# MAGIC       when COD_PERIODO between '10'
# MAGIC       and '12' then LEFT(COD_SCENARIO, 4)
# MAGIC       else cast(LEFT(COD_SCENARIO, 4) as int) -1
# MAGIC     end as Calendar_Year,case
# MAGIC       when COD_PERIODO between '01'
# MAGIC       and '09' then right(CONCAT('0' , CAST((COD_PERIODO + 3) AS INT)),2)
# MAGIC       when COD_PERIODO between '10'
# MAGIC       and '12' then right(CONCAT('0' , CAST((COD_PERIODO - 9) AS INT)),2)
# MAGIC     end as Month,
# MAGIC     COD_VALUTA as Currency,
# MAGIC     CAST(CAMBIO_PERIODO AS decimal(18, 4)) as Period_FX_rate
# MAGIC   FROM
# MAGIC     silver_dev.tag02.dati_cambio
# MAGIC   where
# MAGIC     LEFT(COD_SCENARIO, 4) RLIKE '[0-9]'
# MAGIC     and (
# MAGIC       COD_SCENARIO LIKE '%ACT%'
# MAGIC       or COD_SCENARIO LIKE '%BUD%'
# MAGIC       or COD_SCENARIO LIKE '%FC%'
# MAGIC     )
# MAGIC     AND COD_SCENARIO not like '%AUD%'
# MAGIC     AND COD_SCENARIO not like '%OB%'
# MAGIC     AND Sys_Silver_IsCurrent =1
# MAGIC ),
# MAGIC cte as(
# MAGIC   SELECT
# MAGIC     fb.COD_PERIODO AS Period,
# MAGIC     COD_VALUTA AS Currency_ID,
# MAGIC     COD_CONTO AS Account_ID,
# MAGIC     COD_DEST2 AS RPTRegion_ID,
# MAGIC     COD_DEST4 AS SpecialDeal_ID,CASE
# MAGIC       WHEN COD_DEST1 IS NULL
# MAGIC       OR COD_DEST1 = ''
# MAGIC       OR COD_DEST1 = 'NULL'
# MAGIC       OR COD_DEST1 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST1
# MAGIC     END AS Vendor_ID,
# MAGIC     fb.COD_SCENARIO AS Scenario_ID,
# MAGIC     COD_AZIENDA AS Entity_ID,
# MAGIC     CAST( sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
# MAGIC   FROM
# MAGIC     silver_dev.tag02.dati_saldi_lordi FB
# MAGIC   WHERE
# MAGIC     (COD_SCENARIO like '%ACT%')
# MAGIC     and (COD_SCENARIO not like '%ACT-PFA-04')
# MAGIC     AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4'))
# MAGIC     AND Sys_Silver_IsCurrent = 1
# MAGIC   group by
# MAGIC     COD_PERIODO,
# MAGIC     COD_VALUTA,
# MAGIC     COD_CONTO,
# MAGIC     COD_DEST2,
# MAGIC     COD_SCENARIO,
# MAGIC     COD_AZIENDA,CASE
# MAGIC       WHEN COD_DEST1 IS NULL
# MAGIC       OR COD_DEST1 = ''
# MAGIC       OR COD_DEST1 = 'NULL'
# MAGIC       OR COD_DEST1 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST1
# MAGIC     END,
# MAGIC     COD_DEST4
# MAGIC ),
# MAGIC cte_2 as (
# MAGIC   select
# MAGIC     *,
# MAGIC     MAX(Period) OVER (
# MAGIC       Partition by Currency_ID,
# MAGIC       Account_ID,
# MAGIC       SpecialDeal_ID,
# MAGIC       RPTRegion_ID,
# MAGIC       Vendor_ID,
# MAGIC       Scenario_ID,
# MAGIC       Entity_ID
# MAGIC     ) as maxMonth
# MAGIC   FROM
# MAGIC     cte as base --where  Account_ID ='309988'
# MAGIC ),
# MAGIC cte_3 as(
# MAGIC   select
# MAGIC     '12' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     RPTRegion_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = 11
# MAGIC     and Period = '11'
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     RPTRegion_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Amount_LCY_Original
# MAGIC   from
# MAGIC     cte
# MAGIC ),
# MAGIC cta as(
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     LAG(Amount_LCY_Original, 1, 0) OVER(
# MAGIC       PARTITION BY Currency_ID,
# MAGIC       Account_ID,
# MAGIC       SpecialDeal_ID,
# MAGIC       RPTRegion_ID,
# MAGIC       Vendor_ID,
# MAGIC       Scenario_ID,
# MAGIC       Entity_ID
# MAGIC       ORDER BY
# MAGIC         Period ASC
# MAGIC     ) AS Amount_LCY_Original_Prev
# MAGIC   FROM
# MAGIC     cte_3
# MAGIC ),
# MAGIC act as(
# MAGIC   select
# MAGIC     case
# MAGIC       when CTA.Period between '01'
# MAGIC       and '09' then DATEADD(
# MAGIC         YEAR,
# MAGIC         -1,( DATEADD(month, 3,CONCAT( LEFT(Scenario_ID, 4) , '-' , CAST(CTA.Period AS STRING),'-01') )
# MAGIC         )
# MAGIC       )
# MAGIC       when CTA.Period between '10'
# MAGIC       and '12' then  DATEADD(month, -9,CONCAT( LEFT(Scenario_ID, 4) , '-' , CAST(CTA.Period AS STRING),'-01') )
# MAGIC     end AS Date_ID ,
# MAGIC     cta.Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     case when Account_ID IN(
# MAGIC             '309988'
# MAGIC             ,'310188'
# MAGIC             ,'310288'
# MAGIC             ,'310388'
# MAGIC             ,'310488'
# MAGIC             ,'310588'
# MAGIC             ,'310688'
# MAGIC             ,'310788'
# MAGIC             ,'310888'
# MAGIC             ,'310988'
# MAGIC             ,'320988'
# MAGIC             -- ,'350988'
# MAGIC             -- ,'351988'
# MAGIC             ,'370988'
# MAGIC             ,'371988'
# MAGIC             ,'391988')
# MAGIC      THEN 'TotalRevenue' ELSE 'Others' end as RevenueAccounts,
# MAGIC     SpecialDeal_ID,
# MAGIC     RPTRegion_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     (Amount_LCY_Original - Amount_LCY_Original_Prev) AS Amount_LCY_Original,
# MAGIC     ROUND(
# MAGIC      
# MAGIC        CAST((Amount_LCY_Original - Amount_LCY_Original_Prev) * 1 / fxr.Period_FX_rate AS  DECIMAL(18, 2)
# MAGIC       ),
# MAGIC       3
# MAGIC     ) AS Amount_LCY_in_Euro
# MAGIC   from
# MAGIC     cta
# MAGIC     LEFT JOIN (
# MAGIC       select
# MAGIC         *
# MAGIC       from
# MAGIC         FX
# MAGIC       where
# MAGIC         (
# MAGIC           Scenario LIKE '%ACT%' 
# MAGIC         )
# MAGIC     ) AS fxr ON Currency_ID = fxr.Currency
# MAGIC     AND CONCAT(Scenario_ID ,CAST(CTA.Period AS STRING)) = CONCAT(fxr.Scenario , fxr.Period)
# MAGIC )
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM 
# MAGIC act
# MAGIC
# MAGIC
