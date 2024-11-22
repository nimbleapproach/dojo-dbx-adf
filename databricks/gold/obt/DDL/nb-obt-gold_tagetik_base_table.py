# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

spark.conf.set("tableObject.environment", ENVIRONMENT)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Use SCHEMA obt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW tagetik_base_table AS
# MAGIC with FX AS(
# MAGIC   /****** FX Rate  ******/
# MAGIC   --[(21.02.2024) yz: Adjust FX from gold exchange rate table]
# MAGIC   select
# MAGIC     distinct Scenario,
# MAGIC     Period,
# MAGIC     Month,
# MAGIC     Currency,
# MAGIC     Period_FX_rate
# MAGIC   from
# MAGIC     gold_prod.obt.exchange_rate
# MAGIC ),
# MAGIC --[(17.02.2024) yz: Tagetik consolidation Vendor]
# MAGIC --[(29.02.2024) yz: Take vendor from tagetik vendor table]
# MAGIC vendor as (
# MAGIC   select
# MAGIC     COD_DEST1 as Vendor_ID,
# MAGIC     DESC_DEST10 as Vendor_Name
# MAGIC   from
# MAGIC     silver_prod.tag02.dest1
# MAGIC   where
# MAGIC     Sys_Silver_IsCurrent = 1
# MAGIC ),
# MAGIC -- base table with original entity amounts
# MAGIC base as(
# MAGIC   SELECT
# MAGIC     fb.COD_PERIODO AS Period,
# MAGIC     COD_VALUTA AS Currency_ID,
# MAGIC     COD_CONTO AS Account_ID,
# MAGIC     COD_DEST2 AS Region_ID,
# MAGIC     COD_DEST4 AS SpecialDeal_ID,
# MAGIC     CASE
# MAGIC       WHEN COD_DEST1 IS NULL
# MAGIC       OR COD_DEST1 = ''
# MAGIC       OR COD_DEST1 = 'NULL'
# MAGIC       OR COD_DEST1 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST1
# MAGIC     END AS Vendor_ID,
# MAGIC     CASE
# MAGIC       WHEN COD_DEST3 IS NULL
# MAGIC       OR COD_DEST3 = ''
# MAGIC       OR COD_DEST3 = 'NULL'
# MAGIC       OR COD_DEST3 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST3
# MAGIC     END AS CostCenter_ID,
# MAGIC     fb.COD_SCENARIO AS Scenario_ID,
# MAGIC     COD_AZIENDA AS Entity_ID,
# MAGIC     COD_CATEGORIA as Category,
# MAGIC     CAST(sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
# MAGIC   FROM
# MAGIC     silver_prod.tag02.dati_saldi_lordi FB
# MAGIC   WHERE
# MAGIC     (COD_SCENARIO like '%ACT%')
# MAGIC     and (COD_SCENARIO like '%04')
# MAGIC     and (COD_SCENARIO not like '%OB%')
# MAGIC     AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4')) -- [yz] 2024.02.21 include all categories for finance report
# MAGIC     AND (
# MAGIC       COD_CATEGORIA like "%AMOUNT"
# MAGIC       or COD_CATEGORIA in (
# MAGIC         'ADJ01',
# MAGIC         'ADJ02',
# MAGIC         'ADJ03',
# MAGIC         'CF_GROUP',
# MAGIC         'CF_LOCAL',
# MAGIC         'CF_TDA',
# MAGIC         'INP_HQ03',
# MAGIC         'INP_HQ04',
# MAGIC         'INP_HQ05',
# MAGIC         'INP_HQ06',
# MAGIC         'INP_HQ07',
# MAGIC         'INP_MSP2018',
# MAGIC         'SYN2'
# MAGIC       )
# MAGIC     )
# MAGIC     AND Sys_Silver_IsCurrent = 1
# MAGIC     AND (
# MAGIC       Sys_Silver_IsDeleted = 0
# MAGIC       or Sys_Silver_IsDeleted is null
# MAGIC     )
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC adjustments as(
# MAGIC   SELECT
# MAGIC     fb.COD_PERIODO AS Period,
# MAGIC     COD_VALUTA AS Currency_ID,
# MAGIC     COD_CONTO AS Account_ID,
# MAGIC     COD_DEST2 AS Region_ID,
# MAGIC     COD_DEST4 AS SpecialDeal_ID,
# MAGIC     CASE
# MAGIC       WHEN COD_DEST1 IS NULL
# MAGIC       OR COD_DEST1 = ''
# MAGIC       OR COD_DEST1 = 'NULL'
# MAGIC       OR COD_DEST1 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST1
# MAGIC     END AS Vendor_ID,
# MAGIC     CASE
# MAGIC       WHEN COD_DEST3 IS NULL
# MAGIC       OR COD_DEST3 = ''
# MAGIC       OR COD_DEST3 = 'NULL'
# MAGIC       OR COD_DEST3 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST3
# MAGIC     END AS CostCenter_ID,
# MAGIC     fb.COD_SCENARIO AS Scenario_ID,
# MAGIC     COD_AZIENDA AS Entity_ID,
# MAGIC     COD_CATEGORIA AS Category,
# MAGIC     CAST(sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
# MAGIC   FROM
# MAGIC     silver_prod.tag02.dati_rett_riga FB
# MAGIC   WHERE
# MAGIC     (COD_SCENARIO like '%ACT%')
# MAGIC     and (COD_SCENARIO like '%04')
# MAGIC     and (COD_SCENARIO not like '%OB%')
# MAGIC     AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4')) -- [yz] 2024.02.21 include all manual journals categories for finance report
# MAGIC     AND COD_CATEGORIA like '%ADJ%'
# MAGIC     AND Sys_Silver_IsCurrent = 1
# MAGIC     AND (
# MAGIC       Sys_Silver_IsDeleted = 0
# MAGIC       or Sys_Silver_IsDeleted is null
# MAGIC     )
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC cte as(
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     base
# MAGIC   union all
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     adjustments
# MAGIC ),
# MAGIC cte_2 as (
# MAGIC   select
# MAGIC     *,
# MAGIC     MAX(Period) OVER (
# MAGIC       Partition by Currency_ID,
# MAGIC       Account_ID,
# MAGIC       SpecialDeal_ID,
# MAGIC       Region_ID,
# MAGIC       Vendor_ID,
# MAGIC       CostCenter_ID,
# MAGIC       Scenario_ID,
# MAGIC       Entity_ID,
# MAGIC       Category
# MAGIC     ) as maxMonth
# MAGIC   FROM
# MAGIC     cte as base
# MAGIC ),
# MAGIC cte_3 as(
# MAGIC   select
# MAGIC     '12' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '11'
# MAGIC     and Period = '11'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '11' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '10'
# MAGIC     and Period = '10'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '10' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '09'
# MAGIC     and Period = '09'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '09' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '08'
# MAGIC     and Period = '08'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '08' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '07'
# MAGIC     and Period = '07'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '07' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '06'
# MAGIC     and Period = '06'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '06' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '05'
# MAGIC     and Period = '05'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '05' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '04'
# MAGIC     and Period = '04'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '04' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '03'
# MAGIC     and Period = '03'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '03' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '02'
# MAGIC     and Period = '02'
# MAGIC   UNION ALL
# MAGIC   select
# MAGIC     '02' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '01'
# MAGIC     and Period = '01'
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
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
# MAGIC       Region_ID,
# MAGIC       Vendor_ID,
# MAGIC       CostCenter_ID,
# MAGIC       Scenario_ID,
# MAGIC       Entity_ID,
# MAGIC       Category
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
# MAGIC         -1,(
# MAGIC           DATEADD(
# MAGIC             month,
# MAGIC             3,
# MAGIC             CONCAT(
# MAGIC               LEFT(Scenario_ID, 4),
# MAGIC               '-',
# MAGIC               CAST(CTA.Period AS STRING),
# MAGIC               '-01'
# MAGIC             )
# MAGIC           )
# MAGIC         )
# MAGIC       )
# MAGIC       when CTA.Period between '10'
# MAGIC       and '12' then DATEADD(
# MAGIC         month,
# MAGIC         -9,
# MAGIC         CONCAT(
# MAGIC           LEFT(Scenario_ID, 4),
# MAGIC           '-',
# MAGIC           CAST(CTA.Period AS STRING),
# MAGIC           '-01'
# MAGIC         )
# MAGIC       )
# MAGIC     end AS Date_ID,
# MAGIC     cta.Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     CostCenter_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC     (Amount_LCY_Original - Amount_LCY_Original_Prev) AS Amount_LCY_Original,
# MAGIC     ROUND(
# MAGIC       CAST(
# MAGIC         (Amount_LCY_Original - Amount_LCY_Original_Prev) * 1 / fxr.Period_FX_rate AS DECIMAL(18, 2)
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
# MAGIC         (Scenario LIKE '%ACT%')
# MAGIC     ) AS fxr ON Currency_ID = fxr.Currency
# MAGIC     AND CONCAT(Scenario_ID, CAST(CTA.Period AS STRING)) = CONCAT(fxr.Scenario, left(fxr.Period, 2))
# MAGIC )
# MAGIC select
# MAGIC   act.*
# MAGIC from
# MAGIC   act
# MAGIC where
# MAGIC   act.Amount_LCY_Original <> 0
