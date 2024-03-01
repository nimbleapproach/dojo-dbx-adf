# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Original Date [29/02/2024]\
# MAGIC Created BY [YZ]\
# MAGIC Create a view to capture all postage entries

# COMMAND ----------

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
# MAGIC CREATE OR REPLACE VIEW gold_{ENVIRONMENT}.obt.postage AS 
# MAGIC with FX AS(
# MAGIC   /****** FX Rate  ******/
# MAGIC
# MAGIC --[(21.02.2024) yz: Adjust FX from gold exchange rate table]
# MAGIC select distinct
# MAGIC Scenario,
# MAGIC Period,
# MAGIC Month,
# MAGIC Currency,
# MAGIC Period_FX_rate
# MAGIC  from gold_{ENVIRONMENT}.obt.exchange_rate
# MAGIC  ),
# MAGIC
# MAGIC   base as(
# MAGIC   SELECT
# MAGIC     fb.COD_PERIODO AS Period,
# MAGIC     COD_VALUTA AS Currency_ID,
# MAGIC     COD_CONTO AS Account_ID,
# MAGIC     COD_DEST2 AS Region_ID,
# MAGIC     COD_DEST4 AS SpecialDeal_ID,CASE
# MAGIC       WHEN COD_DEST1 IS NULL
# MAGIC       OR COD_DEST1 = ''
# MAGIC       OR COD_DEST1 = 'NULL'
# MAGIC       OR COD_DEST1 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST1
# MAGIC     END AS Vendor_ID,
# MAGIC     fb.COD_SCENARIO AS Scenario_ID,
# MAGIC     COD_AZIENDA AS Entity_ID,
# MAGIC     COD_CATEGORIA as Category,
# MAGIC     CAST( sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
# MAGIC   FROM
# MAGIC     silver_{ENVIRONMENT}.tag02.dati_saldi_lordi FB
# MAGIC   WHERE
# MAGIC     (COD_SCENARIO like '%ACT%')
# MAGIC     and (COD_SCENARIO  like '%04')
# MAGIC     and (COD_SCENARIO not like '%OB%')
# MAGIC     AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4','5','6','7','8'))
# MAGIC  -- [yz] 2024.02.21 include all categories for finance report 
# MAGIC     AND (COD_CATEGORIA  like"%AMOUNT" or 
# MAGIC COD_CATEGORIA in (
# MAGIC 'ADJ01'
# MAGIC ,'ADJ02'
# MAGIC ,'ADJ03'
# MAGIC ,'CF_GROUP'
# MAGIC ,'CF_LOCAL'
# MAGIC ,'CF_TDA'
# MAGIC ,'INP_HQ03'
# MAGIC ,'INP_HQ04'
# MAGIC ,'INP_HQ05'
# MAGIC ,'INP_HQ06'
# MAGIC ,'INP_HQ07'
# MAGIC ,'INP_MSP2018'
# MAGIC ,'SYN2'
# MAGIC  ) )
# MAGIC     AND Sys_Silver_IsCurrent = 1
# MAGIC     AND ( Sys_Silver_IsDeleted =0 or Sys_Silver_IsDeleted is null)
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
# MAGIC     COD_DEST4,
# MAGIC     COD_CATEGORIA
# MAGIC ),
# MAGIC adjustments as(
# MAGIC   SELECT
# MAGIC     fb.COD_PERIODO AS Period,
# MAGIC     COD_VALUTA AS Currency_ID,
# MAGIC     COD_CONTO AS Account_ID,
# MAGIC     COD_DEST2 AS Region_ID,
# MAGIC     COD_DEST4 AS SpecialDeal_ID,CASE
# MAGIC       WHEN COD_DEST1 IS NULL
# MAGIC       OR COD_DEST1 = ''
# MAGIC       OR COD_DEST1 = 'NULL'
# MAGIC       OR COD_DEST1 IS NULL THEN 'N/A'
# MAGIC       ELSE COD_DEST1
# MAGIC     END AS Vendor_ID,
# MAGIC     fb.COD_SCENARIO AS Scenario_ID,
# MAGIC     COD_AZIENDA AS Entity_ID,
# MAGIC     COD_CATEGORIA AS Category,
# MAGIC     CAST( sum(IMPORTO) * (-1) AS DECIMAL(18, 2)) AS Amount_LCY_Original
# MAGIC   FROM
# MAGIC     silver_{ENVIRONMENT}.tag02.dati_rett_riga FB
# MAGIC   WHERE
# MAGIC     (COD_SCENARIO like '%ACT%')
# MAGIC     and (COD_SCENARIO  like '%04')
# MAGIC     and (COD_SCENARIO not like '%OB%')
# MAGIC     AND (LEFT(FB.COD_CONTO, 1) IN ('3', '4','5','6','7','8'))
# MAGIC -- [yz] 2024.02.21 include all manual journals categories for finance report 
# MAGIC     AND COD_CATEGORIA like '%ADJ%'
# MAGIC     AND Sys_Silver_IsCurrent = 1
# MAGIC     AND ( Sys_Silver_IsDeleted =0 or Sys_Silver_IsDeleted is null)
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
# MAGIC     COD_DEST4,
# MAGIC     COD_CATEGORIA
# MAGIC ),
# MAGIC cte as(
# MAGIC   select *from base
# MAGIC   union all
# MAGIC   select * from adjustments
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
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '11'
# MAGIC     and Period = '11'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '11' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '10'
# MAGIC     and Period = '10'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '10' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '09'
# MAGIC     and Period = '09'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '09' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '08'
# MAGIC     and Period = '08'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '08' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '07'
# MAGIC     and Period = '07'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '07' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '06'
# MAGIC     and Period = '06'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '06' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '05'
# MAGIC     and Period = '05'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '05' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '04'
# MAGIC     and Period = '04'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '04' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '03'
# MAGIC     and Period = '03'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '03' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '02'
# MAGIC     and Period = '02'
# MAGIC   UNION ALL
# MAGIC     select
# MAGIC     '02' AS Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
# MAGIC      0 AS Amount_LCY_Original
# MAGIC   from
# MAGIC     cte_2
# MAGIC   where
# MAGIC     maxMonth = '01'
# MAGIC     and Period = '01'
# MAGIC UNION ALL
# MAGIC   SELECT
# MAGIC     Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
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
# MAGIC         -1,( DATEADD(month, 3,CONCAT( LEFT(Scenario_ID, 4) , '-' , CAST(CTA.Period AS STRING),'-01') )
# MAGIC         )
# MAGIC       )
# MAGIC       when CTA.Period between '10'
# MAGIC       and '12' then  DATEADD(month, -9,CONCAT( LEFT(Scenario_ID, 4) , '-' , CAST(CTA.Period AS STRING),'-01') )
# MAGIC     end AS Date_ID ,
# MAGIC     cta.Period,
# MAGIC     Currency_ID,
# MAGIC     Account_ID,
# MAGIC     SpecialDeal_ID,
# MAGIC     Region_ID,
# MAGIC     Vendor_ID,
# MAGIC     Scenario_ID,
# MAGIC     Entity_ID,
# MAGIC     Category,
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
# MAGIC select 
# MAGIC case when Region_ID = 'AT1' THEN Region_ID ELSE act.Entity_ID END AS Entity_ID,
# MAGIC YEAR (ACT.Date_ID)AS Year,
# MAGIC month (ACT.Date_ID)AS Month,
# MAGIC Date_ID,
# MAGIC act.Account_ID,
# MAGIC coa.Account_Desc,
# MAGIC SUM(Amount_LCY_in_Euro) Amount_EUR
# MAGIC from act
# MAGIC
# MAGIC left join gold_{ENVIRONMENT}.obt.chart_of_accounts as coa
# MAGIC on act.Account_ID = coa.Account_ID
# MAGIC
# MAGIC where act.Amount_LCY_Original <>0
# MAGIC and coa.Level08_Desc ='Gross profit'
# MAGIC AND ACT.Category like '%AMOUNT%'
# MAGIC and act.Account_ID IN (
# MAGIC '480988'
# MAGIC ,'481988'
# MAGIC ,'551188'
# MAGIC ,'551288'
# MAGIC ,'552988'
# MAGIC )
# MAGIC GROUP BY ALL
# MAGIC

# COMMAND ----------

df = spark.sql(f"""
 SELECT
    P.NodeCode,
    P.ParentNodeCode

  FROM
    (
      SELECT
        COD_CONTO_GERARCHIA_PADRE as HierarchyCode,
        COD_CONTO_ELEGER_PADRE as ParentNodeCode,
        COD_CONTO_ELEGER as NodeCode,
        DESC_CONTO_ELEGER0 as AccountName,
        ORDINAMENTO as Sorting
      FROM
        silver_{ENVIRONMENT}.tag02.conto_gerarchia 
      WHERE
        COD_CONTO_GERARCHIA_PADRE = '01'
        and  Sys_Silver_IsCurrent = 1
    )p
""")

# COMMAND ----------

import pandas as pd

df = df.toPandas()
 
relations = dict(zip(df['NodeCode'],df['ParentNodeCode']))

def get_parent_list(element):
    parent = relations.get(element)
    return get_parent_list(parent) + [parent] if parent else []

all_relations = {
    children: {f'level_{idx}': value for idx, value in enumerate(get_parent_list(children))}
    for children in set(df['NodeCode'])
}

df_flat = pd.DataFrame.from_dict(all_relations, orient='index')


# COMMAND ----------

# df_flat.reset_index().where(col("level_1") =='P_L')
df_flat = df_flat[df_flat["level_1"].isin(["P_L"])].reset_index().fillna(0)

# COMMAND ----------

import numpy as np

for index, row in df_flat.iterrows():

    if 0 in list(row): 
        print('fill {} at column {}'.format(row.iloc[0] , list(row).index(0))) 
        df_flat.iloc[index, list(row).index(0)]= row.iloc[0]

    else: 
        print("not exist")
    


# COMMAND ----------

spark.createDataFrame(df_flat).createOrReplaceTempView('Levels')

# COMMAND ----------


df_coa = spark.sql(f"""

with flattend as 
(select 
index,
level_2 as Level01,
 level_3 as Level02,
 level_4 as Level03,
 level_5 as Level04,
 level_6 as Level05,
 level_7 as Level06,
 level_8 as Level07,
 level_9 as Level08,
 level_10 as Level09,
 level_11 as Level10,
case when level_11 <> '0' and index <> level_11 then index else 0 end as Level11
from Levels),

Level_Description as (
  select
     COD_CONTO_ELEGER AS Account,
    DESC_CONTO_ELEGER0 AS AccountName 
  from
    silver_{ENVIRONMENT}.tag02.conto_gerarchia
  where
    COD_CONTO_GERARCHIA = '01'
),
structure as(
  SELECT
    index as AccountR_ID,
    Level01,
    acc1.AccountName as Level01_Desc,
    Level02,
    acc2.AccountName as Level02_Desc,
    Level03,
    acc3.AccountName as Level03_Desc,
    Level04,
    acc4.AccountName as Level04_Desc,
    Level05,
    acc5.AccountName as Level05_Desc,
    Level06,
    acc6.AccountName as Level06_Desc,
    Level07,
    acc7.AccountName as Level07_Desc,
    Level08,
    acc8.AccountName as Level08_Desc,
    Level09,
    acc9.AccountName as Level09_Desc,
    Level10,
    acc10.AccountName as Level10_Desc,
    Level11,
    acc11.AccountName as Level11_Desc
--     Level12,
--     acc12.AccountName as Level12_Desc,
--     Level13,
--     acc13.AccountName as Level13_Desc,
--     Level14,
--     acc14.AccountName as Level14_Desc,
--     Level15,
--     acc15.AccountName as Level15_Desc
  FROM
    flattend as base
--     LEFT JOIN LEVEL_Description as acc ON base.NodeCode = acc.Account
    LEFT JOIN LEVEL_Description as acc1 ON base.Level01 = acc1.Account
    LEFT JOIN LEVEL_Description as acc2 ON base.Level02 = acc2.Account
    LEFT JOIN LEVEL_Description as acc3 ON base.Level03 = acc3.Account
    LEFT JOIN LEVEL_Description as acc4 ON base.Level04 = acc4.Account
    LEFT JOIN LEVEL_Description as acc5 ON base.Level05 = acc5.Account
    LEFT JOIN LEVEL_Description as acc6 ON base.Level06 = acc6.Account
    LEFT JOIN LEVEL_Description as acc7 ON base.Level07 = acc7.Account
    LEFT JOIN LEVEL_Description as acc8 ON base.Level08 = acc8.Account
    LEFT JOIN LEVEL_Description as acc9 ON base.Level09 = acc9.Account
    LEFT JOIN LEVEL_Description as acc10 ON base.Level10 = acc10.Account
    LEFT JOIN LEVEL_Description as acc11 ON base.Level11 = acc11.Account
--     LEFT JOIN LEVEL_Description as acc12 ON base.Level12 = acc12.Account
--     LEFT JOIN LEVEL_Description as acc13 ON base.Level13 = acc13.Account
--     LEFT JOIN LEVEL_Description as acc14 ON base.Level14 = acc14.Account
--     LEFT JOIN LEVEL_Description as acc15 ON base.Level15 = acc15.Account
)
select
  distinct r.COD_CONTO as Account_ID,
  r.DESC_CONTO0 as Account_Desc,
  AccountR_ID,
--   AccountR_Desc,
--   Account_Sort,
  Level01,
  Level01_Desc,
  Level02,
  Level02_Desc,
  Level03,
  Level03_Desc,
  Level04,
  Level04_Desc,
  Level05,
  Level05_Desc,
  Level06,
  Level06_Desc,
  Level07,
  Level07_Desc,
  Level08,
  Level08_Desc,
  Level09,
  Level09_Desc,
  Level10,
  Level10_Desc,
  Level11,
  Level11_Desc
--   Level12,
--   Level12_Desc,
--   Level13,
--   Level13_Desc,
--   Level14,
--   Level14_Desc,
--   Level15,
--   Level15_Desc
from
  structure
  inner join (
    select
      a.COD_CONTO,
      b.DESC_CONTO0,
      a.COD_CONTO_ELEGER
    from
      silver_{ENVIRONMENT}.tag02.conto_gerarchia_ABBI a
      inner join silver_{ENVIRONMENT}.tag02.conto b on a.COD_CONTO = b.COD_CONTO
      and a.Sys_Silver_IsCurrent = 1
      and b.Sys_Silver_IsCurrent = 1
    where
      COD_CONTO_GERARCHIA = '01'
    group by
      a.COD_CONTO,
      b.DESC_CONTO0,
      a.COD_CONTO_ELEGER
  ) r on structure.AccountR_ID = r.COD_CONTO_ELEGER
""")      


# COMMAND ----------

df_coa.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("chart_of_accounts")
