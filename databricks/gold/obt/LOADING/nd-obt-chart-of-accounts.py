# Databricks notebook source
# MAGIC %md
# MAGIC # Create COA Hierarchy

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
# MAGIC CREATE OR REPLACE TABLE chart_of_accounts 
# MAGIC   ( 
# MAGIC   Level01  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level01_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level02  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level02_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level03  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level03_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level04  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level04_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level05  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level05_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level06  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level06_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level07  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level07_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level08  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level08_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level09  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level09_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level10  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level10_Desc  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level11  STRING
# MAGIC       comment 'todo'
# MAGIC   ,Level11_Desc STRING)
# MAGIC   COMMENT 'This table contains the chart of account flattened hierarchy from tagetik. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
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
