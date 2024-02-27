# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------


# Original Date [27/02/2024]
# Created BY [YZ]
# Generate a flattend region hierarcy from three tagetik objects. Grouping entities into their espected region (ex, NO1 DK2, FI1 belong to Nordic as region)

spark.sql(f"""
create or replace view region as 
WITH level_1 as(
  SELECT
    COD_DEST2_ELEGER as Level_1_Code
  FROM
    silver_{ENVIRONMENT}.tag02.dest2_gerarchia
  where
    COD_DEST2_ELEGER_PADRE = 'LS01'
    AND COD_DEST2_GERARCHIA = '03'
     and Sys_Silver_IsCurrent = 1
),
level_2 as (
  SELECT
    COD_DEST2_ELEGER as Level_2_Code,
    COD_DEST2_ELEGER_PADRE as Level_2_join
  FROM
    silver_{ENVIRONMENT}.tag02.dest2_gerarchia
  where
    Sys_Silver_IsCurrent = 1
),
level_3 as (
  SELECT
    distinct COD_DEST2 Level_3_Code,
    COD_DEST2_ELEGER Level_3_join
  FROM
    silver_{ENVIRONMENT}.tag02.dest2_gerarchia_abbi
  where
    Sys_Silver_IsCurrent = 1
    and  COD_DEST2_GERARCHIA='03'
),
region as (
  select
    distinct level12.Level_1_Code,
   level12.Level_2_Code ,
    Level_3_Code,
    Level_3_join
  from
    level_3
    left join (
      select
        distinct level_1.*,
        case
          when Level_2_Code is null then level_1.Level_1_Code
          else Level_2_Code
        end as Level_2_Code
      from
        level_1
        left join level_2 on level_1.Level_1_Code = level_2.Level_2_join
    ) level12 on level_3.Level_3_join = level12.Level_2_Code

)

select 
case when Level_1_Code is null then Level_3_join else Level_1_Code end as RegionCode,
Level_2_Code as CountryCode,
Level_3_Code as RegionID
 from region""")
