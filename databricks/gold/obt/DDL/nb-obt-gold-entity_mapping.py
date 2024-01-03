# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

# MAGIC %python
# MAGIC spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

spark.sql(
  f"""
  CREATE OR REPLACE VIEW obt.entity_mapping (
  SourceSystemName,
  SourceEntityCode,
  TagetikEntityCode,
  ResellerMasterEntity COMMENT 'The entity of the reseller.')
COMMENT 'The entity_mapping table contains information on the mapping of entities between the source system and Tagetik. It includes the SourceSystemName, SourceEntityCode, and TagetikEntityCode columns to identify the entities being mapped. The ResellerMasterEntity column specifies the entity of the reseller. This table is important for ensuring accurate financial reporting and analysis across systems.'
AS WITH SourceEntity AS (
  SELECT
    DISTINCT 'NAV' AS SourceSystemName,
    right(Sys_DatabaseName, 2) AS SourceEntityCode,
    '' as EntityName
  FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_line
  union ALL
  select
    distinct 'NUV' AS SourceSystemName,
    LEGALENTITYID AS SourceEntityCode,
    NAME as EntityName
  from
    silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_omlegalstaging
),
TagEntity AS (
  select
    distinct COD_AZIENDA,
    DESC_AZIENDA0
  FROM
    silver_{ENVIRONMENT}.tag02.azienda
  WHERE
    Sys_Silver_IsCurrent = 1
)
SELECT
  SourceEntity.SourceSystemName,
  SourceEntity.SourceEntityCode,
  -- SourceEntity.EntityName,
  CASE
    when SourceEntity.SourceEntityCode = 'NPO1' then 'PL1'
    when SourceEntity.SourceEntityCode = 'NIT1' then 'IT1'
    when SourceEntity.SourceEntityCode = 'NAU1' then 'AT2'
    when SourceEntity.SourceEntityCode = 'NES1' then 'ES1'
    when SourceEntity.SourceEntityCode = 'NDK1' then 'DK2'
    when SourceEntity.SourceEntityCode = 'NFI1' then 'FI2'
    when SourceEntity.SourceEntityCode = 'dat' then 'NotInTagetik'
    when SourceEntity.SourceEntityCode = 'NNO1' then 'NO2'
    when SourceEntity.SourceEntityCode = 'NNL1' then 'NL2'
    when SourceEntity.SourceEntityCode = 'NSW1' then 'SE2'
    when SourceEntity.SourceEntityCode = 'NME1' then 'NotInTagetik'
    when SourceEntity.SourceEntityCode = 'NME2' then 'NotInTagetik'
    when SourceEntity.SourceEntityCode = 'NME3' then 'NotInTagetik'
    when SourceEntity.SourceEntityCode = 'NME4' then 'NotInTagetik'
    ELSE concat(
      coalesce((TagEntity.COD_AZIENDA), ''),
      coalesce((t2.COD_AZIENDA), '')
    )
  END AS TagetikEntityCode,
  resellergroups.Entity AS ResellerMasterEntity
from
  SourceEntity
  left join TagEntity on case
    when SourceSystemName = 'NAV' THEN concat(SourceEntity.SourceEntityCode, '1')
    ELSE SourceEntity.SourceEntityCode
  END = TagEntity.COD_AZIENDA
  LEFT JOIN TagEntity as t2 on SourceEntity.EntityName = t2.DESC_AZIENDA0
  LEFT JOIN(
    select
      distinct Entity
    FROM
      silver_{ENVIRONMENT}.masterdata.resellergroups
  ) resellergroups on SourceEntity.SourceEntityCode = resellergroups.Entity
  """
)
