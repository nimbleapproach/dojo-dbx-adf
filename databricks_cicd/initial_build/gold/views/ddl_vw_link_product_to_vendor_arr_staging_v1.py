# Databricks notebook source
# Importing Libraries
import os

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

# REMOVE ONCE SOLUTION IS LIVE
if ENVIRONMENT == 'dev':
    spark.sql(f"""
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_link_product_to_vendor_arr_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_link_product_to_vendor_arr_staging as
with cte as (
  select row_number() over (partition by lower((concat(it.Vendor_Name, '|', it.sku)))
  order by it.Sys_Bronze_InsertDateTime_UTC desc) latest_rn
  ,* 
  from silver_dev.masterdata.datanowarr it
 WHERE
  it.Sys_Silver_IsCurrent = true
  )
select distinct
concat(it.Vendor_Name,'|',it.sku) as product_vendor_code,
coalesce(it.sku,'NaN') as product_code ,
coalesce(p.product_pk,-1) product_fk, 
coalesce(v.vendor_pk,-1) vendor_fk, 
coalesce(it.Vendor_Name,'NaN') as vendor_code ,
coalesce(it.Product_Type,'NaN') as product_type,
Commitment_Duration_in_months AS commitment_duration_in_months,
Commitment_Duration2 AS commitment_duration2,
Billing_Frequency AS billing_frequency,
Billing_Frequency2 AS billing_frequency2,
Consumption_Model AS consumption_model,
-- Mapping_Type_Duration AS mapping_type_duration,
-- Mapping_Type_Billing AS mapping_type_billing,
-- null AS mrr_ratio,
Commitment_Duration_Value AS commitment_duration_value,
-- Billing_Frequency_Value2 AS billing_frequency_value2,
  ss.source_system_pk as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
'item' as sys_item_source,
    it.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_InsertedDateTime_UTC,
    it.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_ModifiedDateTime_UTC
FROM cte it 
  cross join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Managed Datasets' and is_current = 1) ss 
LEFT OUTER JOIN {catalog}.{schema}.dim_product p on p.product_code =coalesce(it.sku,'NaN') and p.is_current = 1
LEFT OUTER JOIN {catalog}.{schema}.dim_vendor v on v.vendor_code = coalesce(it.Vendor_Name,'NaN') and v.is_current = 1
WHERE 
latest_rn =1
""")
