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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_link_product_to_vendor_arr_stg2
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_link_product_to_vendor_arr_stg2 AS
WITH cte AS (
  SELECT 
    row_number() OVER (
      PARTITION BY 
        LOWER(
          CONCAT(
            CASE 
              WHEN ID IN (1,2) AND Product IN (
                'D365', 'Azure NCE', 'Software NCE', 'M365 NCE', 'Azure', 'Business Voice', 
                'Power Platform NCE', 'Perpetual Licencing', 'Managed Services', 'D365 NCE', 
                'Windows 365', 'Perpetual Licencing NCE', 'Windows 365 NCE', 'Power Platform', 
                'Copilot NCE ', 'M365', 'Business Voice NCE'
              ) THEN 'Microsoft'
              WHEN ID IN (1,2) AND Vendor ='Microsoft' THEN Product
              ELSE Vendor 
            END, 
            '|', Product
          )
        ) 
      ORDER BY it.Sys_Bronze_InsertDateTime_UTC DESC
    ) AS latest_rn,
    LOWER(
      CONCAT(
        CASE 
          WHEN ID IN (1,2) AND Product IN (
            'D365', 'Azure NCE', 'Software NCE', 'M365 NCE', 'Azure', 'Business Voice', 
            'Power Platform NCE', 'Perpetual Licencing', 'Managed Services', 'D365 NCE', 
            'Windows 365', 'Perpetual Licencing NCE', 'Windows 365 NCE', 'Power Platform', 
            'Copilot NCE ', 'M365', 'Business Voice NCE'
          ) THEN 'Microsoft'
          WHEN ID IN (1,2) AND Vendor ='Microsoft' THEN Product
          ELSE Vendor 
        END, 
        '|', Product
      )
    ) AS product_vendor_code,
    ProductCategory AS ProductTypeMaster,
    LOWER(
      CASE 
        WHEN ID IN (1,2) AND Product IN (
          'D365', 'Azure NCE', 'Software NCE', 'M365 NCE', 'Azure', 'Business Voice', 
          'Power Platform NCE', 'Perpetual Licencing', 'Managed Services', 'D365 NCE', 
          'Windows 365', 'Perpetual Licencing NCE', 'Windows 365 NCE', 'Power Platform', 
          'Copilot NCE ', 'M365', 'Business Voice NCE'
        ) THEN 'Microsoft'
        WHEN ID IN (1,2) AND Vendor ='Microsoft' THEN Product
        ELSE Vendor 
      END
    ) AS vendor_code,
    LOWER(Product) AS product_code,
    ProductCategory,
    Sys_Silver_InsertDateTime_UTC
  FROM silver_{ENVIRONMENT}.vuzion_monthly.vuzion_monthly_revenue it
  WHERE it.Sys_Silver_IsCurrent = TRUE
)
SELECT DISTINCT
  it.product_vendor_code AS product_vendor_code,
  COALESCE(it.product_code,'NaN') AS product_code,
  COALESCE(p.product_pk,-1) AS product_fk, 
  COALESCE(v.vendor_pk,-1) AS vendor_fk, 
  COALESCE(it.vendor_code,'NaN') AS vendor_code,
  COALESCE(it.ProductCategory,'NaN') AS product_type,
  'NaN' AS commitment_duration_in_months,
  'NaN' AS commitment_duration2,
  'NaN' AS billing_frequency,
  'NaN' AS billing_frequency2,
  'NaN' AS consumption_model,
  'NaN' AS commitment_duration_value,
  ss.source_system_pk AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  'vuzion_monthly' AS sys_item_source,
  it.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_InsertedDateTime_UTC,
  it.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_ModifiedDateTime_UTC
FROM cte it
CROSS JOIN (
  SELECT source_system_pk, source_entity 
  FROM {catalog}.{schema}.dim_source_system 
  WHERE source_system = 'Managed Datasets' AND is_current = 1
) ss 
LEFT OUTER JOIN {catalog}.{schema}.dim_product p ON p.product_code = it.product_code AND p.is_current = 1
LEFT OUTER JOIN {catalog}.{schema}.dim_vendor v ON v.vendor_code = COALESCE(it.vendor_code,'NaN') AND v.is_current = 1
WHERE latest_rn=1
AND NOT EXISTS (
  SELECT 1 
  FROM {catalog}.{schema}.link_product_to_vendor_arr x 
  WHERE x.sys_item_source='vuzion_monthly'
  AND x.product_fk=p.product_pk
  AND x.vendor_fk=v.vendor_pk
)
""")
