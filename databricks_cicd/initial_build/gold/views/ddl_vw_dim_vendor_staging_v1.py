# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_vendor_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_vendor_staging 
AS with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.is_current = 1
),
cte_source_data as 
(
  -- vendors per country
  select distinct
    code AS vendor_code,
    Name AS Vendor_Name_Internal,
    SID as local_vendor_ID,
    replace(Sys_DatabaseName,"Reports","") as Country_Code,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(d.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(d.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
  FROM silver_{ENVIRONMENT}.igsql03.dimension_value d
  LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(d.Sys_DatabaseName,2)) AND s.source_system = 'Infinigate ERP'
  WHERE UPPER(DimensionCode) = 'VENDOR'
  AND Sys_Silver_IsCurrent = true
  GROUP BY ALL
  UNION
  SELECT 
    disit.PrimaryVendorID AS Vendor_Code,
    coalesce(disit.PrimaryVendorName, 'N/A') AS Vendor_Name_Internal,
    MAX(disit.SID) as local_vendor_ID,
    'N/A' as Country_Code,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(disit.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(disit.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC

  FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems disit

  LEFT JOIN cte_sources s on 
      CASE
        WHEN UPPER(s.source_entity) = 'NUK1' THEN 'NGS1'
        WHEN UPPER(s.source_entity) IN ('NPO1','NDK1','NNO1','NAU1','NCH1','NSW1','NFR1','NNL1','NES1','NDE1','NFI1') THEN 'NNL2'
        ELSE UPPER(s.source_entity)
      END 
      = 
      CASE
        WHEN UPPER(disit.CompanyID) = 'NUK1' THEN 'NGS1'
        WHEN UPPER(disit.CompanyID) IN ('NPO1','NDK1','NNO1','NAU1','NCH1','NSW1','NFR1','NNL1','NES1','NDE1','NFI1') THEN 'NNL2'
        ELSE UPPER(disit.CompanyID)
      END
  AND s.source_system = 'Nuvias ERP'
  WHERE disit.Sys_Silver_IsCurrent = 1 
  GROUP BY ALL
  UNION
  -- Netsafe
  SELECT
    case when invoice.Vendor_ID = 'NaN' then UPPER(invoice.Vendor_Name) ELSE invoice.Vendor_ID END AS Vendor_Code,
    'N/A' AS Vendor_Name_Internal, --too many duplicates, vendor_id = 002_A, has load of different vendor names
    case when invoice.Vendor_ID = 'NaN' then UPPER(invoice.Vendor_Name) ELSE coalesce(invoice.Vendor_ID, 'N/A') END AS local_vendor_ID,
    'N/A' as Country_Code,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(invoice.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(invoice.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
  FROM silver_{ENVIRONMENT}.netsafe.invoicedata AS invoice
  LEFT JOIN cte_sources s on CASE
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
    END = s.source_entity
  AND s.source_system = 'Netsafe ERP'
  WHERE invoice.Sys_Silver_IsCurrent = 1
  GROUP BY ALL
  UNION
  -- Starlink Netsuite
  SELECT
    coalesce(ven.Vendor_ID, si.SID) AS Vendor_Code,
    coalesce(ven.Vendor_Name,si.Vendor_name, 'N/A') AS Vendor_Name_Internal,
    ven.sid AS local_vendor_ID, 
    'N/A' as Country_Code,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(si.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(si.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
  FROM silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdataVendor AS ven ON si.Vendor_Name = ven.Vendor_Name
  AND ven.Sys_Silver_IsCurrent is not false
  LEFT JOIN cte_sources s on 'AE1' = s.source_entity AND s.source_system = 'Starlink (Netsuite) ERP'
  WHERE si.Sys_Silver_IsCurrent = 1
  GROUP BY ALL
  UNION ALL
  SELECT DISTINCT 
    vendor_code,
    Vendor_Name_Internal,
    local_vendor_ID,
    Country_Code,
    source_system_fk,
    start_datetime,
    end_datetime,
    is_current,
    Sys_Gold_InsertedDateTime_UTC,
    Sys_Gold_ModifiedDateTime_UTC
  FROM {catalog}.{schema}.vw_dim_vendor_cloudblue_staging
)
SELECT DISTINCT 
  csd.vendor_code,
  csd.Vendor_Name_Internal,
  csd.local_vendor_ID,
  csd.Country_Code,
  csd.source_system_fk,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END as start_datetime,
  csd.end_datetime,
  csd.is_current,
  csd.Sys_Gold_InsertedDateTime_UTC,
  csd.Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN  {catalog}.{schema}.dim_vendor d on d.local_vendor_ID = csd.local_vendor_ID
AND d.source_system_fk = csd.source_system_fk
WHERE csd.vendor_code IS NOT NULL
""")
