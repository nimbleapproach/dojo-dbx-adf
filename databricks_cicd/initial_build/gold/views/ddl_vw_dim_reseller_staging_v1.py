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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_reseller_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_reseller_staging 
AS 
with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  --where s.source_system = 'Infinigate ERP' 
  where s.is_current = 1
),
cte_duplicate_netsafe_reseller as 
(
  select Customer_Account, Sys_Country, MAX(Customer_Name) as Reseller_Name_Internal,count(*) as reseller_count
  from silver_dev.netsafe.invoicedata
  where sys_silver_iscurrent = true
  group by Customer_Account, Sys_Country
  having count(*) > 1
),
--cte_nuvias_sources as 
--(
-- select distinct source_system_pk, data_area_id
--  from {catalog}.{schema}.dim_source_system s 
--  where s.source_system = 'Nuvias ERP' 
--  and s.is_current = 1
--),,
cte_source_data as
(
select distinct
    cu.No_ AS Reseller_Code,
    case
      when cu.Name2 = 'N/A' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS Reseller_Name_Internal,
    cu.Country_RegionCode AS Reseller_Geography_Internal,
    to_date(cu.Createdon) AS Reseller_Start_Date,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sih.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(sih.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND UPPER(rg.Entity) = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sih.Sys_DatabaseName,2)) AND s.source_system = 'Infinigate ERP'
WHERE sih.Sys_Silver_IsCurrent = true
GROUP BY 
    cu.No_,
    case
      when cu.Name2 = 'N/A' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END,
    cu.Country_RegionCode,
    to_date(cu.Createdon),
    coalesce(s.source_system_pk,-1)
union all
select distinct
  cu.No_ AS Reseller_Code,
  case
      when cu.Name2 = 'N/A' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS Reseller_Name_Internal,
    cu.Country_RegionCode AS Reseller_Geography_Internal,
    to_date(cu.Createdon) AS Reseller_Start_Date,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sih.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(sih.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
   LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND UPPER(rg.Entity) = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sih.Sys_DatabaseName,2)) AND s.source_system = 'Infinigate ERP'
WHERE sih.Sys_Silver_IsCurrent = true
GROUP BY 
    cu.No_,
    case
      when cu.Name2 = 'N/A' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END,
    cu.Country_RegionCode,
    to_date(cu.Createdon),
    coalesce(s.source_system_pk,-1)
UNION all
--Nuvias Data
SELECT 
    inv.InvoiceAccount AS Reseller_Code,
    MAX(UPPER(inv.InvoicingName)) AS Reseller_Name_Internal,
    UPPER(inv.DataAreaId) AS Reseller_Geography_Internal,
    COALESCE(
      to_date(cust.CREATEDDATETIME),
      to_date('1900-01-01')
    ) AS Reseller_Start_Date,
    -- coalesce(rg.ResellerGroupCode, 'NaN') AS ResellerGroupCode,
    -- coalesce(rg.ResellerGroupName, 'NaN') AS ResellerGroupName,
    coalesce(s.source_system_pk,-1) AS source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(inv.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(inv.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC

FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicejour inv

LEFT JOIN cte_sources s on inv.dataareaid = s.source_entity AND s.source_system = 'Nuvias ERP'

INNER JOIN (
  SELECT inv1.InvoiceAccount, inv1.SID as SID
  FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicejour inv1
  WHERE Sys_Silver_IsCurrent = 1
) AS max_code ON max_code.InvoiceAccount = inv.InvoiceAccount AND max_code.SID = inv.SID

LEFT JOIN (
  SELECT DISTINCT AccountNum, CREATEDDATETIME, DataAreaId
  FROM silver_{ENVIRONMENT}.nuvias_operations.custtable
  WHERE Sys_Silver_IsCurrent = 1
  ) AS cust ON inv.InvoiceAccount = cust.AccountNum AND inv.DataAreaId = cust.DataAreaId
WHERE inv.Sys_Silver_IsCurrent = 1
GROUP BY ALL
UNION all
--Netsuite
SELECT 
  --coalesce(rs.Reseller_Name,'N/A') AS Reseller_Code,
  coalesce(rs.Reseller_Name,si.Reseller_Name) AS reseller_code,
  coalesce(rs.Reseller_Name,'N/A') AS Reseller_Name_Internal,
  'AE1' AS Reseller_Geography_Internal,
  to_date(coalesce(rs.Date_Created, '1900-01-01' )) AS Reseller_Start_Date,  
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(si.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(si.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si 
LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatareseller AS rs ON si.Reseller_Name = rs.Reseller_Name
AND rs.Sys_Silver_IsCurrent = 1
LEFT JOIN cte_sources s on 'AE1' = s.source_entity AND s.source_system = 'Starlink (Netsuite) ERP'
WHERE si.Sys_Silver_IsCurrent = 1
GROUP BY ALL
UNION all
--Netsafe
SELECT
  invoice.Customer_Account AS ResellerCode,
  CASE WHEN d.reseller_count > 1 THEN d.Reseller_Name_Internal ELSE invoice.Customer_Name end AS Reseller_Name_Internal,
  'N/A' AS Reseller_Geography_Internal,
  to_date('1900-01-01') AS Reseller_Start_Date,
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(invoice.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(invoice.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM  silver_{ENVIRONMENT}.netsafe.invoicedata AS invoice
LEFT JOIN cte_sources s on CASE
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
    END = s.source_entity
AND s.source_system = 'Netsafe ERP'
LEFT JOIN cte_duplicate_netsafe_reseller d ON d.Customer_Account = invoice.Customer_Account and d.Sys_Country = invoice.Sys_Country
WHERE invoice.Sys_Silver_IsCurrent = 1
GROUP BY ALL
)
SELECT 
  csd.Reseller_Code,
  csd.Reseller_Name_Internal,
  csd.Reseller_Geography_Internal,
  csd.Reseller_Start_Date,
  csd.source_system_fk,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
  csd.end_datetime,
  csd.is_current,
  MAX(csd.Sys_Gold_InsertedDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(csd.Sys_Gold_ModifiedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_reseller d ON csd.Reseller_Code = d.Reseller_Code 
AND csd.source_system_fk = d.source_system_fk
WHERE csd.Reseller_Code IS NOT NULL
GROUP BY ALL
""")
