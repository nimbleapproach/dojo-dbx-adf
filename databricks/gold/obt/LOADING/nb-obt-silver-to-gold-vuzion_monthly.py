# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]



# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE VIEW vuzion_globaltransactions_monthly_file as

with adjustments as (
select 
 'VU' AS GroupEntityCode , 
 'Adjustment' RevenueType,
Entity AS EntityCode,
to_date(coalesce(InvoiceDate,'1900-01-01')) as TransactionDate,
to_date(InvoiceDate) as  SalesOrderDate,
'NaN' AS SalesOrderID,
ItemID AS SalesOrderItemID,
SKU AS SKUInternal,
  'NaN' AS  resourceID,
  'NaN' AS SKUMaster,
  'NaN' AS Description,
 'NaN' AS   ProductTypeInternal,
 'NaN' AS   ProductTypeMaster,
  'NaN' AS  CommitmentDuration1Master,
  'NaN' AS  CommitmentDuration2Master,
  'NaN' AS  BillingFrequencyMaster,
  'NaN' AS  ConsumptionModelMaster,
  VendorID as VendorCode,
  VendorName as VendorNameInternal,
  'NaN' AS  VendorNameMaster,
  'NaN' AS  VendorGeography,
  to_date('1900-01-01') AS  VendorStartDate,
CustomerAccount as ResellerCode,
CustomerName as ResellerNameInternal,
  'NaN' AS ResellerGeographyInternal,
  to_date('1900-01-01') AS ResellerStartDate,
  'NaN' AS ResellerGroupCode,
  'NaN' AS ResellerGroupName,  
  to_date('1900-01-01') as ResellerGroupStartDate,
Currency AS CurrencyCode,
cast( Revenue_Local as decimal(10,2)) AS RevenueAmount,
cast(0.00 as decimal(10,2))AS CostAmount,
cast(0.00 as decimal(10,2))as GP1

 from bronze_{ENVIRONMENT}.vuzion_monthly.vuzion_monthly_adjustment)

select 

 'VU' AS GroupEntityCode ,
  RevenueType,  
  CASE WHEN UPPER(Territory) = 'VGB'THEN 'UK2' ELSE 'IE1' END  AS EntityCode,
   to_date(coalesce(to_date(Month),'1900-01-01')) as TransactionDate,
   to_date(Month) as  SalesOrderDate,
  ID AS SalesOrderID,
  Product AS SalesOrderItemID,
  Product AS SKUInternal,
  'NaN' AS  resourceID,
  'NaN' AS SKUMaster,
  'NaN' AS Description,
  ProductCategory AS  ProductTypeInternal,
  ProductCategory AS  ProductTypeMaster,
  'NaN' AS  CommitmentDuration1Master,
  'NaN' AS  CommitmentDuration2Master,
  'NaN' AS  BillingFrequencyMaster,
  'NaN' AS  ConsumptionModelMaster,
  VendorID as VendorCode,
  Vendor as VendorNameInternal,
  'NaN' AS  VendorNameMaster,
  'NaN' AS  VendorGeography,
  NULL AS  VendorStartDate,
  ID as ResellerCode,
  Customer as ResellerNameInternal,
  'NaN' AS ResellerGeographyInternal,
  NULL AS ResellerStartDate,
  'NaN' AS ResellerGroupCode,
  'NaN' AS ResellerGroupName,  
  NULL as ResellerGroupStartDate,
   CASE WHEN UPPER(Territory) = 'VGB'THEN 'GBP' ELSE 'EUR' END AS CurrencyCode,
---- Apply fix FX rate for Irland----  
  CASE WHEN UPPER(Territory) = 'VGB' 
       THEN cast(Revenue as decimal(10,2)) 
       ELSE cast(Revenue *(1.15) as decimal(10,2)) 
       END AS RevenueAmount,  
  CASE WHEN UPPER(Territory) = 'VGB' 
       THEN cast(Cost as decimal(10,2)) 
       ELSE cast(Cost *(1.15) as decimal(10,2)) 
       END as CostAmount,
  CASE WHEN UPPER(Territory) = 'VGB' 
       THEN cast(Margin as decimal(10,2)) 
       ELSE cast(Margin *(1.15) as decimal(10,2)) 
       END as GP1

from silver_{ENVIRONMENT}.vuzion_monthly.vuzion_monthly_revenue
where Sys_Silver_IsCurrent=1 

UNION ALL

SELECT * FROM adjustments
"""
)



# COMMAND ----------

#vendor mapping table
spark.sql(
    f"""

-- CLEAN DUPLICATES SELECT THE ONE THAT HAS NUMRIC VENDOR CODE FROM SL
SELECT
  DISTINCT lower(vendor_mapping.VendorNameInternal) AS VendorNameInternal,
  CASE
    WHEN VendorCode = '260' THEN 'discontinued business'
    ELSE lower(vendor_mapping.VendorGroup)
  END AS VendorGroup
FROM
  silver_{ENVIRONMENT}.masterdata.vendor_mapping

INNER JOIN (

    SELECT
      DISTINCT LOWER(VendorNameInternal) AS VendorNameInternal,
       MAX(LEN(VendorGroup)) VendorGroup_max
    FROM
      silver_{ENVIRONMENT}.masterdata.vendor_mapping
    WHERE
      Sys_Silver_IsCurrent = 1
    GROUP BY
      ALL

) max on LEN(vendor_mapping.VendorGroup)=VendorGroup_max
AND max.VendorNameInternal = lower(vendor_mapping.VendorNameInternal)

WHERE
  lower(vendor_mapping.VendorNameInternal) IN (
    SELECT
      DISTINCT lower(VendorNameInternal) AS VendorNameInternal
    FROM
      silver_{ENVIRONMENT}.masterdata.vendor_mapping
    where
      Sys_Silver_IsCurrent = 1
    GROUP BY
      ALL
    HAVING
      count (DISTINCT lower(VendorGroup)) > 1
  )
  AND Sys_Silver_IsCurrent = 1
  AND CASE
    WHEN VendorCode = 'NaN' THEN 1
    WHEN VendorCode not rlike '[^0-9]' THEN 1
    ELSE 0
  END = 1


UNION ALL

-- JOIN WITH ALL NONE DUPLICATES ONES
SELECT DISTINCT 
  TRIM(lower(VendorNameInternal)) AS VendorNameInternal,
  lower(VendorGroup) AS VendorGroup

 FROM 
silver_{ENVIRONMENT}.masterdata.vendor_mapping
WHERE lower(VendorNameInternal) NOT IN (
SELECT
  DISTINCT 
  lower(VendorNameInternal) AS VendorNameInternal
FROM
  silver_{ENVIRONMENT}.masterdata.vendor_mapping
where
  Sys_Silver_IsCurrent = 1
  GROUP BY ALL  
  HAVING count (DISTINCT lower(VendorGroup)) >1)
AND Sys_Silver_IsCurrent = 1
"""
).createOrReplaceTempView("ven")

# COMMAND ----------

# tagetik vendor sum
spark.sql(
    f"""
SELECT
  Year,
  Month,
  EntityCode,
  --Region_ID,
  ven.VendorGroup,
  coalesce(sum(Revenue_EUR),0 ) AS Revenue_EUR,
  coalesce(sum(GP1_EUR),0 ) AS GP1_EUR
FROM
  platinum_{ENVIRONMENT}.obt.globaltransactions_tagetik_reconciliation_ytd tag
  LEFT JOIN ven ON trim(lower(tag.VendorName)) = ven.VendorNameInternal
WHERE
  EntityCode = 'VU'
  AND source ='TAG'
GROUP BY ALL
"""
).createOrReplaceTempView("tag_vu")

# COMMAND ----------

#obt vendor sum
spark.sql(
    f"""
WITH vu AS (
  select
    year(TransactionDate) AS PostingYear,
    month(TransactionDate) AS PostingMonth,
   -- vu.EntityCode,
    ven.VendorGroup,
    -- TAG.VendorName AS TagetikVendorName,
    coalesce(sum(RevenueAmount),0 ) as RevenueAmount,
    coalesce(sum(gp1),0 ) AS GP1_VendorSum -- TAG.GP1_EUR AS GP1_TAG
  FROM
    gold_{ENVIRONMENT}.obt.vuzion_globaltransactions_monthly_file vu
    LEFT JOIN  ven ON lower(ven.VendorNameInternal) = lower(vu.VendorNameInternal)
  GROUP BY
    ALL
)
SELECT  vu.PostingYear,
  vu.PostingMonth,
  --vu.EntityCode,
  vu.VendorGroup,
  vu.RevenueAmount,
  CASE WHEN GP1_VendorSum = 0 OR GP1_VendorSum  IS NULL 
       THEN RevenueAmount ELSE  GP1_VendorSum END  AS GP1_VendorSum,
  tag_vu.GP1_EUR
FROM
  vu
  LEFT JOIN tag_vu ON vu.PostingYear = tag_vu.Year
  AND vu.PostingMonth = tag_vu.Month
  AND (vu.VendorGroup) = tag_vu.VendorGroup
 -- AND vu.EntityCode= tag_vu.Region_ID

  
  """
).createOrReplaceTempView("vu_sum")

# COMMAND ----------

spark.sql(f"""
Create or replace temporary view max_fx_rates as 
SELECT
  Currency,
  MAX(Period_FX_rate) as Period_FX_rate
FROM
  gold_{ENVIRONMENT}.obt.exchange_rate r
WHERE
  ScenarioGroup = 'Actual' 
AND Calendar_Year = (select MAX(Calendar_Year) from gold_{ENVIRONMENT}.obt.exchange_rate m WHERE ScenarioGroup = 'Actual' AND r.Currency = m.Currency)
AND Month = (select MAX(Month) from gold_{ENVIRONMENT}.obt.exchange_rate m WHERE ScenarioGroup = 'Actual' AND r.Calendar_Year = m.Calendar_Year AND r.Currency = m.Currency)
GROUP BY Currency
"""
)

# COMMAND ----------

spark.sql(
    f"""
    Create or replace table gold_{ENVIRONMENT}.obt.globaltransactions_vu_gp1 as 


  SELECT
    vu_obt.GroupEntityCode,
    vu_obt.EntityCode,
    vu_obt.TransactionDate,
    vu_obt.SalesOrderDate,
    vu_obt.SalesOrderID,
    vu_obt.SalesOrderItemID,
    vu_obt.SKUInternal,
    vu_obt.SKUMaster,
    vu_obt.Description,
    vu_obt.ProductTypeInternal,
    vu_obt.ProductTypeMaster,
    vu_obt.CommitmentDuration1Master,
    vu_obt.CommitmentDuration2Master,
    vu_obt.BillingFrequencyMaster,
    vu_obt.ConsumptionModelMaster,
    vu_obt.VendorCode,
    vu_obt.VendorNameInternal,
    ven.VendorGroup,
    vu_obt.VendorNameMaster,
    vu_obt.VendorGeography,

    vu_obt.ResellerCode,
    vu_obt.ResellerNameInternal,
    vu_obt.ResellerGeographyInternal,
    vu_obt.ResellerStartDate,
    vu_obt.ResellerGroupCode,
    vu_obt.ResellerGroupName,
    vu_obt.ResellerGroupStartDate,
    vu_obt.CurrencyCode,
    vu_obt.RevenueAmount,
    vu_obt.CostAmount,
    vu_obt.GP1 AS GP1,
    vu_sum.GP1_VendorSum,
    vu_sum.GP1_EUR AS GP1_Tag,
    (
      (CASE When vu_sum.GP1_VendorSum  = vu_obt.RevenueAmount
            THEN vu_obt.RevenueAmount ELSE  vu_obt.GP1 END / vu_sum.GP1_VendorSum) * coalesce(vu_sum.GP1_EUR, 0 )
    ) AS GP1_Trueup
  FROM
    gold_{ENVIRONMENT}.obt.vuzion_globaltransactions_monthly_file vu_obt
    LEFT JOIN  ven ON  lower(ven.VendorNameInternal) = lower(vu_obt.VendorNameInternal)
    LEFT JOIN  vu_sum ON lower(ven.VendorGroup) = lower(vu_sum.VendorGroup)
    AND year(vu_obt.TransactionDate) = vu_sum.PostingYear
    AND MONTH(vu_obt.TransactionDate) = vu_sum.PostingMonth
    -- AND vu_obt.EntityCode = vu_sum.EntityCode
"""
)
