# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Starlink
spark.sql(
    f"""         
CREATE OR REPLACE VIEW starlink_globaltransactions AS

WITH cte AS 
(SELECT
  'SL' AS GroupEntityCode,
  'AE1' AS EntityCode,
  si.SID,
  to_date(si.Date) AS TransactionDate,
  TO_DATE(si.Sales_Order_Date) AS SalesOrderDate,
  RIGHT(si.Sales_Order_Number,9) AS SalesOrderID,
  si.SKU_ID AS SalesOrderItemID,
  COALESCE(it.SKU_ID, "NaN") AS SKUInternal,
  COALESCE(datanowarr.SKU, 'NaN')  AS SKUMaster,
  COALESCE(it.Description,'NaN') AS Description,
  COALESCE(it.Item_Category,'NaN') AS ProductTypeInternal,
  COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
  coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
  coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
  coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
  coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
  coalesce(ven.Vendor_ID,   'NaN') AS VendorCode,
  coalesce(ven.Vendor_Name, 'NaN') AS VendorNameInternal,
  coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
  'AE1' AS VendorGeography,
  to_date('1900-01-01') AS VendorStartDate,
  --coalesce(cu.Customer_Name,'NaN') AS ResellerCode,
  --coalesce(cu.Customer_Name,'NaN') AS ResellerNameInternal,
  coalesce(rs.Reseller_Name,si.Reseller_Name,'NaN') AS ResellerCode,
  coalesce(rs.Reseller_Name,si.Reseller_Name,'NaN') AS ResellerNameInternal,
  'AE1' AS ResellerGeographyInternal,
  --to_date(coalesce(cu.Date_Created, '1900-01-01' )) AS ResellerStartDate,
  to_date(coalesce(rs.Date_Created, '1900-01-01' )) AS ResellerStartDate,  
  --'NaN' AS ResellerGroupCode,
  --'NaN' AS ResellerGroupName,
  --Comment by MS (30/01/2024) - Start
  --Added reseller group code AND reseller group name
  coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
  coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
  --Comment by MS (30/01/2024) - END
  --to_date(coalesce(cu.Date_Created, '1900-01-01' )) AS ResellerGroupStartDate,
  to_date(coalesce(rs.Date_Created, '1900-01-01' )) AS ResellerGroupStartDate,  
  si.Deal_Currency AS CurrencyCode,
  cASt(si.Revenue_USD AS DECIMAL(10, 2)) AS RevenueAmount,
  cASt((si.Revenue_USD - si.GP_USD)*(-1) AS DECIMAL(10, 2))  AS CostAmount,
  CAST(si.GP_USD AS  DECIMAL(10, 2)) AS GP1


FROM
  silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdataSku AS it ON si.SKU_ID = it.SKU_ID
  AND it.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdataVendor AS ven ON si.Vendor_Name = ven.Vendor_Name
  AND ven.Sys_Silver_IsCurrent is not false
  --LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatacustomer AS cu ON si.Customer_Name = cu.Customer_Name
  --AND cu.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatareseller AS rs ON si.Reseller_Name = rs.Reseller_Name
  AND rs.Sys_Silver_IsCurrent = 1
  LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = it.SKU_ID
  --Comment by MS (30/01/2024) - Start
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg 
  ON si.Reseller_Name = rg.ResellerName
  AND rg.InfinigateCompany = 'Starlink'
  AND rg.Sys_Silver_IsCurrent = true
  --Comment by MS (30/01/2024) - END
where
  si.Sys_Silver_IsCurrent = 1
/*
Change Date [29/02/2024]
Change BY [MS]
Exclude deleted revenue rows
*/
AND
  si.Sys_Silver_IsDeleted != true
  )

  SELECT
  GroupEntityCode,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  CASE WHEN lower(SKUInternal)  LIKE '%discount%' THEN 'Discount'
       WHEN lower(SKUInternal)  LIKE '%round%' THEN 'Rounding'
       WHEN SKUInternal  LIKE '%Funded%' THEN 'Funding'
       WHEN ProductTypeInternal  LIKE '%Shipping%' THEN 'Shipping'
       ELSE SKUMaster END AS SKUMaster,
  Description,
  ProductTypeInternal,
  ProductTypeMaster,
  CommitmentDuration1Master,
  CommitmentDuration2Master,
  BillingFrequencyMaster,
  ConsumptionModelMaster,
  VendorCode,
  VendorNameInternal,
  VendorNameMaster,
  EntityCode AS VendorGeography,
  CASE
    WHEN VendorStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, VendorCode)
    ELSE VendorStartDate
  END AS VendorStartDate,
  substring_index(ResellerCode, ' ', 1)ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  CASE
    WHEN ResellerStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    ELSE ResellerStartDate
  END AS ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
CASE
    WHEN ResellerStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    ELSE ResellerStartDate
  END AS ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount,
  CostAmount,
  GP1
FROM
  cte

"""
)

# COMMAND ----------

spark.sql(
    f"""
SELECT TRIM(VendorNameInternal)VendorNameInternal,
MAX(VendorGroup)VendorGroup
 FROM (
-- CLEAN DUPLICATES SELECT THE ONE THAT HAS NUMRIC VENDOR CODE FROM SL
SELECT DISTINCT 
  lower(VendorNameInternal) AS VendorNameInternal,
  CASE WHEN VendorCode = '260' THEN 'discontinued business' ELSE lower(VendorGroup) END  AS VendorGroup

 FROM 
silver_{ENVIRONMENT}.masterdata.vendor_mapping
WHERE lower(VendorNameInternal) IN (
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
AND CASE WHEN VendorCode ='NaN' THEN 1
        WHEN VendorCode not rlike '[^0-9]' THEN 1 ELSE 0 END =1

UNION ALL

-- JOIN WITH ALL NONE DUPLICATES ONES
SELECT DISTINCT 
  lower(VendorNameInternal) AS VendorNameInternal,
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
AND Sys_Silver_IsCurrent = 1)
GROUP BY ALL
"""
).createOrReplaceTempView("ven")

# COMMAND ----------

spark.sql(
    f"""
SELECT
  Year,
  Month,
  EntityCode,
  Region_ID,
--   VendorName,
  ven.VendorGroup,
  sum(Revenue_EUR)Revenue_EUR,
  sum(GP1_EUR) AS GP1_EUR
FROM
  platinum_{ENVIRONMENT}.obt.globaltransactions_tagetik_reconciliation_ytd tag
  LEFT JOIN ven ON TRIM(lower(tag.VendorName)) = ven.VendorNameInternal
WHERE
  EntityCode = 'AE1'
  AND source ='TAG'
GROUP BY ALL
"""
).createOrReplaceTempView("tag_sl")

# COMMAND ----------

spark.sql(
    f"""
WITH sl AS (
  select
    year(TransactionDate) AS PostingYear,
    month(TransactionDate) AS PostingMonth,
    -- sl.VendorNameInternal,
    ven.VendorGroup,
    -- TAG.VendorName AS TagetikVendorName,
    sum(RevenueAmount) RevenueAmount,
    sum(gp1) AS GP1_VendorSum -- TAG.GP1_EUR AS GP1_TAG
  FROM
    gold_{ENVIRONMENT}.obt.starlink_globaltransactions sl
    LEFT JOIN  ven ON lower(ven.VendorNameInternal) = lower(sl.VendorNameInternal)
  GROUP BY
    ALL
)
SELECT  sl.PostingYear,
  sl.PostingMonth,
  sl.VendorGroup,
  sl.RevenueAmount,
  CASE WHEN GP1_VendorSum = 0 OR GP1_VendorSum  IS NULL 
       THEN RevenueAmount ELSE  GP1_VendorSum END  AS GP1_VendorSum,
  tag_sl.GP1_EUR
FROM
  sl
  LEFT JOIN tag_sl ON sl.PostingYear = tag_sl.Year
  AND sl.PostingMonth = tag_sl.Month
  AND (sl.VendorGroup) = tag_sl.VendorGroup

  
  """
).createOrReplaceTempView("sl_sum")

# COMMAND ----------

spark.sql(
    f"""
          
CREATE OR REPLACE TABLE globaltransactions_sl_gp1
  SELECT
    SL_OBT.GroupEntityCode,
    SL_OBT.EntityCode,
    SL_OBT.TransactionDate,
    SL_OBT.SalesOrderDate,
    SL_OBT.SalesOrderID,
    SL_OBT.SalesOrderItemID,
    SL_OBT.SKUInternal,
    SL_OBT.SKUMaster,
    SL_OBT.Description,
    SL_OBT.ProductTypeInternal,
    SL_OBT.ProductTypeMaster,
    SL_OBT.CommitmentDuration1Master,
    SL_OBT.CommitmentDuration2Master,
    SL_OBT.BillingFrequencyMaster,
    SL_OBT.ConsumptionModelMaster,
    SL_OBT.VendorCode,
    SL_OBT.VendorNameInternal,
    --ven.VendorGroup,
    SL_OBT.VendorNameMaster,
    SL_OBT.VendorGeography,
    SL_OBT.VendorStartDate,
    SL_OBT.ResellerCode,
    SL_OBT.ResellerNameInternal,
    SL_OBT.ResellerGeographyInternal,
    SL_OBT.ResellerStartDate,
    SL_OBT.ResellerGroupCode,
    SL_OBT.ResellerGroupName,
    SL_OBT.ResellerGroupStartDate,
    SL_OBT.CurrencyCode,
    SL_OBT.RevenueAmount,
    SL_OBT.CostAmount,
    SL_OBT.GP1 AS GP1,
    sl_sum.GP1_VendorSum,
    sl_sum.GP1_EUR AS GP1_Tag,
    (
      (CASE When sl_sum.GP1_VendorSum  = SL_OBT.RevenueAmount
            THEN SL_OBT.RevenueAmount ELSE  SL_OBT.GP1 END / sl_sum.GP1_VendorSum) * coalesce(sl_sum.GP1_EUR, 0 )
    ) AS GP1_Trueup
  FROM
    gold_{ENVIRONMENT}.obt.starlink_globaltransactions SL_OBT
    LEFT JOIN  ven ON  lower(ven.VendorNameInternal) = lower(SL_OBT.VendorNameInternal)
    LEFT JOIN  sl_sum ON lower(ven.VendorGroup) = lower(sl_sum.VendorGroup)
    AND year(SL_OBT.TransactionDate) = sl_sum.PostingYear
    AND MONTH(SL_OBT.TransactionDate) = sl_sum.PostingMonth
    """
)
