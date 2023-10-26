# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Vuzion
spark.sql(f"""

CREATE OR Replace VIEW vuzion_globaltransactions AS

WITH initial_query
AS 
(
SELECT
'VU' AS GroupEntityCode
,'NaN' AS EntityCode
,to_date(sa.OrderDate) AS TransactionDate
,to_date(sa.OrderDate) AS SalesOrderDate
,cast(sa.OrderID AS STRING) AS SalesOrderID
,cast(od.DetID AS STRING) AS SalesOrderItemID
,coalesce(bm.MPNumber,'NaN') AS SKUInternal
,'DataNowArr data not available' AS SKUMaster
,coalesce(od.Descr,'NaN') AS Description
,'NaN' AS ProductTypeInternal
,'DataNowArr data not available' AS ProductTypeMaster
,'DataNowArr data not available' AS CommitmentDuration1Master
,'DataNowArr data not available' AS CommitmentDuration2Master
,'DataNowArr data not available' AS BillingFrequencyMaster
,'DataNowArr data not available' AS ConsumptionModelMaster
,cast(coalesce(s.serviceTemplateID,'NaN') AS string) AS VendorCode
,coalesce(bm.ManufacturerName,'NaN') AS VendorNameInternal
,'DataNowArr data not available' AS VendorNameMaster
,'NaN' AS VendorGeography
,cast(coalesce(r.ResellerID,'NaN') AS string) AS ResellerCode
,coalesce(r.ResellerName,'NaN') AS ResellerNameInternal
,'NaN' AS ResellerGeographyInternal
,coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode 
,coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName 
,coalesce(sa.CurrencyID,'NaN') AS CurrencyCode
,cast((coalesce(od.ExtendedPrice_Value,0.00) + coalesce(od.TaxAmt_Value,0.00)) AS DECIMAL(10,2)) AS RevenueAmount
FROM
  silver_{ENVIRONMENT}.cloudblue_pba.salesorder sa
LEFT JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.orddet od
ON 
  sa.OrderID = od.OrderID
AND
  sa.Sys_Silver_IsCurrent = true
AND
  od.Sys_Silver_IsCurrent = true
LEFT JOIN
(
  SELECT DISTINCT AccountID AS ResellerID, CompanyName AS ResellerName
  FROM silver_{ENVIRONMENT}.cloudblue_pba.account
  WHERE Sys_Silver_IsCurrent = true
  AND Type = 2
) r
ON
  sa.Customer_AccountID = r.ResellerID
LEFT JOIN
  (
  SELECT DISTINCT subscriptionID, serviceTemplateID
  FROM silver_{ENVIRONMENT}.cloudblue_pba.subscription
  WHERE Sys_Silver_IsCurrent = true
  ) s
ON
  od.subscriptionID = s.subscriptionID
LEFT JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
ON
  od.resourceID = bm.resourceID
AND
  bm.Sys_Silver_IsCurrent = true
LEFT JOIN 
(
  SELECT DISTINCT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
  WHERE InfinigateCompany = 'Vuzion'
  AND Sys_Silver_IsCurrent = true
) rg
ON 
  cast(r.ResellerID as string) = rg.ResellerID
WHERE
  sa.OrderTypeID IN ('BO','SO','CF','CH')
)
, main_dates
AS
(
SELECT
  GroupEntityCode,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  SKUMaster,
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
  VendorGeography,
  CASE
    WHEN VendorCode <> 'NaN' THEN MIN(TransactionDate) OVER(PARTITION BY VendorCode)
    ELSE to_date('1900-01-01')
  END AS VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  CASE
    WHEN ResellerCode <> 'NaN' THEN MIN(TransactionDate) OVER(PARTITION BY ResellerCode)
    ELSE to_date('1900-01-01')
  END AS ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
  CurrencyCode,
  RevenueAmount
FROM initial_query
)

SELECT
  GroupEntityCode,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  SKUMaster,
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
  VendorGeography,
  to_date(VendorStartDate,'yyyy-MM-dd') AS VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  to_date(ResellerStartDate,'yyyy-MM-dd') AS ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,  
  CASE
    WHEN ResellerGroupName = 'NaN' THEN to_date('1900-01-01')
    WHEN ResellerGroupName = 'No Group'
    THEN (
      CASE
        WHEN ResellerStartDate <> '1900-01-01' THEN MIN(TransactionDate) OVER(PARTITION BY ResellerCode)
        ELSE to_date('1900-01-01')
      END
    )
    ELSE (
      CASE
        WHEN ResellerStartDate <> '1900-01-01' THEN MIN(TransactionDate) OVER(PARTITION BY ResellerGroupName)
        ELSE to_date('1900-01-01')
      END
    )
  END AS ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount
  FROM main_dates
""")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

df_obt = spark.read.table('globaltransactions')
df_vuzion = spark.read.table(f'gold_{ENVIRONMENT}.obt.vuzion_globaltransactions')

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_vuzion.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_vuzion.select(selection_columns)

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'VU'").saveAsTable("globaltransactions")
