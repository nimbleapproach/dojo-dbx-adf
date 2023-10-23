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

WITH ResellerStartDates
AS
(
SELECT MIN(sa.OrderDate) AS VendorStartDate, s.serviceTemplateID
FROM
  silver_{ENVIRONMENT}.cloudblue_pba.salesorder sa
INNER JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.orddet od
ON 
  sa.OrderID = od.OrderID
AND
  sa.Sys_Silver_IsCurrent = true
AND
  od.Sys_Silver_IsCurrent = true
INNER JOIN
  (
  SELECT DISTINCT subscriptionID, serviceTemplateID
  FROM silver_{ENVIRONMENT}.cloudblue_pba.subscription
  WHERE Sys_Silver_IsCurrent = true
 ) s
ON
  od.subscriptionID = s.subscriptionID
GROUP BY serviceTemplateID
)
,ResellerGroupStartDates
AS 
(
SELECT MIN(sa.OrderDate) AS ResellerGroupStartDate, rs.ResellerGroupName
FROM
  silver_{ENVIRONMENT}.cloudblue_pba.salesorder sa
INNER JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.orddet od
ON 
  sa.OrderID = od.OrderID
AND
  sa.Sys_Silver_IsCurrent = true
AND
  od.Sys_Silver_IsCurrent = true
INNER JOIN
  (
  SELECT DISTINCT AccountID AS ResellerID
  FROM silver_{ENVIRONMENT}.cloudblue_pba.account
  WHERE Sys_Silver_IsCurrent = true
  ) r
ON
  od.Vendor_AccountID = r.ResellerID
INNER JOIN
  (
  SELECT DISTINCT ResellerID, ResellerGroupName
  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
  WHERE InfinigateCompany = 'Vuzion'
  AND Sys_Silver_IsCurrent = true
  ) rs
ON
  cast(r.ResellerID as string) = rs.ResellerID
GROUP BY rs.ResellerGroupName
)

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
,to_date(coalesce(vs.VendorStartDate,'1900-01-01')) AS VendorStartDate
,cast(coalesce(r.ResellerID,'NaN') AS string) AS ResellerCode
,coalesce(r.ResellerName,'NaN') AS ResellerNameInternal
,'NaN' AS ResellerGeographyInternal
,to_date('1900-01-01') AS ResellerStartDate
,coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode 
,coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName 
,to_date(coalesce(rgs.ResellerGroupStartDate,'1900-01-01')) AS ResellerGroupStartDate
,coalesce(sa.CurrencyID,'NaN') AS CurrencyCode
,cast(od.ExtendedPrice_Value + od.TaxAmt_Value AS DECIMAL(10,2)) AS RevenueAmount
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
) r
ON
  od.Vendor_AccountID = r.ResellerID
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
LEFT JOIN
  ResellerStartDates vs
ON
  s.serviceTemplateID = vs.serviceTemplateID
LEFT JOIN
  ResellerGroupStartDates rgs
ON
  rg.ResellerGroupName = rgs.ResellerGroupName
""")

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
df_selection = df_vuzion.fillna(value= 'NaN').replace('', 'NaN')

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'VU'").saveAsTable("globaltransactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_dev.obt.globaltransactions
# MAGIC WHERE GroupEntityCode = 'VU'
# MAGIC and SalesOrderID = 1461068
