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

SELECT
'VU' AS GroupEntityCode
,coalesce(rg.Entity,'NaN')  AS EntityCode
,to_date(sa.OrderDate) AS TransactionDate
,cast(sa.OrderID AS STRING) AS SalesOrderID
,to_date(sa.OrderDate) AS SalesOrderDate
,cast(od.DetID AS STRING) AS SalesOrderItemID
,cast(od.ExtendedPrice_Value + od.TaxAmt_Value AS DECIMAL(10,2)) AS RevenueAmount
,coalesce(sa.CurrencyID,'NaN') AS CurrencyCode
,coalesce(bm.MPNumber,'NaN') AS SKU
,coalesce(od.Descr,'NaN') AS Description
,'NaN' AS ProductTypeInternal
,'DataNowArr data not available' AS CommitmentDuration
,'DataNowArr data not available' AS BillingFrequency
,'DataNowArr data not available' AS ConsumptionModel
,cast(coalesce(s.serviceTemplateID,'NaN') AS string) AS VendorCode
,coalesce(bm.ManufacturerName,'NaN') AS VendorName
,'NaN' AS VendorGeography
,to_date('1900-01-01') AS VendorStartDate
,cast(coalesce(r.ResellerID,'NaN') AS string) AS ResellerCode
,coalesce(r.ResellerName,'NaN') AS ResellerNameInternal
,to_date('1900-01-01') AS ResellerStartDate
,coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode 
,coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName 
,'NaN' AS ResellerGeographyInternal
,to_date('1900-01-01') AS ResellerGroupStartDate
FROM
  silver_dev.cloudblue_pba.salesorder sa
LEFT JOIN
  silver_dev.cloudblue_pba.orddet od
ON 
  sa.OrderID = od.OrderID
AND
  sa.Sys_Silver_IsCurrent = true
AND
  od.Sys_Silver_IsCurrent = true
LEFT JOIN
(
  SELECT DISTINCT AccountID AS ResellerID, CompanyName AS ResellerName
  FROM silver_dev.cloudblue_pba.account
  WHERE Sys_Silver_IsCurrent = true
) r
ON
  od.Vendor_AccountID = r.ResellerID
LEFT JOIN
  (
  SELECT DISTINCT subscriptionID, serviceTemplateID
  FROM silver_dev.cloudblue_pba.subscription
  WHERE Sys_Silver_IsCurrent = true
  ) s
ON
  od.subscriptionID = s.subscriptionID
LEFT JOIN
  silver_dev.cloudblue_pba.bmresource bm
ON
  od.resourceID = bm.resourceID
AND
  bm.Sys_Silver_IsCurrent = true
LEFT JOIN 
(
  SELECT DISTINCT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_dev.masterdata.resellergroups
  WHERE InfinigateCompany = 'Vuzion'
  AND Sys_Silver_IsCurrent = true
) rg
ON 
  cast(r.ResellerID as string) = rg.ResellerID
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

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'VU'").saveAsTable("globaltransactions")
