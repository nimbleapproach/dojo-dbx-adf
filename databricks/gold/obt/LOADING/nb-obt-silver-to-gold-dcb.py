# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold DCB
spark.sql(
    f"""         
CREATE OR REPLACE VIEW dcb_globaltransactions AS

WITH initial_query 
AS
(
SELECT
'NU' AS GroupEntityCode,
CASE
WHEN invoice.Entity = '1' THEN 'BE2'
WHEN invoice.Entity = '2' THEN 'NL3'
END AS EntityCode,
to_date(invoice.Invoice_Date) AS TransactionDate,
to_date(invoice.SO_Date) AS SalesOrderDate,
coalesce(invoice.SO_Number,'NaN') AS SalesOrderID,
'NaN' AS SalesOrderItemID,
COALESCE(invoice.SKU,'NaN') AS SKUInternal,
COALESCE(datanowarr.SKU,'NaN') AS SKUMaster,
COALESCE(invoice.Description,'NaN') AS Description,
COALESCE(invoice.Product_Type,'NaN')  AS ProductTypeInternal,
COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
coalesce(datanowarr.Commitment_Duration_in_months,'NaN') AS CommitmentDuration1Master,
coalesce(datanowarr.Commitment_Duration_Value,'NaN') AS CommitmentDuration2Master,
coalesce(datanowarr.Billing_Frequency,'NaN') AS BillingFrequencyMaster,
coalesce(datanowarr.Consumption_Model,'NaN') AS ConsumptionModelMaster,
coalesce(invoice.Vendor_ID,'NaN') AS VendorCode,
coalesce(invoice.Vendor,'NaN') AS VendorNameInternal,
coalesce(datanowarr.Vendor_Name,'NaN') AS VendorNameMaster,
'NaN' AS VendorGeography,
to_date('1900-01-01') AS VendorStartDate,
coalesce(invoice.Reseller_ID,'NaN') AS ResellerCode,
coalesce(invoice.Reseller,'NaN') AS ResellerNameInternal,
'NaN' AS ResellerGeographyInternal,
to_date('1900-01-01') AS ResellerStartDate,
coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
to_date('1900-01-01') AS ResellerGroupStartDate,
'EUR' AS CurrencyCode,
/*
Change Date [23/02/2024]
Change BY [MS]
Use net_price column as revenue amount
*/
cast(invoice.Net_Price as DECIMAL(10, 2)) AS RevenueAmount
FROM 
  silver_{ENVIRONMENT}.dcb.invoicedata AS invoice
LEFT JOIN
  gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr
ON
  datanowarr.SKU = invoice.SKU
LEFT JOIN 
(
  SELECT DISTINCT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
  WHERE InfinigateCompany = 'Nuvias'
  AND Sys_Silver_IsCurrent = true
  /*
  Change Date [22/02/2024]
  Change BY [MS]
  Filter only relevant entities
  */
  AND Entity IN ('BE2', 'NL3')
) rg
ON 
  cast(invoice.Reseller_ID as string) = rg.ResellerID
/*
Change Date [22/02/2024]
Change BY [MS]
Join with entity as well
*/
AND
  CASE
    WHEN invoice.Entity = '1' THEN 'BE2'
    WHEN invoice.Entity = '2' THEN 'NL3'
  END = rg.Entity
WHERE
  invoice.Sys_Silver_IsCurrent = true
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
  EntityCode AS VendorGeography,
  CASE
    WHEN VendorStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, VendorCode)
    ELSE VendorStartDate
  END AS VendorStartDate,
  ResellerCode,
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
  RevenueAmount
FROM
  initial_query""")

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")
df_dcb = spark.read.table(f"gold_{ENVIRONMENT}.obt.dcb_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_dcb.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_dcb.select(selection_columns)

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NU' AND EntityCode IN ('BE2', 'NL3')").saveAsTable("globaltransactions")
